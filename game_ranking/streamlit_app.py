import streamlit as st
import pandas as pd
import numpy as np
import io
import threading

# Module-level shared state for background scraper threads.
# st.session_state is NOT accessible from background threads, so the thread
# writes its result here and the main thread reads it on the next rerun.
_ns_thread_state = {"result": None, "running": False}
_steam_thread_state = {"result": None, "running": False}
import datetime as dt
from src.calculation.process_data import clean_dev_genre_list, flagging, calculate_developer_weighted_points, load_data, calculate_follower_weighted_points, calculate_developer_weighted_points, handle_change
from src.calculation.scraper import scrape_google_trends
from config import INVENTORY_FILE, get_latest_steam_csv, get_latest_nonsteam_csv
from pipeline.state import get_last_run_info, get_next_window
from steam_pipeline import run_steam_scraper, append_from_uploaded_steam_csv
from nonsteam_pipeline import run_nonsteam_scraper, append_from_uploaded_nonsteam_csv, verify_single_game_steam_status

# Page Config
st.set_page_config(page_title="AGS - Game Ranking Tool", layout="wide")

st.title("🎮 Research Team: Game Ranking Algorithm")
st.markdown("""
This tool calculates review priority by awarding 'points' to Steam and Non-Steam games
            based on certain criteria, and weights to determine the final recommendation.
            
            The criteria for each report are displayed on the sidebar, and their weights can be adjusted.

**Instructions:**
1. **Select the dates below and run the steam scraper** to fetch games releasing in the next 2 weeks. The scraper will append new games to the default CSV files.
2. **Review the criteria and adjust weights as needed.** The formulas are displayed for transparency.
3. **Use the filters to narrow down the ranked lists.** You can filter by release date, genre, and more.
            """)

# ── SIDEBAR: FILE UPLOADS ──────────────────────────────────────────────────────
st.sidebar.header("📁 Data Upload")
st.sidebar.markdown("Upload your own CSV files or use the defaults below:")

# Initialize session state for data caching
if "df_steam" not in st.session_state:
    st.session_state.df_steam = None
if "df_nonsteam" not in st.session_state:
    st.session_state.df_nonsteam = None
if "steam_cleaned" not in st.session_state:
    st.session_state.steam_cleaned = False
if "nonsteam_cleaned" not in st.session_state:
    st.session_state.nonsteam_cleaned = False
if "uploaded_steam_bytes" not in st.session_state:
    st.session_state.uploaded_steam_bytes = None
if "uploaded_steam_name" not in st.session_state:
    st.session_state.uploaded_steam_name = None
if "uploaded_nonsteam_bytes" not in st.session_state:
    st.session_state.uploaded_nonsteam_bytes = None
if "uploaded_nonsteam_name" not in st.session_state:
    st.session_state.uploaded_nonsteam_name = None
if "dev_list" not in st.session_state:
    try:
        _, _, st.session_state.dev_list, st.session_state.genre_list = load_data(get_latest_steam_csv(), get_latest_nonsteam_csv())
    except:
        pass

# Helper: load defaults into session state
def load_defaults():
    df_steam, df_nonsteam, dev_list, genre_list, inventory = load_data(get_latest_steam_csv(), get_latest_nonsteam_csv())
    df_steam = clean_dev_genre_list(df_steam)
    df_steam = flagging(df_steam)
    st.session_state.df_steam = df_steam
    st.session_state.steam_source = "default file"
    st.session_state.steam_cleaned = True
    st.session_state.df_nonsteam = df_nonsteam
    st.session_state.nonsteam_source = "default file"
    st.session_state.nonsteam_cleaned = True
    st.session_state.dev_list = dev_list
    st.session_state.genre_list = genre_list
    st.session_state.uploaded_steam_bytes = None
    st.session_state.uploaded_steam_name = None
    st.session_state.uploaded_nonsteam_bytes = None
    st.session_state.uploaded_nonsteam_name = None

# ── Pipeline session state ─────────────────────────────────────────────────────
for _key, _default in {
    "steam_scraper_log": [],
    "nonsteam_scraper_log": [],
}.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default


def highlight_new_rows(df: pd.DataFrame):
    """Return a styled dataframe with today's newly appended rows highlighted yellow."""
    today = dt.date.today().isoformat()

    def _row_style(row):
        if str(row.get("date_appended", "")).startswith(today):
            return ["background-color: #fff59d; color: #000000"] * len(row)
        return [""] * len(row)

    if "date_appended" in df.columns:
        return df.style.apply(_row_style, axis=1)
    return df


def reload_steam_from_csv():
    """Re-read the latest raw_steam_YYYY-MM-DD.csv from disk and update session state."""
    try:
        latest = get_latest_steam_csv()
        tmp = pd.read_csv(latest)
        tmp = clean_dev_genre_list(tmp)
        tmp = flagging(tmp)
        st.session_state.df_steam = tmp
        st.session_state.steam_source = latest.name
        st.session_state.steam_cleaned = True
    except Exception as e:
        st.error(f"Failed to reload Steam CSV: {e}")


def reload_nonsteam_from_csv():
    """Re-read the latest raw_non_steam_YYYY-MM-DD.csv from disk and update session state."""
    try:
        latest = get_latest_nonsteam_csv()
        tmp = pd.read_csv(latest)
        st.session_state.df_nonsteam = tmp
        st.session_state.nonsteam_source = latest.name
        st.session_state.nonsteam_cleaned = True
    except Exception as e:
        st.error(f"Failed to reload Non-Steam CSV: {e}")


def _format_last_run(info: dict) -> str:
    last = info.get("last_run_date")
    if not last:
        return "Never run"
    try:
        ts = dt.datetime.fromisoformat(last)
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return last


def _format_next_window(scraper: str) -> str:
    start, end = get_next_window(scraper, window_days=14)
    return f"{start} → {end}"


# File uploaders
uploaded_steam = st.sidebar.file_uploader("Upload Steam CSV", type="csv", key="steam_upload")
uploaded_nonsteam = st.sidebar.file_uploader("Upload Non-Steam CSV", type="csv", key="nonsteam_upload")

if uploaded_steam and uploaded_steam.name != st.session_state.uploaded_steam_name:
    st.session_state.uploaded_steam_bytes = uploaded_steam.getvalue()
    st.session_state.uploaded_steam_name = uploaded_steam.name

if uploaded_nonsteam and uploaded_nonsteam.name != st.session_state.uploaded_nonsteam_name:
    st.session_state.uploaded_nonsteam_bytes = uploaded_nonsteam.getvalue()
    st.session_state.uploaded_nonsteam_name = uploaded_nonsteam.name

# Preview and load buttons for Steam
if st.session_state.uploaded_steam_bytes:
    with st.sidebar.expander("👀 Preview Steam File"):
        preview_steam = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
        st.dataframe(preview_steam.head(3), width='stretch')
        st.caption(f"Rows: {len(preview_steam)}, Columns: {len(preview_steam.columns)}")
    
    if st.sidebar.button("📥 Load Steam Data", key="load_steam_btn"):
        try:
            steam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
            steam_required_cols = ['Name', 'FollowerCount', 'Developers', 'Genres', 'ReleaseDate']
            steam_missing = [col for col in steam_required_cols if col not in steam_df_upload.columns]
            if steam_missing:
                st.sidebar.error(f"Missing columns: {', '.join(steam_missing)}")
            else:
                n_updated, n_new = append_from_uploaded_steam_csv(steam_df_upload)
                reload_steam_from_csv()
                st.sidebar.success(f"✅ Saved: {n_new} new, {n_updated} updated")
        except Exception as e:
            st.sidebar.error(f"Error loading file: {e}")
else:
    st.sidebar.info("No Steam CSV uploaded. Using default.")

# Preview and load buttons for Non-Steam
if st.session_state.uploaded_nonsteam_bytes:
    with st.sidebar.expander("👀 Preview Non-Steam File"):
        preview_nonsteam = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
        st.dataframe(preview_nonsteam.head(3), width='stretch')
        st.caption(f"Rows: {len(preview_nonsteam)}, Columns: {len(preview_nonsteam.columns)}")
    
    if st.sidebar.button("📥 Load Non-Steam Data", key="load_nonsteam_btn"):
        try:
            nonsteam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
            nonsteam_required_cols = ['Game Title', 'Developers', 'SteamStatus', 'YouTube Views']
            nonsteam_missing = [col for col in nonsteam_required_cols if col not in nonsteam_df_upload.columns]
            if nonsteam_missing:
                st.sidebar.error(f"Missing columns: {', '.join(nonsteam_missing)}")
            else:
                n_updated, n_new = append_from_uploaded_nonsteam_csv(nonsteam_df_upload)
                reload_nonsteam_from_csv()
                st.sidebar.success(f"✅ Saved: {n_new} new, {n_updated} updated")
        except Exception as e:
            st.sidebar.error(f"Error loading file: {e}")
else:
    st.sidebar.info("No Non-Steam CSV uploaded. Using default.")

# Reset to defaults button
st.sidebar.divider()
if st.sidebar.button("🔄 Reset to Defaults"):
    load_defaults()
    st.rerun()

# Load defaults if not already loaded
try:
    if st.session_state.df_steam is None or st.session_state.df_nonsteam is None:
        load_defaults()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Retrieve data from session state
df_steam = st.session_state.df_steam.copy()
df_nonsteam = st.session_state.df_nonsteam.copy()
dev_list = st.session_state.dev_list
genre_list = st.session_state.genre_list
steam_source_name = st.session_state.get("steam_source", "default file")
nonsteam_source_name = st.session_state.get("nonsteam_source", "default file")

# ── Global date range (shared across all tabs) ────────────────────────────────
def _min_date_from_series(series: pd.Series) -> dt.date:
    parsed = pd.to_datetime(series, errors='coerce').dropna()
    return parsed.min().date() if len(parsed) else dt.date(2000, 1, 1)

def _max_date_from_series(series: pd.Series) -> dt.date:
    parsed = pd.to_datetime(series, errors='coerce').dropna()
    return parsed.max().date() if len(parsed) else dt.date.today()

_steam_min = _min_date_from_series(df_steam.get('ReleaseDate',   pd.Series(dtype=str)))
_steam_max = _max_date_from_series(df_steam.get('ReleaseDate',   pd.Series(dtype=str)))
_ns_min    = _min_date_from_series(df_nonsteam.get('YouTube ReleaseDate', pd.Series(dtype=str)))
_ns_max    = _max_date_from_series(df_nonsteam.get('YouTube ReleaseDate', pd.Series(dtype=str)))
_inv_min   = _min_date_from_series(
    st.session_state.game_data.get('Date Purchased', pd.Series(dtype=str))
    if 'game_data' in st.session_state else pd.Series(dtype=str)
)
_inv_max   = _max_date_from_series(
    st.session_state.game_data.get('Date Purchased', pd.Series(dtype=str))
    if 'game_data' in st.session_state else pd.Series(dtype=str)
)
GLOBAL_DATE_MIN = min(_steam_min, _ns_min, _inv_min)
GLOBAL_DATE_MAX = max(_steam_max, _ns_max, _inv_max)

# Clamp any stale session-state date values so they stay within the computed range
for _key in ("steam_start_date", "ns_start_date", "inv_start_date"):
    if _key in st.session_state and isinstance(st.session_state[_key], dt.date):
        st.session_state[_key] = max(st.session_state[_key], GLOBAL_DATE_MIN)
for _key in ("steam_end_date", "ns_end_date", "inv_end_date"):
    if _key in st.session_state and isinstance(st.session_state[_key], dt.date):
        st.session_state[_key] = min(st.session_state[_key], GLOBAL_DATE_MAX)

# ── Date sync callbacks (keep all tabs in lockstep) ───────────────────────────
def _sync_from_steam_dates():
    st.session_state.ns_start_date  = st.session_state.steam_start_date
    st.session_state.ns_end_date    = st.session_state.steam_end_date
    st.session_state.inv_start_date = st.session_state.steam_start_date
    st.session_state.inv_end_date   = st.session_state.steam_end_date

def _sync_from_ns_dates():
    st.session_state.steam_start_date = st.session_state.ns_start_date
    st.session_state.steam_end_date   = st.session_state.ns_end_date
    st.session_state.inv_start_date   = st.session_state.ns_start_date
    st.session_state.inv_end_date     = st.session_state.ns_end_date

def _sync_from_inv_dates():
    st.session_state.steam_start_date = st.session_state.inv_start_date
    st.session_state.steam_end_date   = st.session_state.inv_end_date
    st.session_state.ns_start_date    = st.session_state.inv_start_date
    st.session_state.ns_end_date      = st.session_state.inv_end_date

# ── TABS ───────────────────────────────────────────────────────────────────────
tab_steam, tab_nonsteam, tab_inventory = st.tabs(
    ["🚀 Steam Report", "📽️ Non-Steam Report", "🎮 Game Inventory"]
)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — STEAM REPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_steam:
    st.header("Steam Game Ranking")
    st.caption(f"📊 Loading from {steam_source_name}")

    # ── Scraper status panel ──────────────────────────────────────────────────
    steam_info = get_last_run_info("steam")
    s_col1, s_col2, s_col3 = st.columns([2, 2, 1])
    with s_col1:
        st.metric("Last Scrape", _format_last_run(steam_info))
    with s_col2:
        st.metric("Next Window", _format_next_window("steam"))
    with s_col3:
        run_steam = st.button(
            "🔄 Run Steam Scraper",
            key="run_steam_scraper",
            disabled=_steam_thread_state["running"],
            help="Scrapes Steam for games in the next 2-week window and appends to raw_steam.csv",
        )

    steam_log_area = st.empty()

    if run_steam and not _steam_thread_state["running"]:
        _steam_log_list = ["Starting Steam scraper..."]
        st.session_state.steam_scraper_log = _steam_log_list
        _steam_thread_state["running"] = True
        _steam_thread_state["result"] = None

        def _steam_log(msg, _log=_steam_log_list):
            _log.append(msg)

        def _run_steam(_log_fn=_steam_log, _state=_steam_thread_state):
            result = run_steam_scraper(status_callback=_log_fn)
            _state["result"] = result      # write to plain dict, NOT session_state
            _state["running"] = False

        threading.Thread(target=_run_steam, daemon=True).start()
        st.rerun()

    if _steam_thread_state["running"] or st.session_state.steam_scraper_log:
        with steam_log_area.container():
            if _steam_thread_state["running"]:
                st.info("⏳ Steam scraper is running... this may take several minutes.")
                st.button("🔃 Check status", key="steam_check_status")
            log_text = "\n".join(st.session_state.steam_scraper_log[-30:])
            if log_text:
                st.code(log_text, language=None)

        result = _steam_thread_state["result"]
        if result is not None:
            if result["success"]:
                st.success(
                    f"✅ Steam scrape complete! {result['new_rows']} new games added "
                    f"({result['window_start']} → {result['window_end']})"
                )
                reload_steam_from_csv()
                df_steam = st.session_state.df_steam.copy()
                _steam_thread_state["result"] = None
                st.session_state.steam_scraper_log = []
            else:
                st.error(f"❌ Steam scrape failed: {result['error']}")
                _steam_thread_state["result"] = None

    st.divider()

    # ── Sidebar: Steam weights ────────────────────────────────────────────────
    st.sidebar.header("Steam Report Configuration")
    # min_followers = st.sidebar.number_input("Min Followers", value=10000,
    #                                         help="Min followers considered for points")
    max_followers = st.sidebar.number_input("Max Followers", value=398955,
                                            help="Max followers considered for points")
    w_followers = st.sidebar.slider("Follower Weight", 0, 5, 5)
    w_developers = st.sidebar.slider("Developer Weight", 0, 5, 2)

    # ── Formula display ───────────────────────────────────────────────────────
    
    with st.expander("📐 Steam ranking Formula"):
        st.write("### Current Steam Formula")
        st.latex(r"1. Follower Points = (0.5 \times linear\_norm) + (0.5 \times log\_norm)")
        st.latex(r'linear\_norm = \frac{followers - min\_followers}{max\_followers - min\_followers} \times (5 - 1) + 1')
        st.latex(r'log\_norm = \frac{\log(followers) - \log(min\_followers)}{\log(max\_followers) - \log(min\_followers)} \times (5 - 1) + 1')
        st.latex(r"2. Developer Points = (Avg.of Developer Points)")
        st.latex(r"3. Final Priority Score = ((Follower Points * Follower Weight) + (Developer Points * Developer Weight)")
        st.caption("*_The above formula is based on points from the Developer List_")
        st.caption("**_If any developer is not in the Developer List, they are assigned a default point value of 1._")

    # ── Score calculations ────────────────────────────────────────────────────
    for index, row in df_steam.iterrows():
        df_steam.loc[index, 'Follower Points'] = calculate_follower_weighted_points(
            row['FollowerCount'], min_followers=1000, max_followers=max_followers
        )

    for index, row in df_steam.iterrows():
        points, _ = calculate_developer_weighted_points(row['Developers'])
        df_steam.loc[index, 'Developer Points'] = points

    df_steam['Follower Points']         = df_steam['Follower Points'].round(2)
    df_steam['Developer Points']        = df_steam['Developer Points'].round(2)
    df_steam['Weighted Follower Score'] = df_steam['Follower Points'] * w_followers
    df_steam['Weighted Dev Score']      = df_steam['Developer Points'] * w_developers
    df_steam['Final Priority Score']    = (df_steam['Weighted Follower Score'] + df_steam['Weighted Dev Score']).round(2)
    
    df_ranked = df_steam.sort_values('Final Priority Score', ascending=False, ignore_index=True)

    # Initialize reset flag for Steam filters
    if "steam_reset_filters" not in st.session_state:
        st.session_state.steam_reset_filters = False

    # ── FILTERS ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=False):
        f_col1, f_col2 = st.columns(2)

        # --- Date range ---
        with f_col1:
            st.markdown("**Release Date**")
            if 'ReleaseDate' in df_ranked.columns:
                df_ranked['ReleaseDate'] = pd.to_datetime(df_ranked['ReleaseDate'], errors='coerce')

            # Use default values if reset flag is set
            if st.session_state.steam_reset_filters:
                st.session_state.steam_start_date = GLOBAL_DATE_MIN
                st.session_state.steam_end_date   = GLOBAL_DATE_MAX
                st.session_state.ns_start_date    = GLOBAL_DATE_MIN
                st.session_state.ns_end_date      = GLOBAL_DATE_MAX
                st.session_state.inv_start_date   = GLOBAL_DATE_MIN
                st.session_state.inv_end_date     = GLOBAL_DATE_MAX

            start_date = st.date_input("From", value=st.session_state.get("steam_start_date", GLOBAL_DATE_MIN), min_value=GLOBAL_DATE_MIN, max_value=GLOBAL_DATE_MAX, key="steam_start_date", format="DD/MM/YYYY", on_change=_sync_from_steam_dates)
            end_date   = st.date_input("To",   value=st.session_state.get("steam_end_date",   GLOBAL_DATE_MAX), min_value=GLOBAL_DATE_MIN, max_value=GLOBAL_DATE_MAX, key="steam_end_date",   format="DD/MM/YYYY", on_change=_sync_from_steam_dates)

        # --- Genre multi-select ---
        with f_col2:
            st.markdown("**Genre**")
            if 'Genres' in df_ranked.columns:
                all_genres = sorted(set(
                    g.strip()
                    for genres in df_ranked['Genres'].dropna()
                    for g in (genres if isinstance(genres, list) else str(genres).split(','))
                    if g.strip()
                ))
            else:
                all_genres = []
            
            default_genres = [] if st.session_state.steam_reset_filters else st.session_state.get("steam_genres", [])
            selected_genres = st.multiselect(
                "Select genres", options=all_genres,
                default=default_genres,
                placeholder="All genres", key="steam_genres"
            )

        f_col4, f_col5 = st.columns(2)

        # --- Name search ---
        with f_col4:
            st.markdown("**Game Name Search**")
            default_search = "" if st.session_state.steam_reset_filters else st.session_state.get("steam_name_search", "")
            steam_name_search = st.text_input(
                "Search by name", 
                value=default_search,
                placeholder="Type to search…",
                key="steam_name_search"
            )

        # --- Final Priority Score range ---
        with f_col5:
            st.markdown("**Final Priority Score**")
            if 'Final Priority Score' in df_ranked.columns:
                ps_min = float(df_ranked['Final Priority Score'].min())
                ps_max = float(df_ranked['Final Priority Score'].max())
            else:
                ps_min, ps_max = 0.0, 100.0

            default_score = (ps_min, ps_max) if st.session_state.steam_reset_filters else st.session_state.get("steam_score_range", (ps_min, ps_max))
            score_range = st.slider(
                "Score range",
                min_value=ps_min, max_value=ps_max,
                value=default_score,
                key="steam_score_range"
            )

        f_col6, _ = st.columns(2)

        # --- Follower Count cap ---
        with f_col6:
            st.markdown("**Max Follower Count**")
            FOLLOWER_SLIDER_MAX = 100000
            default_fc_max = FOLLOWER_SLIDER_MAX if st.session_state.steam_reset_filters else st.session_state.get("steam_follower_max", FOLLOWER_SLIDER_MAX)
            follower_max = st.slider(
                "Show games up to",
                min_value=0, max_value=FOLLOWER_SLIDER_MAX,
                value=default_fc_max,
                step=1000,
                key="steam_follower_max"
            )

        # Apply and Revert buttons
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            apply_steam = st.button("Apply Filters", key="steam_apply")
        with filter_col2:
            if st.button("Revert to Default", key="steam_revert"):
                st.session_state.steam_reset_filters = True
                st.rerun()

    # Reset the flag after using it
    if st.session_state.steam_reset_filters:
        st.session_state.steam_reset_filters = False

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_steam = df_ranked.copy()

    if apply_steam:
        # Date
        if 'ReleaseDate' in df_filtered_steam.columns:
            rd = pd.to_datetime(df_filtered_steam['ReleaseDate'], errors='coerce')
            df_filtered_steam = df_filtered_steam[rd.between(pd.Timestamp(start_date), pd.Timestamp(end_date))]

        # Genres
        if selected_genres and 'Genres' in df_filtered_steam.columns:
            def has_genre(genres_val):
                genres_list = genres_val if isinstance(genres_val, list) else [g.strip() for g in str(genres_val).split(',')]
                return any(g in selected_genres for g in genres_list)
            df_filtered_steam = df_filtered_steam[df_filtered_steam['Genres'].apply(has_genre)]

        # Name search
        if steam_name_search and 'Name' in df_filtered_steam.columns:
            df_filtered_steam = df_filtered_steam[
                df_filtered_steam['Name'].str.contains(steam_name_search, case=False, na=False)
            ]

        # Score range
        if 'Final Priority Score' in df_filtered_steam.columns:
            df_filtered_steam = df_filtered_steam[
                df_filtered_steam['Final Priority Score'].between(score_range[0], score_range[1])
            ]

        # Follower count cap
        if 'FollowerCount' in df_filtered_steam.columns:
            df_filtered_steam = df_filtered_steam[
                df_filtered_steam['FollowerCount'].between(0, follower_max)
            ]

    df_filtered_steam = df_filtered_steam.reset_index(drop=True)
    df_filtered_steam.index = df_filtered_steam.index + 1

    # ── Results ───────────────────────────────────────────────────────────────
    tab1, tab2 = st.tabs(["📊 Ranking Results", "🔍 Developer List"])

    with tab1:
        st.subheader("Top Priority Games")
        st.caption(f"Showing **{len(df_filtered_steam)}** of **{len(df_ranked)}** games")
        cols_to_show = ['Name', 'ReleaseDate', 'FollowerCount', 'Follower Points', 'Developers', 'Developer Points', 'Final Priority Score']
        # Include date_appended for highlighting (filtered out of visible cols automatically by styler)
        display_cols = cols_to_show + (["date_appended"] if "date_appended" in df_filtered_steam.columns else [])
        df_display = df_filtered_steam[display_cols].copy()
        for col in ['Follower Points', 'Developer Points', 'Final Priority Score']:
            if col in df_display.columns:
                df_display[col] = df_display[col].round(2)
        _parsed_dates = pd.to_datetime(df_display['ReleaseDate'], errors='coerce', dayfirst=True)
        df_display['ReleaseDate'] = _parsed_dates.dt.strftime('%d/%m/%Y').fillna('Date Not Parsable')
        df_display['Developers'] = df_display['Developers'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else str(x)
        )
        st.dataframe(
            highlight_new_rows(df_display),
            use_container_width=True,
            column_config={
                "Follower Points":      st.column_config.NumberColumn(format="%.2f"),
                "Developer Points":     st.column_config.NumberColumn(format="%.2f"),
                "Final Priority Score": st.column_config.NumberColumn(format="%.2f"),
                "FollowerCount":        st.column_config.NumberColumn(format="%d"),
            },
        )

    with tab2:
        st.subheader("Developer Ranking List")
        st.info("Below is the internal ranking list for developers based on their Average Revenue per Game:")
        st.dataframe(dev_list, width='stretch')


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — NON-STEAM REPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_nonsteam:
    st.header("Non-Steam Game Ranking")
    st.caption(f"📊 Loading from {nonsteam_source_name}")

    # ── Scraper status + run button ───────────────────────────────────────────
    ns_info = get_last_run_info("non_steam")
    ns_col1, ns_col2, ns_col3 = st.columns([2, 2, 1])
    with ns_col1:
        st.metric("Last Scrape", _format_last_run(ns_info))
    with ns_col2:
        st.metric("Next Window", _format_next_window("non_steam"))
    with ns_col3:
        run_nonsteam = st.button(
            "▶ Run Scraper",
            key="run_nonsteam_scraper",
            disabled=_ns_thread_state["running"],
            use_container_width=True,
            help="Runs all 3 stages: follower counts → IGDB enrichment → CSV export",
        )

    # ── Scraper configuration ─────────────────────────────────────────────────
    with st.expander("⚙️ Scraper Configuration", expanded=False):
        ns_max_games = st.number_input(
            "Max games to process",
            min_value=0,
            value=100,
            step=10,
            key="ns_max_games",
            help="Cap on how many games are scraped for follower counts. Set to 0 to process all (may take a long time).",
        )

    # ── Log area + run logic ──────────────────────────────────────────────────
    nonsteam_log_area = st.empty()

    if run_nonsteam and not _ns_thread_state["running"]:
        # All config captured into plain locals — threads cannot touch st.session_state
        _ns_cfg = {
            "max_games": int(st.session_state.get("ns_max_games", 100)) or None,
        }
        _ns_log_list = ["Starting Non-Steam scraper..."]
        st.session_state.nonsteam_scraper_log = _ns_log_list
        _ns_thread_state["running"] = True
        _ns_thread_state["result"] = None

        def _ns_log(msg, _log=_ns_log_list):
            _log.append(msg)

        def _run_nonsteam(_cfg=_ns_cfg, _log_fn=_ns_log, _state=_ns_thread_state):
            result = run_nonsteam_scraper(status_callback=_log_fn, **_cfg)
            _state["result"] = result      # write to plain dict, NOT session_state
            _state["running"] = False

        threading.Thread(target=_run_nonsteam, daemon=True).start()
        st.rerun()

    if _ns_thread_state["running"] or st.session_state.nonsteam_scraper_log:
        with nonsteam_log_area.container():
            if _ns_thread_state["running"]:
                st.info("Non-Steam scraper is running... IGDB enrichment can take several minutes.")
                st.button("🔃 Check status", key="nonsteam_check_status")
            log_text = "\n".join(st.session_state.nonsteam_scraper_log[-40:])
            if log_text:
                st.code(log_text, language=None)

        result = _ns_thread_state["result"]
        if result is not None:
            if result["success"]:
                reload_nonsteam_from_csv()
                st.success(
                    f"✅ Scrape complete — {result['new_rows']} new games added "
                    f"({result['window_start']} → {result['window_end']})"
                )
                _ns_thread_state["result"] = None
                st.session_state.nonsteam_scraper_log = []
                st.rerun()
            else:
                st.error(f"❌ Scrape failed: {result['error']}")
                _ns_thread_state["result"] = None

    # ── Verify Steam Status (single game) ────────────────────────────────────
    st.subheader("Verify Steam Status")
    game_names = sorted(df_nonsteam["Game Title"].dropna().unique().tolist())
    selected_game = st.selectbox("Select a game to verify", game_names, key="verify_game_select")

    if st.button("🔍 Verify", key="ns_verify_single"):
        row_mask = df_nonsteam["Game Title"] == selected_game
        platforms = str(df_nonsteam.loc[row_mask, "Platforms"].iloc[0]) if row_mask.any() else ""
        status = verify_single_game_steam_status(selected_game, platforms)
        # Update the CSV on disk (write to temp file first, then replace)
        source = get_latest_nonsteam_csv()
        try:
            df_disk = pd.read_csv(source)
            df_disk.loc[df_disk["Game Title"] == selected_game, "SteamStatus"] = status
            tmp = source.with_suffix(".tmp")
            df_disk.to_csv(tmp, index=False)
            tmp.replace(source)
            reload_nonsteam_from_csv()
            st.success(f"**{selected_game}** → {status}")
            st.rerun()
        except PermissionError:
            st.error("Cannot write to CSV — the file may be open in another program. Close it and try again.")

    st.divider()

    # ── Sidebar config ────────────────────────────────────────────────────────
    st.sidebar.header("Non-Steam Report Configuration")
    st.info("The Non-Steam ranking is based on YouTube Views adjusted for the time since release. Games with higher adjusted views are prioritised.")

    # Pre-processing: label unchecked games, remove Steam games, parse dates
    df_nonsteam['SteamStatus'] = df_nonsteam['SteamStatus'].fillna('Needs Verification')
    df_nonsteam_filter = df_nonsteam[df_nonsteam['SteamStatus'] != 'PC Game (on Steam)'].copy()
    df_nonsteam_filter['YouTube ReleaseDate'] = pd.to_datetime(
        df_nonsteam_filter['YouTube ReleaseDate'], errors='coerce'
    )
    df_nonsteam_filter['Release Date'] = pd.to_datetime(
        df_nonsteam_filter['Release Date'], errors='coerce'
    )

    # ── Formula display ───────────────────────────────────────────────────────
    with st.expander("📐 Non-Steam Ranking Formula", expanded=False): # type: ignore
        st.write("### Current Non-Steam Formula")
        st.latex(r"1. Days since Release = (Today's Date - Release Date)")
        st.latex(r"2. Adjusted Views = YouTube Views / (1 + (Days Since Release / 365))")
        st.latex(r"Games with the highest Adjusted Views are ranked highest.")

    # ── Score calculation ─────────────────────────────────────────────────────
    # Use YouTube ReleaseDate for days calculation; fall back to game Release Date
    # when YouTube ReleaseDate is missing (common for newly scraped games)
    today = dt.date.today()
    effective_date = df_nonsteam_filter['YouTube ReleaseDate'].fillna(
        df_nonsteam_filter['Release Date']
    )
    df_nonsteam_filter['Days_Since_Release'] = (
        pd.to_datetime(today) - effective_date
    ).dt.days
    df_nonsteam_filter['adjusted_views'] = (
        df_nonsteam_filter['YouTube Views'] / (1 + df_nonsteam_filter['Days_Since_Release'] / 365)
    ).round(2)

    df_non_steam_ranked = df_nonsteam_filter.sort_values('adjusted_views', ascending=False, ignore_index=True)

    # ── Cross-check against Steam titles ──────────────────────────────────────
    steam_titles = set(
        df_steam['Name'].dropna().astype(str).str.strip().str.lower()
    ) if 'Name' in df_steam.columns else set()
    df_non_steam_ranked['_on_steam'] = (
        df_non_steam_ranked['Game Title'].astype(str).str.strip().str.lower().isin(steam_titles)
    )

    # Initialize reset flag for Non-Steam filters
    if "ns_reset_filters" not in st.session_state:
        st.session_state.ns_reset_filters = False

    # ── FILTERS ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=False):
        nf_col1, nf_col2 = st.columns(2)

        #--- YouTube Release Date ---
        with nf_col1:
            st.markdown("**YouTube Release Date**")
            if st.session_state.ns_reset_filters:
                st.session_state.steam_start_date = GLOBAL_DATE_MIN
                st.session_state.steam_end_date   = GLOBAL_DATE_MAX
                st.session_state.ns_start_date    = GLOBAL_DATE_MIN
                st.session_state.ns_end_date      = GLOBAL_DATE_MAX
                st.session_state.inv_start_date   = GLOBAL_DATE_MIN
                st.session_state.inv_end_date     = GLOBAL_DATE_MAX

            ns_start = st.date_input("From", value=st.session_state.get("ns_start_date", GLOBAL_DATE_MIN), min_value=GLOBAL_DATE_MIN, max_value=GLOBAL_DATE_MAX, key="ns_start_date", format="DD/MM/YYYY", on_change=_sync_from_ns_dates)
            ns_end   = st.date_input("To",   value=st.session_state.get("ns_end_date",   GLOBAL_DATE_MAX), min_value=GLOBAL_DATE_MIN, max_value=GLOBAL_DATE_MAX, key="ns_end_date",   format="DD/MM/YYYY", on_change=_sync_from_ns_dates)

        # --- Platform multi-select ---
        with nf_col2:
            st.markdown("**Platform**")
            if 'Platforms' in df_non_steam_ranked.columns:
                all_platforms = sorted(set(
                    p.strip()
                    for plats in df_non_steam_ranked['Platforms'].dropna()
                    for p in (plats if isinstance(plats, list) else str(plats).split(','))
                    if p.strip()
                ))
            else:
                all_platforms = []
            
            default_platforms = [] if st.session_state.ns_reset_filters else st.session_state.get("ns_platforms", [])
            selected_platforms = st.multiselect(
                "Select platforms", options=all_platforms,
                default=default_platforms,
                placeholder="All platforms", key="ns_platforms"
            )

        nf_col3, nf_col4 = st.columns(2)

        # --- SteamStatus filter ---
        with nf_col3:
            st.markdown("**Console or PC**")
            if 'SteamStatus' in df_non_steam_ranked.columns:
                steam_statuses = sorted(df_non_steam_ranked['SteamStatus'].dropna().unique().tolist())
            else:
                steam_statuses = []

            default_statuses = [] if st.session_state.ns_reset_filters else st.session_state.get("ns_steam_status", [])
            selected_statuses = st.multiselect(
                "Select status", options=steam_statuses,
                default=default_statuses,
                placeholder="All statuses", key="ns_steam_status"
            )

        # --- Hide on Steam filter ---
        with nf_col4:
            st.markdown("**Steam Overlap**")
            n_on_steam = int(df_non_steam_ranked['_on_steam'].sum())
            default_hide_steam = True if st.session_state.ns_reset_filters else st.session_state.get("ns_hide_on_steam", True)
            hide_on_steam = st.checkbox(
                f"Hide games already on Steam ({n_on_steam} found)",
                value=default_hide_steam,
                key="ns_hide_on_steam",
            )

        # Apply and Revert buttons
        ns_filter_col1, ns_filter_col2 = st.columns(2)
        with ns_filter_col1:
            apply_ns = st.button("Apply Filters", key="ns_apply")
        with ns_filter_col2:
            if st.button("Revert to Default", key="ns_revert"):
                st.session_state.ns_reset_filters = True
                st.rerun()

    # Reset the flag after using it
    if st.session_state.ns_reset_filters:
        st.session_state.ns_reset_filters = False

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_ns = df_non_steam_ranked.copy()
    
    if apply_ns:
        # Date — keep rows where date is null OR within range
        if 'YouTube ReleaseDate' in df_filtered_ns.columns:
            yt_dates = pd.to_datetime(df_filtered_ns['YouTube ReleaseDate'], errors='coerce')
            in_range = yt_dates.between(pd.Timestamp(ns_start), pd.Timestamp(ns_end))
            df_filtered_ns = df_filtered_ns[yt_dates.isna() | in_range]

        # Platforms
        if selected_platforms and 'Platforms' in df_filtered_ns.columns:
            def has_platform(val):
                plats = val if isinstance(val, list) else [p.strip() for p in str(val).split(',')]
                return any(p in selected_platforms for p in plats)
            df_filtered_ns = df_filtered_ns[df_filtered_ns['Platforms'].apply(has_platform)]

        # Steam Status
        if selected_statuses and 'SteamStatus' in df_filtered_ns.columns:
            df_filtered_ns = df_filtered_ns[df_filtered_ns['SteamStatus'].isin(selected_statuses)]

        # Hide games already on Steam
        if hide_on_steam:
            df_filtered_ns = df_filtered_ns[~df_filtered_ns['_on_steam']]

    df_filtered_ns = df_filtered_ns.reset_index(drop=True)
    df_filtered_ns.index = df_filtered_ns.index + 1

    # ── Results ───────────────────────────────────────────────────────────────
    st.subheader("Top Priority Non-Steam Games")
    st.caption(f"Showing **{len(df_filtered_ns)}** of **{len(df_non_steam_ranked)}** games")

    cols_to_show = [
        'Game Title', 'adjusted_views', 'YouTube Views', 'Days_Since_Release',
        'Release Date', 'Developers', 'Platforms', 'Genres',
        'YouTube URL', 'YouTube ReleaseDate', 'SteamStatus'
    ]
    # Only keep columns that exist
    cols_to_show = [c for c in cols_to_show if c in df_filtered_ns.columns]
    df_nonsteam_display = df_filtered_ns[cols_to_show].copy()

    def format_list_column(value):
        if isinstance(value, list):
            return ', '.join(str(v).strip() for v in value if v and str(v).strip())
        return str(value)

    for col in ['Developers', 'Genres']:
        if col in df_nonsteam_display.columns:
            df_nonsteam_display[col] = df_nonsteam_display[col].apply(format_list_column)

    for col in ['adjusted_views', 'YouTube Views', 'Days_Since_Release']:
        if col in df_nonsteam_display.columns:
            df_nonsteam_display[col] = pd.to_numeric(df_nonsteam_display[col], errors='coerce').round(2)

    # Include date_appended for highlighting (if present in source data)
    if "date_appended" in df_filtered_ns.columns and "date_appended" not in cols_to_show:
        df_nonsteam_display["date_appended"] = df_filtered_ns["date_appended"].values

    st.dataframe(
        highlight_new_rows(df_nonsteam_display),
        use_container_width=True,
        column_config={
            "adjusted_views":     st.column_config.NumberColumn(format="%.2f"),
            "YouTube Views":      st.column_config.NumberColumn(format="%d"),
            "Days_Since_Release": st.column_config.NumberColumn(format="%d"),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — GAME INVENTORY
# ══════════════════════════════════════════════════════════════════════════════
with tab_inventory:
    if "game_data" not in st.session_state:
        st.session_state.game_data = pd.read_csv(INVENTORY_FILE)

    st.header("🎮 Game Tracker")
    st.subheader("Game Library")

    # Initialize reset flag for Inventory filters
    if "inv_reset_filters" not in st.session_state:
        st.session_state.inv_reset_filters = False

    # ── FILTERS ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=True):
        inv_f1, inv_f2, inv_f3 = st.columns(3)

        # --- Purchase date ---
        with inv_f1:
            st.markdown("**Date Purchased**")
            gd = st.session_state.game_data.copy()
            gd['Date Purchased'] = pd.to_datetime(gd['Date Purchased'], errors='coerce', format='%Y-%m-%d')

            if st.session_state.inv_reset_filters:
                st.session_state.steam_start_date = GLOBAL_DATE_MIN
                st.session_state.steam_end_date   = GLOBAL_DATE_MAX
                st.session_state.ns_start_date    = GLOBAL_DATE_MIN
                st.session_state.ns_end_date      = GLOBAL_DATE_MAX
                st.session_state.inv_start_date   = GLOBAL_DATE_MIN
                st.session_state.inv_end_date     = GLOBAL_DATE_MAX

            inv_start = st.date_input("From", value=st.session_state.get("inv_start_date", GLOBAL_DATE_MIN), min_value=GLOBAL_DATE_MIN, max_value=GLOBAL_DATE_MAX, key="inv_start_date", format="DD/MM/YYYY", on_change=_sync_from_inv_dates)
            inv_end   = st.date_input("To",   value=st.session_state.get("inv_end_date",   GLOBAL_DATE_MAX), min_value=GLOBAL_DATE_MIN, max_value=GLOBAL_DATE_MAX, key="inv_end_date",   format="DD/MM/YYYY", on_change=_sync_from_inv_dates)

        # --- Platform multi-select ---
        with inv_f2:
            st.markdown("**Platform**")
            if 'Platform' in gd.columns:
                inv_platforms = sorted(gd['Platform'].dropna().unique().tolist())
            else:
                inv_platforms = []
            
            default_inv_platforms = [] if st.session_state.inv_reset_filters else st.session_state.get("inv_platforms", [])
            selected_inv_platforms = st.multiselect(
                "Select platform", options=inv_platforms,
                default=default_inv_platforms,
                placeholder="All platforms", key="inv_platforms"
            )

        # --- Game name search ---
        with inv_f3:
            st.markdown("**Game Name Search**")
            default_inv_search = "" if st.session_state.inv_reset_filters else st.session_state.get("inv_name_search", "")
            inv_name_search = st.text_input(
                "Search by name", 
                value=default_inv_search,
                placeholder="Type to search…",
                key="inv_name_search"
            )

        inv_f4, inv_f5, inv_f6, inv_f7 = st.columns(4)

        # --- Status checkboxes ---
        with inv_f4:
            st.markdown("**Active**")
            default_active = "All" if st.session_state.inv_reset_filters else st.session_state.get("inv_active", "All")
            inv_active = st.selectbox("Active", ["All", "Yes", "No"], index=["All", "Yes", "No"].index(default_active), key="inv_active")
        with inv_f5:
            st.markdown("**On Hold**")
            default_hold = "All" if st.session_state.inv_reset_filters else st.session_state.get("inv_on_hold", "All")
            inv_on_hold = st.selectbox("On Hold", ["All", "Yes", "No"], index=["All", "Yes", "No"].index(default_hold), key="inv_on_hold")
        with inv_f6:
            st.markdown("**Reviewed**")
            default_reviewed = "All" if st.session_state.inv_reset_filters else st.session_state.get("inv_reviewed", "All")
            inv_reviewed = st.selectbox("Reviewed", ["All", "Yes", "No"], index=["All", "Yes", "No"].index(default_reviewed), key="inv_reviewed")
        with inv_f7:
            st.markdown("**Inactive**")
            default_inactive = "All" if st.session_state.inv_reset_filters else st.session_state.get("inv_inactive", "All")
            inv_inactive = st.selectbox("Inactive", ["All", "Yes", "No"], index=["All", "Yes", "No"].index(default_inactive), key="inv_inactive")

        # Apply and Revert buttons for inventory
        inv_filter_col1, inv_filter_col2 = st.columns(2)
        with inv_filter_col1:
            apply_inv = st.button("Apply Filters", key="inv_apply")
        with inv_filter_col2:
            if st.button("Revert to Default", key="inv_revert"):
                st.session_state.inv_reset_filters = True
                st.rerun()

    # Reset the flag after using it
    if st.session_state.inv_reset_filters:
        st.session_state.inv_reset_filters = False

    # ── Apply filters ─────────────────────────────────────────────────────────
    filtered = st.session_state.game_data.copy()
    filtered['Date Purchased'] = pd.to_datetime(filtered['Date Purchased'], errors='coerce', format='%Y-%m-%d')

    if apply_inv:
        # Date range
        filtered = filtered[filtered['Date Purchased'].between(pd.Timestamp(inv_start), pd.Timestamp(inv_end))]

        # Platform
        if selected_inv_platforms and 'Platform' in filtered.columns:
            filtered = filtered[filtered['Platform'].isin(selected_inv_platforms)]

        # Name search
        if inv_name_search and 'Game Name' in filtered.columns:
            filtered = filtered[
                filtered['Game Name'].str.contains(inv_name_search, case=False, na=False)
            ]

        # Boolean status filters
        bool_map = {"Yes": True, "No": False}
        for col, sel in [('Active', inv_active), ('On Hold', inv_on_hold),
                         ('Reviewed', inv_reviewed), ('Inactive', inv_inactive)]:
            if sel != "All" and col in filtered.columns:
                try:
                    filtered[col] = filtered[col].astype(bool)
                    filtered = filtered[filtered[col] == bool_map[sel]]
                except Exception:
                    pass

    # ── Metrics ───────────────────────────────────────────────────────────────
    st.divider()
    col1, col2, col3, col4, col5 = st.columns(5)
    game_data_bools = st.session_state.game_data.copy()
    for bc in ['Active', 'On Hold', 'Reviewed', 'Inactive']:
        try:
            game_data_bools[bc] = game_data_bools[bc].astype(bool)
        except Exception:
            pass

    with col1:
        st.metric("Total Games", len(game_data_bools), border=True)
    with col2:
        st.metric("Active", int(game_data_bools['Active'].sum()) if 'Active' in game_data_bools.columns else 0, border=True)
    with col3:
        st.metric("On Hold", int(game_data_bools['On Hold'].sum()) if 'On Hold' in game_data_bools.columns else 0, border=True)
    with col4:
        st.metric("Reviewed", int(game_data_bools['Reviewed'].sum()) if 'Reviewed' in game_data_bools.columns else 0, border=True)
    with col5:
        st.metric("Inactive", int(game_data_bools['Inactive'].sum()) if 'Inactive' in game_data_bools.columns else 0, border=True)

    # ── Filtered preview ──────────────────────────────────────────────────────
    st.divider()
    st.subheader("Filtered Library")
    st.caption(f"Showing **{len(filtered)}** of **{len(st.session_state.game_data)}** games")
    st.dataframe(filtered, width='stretch')

    # ── Editable full table ───────────────────────────────────────────────────
    st.info("💡 Edit the table directly, toggle checkboxes, or use the + button to add rows. Changes save automatically!")
    st.data_editor(
        st.session_state.game_data,
        num_rows="dynamic",
        key="game_editor",
        column_config={
            "Game Name":      st.column_config.TextColumn("Game", width="medium", required=True),
            "Date Purchased": st.column_config.TextColumn("Date Purchased"),
            "Physical":       st.column_config.CheckboxColumn("Physical"),
            "Digital":        st.column_config.CheckboxColumn("Digital"),
            "Platform":       st.column_config.TextColumn("Platform"),
            "Account":        st.column_config.TextColumn("Account"),
            "Inactive":       st.column_config.CheckboxColumn("Inactive"),
            "On Hold":        st.column_config.CheckboxColumn("On Hold"),
            "Active":         st.column_config.CheckboxColumn("Active"),
            "Reviewed":       st.column_config.CheckboxColumn("Reviewed"),
            "Links":          st.column_config.LinkColumn("Links", display_text="Open Link"),
        },
        disabled=["Game Name"],
        on_change=handle_change,
    )