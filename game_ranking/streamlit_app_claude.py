import streamlit as st
import pandas as pd
import numpy as np
import io
import datetime as dt
import threading
import queue
from src.calculation.process_data import (
    clean_dev_genre_list, flagging,
    calculate_developer_weighted_points, load_data,
    calculate_follower_weighted_points, handle_change,
)
from src.calculation.scraper import scrape_google_trends
from config import CSV_STEAM, CSV_NON_STEAM, INVENTORY_FILE
from pipeline.state import get_last_run_info, get_next_window
from steam_pipeline import run_steam_scraper
from nonsteam_pipeline import run_nonsteam_scraper

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="AGS - Game Ranking Tool", layout="wide")

st.title("🎮 Research Team: Game Ranking Algorithm")
st.markdown("""
This tool calculates review priority by awarding 'points' to Steam and Non-Steam games
based on certain criteria, and weights to determine the final recommendation.

The criteria for each report are displayed on the sidebar, and their weights can be adjusted.

**Instructions:**
1. **Upload your own CSV files** for Steam and Non-Steam reports using the sidebar file uploaders. If no files are uploaded, default files will be used.
2. **Preview the uploaded files** before loading to ensure they have the correct format and columns.
3. **Adjust the criteria weights** in the sidebar to see how they affect the rankings.
4. **Use the Refresh buttons** on each tab to run the scrapers and pull in new data.
""")

# ── Session state init ─────────────────────────────────────────────────────────
for key, default in {
    "df_steam": None,
    "df_nonsteam": None,
    "steam_cleaned": False,
    "nonsteam_cleaned": False,
    "uploaded_steam_bytes": None,
    "uploaded_steam_name": None,
    "uploaded_nonsteam_bytes": None,
    "uploaded_nonsteam_name": None,
    # scraper run state
    "steam_scraper_running": False,
    "steam_scraper_log": [],
    "steam_scraper_result": None,
    "nonsteam_scraper_running": False,
    "nonsteam_scraper_log": [],
    "nonsteam_scraper_result": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ── Helpers ────────────────────────────────────────────────────────────────────
def load_defaults():
    df_steam, df_nonsteam, dev_list, genre_list, inventory = load_data(CSV_STEAM, CSV_NON_STEAM)
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


def reload_steam_from_csv():
    """Re-read raw_steam.csv from disk and update session state."""
    try:
        tmp = pd.read_csv(CSV_STEAM)
        tmp = clean_dev_genre_list(tmp)
        tmp = flagging(tmp)
        st.session_state.df_steam = tmp
        st.session_state.steam_source = "default file (refreshed)"
        st.session_state.steam_cleaned = True
    except Exception as e:
        st.error(f"Failed to reload Steam CSV: {e}")


def reload_nonsteam_from_csv():
    """Re-read raw_non_steam.csv from disk and update session state."""
    try:
        tmp = pd.read_csv(CSV_NON_STEAM)
        st.session_state.df_nonsteam = tmp
        st.session_state.nonsteam_source = "default file (refreshed)"
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


# ── Sidebar: File Uploads ──────────────────────────────────────────────────────
st.sidebar.header(" Filters")
# st.sidebar.markdown("Upload your own CSV files or use the defaults below:")

# uploaded_steam = st.sidebar.file_uploader("Upload Steam CSV", type="csv", key="steam_upload")
# uploaded_nonsteam = st.sidebar.file_uploader("Upload Non-Steam CSV", type="csv", key="nonsteam_upload")

# if uploaded_steam and uploaded_steam.name != st.session_state.uploaded_steam_name:
#     st.session_state.uploaded_steam_bytes = uploaded_steam.getvalue()
#     st.session_state.uploaded_steam_name = uploaded_steam.name

# if uploaded_nonsteam and uploaded_nonsteam.name != st.session_state.uploaded_nonsteam_name:
#     st.session_state.uploaded_nonsteam_bytes = uploaded_nonsteam.getvalue()
#     st.session_state.uploaded_nonsteam_name = uploaded_nonsteam.name

# # Steam upload preview & load
# if st.session_state.uploaded_steam_bytes:
#     with st.sidebar.expander("👀 Preview Steam File"):
#         preview_steam = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
#         st.dataframe(preview_steam.head(3), width='stretch')
#         st.caption(f"Rows: {len(preview_steam)}, Columns: {len(preview_steam.columns)}")
#     if st.sidebar.button("📥 Load Steam Data", key="load_steam_btn"):
#         try:
#             steam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
#             steam_required_cols = ['Name', 'FollowerCount', 'Developers', 'Genres', 'ReleaseDate']
#             steam_missing = [col for col in steam_required_cols if col not in steam_df_upload.columns]
#             if steam_missing:
#                 st.sidebar.error(f"Missing columns: {', '.join(steam_missing)}")
#             else:
#                 steam_df_upload = clean_dev_genre_list(steam_df_upload)
#                 steam_df_upload = flagging(steam_df_upload)
#                 st.session_state.df_steam = steam_df_upload
#                 st.session_state.steam_source = st.session_state.uploaded_steam_name
#                 st.session_state.steam_cleaned = True
#                 st.sidebar.success(f"✅ Loaded {st.session_state.uploaded_steam_name}")
#         except Exception as e:
#             st.sidebar.error(f"Error loading file: {e}")
# else:
#     st.sidebar.info("No Steam CSV uploaded. Using default.")

# # Non-steam upload preview & load
# if st.session_state.uploaded_nonsteam_bytes:
#     with st.sidebar.expander("👀 Preview Non-Steam File"):
#         preview_nonsteam = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
#         st.dataframe(preview_nonsteam.head(3), width='stretch')
#         st.caption(f"Rows: {len(preview_nonsteam)}, Columns: {len(preview_nonsteam.columns)}")
#     if st.sidebar.button("📥 Load Non-Steam Data", key="load_nonsteam_btn"):
#         try:
#             nonsteam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
#             nonsteam_required_cols = ['Game Title', 'Developers', 'SteamStatus', 'YouTube Views']
#             nonsteam_missing = [col for col in nonsteam_required_cols if col not in nonsteam_df_upload.columns]
#             if nonsteam_missing:
#                 st.sidebar.error(f"Missing columns: {', '.join(nonsteam_missing)}")
#             else:
#                 st.session_state.df_nonsteam = nonsteam_df_upload
#                 st.session_state.nonsteam_source = st.session_state.uploaded_nonsteam_name
#                 st.session_state.nonsteam_cleaned = True
#                 st.sidebar.success(f"✅ Loaded {st.session_state.uploaded_nonsteam_name}")
#         except Exception as e:
#             st.sidebar.error(f"Error loading file: {e}")
# else:
#     st.sidebar.info("No Non-Steam CSV uploaded. Using default.")

st.sidebar.divider()
if st.sidebar.button("🔄 Reset to Defaults"):
    load_defaults()
    st.rerun()

# Load defaults on first run
try:
    if st.session_state.df_steam is None or st.session_state.df_nonsteam is None:
        load_defaults()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Retrieve working copies
df_steam = st.session_state.df_steam.copy()
df_nonsteam = st.session_state.df_nonsteam.copy()
dev_list = st.session_state.dev_list
genre_list = st.session_state.genre_list
steam_source_name = st.session_state.get("steam_source", "default file")
nonsteam_source_name = st.session_state.get("nonsteam_source", "default file")

# ── Tabs ───────────────────────────────────────────────────────────────────────
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
    s_col1, s_col2 = st.columns([2, 2])
    with s_col1:
        st.metric("Last Scrape", _format_last_run(steam_info))
    with s_col2:
        st.metric("Next Window", _format_next_window("steam"))

    # ── Scraper input fields ──────────────────────────────────────────────────
    st.markdown("### 🎮 Steam Scraper Configuration")
    scol1, scol2, scol3 = st.columns([2, 2, 1])
    
    with scol1:
        default_start, default_end = get_next_window("steam", window_days=14)
        steam_start_date = st.date_input(
            "Start Date",
            value=dt.datetime.strptime(default_start, "%Y-%m-%d").date(),
            key="steam_scraper_start_date",
            help="Start date for scraping (inclusive)"
        )
    
    with scol2:
        steam_end_date = st.date_input(
            "End Date", 
            value=dt.datetime.strptime(default_end, "%Y-%m-%d").date(),
            key="steam_scraper_end_date",
            help="End date for scraping (inclusive)"
        )
    
    with scol3:
        st.write("")  # Spacing
        st.write("")  # Spacing
        run_steam = st.button(
            "▶️ Run Scraper",
            key="run_steam_scraper",
            disabled=st.session_state.steam_scraper_running,
            help="Scrapes Steam for games released in the specified date range",
        )

    # Steam scraper log area
    steam_log_area = st.empty()

    if run_steam and not st.session_state.steam_scraper_running:
        st.session_state.steam_scraper_running = True
        st.session_state.steam_scraper_log = ["⏳ Starting Steam scraper..."]
        st.session_state.steam_scraper_result = None

        # Create thread-safe queue for logging
        if 'steam_log_queue' not in st.session_state:
            st.session_state.steam_log_queue = queue.Queue()
        
        log_queue = st.session_state.steam_log_queue

        def _steam_log(msg):
            # Thread-safe: put message in queue instead of accessing session_state
            log_queue.put(('log', msg))

        def _run_steam():
            try:
                # Pass the date parameters from the UI
                result = run_steam_scraper(
                    start_date=steam_start_date.strftime("%Y-%m-%d"),
                    end_date=steam_end_date.strftime("%Y-%m-%d"),
                    status_callback=_steam_log
                )
                log_queue.put(('result', result))
            except Exception as e:
                log_queue.put(('log', f"❌ Error: {str(e)}"))
                log_queue.put(('result', {"success": False, "error": str(e)}))
            finally:
                log_queue.put(('done', True))

        t = threading.Thread(target=_run_steam, daemon=True)
        t.start()
        st.rerun()

    # Process queue messages (drain all available messages)
    if 'steam_log_queue' in st.session_state:
        try:
            while True:
                msg_type, msg_data = st.session_state.steam_log_queue.get_nowait()
                if msg_type == 'log':
                    st.session_state.steam_scraper_log.append(msg_data)
                elif msg_type == 'result':
                    st.session_state.steam_scraper_result = msg_data
                elif msg_type == 'done':
                    st.session_state.steam_scraper_running = False
        except queue.Empty:
            pass

    # Show live log while running
    if st.session_state.steam_scraper_running or st.session_state.steam_scraper_log:
        with steam_log_area.container():
            if st.session_state.steam_scraper_running:
                st.info("⏳ Steam scraper is running... this may take several minutes.")
                st.button("🔃 Check status", key="steam_check_status")
            log_text = "\n".join(st.session_state.steam_scraper_log[-30:])
            if log_text:
                st.code(log_text, language=None)

        result = st.session_state.steam_scraper_result
        if result is not None:
            if result["success"]:
                st.success(
                    f"✅ Steam scrape complete! {result['new_rows']} new games added "
                    f"({result['window_start']} → {result['window_end']})"
                )
                # Reload CSV into session state
                reload_steam_from_csv()
                df_steam = st.session_state.df_steam.copy()
                st.session_state.steam_scraper_result = None
                st.session_state.steam_scraper_log = []
            else:
                st.error(f"❌ Steam scrape failed: {result['error']}")
                st.session_state.steam_scraper_result = None

    st.divider()

    # ── Sidebar: Steam weights ────────────────────────────────────────────────
    st.sidebar.header("Steam Report Configuration")
    min_followers = st.sidebar.number_input("Min Followers", value=10000,
                                            help="Min followers considered for points")
    max_followers = st.sidebar.number_input("Max Followers", value=398955,
                                            help="Max followers considered for points")
    w_followers = st.sidebar.slider("Follower Weight", 0, 5, 5)
    w_developers = st.sidebar.slider("Developer Weight", 0, 5, 2)

    # ── Formula display ───────────────────────────────────────────────────────
    st.info("### Current Steam Formula")
    st.latex(r"1. Follower Points = (0.5 \times linear\_norm) + (0.5 \times log\_norm)")
    with st.expander("📐 View Linear & Log Details"):
        st.latex(r'linear\_norm = \frac{followers - min\_followers}{max\_followers - min\_followers} \times (5 - 1) + 1')
        st.latex(r'log\_norm = \frac{\log(followers) - \log(min\_followers)}{\log(max\_followers) - \log(min\_followers)} \times (5 - 1) + 1')
    st.latex(r"2. Developer Points = (Avg.of Developer Points)")
    st.latex(r"3. Final Priority Score = ((Follower Points * Follower Weight) + (Developer Points * Developer Weight)")
    st.caption("*_The above formula is based on points from the Developer List_")
    st.caption("**_If any developer is not in the Developer List, they are assigned a default point value of 1._")

    # ── Score calculations ────────────────────────────────────────────────────
    for index, row in df_steam.iterrows():
        df_steam.loc[index, 'Follower Points'] = calculate_follower_weighted_points(
            row['FollowerCount'], min_followers=min_followers, max_followers=max_followers
        )

    for index, row in df_steam.iterrows():
        points, _ = calculate_developer_weighted_points(row['Developers'])
        df_steam.loc[index, 'Developer Points'] = points

    df_steam['Weighted Follower Score'] = df_steam['Follower Points'] * w_followers
    df_steam['Weighted Dev Score']      = df_steam['Developer Points'] * w_developers
    df_steam['Final Priority Score']    = df_steam['Weighted Follower Score'] + df_steam['Weighted Dev Score']

    df_ranked = df_steam.sort_values('Final Priority Score', ascending=False, ignore_index=True)

    # ── FILTERS ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=True):
        f_col1, f_col2 = st.columns(2)

        with f_col1:
            st.markdown("**Release Date**")
            if 'ReleaseDate' in df_ranked.columns:
                df_ranked['ReleaseDate'] = pd.to_datetime(df_ranked['ReleaseDate'], errors='coerce', format='%d-%b-%y')
                valid_dates = df_ranked['ReleaseDate'].dropna()
                min_date = valid_dates.min().date() if len(valid_dates) else dt.date(2000, 1, 1)
                max_date = valid_dates.max().date() if len(valid_dates) else dt.date.today()
            else:
                min_date, max_date = dt.date(2000, 1, 1), dt.date.today()
            start_date = st.date_input("From", value=min_date, key="steam_start_date")
            end_date   = st.date_input("To",   value=max_date, key="steam_end_date")

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
            selected_genres = st.multiselect(
                "Select genres", options=all_genres,
                placeholder="All genres", key="steam_genres"
            )

        st.markdown("**Game Name Search**")
        steam_name_search = st.text_input(
            "Search by name", placeholder="Type to search…",
            key="steam_name_search"
        )

        # with f_col5:
        #     st.markdown("**Final Priority Score**")
        #     if 'Final Priority Score' in df_ranked.columns:
        #         ps_min = float(df_ranked['Final Priority Score'].min())
        #         ps_max = float(df_ranked['Final Priority Score'].max())
        #     else:
        #         ps_min, ps_max = 0.0, 100.0
        #     score_range = st.slider(
        #         "Score range",
        #         min_value=ps_min, max_value=ps_max,
        #         value=(ps_min, ps_max),
        #         key="steam_score_range"
        #     )

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_steam = df_ranked.copy()

    if 'ReleaseDate' in df_filtered_steam.columns:
        rd = pd.to_datetime(df_filtered_steam['ReleaseDate'], errors='coerce')
        df_filtered_steam = df_filtered_steam[rd.between(pd.Timestamp(start_date), pd.Timestamp(end_date))]

    # if 'FollowerCount' in df_filtered_steam.columns:
    #     df_filtered_steam = df_filtered_steam[
    #         df_filtered_steam['FollowerCount'].between(follower_range[0], follower_range[1])
    #     ]

    if selected_genres and 'Genres' in df_filtered_steam.columns:
        def has_genre(genres_val):
            genres_list = genres_val if isinstance(genres_val, list) else [g.strip() for g in str(genres_val).split(',')]
            return any(g in selected_genres for g in genres_list)
        df_filtered_steam = df_filtered_steam[df_filtered_steam['Genres'].apply(has_genre)]

    if steam_name_search and 'Name' in df_filtered_steam.columns:
        df_filtered_steam = df_filtered_steam[
            df_filtered_steam['Name'].str.contains(steam_name_search, case=False, na=False)
        ]

    # if 'Final Priority Score' in df_filtered_steam.columns:
    #     df_filtered_steam = df_filtered_steam[
    #         df_filtered_steam['Final Priority Score'].between(score_range[0], score_range[1])
    #     ]

    df_filtered_steam = df_filtered_steam.reset_index(drop=True)
    df_filtered_steam.index = df_filtered_steam.index + 1

    # ── Results ───────────────────────────────────────────────────────────────
    tab1, tab2 = st.tabs(["📊 Ranking Results", "🔍 Developer List"])

    with tab1:
        st.subheader("Top Priority Games")
        st.caption(f"Showing **{len(df_filtered_steam)}** of **{len(df_ranked)}** games")
        cols_to_show = ['Name', 'FollowerCount', 'Follower Points', 'Developers', 'Developer Points', 'Final Priority Score']
        df_display = df_filtered_steam[cols_to_show].copy()
        df_display['Developers'] = df_display['Developers'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else str(x)
        )
        st.dataframe(df_display, width='stretch')

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

    ns_info = get_last_run_info("non_steam")
    ns_col1, ns_col2, ns_col3 = st.columns([2, 2, 1])
    with ns_col1:
        st.metric("Last Scrape", _format_last_run(ns_info))
    with ns_col2:
        st.metric("Next Window", _format_next_window("non_steam"))
    with ns_col3:
        if st.button("🔄 Refresh Data", key="refresh_nonsteam_data", help="Reload Non-Steam data from CSV"):
            reload_nonsteam_from_csv()
            df_nonsteam = st.session_state.df_nonsteam.copy()
            st.success("✅ Non-Steam data refreshed!")
            st.rerun()
    
    # ── Scraper input fields ──────────────────────────────────────────────────
    st.markdown("### 🎬 Non-Steam Scraper Configuration")
    nscol1, nscol2, nscol3 = st.columns([2, 2, 1])
    
    with nscol1:
        default_ns_start, default_ns_end = get_next_window("non_steam", window_days=14)
        nonsteam_start_date = st.date_input(
            "Start Date",
            value=dt.datetime.strptime(default_ns_start, "%Y-%m-%d").date(),
            key="nonsteam_scraper_start_date",
            help="Start date for scraping (inclusive)"
        )
    
    with nscol2:
        nonsteam_end_date = st.date_input(
            "End Date",
            value=dt.datetime.strptime(default_ns_end, "%Y-%m-%d").date(),
            key="nonsteam_scraper_end_date",
            help="End date for scraping (inclusive)"
        )
    
    with nscol3:
        st.write("")  # Spacing
        st.write("")  # Spacing
        run_nonsteam = st.button(
            "▶️ Run Scraper",
            key="run_nonsteam_scraper",
            disabled=st.session_state.nonsteam_scraper_running,
            help="Scrapes IGDB/YouTube for non-Steam games in the specified date range",
        )

    # Non-steam scraper log area
    nonsteam_log_area = st.empty()

    if run_nonsteam and not st.session_state.nonsteam_scraper_running:
        st.session_state.nonsteam_scraper_running = True
        st.session_state.nonsteam_scraper_log = ["⏳ Starting Non-Steam scraper..."]
        st.session_state.nonsteam_scraper_result = None

        # Create thread-safe queue for logging
        if 'nonsteam_log_queue' not in st.session_state:
            st.session_state.nonsteam_log_queue = queue.Queue()
        
        log_queue = st.session_state.nonsteam_log_queue

        def _ns_log(msg):
            # Thread-safe: put message in queue instead of accessing session_state
            log_queue.put(('log', msg))

        def _run_nonsteam():
            try:
                # Pass the date parameters from the UI
                result = run_nonsteam_scraper(
                    start_date=nonsteam_start_date.strftime("%Y-%m-%d"),
                    end_date=nonsteam_end_date.strftime("%Y-%m-%d"),
                    status_callback=_ns_log
                )
                log_queue.put(('result', result))
            except Exception as e:
                log_queue.put(('log', f"❌ Error: {str(e)}"))
                log_queue.put(('result', {"success": False, "error": str(e)}))
            finally:
                log_queue.put(('done', True))

        t2 = threading.Thread(target=_run_nonsteam, daemon=True)
        t2.start()
        st.rerun()

    # Process queue messages (drain all available messages)
    if 'nonsteam_log_queue' in st.session_state:
        try:
            while True:
                msg_type, msg_data = st.session_state.nonsteam_log_queue.get_nowait()
                if msg_type == 'log':
                    st.session_state.nonsteam_scraper_log.append(msg_data)
                elif msg_type == 'result':
                    st.session_state.nonsteam_scraper_result = msg_data
                elif msg_type == 'done':
                    st.session_state.nonsteam_scraper_running = False
        except queue.Empty:
            pass

    if st.session_state.nonsteam_scraper_running or st.session_state.nonsteam_scraper_log:
        with nonsteam_log_area.container():
            if st.session_state.nonsteam_scraper_running:
                st.info("⏳ Non-Steam scraper is running... this may take several minutes.")
                st.button("🔃 Check status", key="nonsteam_check_status")
            log_text = "\n".join(st.session_state.nonsteam_scraper_log[-30:])
            if log_text:
                st.code(log_text, language=None)

        result = st.session_state.nonsteam_scraper_result
        if result is not None:
            if result["success"]:
                st.success(
                    f"✅ Non-Steam scrape complete! {result['new_rows']} new games added "
                    f"({result['window_start']} → {result['window_end']})"
                )
                reload_nonsteam_from_csv()
                df_nonsteam = st.session_state.df_nonsteam.copy()
                st.session_state.nonsteam_scraper_result = None
                st.session_state.nonsteam_scraper_log = []
            else:
                st.error(f"❌ Non-Steam scrape failed: {result['error']}")
                st.session_state.nonsteam_scraper_result = None

    st.divider()

    # ── Sidebar config ────────────────────────────────────────────────────────
    st.sidebar.header("Non-Steam Report Configuration")
    st.info("The Non-Steam ranking is based on YouTube Views adjusted for the time since release. Games with higher adjusted views are prioritised.")

    df_nonsteam_filter = df_nonsteam[df_nonsteam['SteamStatus'] != 'PC Game (on Steam)'].copy()
    df_nonsteam_filter['YouTube ReleaseDate'] = pd.to_datetime(
        df_nonsteam_filter['YouTube ReleaseDate'], errors='coerce'
    )

    # ── Formula display ───────────────────────────────────────────────────────
    st.info("### Current Non-Steam Formula")
    st.latex(r"1. Days since Release = (Today's Date - Release Date)")
    st.latex(r"2. Adjusted Views = YouTube Views / (1 + (Days Since Release / 365))")
    st.latex(r"Games with the highest Adjusted Views are ranked highest.")

    # ── Score calculation ─────────────────────────────────────────────────────
    today = dt.date.today()
    df_nonsteam_filter['Days_Since_Release'] = (
        pd.to_datetime(today) - df_nonsteam_filter['YouTube ReleaseDate']
    ).dt.days
    df_nonsteam_filter['adjusted_views'] = (
        df_nonsteam_filter['YouTube Views'] / (1 + df_nonsteam_filter['Days_Since_Release'] / 365)
    )

    df_non_steam_ranked = df_nonsteam_filter.sort_values('adjusted_views', ascending=False, ignore_index=True)

    # ── FILTERS ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=True):
        nf_col1, nf_col2 = st.columns(2)

        with nf_col1:
            st.markdown("**YouTube Release Date**")
            valid_yt = df_non_steam_ranked['YouTube ReleaseDate'].dropna()
            ns_min = valid_yt.min().date() if len(valid_yt) else dt.date(2000, 1, 1)
            ns_max = valid_yt.max().date() if len(valid_yt) else dt.date.today()
            ns_start = st.date_input("From", value=ns_min, key="ns_start_date")
            ns_end   = st.date_input("To",   value=ns_max, key="ns_end_date")

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
            selected_platforms = st.multiselect(
                "Select platforms", options=all_platforms,
                placeholder="All platforms", key="ns_platforms"
            )

        nf_col4 = st.columns(1)[0]
        with nf_col4:
            st.markdown("**Console or PC**")
            if 'SteamStatus' in df_non_steam_ranked.columns:
                steam_statuses = sorted(df_non_steam_ranked['SteamStatus'].dropna().unique().tolist())
            else:
                steam_statuses = []
            selected_statuses = st.multiselect(
                "Select status", options=steam_statuses,
                placeholder="All statuses", key="ns_steam_status"
            )

        apply_ns = st.button("Apply filters", key="ns_apply")

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_ns = df_non_steam_ranked.copy()
    if apply_ns:
        if 'YouTube ReleaseDate' in df_filtered_ns.columns:
            yt_dates = pd.to_datetime(df_filtered_ns['YouTube ReleaseDate'], errors='coerce')
            df_filtered_ns = df_filtered_ns[yt_dates.between(pd.Timestamp(ns_start), pd.Timestamp(ns_end))]

        if selected_platforms and 'Platforms' in df_filtered_ns.columns:
            def has_platform(val):
                plats = val if isinstance(val, list) else [p.strip() for p in str(val).split(',')]
                return any(p in selected_platforms for p in plats)
            df_filtered_ns = df_filtered_ns[df_filtered_ns['Platforms'].apply(has_platform)]

        if selected_statuses and 'SteamStatus' in df_filtered_ns.columns:
            df_filtered_ns = df_filtered_ns[df_filtered_ns['SteamStatus'].isin(selected_statuses)]

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
    cols_to_show = [c for c in cols_to_show if c in df_filtered_ns.columns]
    df_nonsteam_display = df_filtered_ns[cols_to_show].copy()

    def format_list_column(value):
        if isinstance(value, list):
            return ', '.join(str(v).strip() for v in value if v and str(v).strip())
        return str(value)

    for col in ['Developers', 'Genres']:
        if col in df_nonsteam_display.columns:
            df_nonsteam_display[col] = df_nonsteam_display[col].apply(format_list_column)

    st.dataframe(df_nonsteam_display, width='stretch')


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — GAME INVENTORY
# ══════════════════════════════════════════════════════════════════════════════
with tab_inventory:
    if "game_data" not in st.session_state:
        st.session_state.game_data = pd.read_csv(INVENTORY_FILE)

    st.header("🎮 Game Tracker")
    st.subheader("Game Library")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Refresh Inventory"):
            st.session_state.game_data = pd.read_csv(INVENTORY_FILE)
            st.rerun()

    # ── FILTERS ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=True):
        inv_f1, inv_f2, inv_f3 = st.columns(3)

        with inv_f1:
            st.markdown("**Date Purchased**")
            gd = st.session_state.game_data.copy()
            gd['Date Purchased'] = pd.to_datetime(gd['Date Purchased'], errors='coerce', format='%Y-%m-%d')
            valid_inv = gd['Date Purchased'].dropna()
            inv_min = valid_inv.min().date() if len(valid_inv) else dt.date(2000, 1, 1)
            inv_max = valid_inv.max().date() if len(valid_inv) else dt.date.today()
            inv_start = st.date_input("From", value=inv_min, key="inv_start_date")
            inv_end   = st.date_input("To",   value=inv_max, key="inv_end_date")

        with inv_f2:
            st.markdown("**Platform**")
            if 'Platform' in gd.columns:
                inv_platforms = sorted(gd['Platform'].dropna().unique().tolist())
            else:
                inv_platforms = []
            selected_inv_platforms = st.multiselect(
                "Select platform", options=inv_platforms,
                placeholder="All platforms", key="inv_platforms"
            )

        with inv_f3:
            st.markdown("**Game Name Search**")
            inv_name_search = st.text_input(
                "Search by name", placeholder="Type to search…",
                key="inv_name_search"
            )

        inv_f4, inv_f5, inv_f6, inv_f7 = st.columns(4)

        with inv_f4:
            st.markdown("**Active**")
            inv_active = st.selectbox("Active", ["All", "Yes", "No"], key="inv_active")
        with inv_f5:
            st.markdown("**On Hold**")
            inv_on_hold = st.selectbox("On Hold", ["All", "Yes", "No"], key="inv_on_hold")
        with inv_f6:
            st.markdown("**Reviewed**")
            inv_reviewed = st.selectbox("Reviewed", ["All", "Yes", "No"], key="inv_reviewed")
        with inv_f7:
            st.markdown("**Inactive**")
            inv_inactive = st.selectbox("Inactive", ["All", "Yes", "No"], key="inv_inactive")

    # ── Apply filters ─────────────────────────────────────────────────────────
    filtered = st.session_state.game_data.copy()
    filtered['Date Purchased'] = pd.to_datetime(filtered['Date Purchased'], errors='coerce', format='%Y-%m-%d')
    filtered = filtered[filtered['Date Purchased'].between(pd.Timestamp(inv_start), pd.Timestamp(inv_end))]

    if selected_inv_platforms and 'Platform' in filtered.columns:
        filtered = filtered[filtered['Platform'].isin(selected_inv_platforms)]

    if inv_name_search and 'Game Name' in filtered.columns:
        filtered = filtered[
            filtered['Game Name'].str.contains(inv_name_search, case=False, na=False)
        ]

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