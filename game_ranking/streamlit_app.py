import streamlit as st
import pandas as pd
import numpy as np
import io
import datetime as dt
from src.calculation.process_data import clean_dev_genre_list, flagging, calculate_developer_weighted_points, load_data, calculate_follower_weighted_points, calculate_developer_weighted_points, handle_change
from src.calculation.scraper import scrape_google_trends
from config import CSV_STEAM, CSV_NON_STEAM, INVENTORY_FILE

# Page Config
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
        _, _, st.session_state.dev_list, st.session_state.genre_list = load_data(CSV_STEAM, CSV_NON_STEAM)
    except:
        pass

# Helper: load defaults into session state
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

def refresh_steam_data():
    if st.session_state.uploaded_steam_bytes:
        try:
            tmp = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
            tmp = clean_dev_genre_list(tmp)
            tmp = flagging(tmp)
            st.session_state.df_steam = tmp
            st.sidebar.success("✅ Steam data refreshed")
        except Exception as e:
            st.sidebar.error(f"Error refreshing steam data: {e}")
    else:
        load_defaults()
    st.experimental_rerun()


def refresh_nonsteam_data():
    if st.session_state.uploaded_nonsteam_bytes:
        try:
            tmp = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
            st.session_state.df_nonsteam = tmp
            st.sidebar.success("✅ Non‑Steam data refreshed")
        except Exception as e:
            st.sidebar.error(f"Error refreshing non‑steam data: {e}")
    else:
        load_defaults()
    st.experimental_rerun()


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
                steam_df_upload = clean_dev_genre_list(steam_df_upload)
                steam_df_upload = flagging(steam_df_upload)
                st.session_state.df_steam = steam_df_upload
                st.session_state.steam_source = st.session_state.uploaded_steam_name
                st.session_state.steam_cleaned = True
                st.sidebar.success(f"✅ Loaded {st.session_state.uploaded_steam_name}")
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
                st.session_state.df_nonsteam = nonsteam_df_upload
                st.session_state.nonsteam_source = st.session_state.uploaded_nonsteam_name
                st.session_state.nonsteam_cleaned = True
                st.sidebar.success(f"✅ Loaded {st.session_state.uploaded_nonsteam_name}")
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

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Refresh Steam Data"):
            refresh_steam_data()

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

        # --- Date range ---
        with f_col1:
            st.markdown("**Release Date**")
            if 'ReleaseDate' in df_ranked.columns:
                df_ranked['ReleaseDate'] = pd.to_datetime(df_ranked['ReleaseDate'], errors='coerce')
                valid_dates = df_ranked['ReleaseDate'].dropna()
                min_date = valid_dates.min().date() if len(valid_dates) else dt.date(2000, 1, 1)
                max_date = valid_dates.max().date() if len(valid_dates) else dt.date.today()
            else:
                min_date, max_date = dt.date(2000, 1, 1), dt.date.today()
            start_date = st.date_input("From", value=min_date, key="steam_start_date")
            end_date   = st.date_input("To",   value=max_date, key="steam_end_date")

        # --- Follower count range ---
        # with f_col2:
        #     st.markdown("**Follower Count**")
        #     if 'FollowerCount' in df_ranked.columns:
        #         fc_min = int(df_ranked['FollowerCount'].min())
        #         fc_max = int(df_ranked['FollowerCount'].max())
        #     else:
        #         fc_min, fc_max = 0, 1_000_000
        #     follower_range = st.slider(
        #         "Follower range",
        #         min_value=fc_min, max_value=fc_max,
        #         value=(fc_min, fc_max),
        #         key="steam_follower_range",
        #         format="%d"
        #     )

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
            selected_genres = st.multiselect(
                "Select genres", options=all_genres,
                placeholder="All genres", key="steam_genres"
            )

        f_col4, f_col5 = st.columns(2)

        # --- Name search ---
        with f_col4:
            st.markdown("**Game Name Search**")
            steam_name_search = st.text_input(
                "Search by name", placeholder="Type to search…",
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
            score_range = st.slider(
                "Score range",
                min_value=ps_min, max_value=ps_max,
                value=(ps_min, ps_max),
                key="steam_score_range"
            )

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_steam = df_ranked.copy()

    # Date
    if 'ReleaseDate' in df_filtered_steam.columns:
        rd = pd.to_datetime(df_filtered_steam['ReleaseDate'], errors='coerce')
        df_filtered_steam = df_filtered_steam[rd.between(pd.Timestamp(start_date), pd.Timestamp(end_date))]

    # Followers
    # if 'FollowerCount' in df_filtered_steam.columns:
    #     df_filtered_steam = df_filtered_steam[
    #         df_filtered_steam['FollowerCount'].between(follower_range[0], follower_range[1])
    #     ]

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

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Refresh Non-Steam Data"):
            refresh_nonsteam_data()

    # ── Sidebar config ────────────────────────────────────────────────────────
    st.sidebar.header("Non-Steam Report Configuration")
    st.info("The Non-Steam ranking is based on YouTube Views adjusted for the time since release. Games with higher adjusted views are prioritised.")

    # Pre-processing: remove Steam games, parse dates
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

        #--- YouTube Release Date ---
        with nf_col1:
            st.markdown("**YouTube Release Date**")
            valid_yt = df_non_steam_ranked['YouTube ReleaseDate'].dropna()
            ns_min = valid_yt.min().date() if len(valid_yt) else dt.date(2000, 1, 1)
            ns_max = valid_yt.max().date() if len(valid_yt) else dt.date.today()
            ns_start = st.date_input("From", value=ns_min, key="ns_start_date")
            ns_end   = st.date_input("To",   value=ns_max, key="ns_end_date")

        # --- YouTube Views range ---
        # with nf_col2:
        #     st.markdown("**YouTube Views**")
        #     if 'YouTube Views' in df_non_steam_ranked.columns:
        #         yv_min = int(df_non_steam_ranked['YouTube Views'].min())
        #         yv_max = int(df_non_steam_ranked['YouTube Views'].max())
        #     else:
        #         yv_min, yv_max = 0, 10_000_000
        #     yt_views_range = st.slider(
        #         "Views range",
        #         min_value=yv_min, max_value=yv_max,
        #         value=(yv_min, yv_max),
        #         key="ns_views_range",
        #         format="%d"
        #     )

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
            selected_platforms = st.multiselect(
                "Select platforms", options=all_platforms,
                placeholder="All platforms", key="ns_platforms"
            )

        nf_col4 = st.columns(1)[0]
        #         "Search by title", placeholder="Type to search…",
        #         key="ns_title_search"
        #     )

        # --- SteamStatus filter ---
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

        # apply button
        apply_ns = st.button("Apply filters", key="ns_apply")

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_ns = df_non_steam_ranked.copy()
    if apply_ns:
        # Date
        if 'YouTube ReleaseDate' in df_filtered_ns.columns:
            yt_dates = pd.to_datetime(df_filtered_ns['YouTube ReleaseDate'], errors='coerce')
            df_filtered_ns = df_filtered_ns[yt_dates.between(pd.Timestamp(ns_start), pd.Timestamp(ns_end))]

        # # YouTube Views
        # if 'YouTube Views' in df_filtered_ns.columns:
        #     df_filtered_ns = df_filtered_ns[
        #         df_filtered_ns['YouTube Views'].between(yt_views_range[0], yt_views_range[1])
        #     ]

        # Platforms
        if selected_platforms and 'Platforms' in df_filtered_ns.columns:
            def has_platform(val):
                plats = val if isinstance(val, list) else [p.strip() for p in str(val).split(',')]
                return any(p in selected_platforms for p in plats)
            df_filtered_ns = df_filtered_ns[df_filtered_ns['Platforms'].apply(has_platform)]

        # Title search
        # if ns_title_search and 'Game Title' in df_filtered_ns.columns:
        #     df_filtered_ns = df_filtered_ns[
        #         df_filtered_ns['Game Title'].str.contains(ns_title_search, case=False, na=False)
        #     ]

        # Steam Status
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
            st.experimental_rerun()

    # ── FILTERS ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=True):
        inv_f1, inv_f2, inv_f3 = st.columns(3)

        # --- Purchase date ---
        with inv_f1:
            st.markdown("**Date Purchased**")
            gd = st.session_state.game_data.copy()
            gd['Date Purchased'] = pd.to_datetime(gd['Date Purchased'], errors='coerce', format='%Y-%m-%d')
            valid_inv = gd['Date Purchased'].dropna()
            inv_min = valid_inv.min().date() if len(valid_inv) else dt.date(2000, 1, 1)
            inv_max = valid_inv.max().date() if len(valid_inv) else dt.date.today()
            inv_start = st.date_input("From", value=inv_min, key="inv_start_date")
            inv_end   = st.date_input("To",   value=inv_max, key="inv_end_date")

        # --- Platform multi-select ---
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

        # --- Game name search ---
        with inv_f3:
            st.markdown("**Game Name Search**")
            inv_name_search = st.text_input(
                "Search by name", placeholder="Type to search…",
                key="inv_name_search"
            )

        inv_f4, inv_f5, inv_f6, inv_f7 = st.columns(4)

        # --- Status checkboxes ---
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