"""
Steam Report tab.
"""

import datetime as dt
import time
# import threading  # scraper disabled

import pandas as pd
import streamlit as st

# from app.thread_state import _steam_thread_state  # scraper disabled
from app.helpers import (highlight_new_rows, reload_steam_from_csv,
                         filter_stale_trends_games, load_trends_cache_timestamps)
from calculation.process_data import (
    calculate_hybrid_score,
    calculate_developer_weighted_points,
    calculate_trends_weighted_points,
    calculate_google_trends_points,
)
from calculation.steam_players import parse_owners_midpoint, load_appid_cache
from config import TRENDS_CACHE_FILE
# from pipelines.steam_pipeline import run_steam_scraper  # scraper disabled


def _sync_from_steam_dates():
    st.session_state.ns_start_date  = st.session_state.steam_start_date
    st.session_state.ns_end_date    = st.session_state.steam_end_date
    st.session_state.inv_start_date = st.session_state.steam_start_date
    st.session_state.inv_end_date   = st.session_state.steam_end_date


def render(global_date_min: dt.date, global_date_max: dt.date):
    df_steam = st.session_state.df_steam.copy()
    steam_source_name = st.session_state.get("steam_source", "default file")

    # ── Sidebar: Steam weights ────────────────────────────────────────────────
    st.sidebar.header("Steam Scoring")
    max_followers = st.sidebar.number_input(
        "Max Followers (cap)", value=398955,
        help="Follower counts above this are treated as the maximum for scoring purposes."
    )
    w_followers  = st.sidebar.slider("Follower Weight",  0, 5, 5)
    w_developers = st.sidebar.slider("Developer Weight", 0, 5, 2)
    w_trends     = st.sidebar.slider("Trends Weight",    0, 5, 2, key="steam_w_trends")

    # ── Score calculations ────────────────────────────────────────────────────
    for index, row in df_steam.iterrows():
        df_steam.loc[index, 'Follower Points'] = calculate_hybrid_score(
            row['FollowerCount'], min_value=1000, max_value=max_followers
        )

    for index, row in df_steam.iterrows():
        points, _ = calculate_developer_weighted_points(row['Developers'])
        df_steam.loc[index, 'Developer Points'] = points

    df_steam['Follower Points']         = df_steam['Follower Points'].round(2)
    df_steam['Developer Points']        = df_steam['Developer Points'].round(2)
    df_steam['trends_score']            = (
        df_steam['Name'].map(st.session_state.nonsteam_trends).fillna(0).astype(int)
    )
    df_steam['trends_points']           = df_steam['trends_score'].apply(
        calculate_trends_weighted_points
    ).round(2)
    df_steam['Weighted Follower Score'] = df_steam['Follower Points'] * w_followers
    df_steam['Weighted Dev Score']      = df_steam['Developer Points'] * w_developers
    df_steam['Weighted Trends Score']   = df_steam['trends_points'] * w_trends
    df_steam['Final Priority Score']    = (
        df_steam['Weighted Follower Score'] +
        df_steam['Weighted Dev Score'] +
        df_steam['Weighted Trends Score']
    ).round(2)

    df_ranked = df_steam.sort_values('Final Priority Score', ascending=False, ignore_index=True)

    if "steam_reset_filters" not in st.session_state:
        st.session_state.steam_reset_filters = False

    # ── Summary metrics ───────────────────────────────────────────────────────
    _today_str = dt.date.today().isoformat()
    _new_today = int(
        df_ranked['date_appended'].astype(str).str.startswith(_today_str).sum()
    ) if 'date_appended' in df_ranked.columns else 0
    _top_score = df_ranked['Final Priority Score'].max() if len(df_ranked) else 0
    _unknown_devs = int(
        (df_ranked['Developer Points'] == 1.0).sum()
    ) if 'Developer Points' in df_ranked.columns else 0
    _trends_cached = int(
        (df_ranked['trends_score'] > 0).sum()
    ) if 'trends_score' in df_ranked.columns else 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Games", len(df_ranked), border=True)
    m2.metric("Top Priority Score", f"{_top_score:.1f}", border=True)
    m3.metric("New Today", _new_today, border=True)
    m4.metric("Unknown Devs", _unknown_devs, help="Developers not in the dev list — scored as 1 by default", border=True)
    m5.metric("Trends Cached", _trends_cached, help="Games with a cached Google Trends score", border=True)

    st.divider()

    # ── Filters ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=False):
        f_col1, f_col2, f_col3 = st.columns(3)

        with f_col1:
            st.markdown("**Release Date**")
            if 'ReleaseDate' in df_ranked.columns:
                df_ranked['ReleaseDate'] = pd.to_datetime(df_ranked['ReleaseDate'], errors='coerce', dayfirst=True)
            start_date = st.date_input(
                "From", value=st.session_state.get("steam_start_date", global_date_min),
                min_value=global_date_min, max_value=global_date_max,
                key="steam_start_date", format="DD/MM/YYYY", on_change=_sync_from_steam_dates
            )
            end_date = st.date_input(
                "To", value=st.session_state.get("steam_end_date", global_date_max),
                min_value=global_date_min, max_value=global_date_max,
                key="steam_end_date", format="DD/MM/YYYY", on_change=_sync_from_steam_dates
            )

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
                "Select genres", options=all_genres, default=default_genres,
                placeholder="All genres", key="steam_genres"
            )

            st.markdown("**Game Name**")
            default_search = "" if st.session_state.steam_reset_filters else st.session_state.get("steam_name_search", "")
            steam_name_search = st.text_input(
                "Search by name", value=default_search,
                placeholder="Type to search…", key="steam_name_search"
            )

        with f_col3:
            st.markdown("**Priority Score Range**")
            if 'Final Priority Score' in df_ranked.columns:
                ps_min = float(df_ranked['Final Priority Score'].min())
                ps_max = float(df_ranked['Final Priority Score'].max())
            else:
                ps_min, ps_max = 0.0, 100.0
            default_score = (ps_min, ps_max) if st.session_state.steam_reset_filters else st.session_state.get("steam_score_range", (ps_min, ps_max))
            score_range = st.slider(
                "Score range", min_value=ps_min, max_value=ps_max,
                value=default_score, key="steam_score_range"
            )

            st.markdown("**Max Followers**")
            FOLLOWER_SLIDER_MAX = int(df_ranked['FollowerCount'].max()) if 'FollowerCount' in df_ranked.columns and not df_ranked['FollowerCount'].isna().all() else 500000
            default_fc_max = FOLLOWER_SLIDER_MAX if st.session_state.steam_reset_filters else st.session_state.get("steam_follower_max", FOLLOWER_SLIDER_MAX)
            follower_max = st.slider(
                "Show games up to", min_value=0, max_value=FOLLOWER_SLIDER_MAX,
                value=default_fc_max, step=1000, key="steam_follower_max"
            )

        btn_c1, btn_c2 = st.columns([1, 1])
        with btn_c1:
            apply_steam = st.button("Apply Filters", key="steam_apply", use_container_width=True)
        with btn_c2:
            if st.button("Reset Filters", key="steam_revert", use_container_width=True):
                st.session_state.steam_reset_filters = True
                st.rerun()

    if st.session_state.steam_reset_filters:
        st.session_state.steam_reset_filters = False

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_steam = df_ranked.copy()

    if 'ReleaseDate' in df_filtered_steam.columns:
        rd = pd.to_datetime(df_filtered_steam['ReleaseDate'], errors='coerce', dayfirst=True)
        df_filtered_steam = df_filtered_steam[rd.between(pd.Timestamp(start_date), pd.Timestamp(end_date)) | rd.isna()]

    if selected_genres and 'Genres' in df_filtered_steam.columns:
        def has_genre(genres_val):
            genres_list = genres_val if isinstance(genres_val, list) else [g.strip() for g in str(genres_val).split(',')]
            return any(g in selected_genres for g in genres_list)
        df_filtered_steam = df_filtered_steam[df_filtered_steam['Genres'].apply(has_genre)]

    if steam_name_search and 'Name' in df_filtered_steam.columns:
        df_filtered_steam = df_filtered_steam[
            df_filtered_steam['Name'].str.contains(steam_name_search, case=False, na=False)
        ]

    if 'Final Priority Score' in df_filtered_steam.columns:
        df_filtered_steam = df_filtered_steam[
            df_filtered_steam['Final Priority Score'].between(score_range[0], score_range[1])
        ]

    if 'FollowerCount' in df_filtered_steam.columns:
        df_filtered_steam = df_filtered_steam[
            df_filtered_steam['FollowerCount'].fillna(0).between(0, follower_max)
        ]

    df_filtered_steam = df_filtered_steam.reset_index(drop=True)
    df_filtered_steam.index = df_filtered_steam.index + 1

    # ── Results tabs ──────────────────────────────────────────────────────────
    results_tab, devlist_tab = st.tabs(["📊 Ranking Results", "🔍 Developer List"])

    with results_tab:
        tbl_col, btn_col, meta_col = st.columns([4, 1, 1])
        with tbl_col:
            st.subheader("Priority Rankings")
        with btn_col:
            if st.button("📊 Refresh Trends", key="fetch_steam_trends",
                         help="Fetch Google Trends scores for filtered games", use_container_width=True):
                games = df_filtered_steam["Name"].dropna().unique().tolist()
                _cached_ts = load_trends_cache_timestamps(TRENDS_CACHE_FILE)
                games_to_fetch = filter_stale_trends_games(games, _cached_ts)
                if not games_to_fetch:
                    st.toast("All trends data is fresh (< 24 h)", icon="✅")
                else:
                    bar = st.progress(0, text="Starting…")
                    _refresh_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    for i, game in enumerate(games_to_fetch):
                        try:
                            score = calculate_google_trends_points(game)
                            st.session_state.nonsteam_trends[game] = int(score) if isinstance(score, (int, float)) else 0
                        except Exception as e:
                            st.session_state.nonsteam_trends[game] = 0
                            st.error(f"Trends fetch failed for '{game}': {e}")
                        bar.progress((i + 1) / len(games_to_fetch), text=f"{i+1}/{len(games_to_fetch)}: {game}")
                        try:
                            pd.DataFrame([
                                {"game_name": k, "trends_score": v,
                                 "fetched_at": _refresh_ts if k in games_to_fetch else _cached_ts.get(k, _refresh_ts)}
                                for k, v in st.session_state.nonsteam_trends.items()
                            ]).to_csv(TRENDS_CACHE_FILE, index=False)
                        except Exception as e:
                            st.error(f"Cache write failed: {e}")
                        time.sleep(1.5)
                    st.session_state.trends_last_fetched_at = _refresh_ts
                    st.toast(f"Updated {len(games_to_fetch)} game(s)", icon="📊")
            _ts = st.session_state.get("trends_last_fetched_at")
            if _ts:
                try:
                    _dt = dt.datetime.strptime(_ts, "%Y-%m-%d %H:%M:%S")
                    st.caption(f"Last fetched: {_dt.strftime('%d %b %Y, %H:%M')}")
                except Exception:
                    st.caption(f"Last fetched: {_ts}")
            else:
                st.caption("Never fetched")
        with meta_col:
            st.caption(f"Showing **{len(df_filtered_steam)}** of **{len(df_ranked)}**")
            st.caption(f"Source: *{steam_source_name}*")

        cols_to_show = [
            'Name', 'ReleaseDate', 'FollowerCount', 'Follower Points',
            'Developers', 'Developer Points', 'trends_score', 'trends_points',
            'Final Priority Score',
        ]
        display_cols = cols_to_show + (["date_appended"] if "date_appended" in df_filtered_steam.columns else [])
        df_display = df_filtered_steam[display_cols].copy()

        for col in ['Follower Points', 'Developer Points', 'trends_score', 'trends_points', 'Final Priority Score']:
            if col in df_display.columns:
                df_display[col] = df_display[col].round(2)

        _parsed_dates = pd.to_datetime(df_display['ReleaseDate'], errors='coerce', dayfirst=True)
        df_display['ReleaseDate'] = _parsed_dates.dt.strftime('%d/%m/%Y').fillna('Unknown')
        df_display['Developers'] = df_display['Developers'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else str(x)
        )

        df_display = df_display.rename(columns={
            'FollowerCount':        'Followers',
            'Follower Points':      'Follower Score',
            'Developer Points':     'Dev Score',
            'trends_score':         'Trends Score (raw)',
            'trends_points':        'Trends Points',
            'Final Priority Score': 'Priority Score',
            'ReleaseDate':          'Release Date',
        })

        st.dataframe(
            highlight_new_rows(df_display),
            use_container_width=True,
            column_config={
                'Follower Score':     st.column_config.NumberColumn(format="%.2f"),
                'Dev Score':         st.column_config.NumberColumn(format="%.2f"),
                'Trends Score (raw)':st.column_config.NumberColumn(format="%d"),
                'Trends Points':     st.column_config.NumberColumn(format="%.2f"),
                'Priority Score':    st.column_config.NumberColumn(format="%.2f"),
                'Followers':         st.column_config.NumberColumn(format="%d"),
            },
        )

        with st.expander("📐 How scores are calculated"):
            st.latex(r"Priority Score = (Follower Score \times w_{followers}) + (Dev Score \times w_{dev}) + (Trends Points \times w_{trends})")
            st.latex(r"Follower Score = 0.5 \times linear\_norm + 0.5 \times log\_norm \quad \in [1, 5]")
            st.latex(r"Trends Points = \frac{Trends Score}{100} \times 4 + 1 \quad \in [1, 5]")
            st.caption("Developer Score is the average score from the Developer List. Unknown developers default to 1.")

    with devlist_tab:
        st.caption("Internal developer ranking based on average revenue per game.")
        st.dataframe(st.session_state.dev_list, use_container_width=True, hide_index=True)

        with st.expander("📊 SteamSpy vs VG Insights — Revenue Comparison"):
            dev_list_df = st.session_state.dev_list
            if "Average Revenue Per Game" not in dev_list_df.columns:
                st.warning("Developer list is missing 'Average Revenue Per Game' column.")
            else:
                cache = load_appid_cache()

                # Collect games that have been fetched with revenue data
                spy_rows = []
                for game_name, entry in cache.items():
                    if "initialprice_cents" not in entry:
                        continue
                    owners_str = entry.get("owners_range") or entry.get("owners", "")
                    initialprice = entry.get("initialprice_cents", 0)
                    if not initialprice:
                        continue  # F2P — skip
                    midpoint = parse_owners_midpoint(owners_str)
                    if midpoint == 0:
                        continue
                    spy_rows.append({
                        "game_name": game_name,
                        "spy_revenue": midpoint * (initialprice / 100),
                    })

                if not spy_rows:
                    st.info(
                        "No data yet — run **🔄 Fetch Player Counts** in the Inventory tab "
                        "to populate SteamSpy revenue estimates."
                    )
                else:
                    spy_df = pd.DataFrame(spy_rows)

                    # Map game → developers using df_steam (already in scope)
                    steam_devs = df_steam[["Name", "Developers"]].copy()
                    steam_devs["Developers"] = steam_devs["Developers"].apply(
                        lambda x: x if isinstance(x, list)
                        else [d.strip() for d in str(x).split(",")]
                        if pd.notna(x) else []
                    )
                    merged = spy_df.merge(steam_devs, left_on="game_name", right_on="Name", how="inner")
                    merged = merged.explode("Developers")
                    merged = merged[merged["Developers"].str.strip() != ""]

                    # Average revenue per developer
                    dev_spy = (
                        merged.groupby("Developers")
                        .agg(spy_revenue=("spy_revenue", "mean"), games_used=("game_name", "count"))
                        .reset_index()
                    )

                    # Join with VGI data (case-insensitive)
                    vgi = dev_list_df[["Developer Name", "Average Revenue Per Game"]].copy()
                    vgi["_key"] = vgi["Developer Name"].str.strip().str.lower()
                    dev_spy["_key"] = dev_spy["Developers"].str.strip().str.lower()

                    comparison = dev_spy.merge(vgi, on="_key", how="inner").drop(columns=["_key"])
                    comparison = comparison.rename(columns={
                        "Developers":               "Developer",
                        "spy_revenue":              "SteamSpy Revenue",
                        "games_used":               "Games Used",
                        "Average Revenue Per Game": "VGI Revenue",
                    })
                    comparison["Ratio (VGI/Spy)"] = (
                        comparison["VGI Revenue"] / comparison["SteamSpy Revenue"]
                    ).round(2)
                    comparison = comparison.sort_values("VGI Revenue", ascending=False)
                    comparison = comparison[["Developer", "VGI Revenue", "SteamSpy Revenue", "Ratio (VGI/Spy)", "Games Used"]]

                    st.caption(
                        f"{len(comparison)} developer(s) with cached SteamSpy data. "
                        "F2P games excluded from estimates."
                    )
                    st.dataframe(
                        comparison,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "VGI Revenue":     st.column_config.NumberColumn("VGI Revenue ($)", format="$%,.0f"),
                            "SteamSpy Revenue": st.column_config.NumberColumn("SteamSpy Revenue ($)", format="$%,.0f"),
                            "Ratio (VGI/Spy)": st.column_config.NumberColumn(format="%.2f×"),
                            "Games Used":      st.column_config.NumberColumn(format="%d"),
                        },
                    )
