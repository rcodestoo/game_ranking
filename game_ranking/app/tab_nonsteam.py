"""
Non-Steam Report tab.
"""

import datetime as dt
# import threading   # scraper disabled
import time

import pandas as pd
import streamlit as st

# from app.thread_state import _ns_thread_state, _ns_verify_thread_state  # scraper disabled
from app.helpers import (highlight_new_rows, reload_nonsteam_from_csv,
                         filter_stale_trends_games, load_trends_cache_timestamps)
from calculation.process_data import calculate_google_trends_points, calculate_hybrid_score, calculate_trends_weighted_points
from config import TRENDS_CACHE_FILE  # get_latest_nonsteam_csv unused (scraper disabled)
# from pipelines.nonsteam_pipeline import run_nonsteam_scraper, verify_single_game_steam_status  # scraper disabled
# from pipelines.state import get_last_run_info  # scraper disabled


def _sync_from_ns_dates():
    st.session_state.steam_start_date = st.session_state.ns_start_date
    st.session_state.steam_end_date   = st.session_state.ns_end_date
    st.session_state.inv_start_date   = st.session_state.ns_start_date
    st.session_state.inv_end_date     = st.session_state.ns_end_date


def render(df_steam: pd.DataFrame, global_date_min: dt.date, global_date_max: dt.date):
    df_nonsteam = st.session_state.df_nonsteam.copy()
    nonsteam_source_name = st.session_state.get("nonsteam_source", "default file")

    # Normalise date_appended to YYYY-MM-DD so sorting and "New Today" checks
    # work correctly regardless of whether the CSV used M/D/YYYY or ISO format.
    if 'date_appended' in df_nonsteam.columns:
        df_nonsteam['date_appended'] = (
            pd.to_datetime(df_nonsteam['date_appended'], errors='coerce')
            .dt.strftime('%Y-%m-%d')
        )

    # ── Deduplicate: keep most recent row per Game Title ──────────────────────
    if 'Game Title' in df_nonsteam.columns:
        # Determine recency column: prefer date_appended, then YouTube ReleaseDate, then Release Date
        if 'date_appended' in df_nonsteam.columns:
            _sort_col = 'date_appended'
        elif 'YouTube ReleaseDate' in df_nonsteam.columns:
            _sort_col = 'YouTube ReleaseDate'
        elif 'Release Date' in df_nonsteam.columns:
            _sort_col = 'Release Date'
        else:
            _sort_col = None

        if _sort_col:
            df_nonsteam = (
                df_nonsteam
                .sort_values(_sort_col, ascending=True, na_position='first')
                .drop_duplicates(subset=['Game Title'], keep='last')
                .reset_index(drop=True)
            )
        else:
            df_nonsteam = df_nonsteam.drop_duplicates(subset=['Game Title'], keep='last').reset_index(drop=True)

    st.header("Non-Steam Game Ranking")
    st.caption(f"📊 Loading from {nonsteam_source_name}")

    # ── Sidebar config ────────────────────────────────────────────────────────
    st.sidebar.header("Non-Steam Scoring")
    w_youtube = st.sidebar.slider("YouTube Weight", 0, 5, 5)
    w_trends  = st.sidebar.slider("Trends Weight",  0, 5, 2, key="ns_w_trends")

    # ── Pre-processing: filter to ranked games ────────────────────────────────
    df_nonsteam['SteamStatus'] = df_nonsteam['SteamStatus'].fillna('Needs Verification')
    df_nonsteam_filter = df_nonsteam[
        (df_nonsteam['SteamStatus'] != 'PC Game (on Steam)') &
        (df_nonsteam['SteamStatus'] != 'Needs Verification') &
        (df_nonsteam['Category'].str.strip().str.lower() == 'main game')
    ].copy()
    # Parse date columns robustly — CSVs may contain mixed D/M/YYYY and M/D/YYYY.
    # Detection logic: if part[0] > 12 → D/M; if part[1] > 12 → M/D; ambiguous → D/M.
    # Also replaces literal 'N/A' strings and handles ISO format passthrough.
    def _parse_date_series(series: pd.Series) -> pd.Series:
        def _parse(val):
            if pd.isna(val) or str(val).strip() == '':
                return pd.NaT
            s = str(val).strip()
            if s.lower() in ('n/a', 'na', 'none'):
                return pd.NaT
            if len(s) >= 10 and s[4] == '-':
                return pd.to_datetime(s, errors='coerce')
            if '/' in s:
                parts = s.split('/')
                if len(parts) == 3:
                    try:
                        p1, p2 = int(parts[0]), int(parts[1])
                        if p1 > 12:
                            dayfirst = True
                        elif p2 > 12:
                            dayfirst = False
                        else:
                            dayfirst = True   # ambiguous → D/M
                        return pd.to_datetime(s, dayfirst=dayfirst, errors='coerce')
                    except (ValueError, TypeError):
                        pass
            return pd.to_datetime(s, dayfirst=True, errors='coerce')
        return series.apply(_parse)

    for _dc in ['YouTube ReleaseDate', 'Release Date']:
        df_nonsteam_filter[_dc] = _parse_date_series(df_nonsteam_filter[_dc])

    # ── Score calculation ─────────────────────────────────────────────────────
    today = dt.date.today()

    # Fix YouTube Views strings (e.g. "1,234,567" → 1234567)
    df_nonsteam_filter['YouTube Views'] = pd.to_numeric(
        df_nonsteam_filter['YouTube Views'].astype(str).str.replace(',', '', regex=False),
        errors='coerce'
    ).fillna(0)

    effective_date = df_nonsteam_filter['YouTube ReleaseDate'].fillna(df_nonsteam_filter['Release Date'])
    df_nonsteam_filter['Days_Since_Release'] = (
        (pd.to_datetime(today) - effective_date).dt.days
    ).clip(lower=0)  # future release dates → 0 days decay

    # Time-adjusted views, clamped ≥ 1 to avoid log(0)
    df_nonsteam_filter['adj_views'] = (
        df_nonsteam_filter['YouTube Views'] / (1 + df_nonsteam_filter['Days_Since_Release'] / 365)
    ).clip(lower=1).round(2)

    # Normalise adj_views → 1–5 (hybrid linear/log across dataset)
    _min_adj = float(max(df_nonsteam_filter['adj_views'].min(), 1))
    _max_adj = float(max(df_nonsteam_filter['adj_views'].max(), _min_adj + 1))
    df_nonsteam_filter['youtube_score'] = df_nonsteam_filter['adj_views'].apply(
        lambda v: calculate_hybrid_score(max(float(v), 1), _min_adj, _max_adj)
    ).round(2)

    # Raw Google Trends score (0–100)
    df_nonsteam_filter['trends_score'] = (
        df_nonsteam_filter['Game Title'].map(st.session_state.nonsteam_trends).fillna(0).astype(int)
    )

    # Normalise trends → 1–5 (linear)
    df_nonsteam_filter['trends_points'] = df_nonsteam_filter['trends_score'].apply(
        calculate_trends_weighted_points
    ).round(2)

    # Weighted priority score (max = 35, same as Steam)
    df_nonsteam_filter['priority_score'] = (
        df_nonsteam_filter['youtube_score'] * w_youtube +
        df_nonsteam_filter['trends_points'] * w_trends
    ).round(2)

    df_non_steam_ranked = df_nonsteam_filter.sort_values('priority_score', ascending=False, ignore_index=True)

    # Cross-check against Steam titles
    steam_titles = set(
        df_steam['Name'].dropna().astype(str).str.strip().str.lower()
    ) if 'Name' in df_steam.columns else set()
    df_non_steam_ranked['_on_steam'] = (
        df_non_steam_ranked['Game Title'].astype(str).str.strip().str.lower().isin(steam_titles)
    )
    df_non_steam_ranked = df_non_steam_ranked[~df_non_steam_ranked['_on_steam']].reset_index(drop=True)

    # ── Scraper status + run button (disabled) ────────────────────────────────
    # ns_info = get_last_run_info("non_steam")
    # run_nonsteam = st.button("▶ Run Scraper", ...)
    # Auto-verify background thread (disabled)
    # _unverified_count = int(...)

    # ── Summary metrics ───────────────────────────────────────────────────────
    _today_str = today.isoformat()
    _new_today = int(
        df_non_steam_ranked['date_appended'].astype(str).str.startswith(_today_str).sum()
    ) if 'date_appended' in df_non_steam_ranked.columns else 0
    _top_score = df_non_steam_ranked['priority_score'].max() if len(df_non_steam_ranked) else 0
    _trends_cached = int((df_non_steam_ranked['trends_score'] > 0).sum()) if len(df_non_steam_ranked) else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Games", len(df_non_steam_ranked), border=True)
    m2.metric("Top Priority Score", f"{_top_score:.1f}", border=True)
    m3.metric("New Today", _new_today, border=True)
    m4.metric("Trends Cached", _trends_cached, help="Games with a cached Google Trends score", border=True)

    st.divider()

    if "ns_reset_filters" not in st.session_state:
        st.session_state.ns_reset_filters = False

    # ── Filters ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=False):
        nf_col1, nf_col2, nf_col3 = st.columns(3)

        with nf_col1:
            st.markdown("**Release Date**")
            ns_start = st.date_input(
                "From", value=st.session_state.get("ns_start_date", global_date_min),
                min_value=global_date_min, max_value=global_date_max,
                key="ns_start_date", format="DD/MM/YYYY", on_change=_sync_from_ns_dates
            )
            ns_end = st.date_input(
                "To", value=st.session_state.get("ns_end_date", global_date_max),
                min_value=global_date_min, max_value=global_date_max,
                key="ns_end_date", format="DD/MM/YYYY", on_change=_sync_from_ns_dates
            )

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
                "Select platforms", options=all_platforms, default=default_platforms,
                placeholder="All platforms", key="ns_platforms"
            )

        with nf_col3:
            st.markdown("**PC / Console**")
            if 'SteamStatus' in df_non_steam_ranked.columns:
                steam_statuses = sorted(df_non_steam_ranked['SteamStatus'].dropna().unique().tolist())
            else:
                steam_statuses = []
            default_statuses = [] if st.session_state.ns_reset_filters else st.session_state.get("ns_steam_status", [])
            selected_statuses = st.multiselect(
                "Select status", options=steam_statuses, default=default_statuses,
                placeholder="All statuses", key="ns_steam_status"
            )

        btn_c1, btn_c2 = st.columns([1, 1])
        with btn_c1:
            apply_ns = st.button("Apply Filters", key="ns_apply", use_container_width=True)
        with btn_c2:
            if st.button("Reset Filters", key="ns_revert", use_container_width=True):
                st.session_state.ns_reset_filters = True
                st.rerun()

    if st.session_state.ns_reset_filters:
        st.session_state.ns_reset_filters = False

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_filtered_ns = df_non_steam_ranked.copy()

    if 'Release Date' in df_filtered_ns.columns:
        rel_dates = pd.to_datetime(df_filtered_ns['Release Date'], errors='coerce')
        in_range = rel_dates.between(pd.Timestamp(ns_start), pd.Timestamp(ns_end))
        df_filtered_ns = df_filtered_ns[rel_dates.isna() | in_range]

    if selected_platforms and 'Platforms' in df_filtered_ns.columns:
        def has_platform(val):
            plats = val if isinstance(val, list) else [p.strip() for p in str(val).split(',')]
            return any(p in selected_platforms for p in plats)
        df_filtered_ns = df_filtered_ns[df_filtered_ns['Platforms'].apply(has_platform)]

    if selected_statuses and 'SteamStatus' in df_filtered_ns.columns:
        df_filtered_ns = df_filtered_ns[df_filtered_ns['SteamStatus'].isin(selected_statuses)]

    df_filtered_ns = df_filtered_ns.reset_index(drop=True)
    df_filtered_ns.index = df_filtered_ns.index + 1

    # ── Ranking table ─────────────────────────────────────────────────────────
    tbl_col, btn_col, meta_col = st.columns([4, 1, 1])
    with tbl_col:
        st.subheader("Priority Rankings")
    with btn_col:
        if st.button("📊 Refresh Trends", key="fetch_nonsteam_trends",
                     help="Fetch Google Trends scores for filtered games", use_container_width=True):
            games = df_filtered_ns["Game Title"].dropna().unique().tolist()
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
        st.caption(f"Showing **{len(df_filtered_ns)}** of **{len(df_non_steam_ranked)}**")
        st.caption(f"Source: *{nonsteam_source_name}*")

    cols_to_show = [
        'Game Title', 'priority_score', 'youtube_score', 'trends_points',
        'adj_views', 'trends_score', 'YouTube Views', 'Days_Since_Release',
        'Release Date', 'Developers', 'Platforms', 'Genres',
        'YouTube URL', 'YouTube ReleaseDate', 'SteamStatus',
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

    for col in ['priority_score', 'youtube_score', 'trends_points', 'adj_views', 'YouTube Views', 'Days_Since_Release', 'trends_score']:
        if col in df_nonsteam_display.columns:
            df_nonsteam_display[col] = pd.to_numeric(df_nonsteam_display[col], errors='coerce').round(2)

    for col in ['Release Date', 'YouTube ReleaseDate']:
        if col in df_nonsteam_display.columns:
            df_nonsteam_display[col] = pd.to_datetime(df_nonsteam_display[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna('N/A')

    if "date_appended" in df_filtered_ns.columns and "date_appended" not in cols_to_show:
        df_nonsteam_display["date_appended"] = df_filtered_ns["date_appended"].values

    df_nonsteam_display = df_nonsteam_display.rename(columns={
        'priority_score':     'Priority Score',
        'youtube_score':      'YouTube Score',
        'trends_points':      'Trends Points',
        'adj_views':          'Adj. Views',
        'trends_score':       'Trends Score (raw)',
        'Days_Since_Release': 'Days Old',
        'YouTube ReleaseDate': 'YT Release Date',
    })

    st.dataframe(
        highlight_new_rows(df_nonsteam_display),
        use_container_width=True,
        column_config={
            'Priority Score':    st.column_config.NumberColumn(format="%.2f"),
            'YouTube Score':     st.column_config.NumberColumn(format="%.2f"),
            'Trends Points':     st.column_config.NumberColumn(format="%.2f"),
            'Adj. Views':        st.column_config.NumberColumn(format="%.0f"),
            'Trends Score (raw)':st.column_config.NumberColumn(format="%d"),
            'YouTube Views':     st.column_config.NumberColumn(format="%d"),
            'Days Old':          st.column_config.NumberColumn(format="%d"),
        },
    )

    # ── Supporting info ───────────────────────────────────────────────────────
    with st.expander("📐 How scores are calculated"):
        st.caption("Both signals are normalised to a 1–5 scale then weighted — identical structure to the Steam tab (max score = 35).")
        st.latex(r"\text{Adj. Views} = \frac{\text{YouTube Views}}{1 + \text{Days Old} / 365}")
        st.latex(r"\text{YouTube Score} = 0.5 \times \text{linear\_norm}(\text{Adj. Views}) + 0.5 \times \text{log\_norm}(\text{Adj. Views}) \quad \in [1, 5]")
        st.latex(r"\text{Trends Points} = \frac{\text{Trends Score}}{100} \times 4 + 1 \quad \in [1, 5]")
        st.latex(r"\text{Priority Score} = (\text{YouTube Score} \times w_{yt}) + (\text{Trends Points} \times w_{trends})")

    # ── Disabled sections ─────────────────────────────────────────────────────
    # Verify Steam Status (disabled — scraper off)
    # Auto-verify progress (disabled — scraper off)
