"""
Non-Steam Report tab.
"""

import datetime as dt
import time

import pandas as pd
import streamlit as st

from app.thread_state import _trends_thread_state
from app.helpers import (highlight_new_rows, reload_nonsteam_from_csv,
                         filter_stale_trends_games, load_trends_cache_timestamps)
from calculation.process_data import calculate_hybrid_score, calculate_trends_weighted_points
from calculation.dataforseo_trends import load_credentials
from pipelines.refresh_trends_pipeline import (
    load_anchor_pool,
    load_state as load_refresh_state,
    save_refresh_anchor,
    load_refresh_anchor,
    submit_refresh,
    collect_refresh,
    write_scores_to_csv,
)
from config import TRENDS_CACHE_FILE, REFRESH_TRENDS_STATE_FILE_NONSTEAM
from pipelines.trends_pipeline import load_tournament_anchor


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
    _raw_row_count = len(df_nonsteam)
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

    _after_dedup_count = len(df_nonsteam)
    _dedup_merged = _raw_row_count - _after_dedup_count

    st.header("Non-Steam Game Ranking")
    st.caption(f"📊 Loading from {nonsteam_source_name}")

    _anchor = st.session_state.get("trends_anchor")
    _anchor_meta_top = load_tournament_anchor()
    if _trends_thread_state["running"]:
        st.info(f"🔄 {_trends_thread_state.get('progress', 'Trends updating...')}")
    elif _anchor:
        _run_at_top = _anchor_meta_top.get("run_at") if _anchor_meta_top else None
        if _run_at_top:
            try:
                _run_at_fmt = dt.datetime.strptime(_run_at_top, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M")
            except Exception:
                _run_at_fmt = _run_at_top
            st.caption(f"Trends anchor: **{_anchor}** — set {_run_at_fmt}")
        else:
            st.caption(f"Trends anchor: **{_anchor}**")
    else:
        st.caption("No trends anchor yet — upload a CSV to run tournament")

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
            return pd.to_datetime(s, errors='coerce', format='mixed', dayfirst=True)
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
    _before_steam_filter = len(df_non_steam_ranked)
    df_non_steam_ranked = df_non_steam_ranked[~df_non_steam_ranked['_on_steam']].reset_index(drop=True)
    _steam_removed = _before_steam_filter - len(df_non_steam_ranked)

    # ── Data info caption ─────────────────────────────────────────────────────
    _info_parts = []
    if _dedup_merged > 0:
        _info_parts.append(f"{_dedup_merged} duplicate title(s) merged")
    if _steam_removed > 0:
        _info_parts.append(f"{_steam_removed} already on Steam removed")
    if _info_parts:
        st.caption(f"ℹ️ From {_raw_row_count} raw rows → {len(df_non_steam_ranked)} ranked games ({', '.join(_info_parts)})")

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
    if "applied_filters_ns" not in st.session_state:
        st.session_state.applied_filters_ns = None  # None = show all (no filter applied yet)

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
            if st.button("Apply Filters", key="ns_apply", width="stretch"):
                st.session_state.applied_filters_ns = {
                    "start_date": ns_start,
                    "end_date": ns_end,
                    "platforms": selected_platforms,
                    "statuses": selected_statuses,
                }
                st.rerun()
        with btn_c2:
            if st.button("Reset Filters", key="ns_revert", width="stretch"):
                st.session_state.ns_reset_filters = True
                st.session_state.applied_filters_ns = None
                st.rerun()

    if st.session_state.ns_reset_filters:
        st.session_state.ns_reset_filters = False

    # ── Apply filters (lazy — only when "Apply Filters" has been clicked) ─────
    df_filtered_ns = df_non_steam_ranked.copy()
    _afns = st.session_state.applied_filters_ns

    if _afns is not None:
        if 'Release Date' in df_filtered_ns.columns:
            rel_dates = pd.to_datetime(df_filtered_ns['Release Date'], errors='coerce', format='mixed', dayfirst=True)
            in_range = rel_dates.between(pd.Timestamp(_afns["start_date"]), pd.Timestamp(_afns["end_date"]))
            df_filtered_ns = df_filtered_ns[rel_dates.isna() | in_range]

        if _afns["platforms"] and 'Platforms' in df_filtered_ns.columns:
            def has_platform(val):
                plats = val if isinstance(val, list) else [p.strip() for p in str(val).split(',')]
                return any(p in _afns["platforms"] for p in plats)
            df_filtered_ns = df_filtered_ns[df_filtered_ns['Platforms'].apply(has_platform)]

        if _afns["statuses"] and 'SteamStatus' in df_filtered_ns.columns:
            df_filtered_ns = df_filtered_ns[df_filtered_ns['SteamStatus'].isin(_afns["statuses"])]

    df_filtered_ns = df_filtered_ns.reset_index(drop=True)
    df_filtered_ns.index = df_filtered_ns.index + 1

    # ── Ranking table ─────────────────────────────────────────────────────────
    _refresh_state  = load_refresh_state(state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)
    _login, _password = load_credentials()
    _has_creds      = bool(_login and _password)
    _effective_anchor = (
        st.session_state.get("ns_anchor_selectbox")
        or load_refresh_anchor(state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)
    )
    _show_collect   = _refresh_state["status"] in ("submitted", "collecting")
    _pending_count  = sum(1 for t in _refresh_state["tasks"] if t["status"] == "pending") if _show_collect else 0

    # ── Collect loop for the Non-Steam tab ───────────────────────────────────
    def _run_collect_loop(grand_total: int) -> None:
        _POLL_SLEEP  = 120  # seconds between tasks_ready calls (2 min)
        _MAX_POLLS   = 200  # safety ceiling (~10 min at 3 s/poll)

        _bar         = st.progress(0.0, text="Checking DataForSEO for ready tasks…")
        _log         = st.empty()
        _status_line = st.empty()
        _log_lines:  list[str] = []
        _all_scores: dict[str, int] = {}

        def _on_task(game: str, score: int, n_done: int, n_total: int, failed: bool) -> None:
            _all_scores[game] = score
            # Update session state immediately so the table reflects the score on rerun
            st.session_state.nonsteam_trends[game] = score
            pct = len(_all_scores) / max(grand_total, 1)
            _bar.progress(min(pct, 1.0), text=f"Collecting… {len(_all_scores)} / {grand_total}")
            icon = "⚠️" if failed else "✅"
            _log_lines.append(f"{icon} **{game}** → {score}")
            _log.markdown("\n\n".join(_log_lines))

        _poll_num = 0
        while _poll_num < _MAX_POLLS:
            _poll_num += 1
            _result = collect_refresh(_login, _password, on_task_complete=_on_task,
                                      state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)

            if _result["complete"]:
                _bar.progress(1.0, text=f"All done — {_result['collected']} collected, {_result['errors']} failed")
                _status_line.empty()
                break

            _remaining = sum(
                1 for t in load_refresh_state(state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)["tasks"]
                if t["status"] == "pending"
            )
            if _remaining == 0:
                _bar.progress(1.0, text="All done")
                _status_line.empty()
                break

            _status_line.caption(
                f"Poll {_poll_num} — {_remaining} task(s) still pending, "
                f"next check in {_POLL_SLEEP}s…"
            )
            time.sleep(_POLL_SLEEP)
        else:
            _bar.progress(1.0, text="Timed out — some tasks may still be pending")

        if _all_scores:
            _cached_ts = load_trends_cache_timestamps(TRENDS_CACHE_FILE)
            write_scores_to_csv(
                _all_scores,
                st.session_state.nonsteam_trends,
                list(_all_scores.keys()),
                _cached_ts,
            )
            st.session_state.ns_trends_last_fetched_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _ok  = sum(1 for s in _all_scores.values() if s > 0)
        _bad = sum(1 for s in _all_scores.values() if s == 0)
        st.toast(f"Done! {_ok} collected, {_bad} zero-score", icon="✅")
        st.rerun()

    tbl_col, btn_col, meta_col = st.columns([4, 1, 1])
    with tbl_col:
        st.subheader("Priority Rankings")
    with btn_col:
        if st.button("📊 Refresh Trends", key="fetch_nonsteam_trends_filter",
                     help="Fetch trends for all stale games in the current filter",
                     width="stretch", disabled=not _has_creds):
            games = df_filtered_ns["Game Title"].dropna().unique().tolist()
            _cached_ts = load_trends_cache_timestamps(TRENDS_CACHE_FILE)
            games_to_fetch = filter_stale_trends_games(games, _cached_ts)
            if not games_to_fetch:
                st.toast("All trends data is fresh (< 24 h)", icon="✅")
            elif not _effective_anchor:
                st.warning("No anchor set. Run the tournament first or select an anchor from the dropdown.")
            else:
                _state = submit_refresh(games_to_fetch, _effective_anchor, _login, _password,
                                        source="refresh_all",
                                        state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)
                n_submitted = sum(1 for t in _state["tasks"] if t["status"] == "pending")
                n_failed    = sum(1 for t in _state["tasks"] if t["status"] == "failed")
                if n_failed:
                    st.warning(f"Submitted {n_submitted} task(s). {n_failed} failed to submit.")
                _run_collect_loop(n_submitted)  # starts polling immediately after submit

        _collect_clicked = (
            st.button(f"📥 Collect Results ({_pending_count})",
                      key="collect_nonsteam_trends",
                      help="Resume collecting results from a previous submission",
                      width="stretch", disabled=not _has_creds)
            if _show_collect else False
        )

        _ts = st.session_state.get("ns_trends_last_fetched_at")
        if _ts:
            try:
                _dt = dt.datetime.strptime(_ts, "%Y-%m-%d %H:%M:%S")
                st.caption(f"Last fetched: {_dt.strftime('%d/%m/%Y %H:%M')}")
            except Exception:
                st.caption(f"Last fetched: {_ts}")
        else:
            st.caption("Never fetched")
    with meta_col:
        st.caption(f"Showing **{len(df_filtered_ns)}** of **{len(df_non_steam_ranked)}**")
        st.caption(f"Source: *{nonsteam_source_name}*")
        _anchor_pool_meta = load_anchor_pool()
        _saved_anchor = load_refresh_anchor(state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)
        if _anchor_pool_meta:
            _default_idx = _anchor_pool_meta.index(_saved_anchor) if _saved_anchor in _anchor_pool_meta else 0
            _selected_anchor = st.selectbox(
                "Trends anchor", options=_anchor_pool_meta, index=_default_idx,
                key="ns_anchor_selectbox",
                help="Reference game for normalizing Trends scores (anchor = 100)",
            )
            if _selected_anchor != _saved_anchor:
                save_refresh_anchor(_selected_anchor, state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)
                st.rerun()
        else:
            if _saved_anchor:
                st.caption(f"Trends anchor: **{_saved_anchor}**")
            else:
                st.caption("Trends anchor: *none — run tournament first*")

    # ── Fallback: resume collection if page reloaded mid-run ─────────────────
    if _collect_clicked:
        _run_collect_loop(_pending_count)

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
            df_nonsteam_display[col] = pd.to_datetime(df_nonsteam_display[col], errors='coerce', format='mixed', dayfirst=True).dt.strftime('%d/%m/%Y').fillna('N/A')

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

    df_nonsteam_display.insert(0, "Fetch", False)
    _ns_edited = st.data_editor(
        df_nonsteam_display,
        width="stretch",
        hide_index=False,
        disabled=[c for c in df_nonsteam_display.columns if c != "Fetch"],
        column_config={
            'Fetch':             st.column_config.CheckboxColumn("Fetch", default=False),
            'Priority Score':    st.column_config.NumberColumn(format="%.2f"),
            'YouTube Score':     st.column_config.NumberColumn(format="%.2f"),
            'Trends Points':     st.column_config.NumberColumn(format="%.2f"),
            'Adj. Views':        st.column_config.NumberColumn(format="%.0f"),
            'Trends Score (raw)':st.column_config.NumberColumn(format="%d"),
            'YouTube Views':     st.column_config.NumberColumn(format="%d"),
            'Days Old':          st.column_config.NumberColumn(format="%d"),
        },
    )

    _ns_selected = _ns_edited[_ns_edited["Fetch"] == True]["Game Title"].tolist()
    _ns_btn_c, _ = st.columns([1, 3])
    with _ns_btn_c:
        if st.button(
            f"📊 Fetch Trends for Selected ({len(_ns_selected)})",
            key="fetch_nonsteam_trends",
            disabled=not _ns_selected or not _has_creds,
            width="stretch",
        ):
            if not _effective_anchor:
                st.warning("No anchor set. Run the tournament first or select an anchor from the dropdown.")
            else:
                _state = submit_refresh(_ns_selected, _effective_anchor, _login, _password,
                                        source="refresh_selected",
                                        state_file=REFRESH_TRENDS_STATE_FILE_NONSTEAM)
                n_submitted = sum(1 for t in _state["tasks"] if t["status"] == "pending")
                if n_submitted:
                    _run_collect_loop(n_submitted)

    # ── Supporting info ───────────────────────────────────────────────────────
    with st.expander("📐 How scores are calculated"):
        st.caption("Both signals are normalised to a 1–5 scale then weighted — identical structure to the Steam tab (max score = 35).")
        st.latex(r"\text{Adj. Views} = \frac{\text{YouTube Views}}{1 + \text{Days Old} / 365}")
        st.latex(r"\text{YouTube Score} = 0.5 \times \text{linear\_norm}(\text{Adj. Views}) + 0.5 \times \text{log\_norm}(\text{Adj. Views}) \quad \in [1, 5]")
        st.latex(r"\text{Trends Points} = \frac{\text{Trends Score}}{100} \times 4 + 1 \quad \in [1, 5]")
        st.latex(r"\text{Priority Score} = (\text{YouTube Score} \times w_{yt}) + (\text{Trends Points} \times w_{trends})")
