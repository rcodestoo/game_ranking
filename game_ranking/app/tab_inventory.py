"""
Game Inventory tab.
"""

import datetime as dt
import time

import pandas as pd
import streamlit as st

try:
    import altair as alt
    _HAS_ALTAIR = True
except ImportError:
    _HAS_ALTAIR = False

from config import INVENTORY_FILE, STEAMSPY_CACHE_FILE, TRENDS_CACHE_FILE
from calculation.steam_players import fetch_player_data, fetch_player_counts_if_needed
from calculation.process_data import calculate_google_trends_points
from app.helpers import filter_stale_trends_games, load_trends_cache_timestamps


def _sync_from_inv_dates():
    st.session_state.steam_start_date = st.session_state.inv_start_date
    st.session_state.steam_end_date   = st.session_state.inv_end_date
    st.session_state.ns_start_date    = st.session_state.inv_start_date
    st.session_state.ns_end_date      = st.session_state.inv_end_date


def handle_change():
    """Apply data_editor changes (edits, additions, deletions) to the inventory CSV."""
    changes = st.session_state["game_editor"]
    df = st.session_state.game_data.copy()

    for row_idx, updates in changes["edited_rows"].items():
        for col, new_val in updates.items():
            df.at[row_idx, col] = new_val

    new_rows = [r for r in changes["added_rows"] if r]

    if new_rows:
        df = pd.concat(
            [df, pd.DataFrame(new_rows, columns=df.columns)],
            ignore_index=True,
        )

    if changes["deleted_rows"]:
        df = df.drop(index=changes["deleted_rows"]).reset_index(drop=True)

    did_change = bool(changes["edited_rows"] or new_rows or changes["deleted_rows"])
    if did_change:
        st.session_state.df = df
        st.session_state.game_data = df
        st.session_state.sum = int(df["Game Name"].count())
    try:
        df.to_csv(INVENTORY_FILE, index=True)
    except Exception as e:
        st.error(f"Failed to save changes to CSV: {e}")


def render(global_date_min: dt.date, global_date_max: dt.date):
    if "game_data" not in st.session_state:
        st.session_state.game_data = pd.read_csv(INVENTORY_FILE, index_col=0)

    if "inv_reset_filters" not in st.session_state:
        st.session_state.inv_reset_filters = False

    st.header("🎮 Game Tracker")

    # ── Summary metrics ───────────────────────────────────────────────────────
    game_data_bools = st.session_state.game_data.copy()
    for bc in ['Active', 'On Hold', 'Reviewed', 'Inactive']:
        try:
            game_data_bools[bc] = game_data_bools[bc].astype(bool)
        except Exception:
            pass

    col1, col2, col3, col4, col5 = st.columns(5)
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

    st.divider()

    # ── Status quick-filter buttons ───────────────────────────────────────────
    if "inv_status_quick_filter" not in st.session_state:
        st.session_state.inv_status_quick_filter = None

    _qf_cols = st.columns(5)
    for _col, (_label, _val) in zip(_qf_cols, [
        ("All", None), ("Active", "Active"), ("On Hold", "On Hold"),
        ("Reviewed", "Reviewed"), ("Inactive", "Inactive")
    ]):
        with _col:
            _is_selected = st.session_state.inv_status_quick_filter == _val
            if st.button(_label, key=f"inv_qf_{_label}",
                         type="primary" if _is_selected else "secondary",
                         use_container_width=True):
                st.session_state.inv_status_quick_filter = _val
                st.rerun()

    # ── Filters ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=False):
        gd = st.session_state.game_data.copy()

        inv_f1, inv_f2, inv_f3 = st.columns(3)

        with inv_f1:
            st.markdown("**Date Purchased**")
            inv_start = st.date_input(
                "From", value=st.session_state.get("inv_start_date", global_date_min),
                min_value=global_date_min, max_value=global_date_max,
                key="inv_start_date", format="DD/MM/YYYY", on_change=_sync_from_inv_dates
            )
            inv_end = st.date_input(
                "To", value=st.session_state.get("inv_end_date", global_date_max),
                min_value=global_date_min, max_value=global_date_max,
                key="inv_end_date", format="DD/MM/YYYY", on_change=_sync_from_inv_dates
            )

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

        with inv_f3:
            st.markdown("**Game Name**")
            default_inv_search = "" if st.session_state.inv_reset_filters else st.session_state.get("inv_name_search", "")
            inv_name_search = st.text_input(
                "Search by name", value=default_inv_search,
                placeholder="Type to search…", key="inv_name_search"
            )

        btn_c1, btn_c2 = st.columns([1, 1])
        with btn_c1:
            apply_inv = st.button("Apply Filters", key="inv_apply", use_container_width=True)
        with btn_c2:
            if st.button("Reset Filters", key="inv_revert", use_container_width=True):
                st.session_state.inv_reset_filters = True
                st.rerun()

    if st.session_state.inv_reset_filters:
        st.session_state.inv_reset_filters = False
        st.session_state.inv_status_quick_filter = None

    # ── Apply filters ─────────────────────────────────────────────────────────
    filtered = st.session_state.game_data.copy()

    if apply_inv:
        _dates = pd.to_datetime(filtered['Date Purchased'], errors='coerce', dayfirst=True)
        filtered = filtered[_dates.between(pd.Timestamp(inv_start), pd.Timestamp(inv_end))]

        if selected_inv_platforms and 'Platform' in filtered.columns:
            filtered = filtered[filtered['Platform'].isin(selected_inv_platforms)]

        if inv_name_search and 'Game Name' in filtered.columns:
            filtered = filtered[
                filtered['Game Name'].str.contains(inv_name_search, case=False, na=False)
            ]

    _qf = st.session_state.inv_status_quick_filter
    if _qf is not None and _qf in filtered.columns:
        try:
            filtered[_qf] = filtered[_qf].astype(bool)
            filtered = filtered[filtered[_qf] == True]
        except Exception:
            pass

    # Add Trends Score from shared cache (display only — not persisted to CSV)
    filtered = filtered.copy()
    filtered['Trends Score'] = (
        filtered['Game Name'].map(st.session_state.nonsteam_trends).fillna(0).astype(int)
    )

    # ── Game Library table ────────────────────────────────────────────────────
    tbl_col, btn_col = st.columns([5, 1])
    with tbl_col:
        st.subheader("Game Library")
        st.caption(f"Showing **{len(filtered)}** of **{len(st.session_state.game_data)}** games")

    if "inv_edit_mode" not in st.session_state:
        st.session_state.inv_edit_mode = False

    if not st.session_state.inv_edit_mode:
        with btn_col:
            st.markdown("<div style='padding-top: 28px'>", unsafe_allow_html=True)
            if st.button("✏️ Edit", key="inv_edit_btn", use_container_width=True):
                st.session_state.inv_edit_mode = True
                st.session_state.inv_edit_data = filtered.copy()
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        st.dataframe(filtered, use_container_width=True, hide_index=True)
    else:
        with btn_col:
            st.markdown("<div style='padding-top: 28px'>", unsafe_allow_html=True)
            if st.button("✅ Done", key="inv_done_btn", use_container_width=True):
                st.session_state.inv_edit_mode = False
                if "game_editor" in st.session_state:
                    del st.session_state["game_editor"]
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        st.caption("Edit cells, toggle checkboxes, or use + to add rows. Changes save automatically.")
        st.data_editor(
            st.session_state.inv_edit_data,
            num_rows="dynamic",
            key="game_editor",
            use_container_width=True,
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
                "Trends Score":   st.column_config.NumberColumn("Trends Score", disabled=True, format="%d"),
            },
            on_change=handle_change,
        )

    # ── Player Trend ──────────────────────────────────────────────────────────
    st.divider()
    with st.expander("📈 Player Trend — Steam Games", expanded=True):
        st.caption(
            "Hourly concurrent player snapshots for all Steam games in the inventory. "
            "Collected automatically while the app is open."
        )

        if st.button("🔄 Fetch Now", key="fetch_players_now", help="Force an immediate player count snapshot"):
            with st.spinner("Fetching player counts..."):
                _inv = pd.read_csv(INVENTORY_FILE, index_col=0)
                st.session_state.player_count_history = fetch_player_counts_if_needed(_inv, force=True)
            st.toast("Player counts updated!", icon="✅")
            st.rerun()

        history_df = st.session_state.player_count_history

        if history_df.empty:
            st.info(
                "No data yet — click **Fetch Now** to collect the first snapshot, "
                "then the app will collect one automatically each hour."
            )
        else:
            if not _HAS_ALTAIR:
                st.warning("Install altair to see the chart.")
            else:
                history_df = history_df.copy()
                history_df["date"] = pd.to_datetime(history_df["date"])
                last_snapshot = history_df["date"].max()
                st.caption(f"Last snapshot: {last_snapshot.strftime('%Y-%m-%d %H:%M')}")

                cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
                recent = history_df[history_df["date"] >= cutoff].copy()

                if recent.empty:
                    st.info("No snapshots in the last 30 days — click Fetch Now to start collecting data.")
                else:
                    recent["date_str"] = recent["date"].dt.strftime("%Y-%m-%d")
                    daily = (
                        recent.groupby(["date_str", "game_name"], as_index=False)["player_count"].max()
                    )
                    daily = daily.sort_values(["game_name", "date_str"])

                    all_trend_games = sorted(daily["game_name"].unique().tolist())
                    _filtered_names = set(filtered['Game Name'].dropna().astype(str).tolist()) if 'Game Name' in filtered.columns else set()
                    _default_games = [g for g in all_trend_games if g in _filtered_names]
                    if not _default_games:
                        _default_games = (
                            daily.groupby("game_name")["player_count"]
                            .max().nlargest(10).index.tolist()
                        )
                    sel_col, scale_col = st.columns([4, 1])
                    with sel_col:
                        selected_trend_games = st.multiselect(
                            "Games to display",
                            options=all_trend_games,
                            default=_default_games,
                            placeholder="Select games…",
                            key="trend_game_select",
                        )
                    with scale_col:
                        log_scale = st.checkbox("Log scale", value=False, key="trend_log_scale")

                    plot_daily = daily[daily["game_name"].isin(selected_trend_games)] if selected_trend_games else daily

                    n_games = plot_daily["game_name"].nunique()
                    n_days  = plot_daily["date_str"].nunique()
                    st.caption(f"**{n_games} games** · **{n_days} day{'s' if n_days != 1 else ''}** of data")

                    y_scale = alt.Scale(type="log") if log_scale else alt.Scale(type="linear", zero=False)

                    trend_chart = (
                        alt.Chart(plot_daily)
                        .mark_line(point=alt.OverlayMarkDef(filled=True, size=60))
                        .encode(
                            x=alt.X("date_str:O", title="Date", sort="ascending",
                                    axis=alt.Axis(labelAngle=-40, labelOverlap="greedy")),
                            y=alt.Y("player_count:Q", title="Concurrent Players",
                                    scale=y_scale, axis=alt.Axis(format="~s")),
                            color=alt.Color("game_name:N", title="Game"),
                            tooltip=[
                                alt.Tooltip("game_name:N",    title="Game"),
                                alt.Tooltip("date_str:O",     title="Date"),
                                alt.Tooltip("player_count:Q", title="Peak Players", format=","),
                            ],
                        )
                        .properties(height=420)
                        .interactive()
                    )
                    st.altair_chart(trend_chart, use_container_width=True)

    # ── Peak Player Counts ─────────────────────────────────────────────────────
    with st.expander("📊 Peak Player Counts (SteamSpy)", expanded=False):
        st.caption(
            "All-time peak concurrent players and playtime from SteamSpy. Results are cached for 24 hours."
        )

        if "player_count_df" not in st.session_state:
            if STEAMSPY_CACHE_FILE.exists():
                _sc = pd.read_csv(STEAMSPY_CACHE_FILE)
                st.session_state.player_count_last_fetched = _sc["fetched_at"].iloc[0]
                st.session_state.player_count_df = _sc.drop(columns=["fetched_at"])
            else:
                st.session_state.player_count_df = None
                st.session_state.player_count_last_fetched = None
        if "fetching_players" not in st.session_state:
            st.session_state.fetching_players = False

        fetch_col, trends_col, info_col = st.columns([2, 2, 3])
        with fetch_col:
            fetch_btn = st.button(
                "🔄 Fetch Player Counts",
                key="inv_fetch_players",
                disabled=st.session_state.fetching_players,
            )
        with trends_col:
            if st.button("📊 Refresh Trends", key="fetch_inv_trends",
                         help="Fetch Google Trends scores for filtered games", use_container_width=True):
                games = filtered["Game Name"].dropna().unique().tolist()
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
                    st.rerun()
            _ts = st.session_state.get("trends_last_fetched_at")
            if _ts:
                try:
                    _dt = dt.datetime.strptime(_ts, "%Y-%m-%d %H:%M:%S")
                    st.caption(f"Last fetched: {_dt.strftime('%d %b %Y, %H:%M')}")
                except Exception:
                    st.caption(f"Last fetched: {_ts}")
            else:
                st.caption("Never fetched")
        with info_col:
            if st.session_state.player_count_last_fetched:
                st.caption(f"Last fetched: {st.session_state.player_count_last_fetched}")
            else:
                st.caption("Not yet fetched.")

        if fetch_btn:
            st.session_state.fetching_players = True
            game_names = filtered["Game Name"].dropna().unique().tolist()
            progress_bar = st.progress(0, text="Starting…")

            def _on_progress(i, total, name):
                pct = int(i / total * 100) if total > 0 else 100
                label = f"Fetching: {name}" if i < total else "Done"
                progress_bar.progress(pct, text=label)

            import datetime as _dt
            result_df = fetch_player_data(game_names, progress_callback=_on_progress)
            st.session_state.player_count_last_fetched = _dt.datetime.now().strftime("%Y-%m-%d %H:%M")
            result_df["fetched_at"] = st.session_state.player_count_last_fetched
            result_df.to_csv(STEAMSPY_CACHE_FILE, index=False)
            st.session_state.player_count_df = result_df.drop(columns=["fetched_at"])
            st.session_state.fetching_players = False
            progress_bar.empty()
            st.rerun()

        if st.session_state.player_count_df is not None:
            if not _HAS_ALTAIR:
                st.warning("Install altair to see charts.")
            else:
                pcdf = st.session_state.player_count_df

                st.markdown("#### Current vs All-time Peak")

                _hist = st.session_state.player_count_history
                if _hist.empty:
                    st.info(
                        "No current player data yet — click **Fetch Now** in the Player Trend section "
                        "above to collect a snapshot, then the bars will show trend colours."
                    )
                else:
                    _hist = _hist.copy()
                    _hist["date"] = pd.to_datetime(_hist["date"])
                    latest_ccu = (
                        _hist.sort_values("date")
                        .groupby("game_name", as_index=False)
                        .last()[["game_name", "player_count"]]
                        .rename(columns={"game_name": "Game Name", "player_count": "Current CCU"})
                    )

                    plot_df = (
                        pcdf[["Game Name", "Peak CCU", "Peak CCU Numeric", "Avg Playtime (2wk hrs)"]]
                        [pcdf["Peak CCU Numeric"] > 0]
                        .copy()
                        .merge(latest_ccu, on="Game Name", how="left")
                    )
                    plot_df["Trends Score"] = (
                        plot_df["Game Name"].map(st.session_state.nonsteam_trends).fillna(0).astype(int)
                    )

                    plot_df["pct_of_peak"] = (
                        plot_df["Current CCU"] / plot_df["Peak CCU Numeric"] * 100
                    ).round(1)

                    def _trend(row):
                        if pd.isna(row["Current CCU"]) or row["Peak CCU Numeric"] == 0:
                            return "No data", "–", "#9E9E9E"
                        if row["Current CCU"] >= row["Peak CCU Numeric"]:
                            return "Rising", "▲", "#4CAF50"
                        if row["Current CCU"] >= row["Peak CCU Numeric"] * 0.5:
                            return "Flat", "→", "#2196F3"
                        return "Declining", "▼", "#F44336"

                    plot_df[["trend_label", "symbol", "bar_color"]] = plot_df.apply(
                        _trend, axis=1, result_type="expand"
                    )
                    plot_df["bar_label"] = plot_df.apply(
                        lambda r: f"{r['symbol']} {r['pct_of_peak']:.0f}%" if pd.notna(r["pct_of_peak"]) else f"{r['symbol']} N/A",
                        axis=1,
                    )
                    plot_df["bar_value"] = plot_df["Current CCU"].fillna(0)

                    if plot_df.empty:
                        st.info("No CCU data available to chart — all games returned N/A.")
                    else:
                        bars = (
                            alt.Chart(plot_df)
                            .mark_bar()
                            .encode(
                                x=alt.X("bar_value:Q", title="Current Concurrent Players",
                                        axis=alt.Axis(format=",d")),
                                y=alt.Y("Game Name:N",
                                        sort=alt.EncodingSortField(field="bar_value", order="descending"),
                                        title=""),
                                color=alt.Color("bar_color:N", scale=None, legend=None),
                                tooltip=[
                                    alt.Tooltip("Game Name:N"),
                                    alt.Tooltip("bar_value:Q",              title="Current Players", format=","),
                                    alt.Tooltip("Peak CCU:N",               title="All-time Peak"),
                                    alt.Tooltip("Avg Playtime (2wk hrs):Q", title="Avg Playtime 2wk (hrs)"),
                                    alt.Tooltip("pct_of_peak:Q",            title="% of Peak", format=".1f"),
                                    alt.Tooltip("trend_label:N",            title="Trend"),
                                    alt.Tooltip("Trends Score:Q",           title="Google Trends Score"),
                                ],
                            )
                        )
                        labels = (
                            alt.Chart(plot_df)
                            .mark_text(align="left", dx=4, fontSize=12)
                            .encode(
                                x=alt.X("bar_value:Q"),
                                y=alt.Y("Game Name:N",
                                        sort=alt.EncodingSortField(field="bar_value", order="descending")),
                                text=alt.Text("bar_label:N"),
                                color=alt.Color("bar_color:N", scale=None),
                            )
                        )
                        st.altair_chart(
                            (bars + labels).properties(height=max(200, len(plot_df) * 32)),
                            use_container_width=True,
                        )
                        st.caption(
                            "🟢 **Rising ▲** — at or above all-time peak &nbsp;|&nbsp; "
                            "🔵 **Flat →** — 50–99% of peak &nbsp;|&nbsp; "
                            "🔴 **Declining ▼** — below 50% of peak &nbsp;|&nbsp; "
                            "⚫ **–** — no current data (fetch Player Trend first)"
                        )
