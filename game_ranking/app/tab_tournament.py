"""
Trends Tournament tab.

Auto tournament: batch-submits DataForSEO tasks per round, collects via polling.
Manual brackets: synchronous per-click for Steam / Non-Steam / Grand Final.
All searches scoped to category 41 (Computer & Video Games), worldwide, past month.
"""

import datetime as dt
import math
import pandas as pd
import streamlit as st

from calculation.trends_tournament import (
    run_tournament, run_cross_final, TOURNAMENT_GROUP_SIZE,
)
from calculation.dataforseo_trends import load_credentials, save_credentials
from pipelines.tournament_state import load_state, save_state, PINGBACK_URL
from pipelines.tournament_pipeline import start_tournament, collect_results
from pipelines.trends_pipeline import load_tournament_anchor, save_tournament_anchor


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_steam_games(n: int | None = None, appended_since=None) -> list[str]:
    df = st.session_state.get("df_steam")
    if df is None or df.empty:
        return []
    if "priority_score" in df.columns:
        df = df.sort_values("priority_score", ascending=False)
    if "Name" not in df.columns:
        return []
    if appended_since is not None and "date_appended" in df.columns:
        da = pd.to_datetime(df["date_appended"], errors="coerce")
        df = df[da >= pd.Timestamp(appended_since)]
    series = df["Name"].dropna().astype(str)
    return series.tolist() if n is None else series.head(n).tolist()


def _get_nonsteam_games(n: int | None = None, appended_since=None) -> list[str]:
    df = st.session_state.get("df_nonsteam")
    if df is None or df.empty:
        return []
    col = "Game Title"
    if col not in df.columns:
        return []
    df = df[
        (df.get("SteamStatus", pd.Series(dtype=str)).fillna("Needs Verification") != "PC Game (on Steam)") &
        (df.get("SteamStatus", pd.Series(dtype=str)).fillna("Needs Verification") != "Needs Verification") &
        (df.get("Category",    pd.Series(dtype=str)).str.strip().str.lower() == "main game")
    ].copy()
    if "priority_score" in df.columns:
        df = df.sort_values("priority_score", ascending=False)
    if appended_since is not None and "date_appended" in df.columns:
        da = pd.to_datetime(df["date_appended"], errors="coerce")
        df = df[da >= pd.Timestamp(appended_since)]
    series = df[col].dropna().astype(str)
    return series.tolist() if n is None else series.head(n).tolist()


def _results_to_df(results: list[dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df = df[~df["score"].isna()].copy()
    df["score"] = df["score"].round(2)
    df["Result"] = df.apply(
        lambda r: (
            "🏆 Champion" if r["champion"]
            else "⚠️ API Error" if r.get("api_failed")
            else "✅ Advanced" if not r["eliminated"]
            else "❌ Eliminated"
        ),
        axis=1,
    )
    return df[["round", "group", "game", "score", "Result"]].rename(columns={
        "round": "Round", "group": "Group", "game": "Game", "score": "Score",
    })


def _estimate_total_groups(n: int, group_size: int = TOURNAMENT_GROUP_SIZE) -> int:
    total, pool = 0, n
    while pool > 1:
        g = math.ceil(pool / group_size)
        total += g
        pool = g
    return max(total, 1)


def _champion_from_results(results: list[dict]) -> str | None:
    for r in results:
        if r.get("champion"):
            return r["game"]
    for r in reversed(results):
        if not r["eliminated"]:
            return r["game"]
    return None


def _get_creds() -> tuple[str, str]:
    """Return (login, password) from session_state, falling back to file."""
    login    = st.session_state.get("dfs_login", "")
    password = st.session_state.get("dfs_password", "")
    if not login or not password:
        login, password = load_credentials()
        st.session_state["dfs_login"]    = login
        st.session_state["dfs_password"] = password
    return login, password


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.header("🏆 Trends Tournament")
    st.caption(
        "Compares games head-to-head using Google Trends via DataForSEO. "
        "Category: Computer & Video Games · Location: Worldwide · Timeframe: past month. "
        "Groups of 5 per round (Google Trends limit). Highest mean interest advances."
    )

    # ── Credentials ───────────────────────────────────────────────────────────
    login, password = _get_creds()

    with st.expander("🔑 DataForSEO Credentials", expanded=not bool(login)):
        c1, c2 = st.columns(2)
        with c1:
            new_login = st.text_input("Login (email)", value=login, key="dfs_login_input")
        with c2:
            new_pass = st.text_input("Password", value=password, type="password", key="dfs_pass_input")
        if st.button("Save Credentials", key="save_dfs_creds"):
            save_credentials(new_login, new_pass)
            st.session_state["dfs_login"]    = new_login
            st.session_state["dfs_password"] = new_pass
            login, password = new_login, new_pass
            st.success("Credentials saved.")

    if not login or not password:
        st.warning("Enter DataForSEO credentials above to use the tournament.")
        return

    st.divider()

    # ── Config ────────────────────────────────────────────────────────────────
    with st.expander("⚙️ Settings", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            limit_games = st.checkbox(
                "Limit to top N games per source", value=False,
                key="tournament_limit_games",
                help="By default all games are entered. Check to restrict to the top N by priority score.",
            )
            top_n = None
            if limit_games:
                top_n = st.slider(
                    "Top N games per source", min_value=8, max_value=256, value=32, step=8,
                    key="tournament_top_n",
                )
        with c2:
            _df_s = st.session_state.get("df_steam")
            _df_ns = st.session_state.get("df_nonsteam")
            _dates = []
            for _df in [_df_s, _df_ns]:
                if _df is not None and "date_appended" in _df.columns:
                    _d = pd.to_datetime(_df["date_appended"], errors="coerce").max()
                    if pd.notna(_d):
                        _dates.append(_d)
            _default_appended = max(_dates).date() if _dates else dt.date.today()
            appended_since = st.date_input(
                "Include games appended from",
                value=_default_appended,
                format="DD/MM/YYYY",
                key="tournament_appended_since",
            )
            if st.checkbox(
                "Include all games (ignore date filter)",
                value=False,
                key="tournament_include_all_games",
            ):
                appended_since = None
        with c3:
            st.caption("**Category:** Computer & Video Games (41) · **Location:** Worldwide · **Timeframe:** past month")

    st.divider()

    # ── Auto Trends Tournament ────────────────────────────────────────────────
    st.subheader("Auto Trends Tournament")

    _anchor_meta = load_tournament_anchor()
    _anchor = _anchor_meta["anchor"] if _anchor_meta else st.session_state.get("trends_anchor")
    if _anchor:
        st.success(f"🏆 Current anchor: **{_anchor}**")

    _t_state = load_state()
    _status   = _t_state.get("status", "idle")

    # ── Compute game preview counts ───────────────────────────────────────────
    _include_all = st.session_state.get("tournament_include_all_games", False)
    _since_eff   = None if _include_all else appended_since
    _limit       = st.session_state.get("tournament_limit_games", False)
    _top_n_eff   = st.session_state.get("tournament_top_n") if _limit else None
    _n_steam = len(_get_steam_games(_top_n_eff, appended_since=_since_eff))
    _n_ns    = len(_get_nonsteam_games(_top_n_eff, appended_since=_since_eff))
    _n_total = _n_steam + _n_ns

    # ── Status: IDLE ──────────────────────────────────────────────────────────
    if _status == "idle":
        st.caption(f"Ready to start: **{_n_total} games** ({_n_steam} Steam + {_n_ns} Non-Steam).")
        if _n_total > 50:
            st.warning(
                f"⚠️ {_n_total} games is a large pool. "
                "Consider enabling **Limit to top N** in Settings."
            )
        if st.button("▶ Start Tournament", key="auto_trends_start"):
            if not _n_total:
                st.warning("Load Steam and Non-Steam data first.")
            else:
                steam_list    = _get_steam_games(_top_n_eff, appended_since=_since_eff)
                nonsteam_list = _get_nonsteam_games(_top_n_eff, appended_since=_since_eff)
                with st.spinner("Submitting round 1 tasks…"):
                    start_tournament(steam_list, nonsteam_list, login, password,
                                     pingback_url=PINGBACK_URL)
                st.rerun()

    # ── Status: RUNNING ───────────────────────────────────────────────────────
    elif _status == "running":
        _s_bracket  = _t_state.get("steam", {})
        _ns_bracket = _t_state.get("non_steam", {})

        def _bracket_status(bracket_data: dict, label: str) -> None:
            rnum = bracket_data.get("current_round", 1)
            rdata = bracket_data.get("rounds", {}).get(str(rnum), {})
            tasks = rdata.get("tasks", [])
            done  = sum(1 for t in tasks if t["status"] in ("complete", "failed"))
            total = len(tasks)
            byes  = len(rdata.get("bye_games", []))
            is_fin = rdata.get("is_final", False)
            fin_tag = " (final round)" if is_fin else ""
            if bracket_data.get("finalists"):
                st.success(f"**{label}**: finalists found — {', '.join(bracket_data['finalists'])}")
            elif not bracket_data.get("pool"):
                st.info(f"**{label}**: no games entered")
            else:
                st.info(f"**{label}**: Round {rnum}{fin_tag} — {done}/{total} tasks complete"
                        + (f", {byes} bye(s)" if byes else ""))

        _bracket_status(_s_bracket,  "Steam")
        _bracket_status(_ns_bracket, "Non-Steam")

        _col1, _col2 = st.columns(2)
        with _col1:
            if st.button("🔄 Collect Results", key="collect_results_btn"):
                with st.spinner("Polling DataForSEO tasks_ready…"):
                    _summary = collect_results(login, password)
                st.success(
                    f"Checked {_summary['checked']} ready — "
                    f"collected {_summary['collected']}, "
                    f"{_summary['rounds_advanced']} round(s) advanced, "
                    f"{_summary['errors']} error(s)."
                )
                st.rerun()
        with _col2:
            if st.button("🗑 Reset Tournament", key="reset_running_btn"):
                st.session_state["_confirm_reset"] = True

        if st.session_state.get("_confirm_reset"):
            st.warning("This will clear all tournament progress. Are you sure?")
            _r1, _r2 = st.columns(2)
            with _r1:
                if st.button("Yes, reset", key="confirm_reset_yes"):
                    save_state({"pingback_url": PINGBACK_URL, "status": "idle",
                                "steam": {"rounds": {}, "current_round": 1, "pool": [],
                                          "finalists": [], "all_bye_games": []},
                                "non_steam": {"rounds": {}, "current_round": 1, "pool": [],
                                              "finalists": [], "all_bye_games": []},
                                "anchor_pool": [], "selected_anchors": []})
                    st.session_state.pop("_confirm_reset", None)
                    st.rerun()
            with _r2:
                if st.button("Cancel", key="confirm_reset_no"):
                    st.session_state.pop("_confirm_reset", None)
                    st.rerun()

        # Live results tables per bracket
        for _b_key, _b_label in (("steam", "🚀 Steam"), ("non_steam", "📽️ Non-Steam")):
            _b = _t_state.get(_b_key, {})
            if not _b.get("rounds"):
                continue
            with st.expander(f"{_b_label} bracket rounds", expanded=False):
                for _rn in sorted(_b["rounds"].keys(), key=int):
                    _rd = _b["rounds"][_rn]
                    _fin_tag = " (final)" if _rd.get("is_final") else ""
                    st.caption(f"**Round {_rn}{_fin_tag}** — byes: {_rd.get('bye_games', [])}")
                    _rows = []
                    for _ti, _t in enumerate(_rd.get("tasks", [])):
                        _scores = _t.get("scores", {})
                        _winner = _t.get("winner", "—")
                        _rows.append({
                            "Group":    _ti + 1,
                            "Keywords": ", ".join(_t.get("keywords", [])),
                            "Scores":   ", ".join(f"{k}: {v:.1f}" for k, v in _scores.items()) if _scores else "pending",
                            "Winner":   _winner or "—",
                            "Status":   _t.get("status", "pending"),
                        })
                    if _rows:
                        st.dataframe(pd.DataFrame(_rows), use_container_width=True, hide_index=True)

    # ── Status: COMPLETE ──────────────────────────────────────────────────────
    elif _status == "complete":
        st.success("✅ Tournament complete!")

        # Bracket summary expandables
        for _b_key, _b_label in (("steam", "🚀 Steam"), ("non_steam", "📽️ Non-Steam")):
            _b = _t_state.get(_b_key, {})
            _finalists = _b.get("finalists", [])
            with st.expander(f"{_b_label} — finalists: {', '.join(_finalists) if _finalists else 'none'}", expanded=False):
                for _rn in sorted(_b.get("rounds", {}).keys(), key=int):
                    _rd = _b["rounds"][_rn]
                    _fin_tag = " (final)" if _rd.get("is_final") else ""
                    st.caption(f"**Round {_rn}{_fin_tag}** — byes: {_rd.get('bye_games', [])}")
                    _rows = []
                    for _ti, _t in enumerate(_rd.get("tasks", [])):
                        _scores = _t.get("scores", {})
                        _rows.append({
                            "Group":    _ti + 1,
                            "Keywords": ", ".join(_t.get("keywords", [])),
                            "Scores":   ", ".join(f"{k}: {v:.1f}" for k, v in _scores.items()) if _scores else "—",
                            "Winner":   _t.get("winner") or "—",
                            "Status":   _t.get("status", "—"),
                        })
                    if _rows:
                        st.dataframe(pd.DataFrame(_rows), use_container_width=True, hide_index=True)

        # Anchor pool selection
        st.divider()
        st.subheader("🎯 Anchor Pool")
        _anchor_pool = _t_state.get("anchor_pool", [])
        if _anchor_pool:
            st.caption(
                "Top 2 Steam + Top 2 Non-Steam finalists + any bye-advanced games. "
                "Select one (or more) to use as the anchor for trend scoring."
            )
            _prev_selected = _t_state.get("selected_anchors", [])
            _selected = st.multiselect(
                "Select anchor(s)", options=_anchor_pool,
                default=[g for g in _prev_selected if g in _anchor_pool],
                key="anchor_pool_selection",
            )
            if st.button("💾 Set as Anchor", key="set_anchor_btn"):
                if _selected:
                    _t_state["selected_anchors"] = _selected
                    save_state(_t_state)
                    # Use first selected game as the primary anchor
                    save_tournament_anchor(_selected[0])
                    st.session_state["trends_anchor"] = _selected[0]
                    st.success(f"Anchor set to: **{_selected[0]}**")
                    st.rerun()
                else:
                    st.warning("Select at least one game.")
        else:
            st.info("Anchor pool is empty — no finalists found.")

        if st.button("🗑 Reset Tournament", key="reset_complete_btn"):
            save_state({"pingback_url": PINGBACK_URL, "status": "idle",
                        "steam": {"rounds": {}, "current_round": 1, "pool": [],
                                  "finalists": [], "all_bye_games": []},
                        "non_steam": {"rounds": {}, "current_round": 1, "pool": [],
                                      "finalists": [], "all_bye_games": []},
                        "anchor_pool": [], "selected_anchors": []})
            st.rerun()

    st.divider()

    # ── Steam bracket (manual) ────────────────────────────────────────────────
    st.subheader("🚀 Steam Bracket")
    steam_games = _get_steam_games(top_n, appended_since=appended_since)

    if not steam_games:
        st.warning("No Steam games loaded.")
    else:
        st.caption(f"{len(steam_games)} games entered")
        with st.expander("Games entering tournament", expanded=False):
            st.write(", ".join(steam_games))

        if st.button("▶ Run Steam Tournament", key="run_steam_tournament", width="stretch"):
            st.session_state.pop("tournament_steam_results", None)
            st.session_state.pop("tournament_steam_champion", None)

            progress_text = st.empty()
            bar = st.progress(0)
            total_groups = _estimate_total_groups(len(steam_games))
            groups_done = [0]

            def _steam_progress(msg: str):
                groups_done[0] += 1
                bar.progress(min(groups_done[0] / total_groups, 0.99))
                progress_text.caption(msg)

            results = run_tournament(
                steam_games, login=login, password=password,
                progress_callback=_steam_progress,
                label="Steam",
            )

            bar.progress(1.0)
            progress_text.empty()

            st.session_state.tournament_steam_results  = results
            st.session_state.tournament_steam_champion = _champion_from_results(results)
            st.rerun()

    if "tournament_steam_results" in st.session_state:
        champ = st.session_state.get("tournament_steam_champion")
        if champ:
            st.success(f"🏆 Steam Champion: **{champ}**")
        df_res = _results_to_df(st.session_state.tournament_steam_results)
        if not df_res.empty:
            st.dataframe(df_res, width="stretch", hide_index=True,
                         column_config={"Score": st.column_config.NumberColumn(format="%.2f")})
            _failed = df_res[df_res["Result"] == "⚠️ API Error"]
            if not _failed.empty:
                st.warning(f"{len(_failed)} game(s) in groups where the API returned all zeros — scores may be unreliable.")

    st.divider()

    # ── Non-Steam bracket ─────────────────────────────────────────────────────
    st.subheader("📽️ Non-Steam Bracket")
    nonsteam_games = _get_nonsteam_games(top_n, appended_since=appended_since)

    if not nonsteam_games:
        st.warning("No Non-Steam games loaded.")
    else:
        st.caption(f"{len(nonsteam_games)} games entered")
        with st.expander("Games entering tournament", expanded=False):
            st.write(", ".join(nonsteam_games))

        if st.button("▶ Run Non-Steam Tournament", key="run_nonsteam_tournament", width="stretch"):
            st.session_state.pop("tournament_nonsteam_results", None)
            st.session_state.pop("tournament_nonsteam_champion", None)

            progress_text = st.empty()
            bar = st.progress(0)
            total_groups = _estimate_total_groups(len(nonsteam_games))
            groups_done = [0]

            def _nonsteam_progress(msg: str):
                groups_done[0] += 1
                bar.progress(min(groups_done[0] / total_groups, 0.99))
                progress_text.caption(msg)

            results = run_tournament(
                nonsteam_games, login=login, password=password,
                progress_callback=_nonsteam_progress,
                label="Non-Steam",
            )

            bar.progress(1.0)
            progress_text.empty()

            st.session_state.tournament_nonsteam_results  = results
            st.session_state.tournament_nonsteam_champion = _champion_from_results(results)
            st.rerun()

    if "tournament_nonsteam_results" in st.session_state:
        champ = st.session_state.get("tournament_nonsteam_champion")
        if champ:
            st.success(f"🏆 Non-Steam Champion: **{champ}**")
        df_res = _results_to_df(st.session_state.tournament_nonsteam_results)
        if not df_res.empty:
            st.dataframe(df_res, width="stretch", hide_index=True,
                         column_config={"Score": st.column_config.NumberColumn(format="%.2f")})
            _failed = df_res[df_res["Result"] == "⚠️ API Error"]
            if not _failed.empty:
                st.warning(f"{len(_failed)} game(s) in groups where the API returned all zeros — scores may be unreliable.")

    st.divider()

    # ── Cross-final ───────────────────────────────────────────────────────────
    st.subheader("⚡ Grand Final")

    steam_champ = st.session_state.get("tournament_steam_champion")
    ns_champ    = st.session_state.get("tournament_nonsteam_champion")

    if not steam_champ or not ns_champ:
        st.info("Run both the Steam and Non-Steam tournaments first to unlock the Grand Final.")
    else:
        st.caption(f"**Steam champion:** {steam_champ} vs **Non-Steam champion:** {ns_champ}")

        if st.button("▶ Run Grand Final", key="run_grand_final", width="stretch"):
            st.session_state.pop("tournament_final_result", None)
            with st.spinner(f"Comparing {steam_champ} vs {ns_champ}…"):
                result = run_cross_final(steam_champ, ns_champ, login=login, password=password)  # direct comparison, no anchor
            st.session_state.tournament_final_result = result
            st.rerun()

    if "tournament_final_result" in st.session_state:
        res    = st.session_state.tournament_final_result
        winner = res["winner"]
        st.balloons()
        st.success(f"🥇 Overall Winner: **{winner}**")
        f1, f2 = st.columns(2)
        with f1:
            label = "🚀 Steam" + (" 🥇" if res["steam_champion"] == winner else "")
            st.metric(label, res["steam_champion"], f"{res['steam_score']:.1f} pts")
        with f2:
            label = "📽️ Non-Steam" + (" 🥇" if res["nonsteam_champion"] == winner else "")
            st.metric(label, res["nonsteam_champion"], f"{res['nonsteam_score']:.1f} pts")
