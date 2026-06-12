"""
Trends Tournament tab.

Auto tournament: batch-submits DataForSEO tasks per round, collects via polling.
Manual brackets: synchronous per-click for Steam / Non-Steam / Grand Final.
All searches scoped to category 41 (Computer & Video Games), worldwide, past month.
"""

import datetime as dt
import time
import pandas as pd
import streamlit as st

from calculation.trends_tournament import TOURNAMENT_GROUP_SIZE
from calculation.dataforseo_trends import load_credentials, save_credentials
from pipelines.tournament_state import load_state, save_state, load_manual_state, save_manual_state, PINGBACK_URL
from pipelines.tournament_pipeline import (
    start_tournament, collect_results,
    start_manual_bracket, collect_manual_bracket,
    submit_grand_final, collect_grand_final,
)
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


def _manual_bracket_status(bracket_data: dict) -> str:
    if bracket_data.get("finalists"):
        return "complete"
    if bracket_data.get("rounds"):
        return "running"
    return "idle"


def _render_bracket_rounds(bracket_data: dict) -> None:
    """Render all rounds for a bracket as dataframes (mirrors auto-tournament display)."""
    for rn in sorted(bracket_data.get("rounds", {}).keys(), key=int):
        rd = bracket_data["rounds"][rn]
        fin_tag = " (final)" if rd.get("is_final") else ""
        st.caption(f"**Round {rn}{fin_tag}** — byes: {rd.get('bye_games', [])}")
        rows = []
        for ti, t in enumerate(rd.get("tasks", [])):
            scores = t.get("scores", {})
            rows.append({
                "Group":    ti + 1,
                "Keywords": ", ".join(t.get("keywords", [])),
                "Scores":   ", ".join(f"{k}: {v:.1f}" for k, v in scores.items()) if scores else "pending",
                "Winner":   t.get("winner") or "—",
                "Status":   t.get("status", "pending"),
            })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _creds_from_secrets() -> tuple[str, str] | None:
    """Return (login, password) if both are present in st.secrets, else None."""
    try:
        login    = st.secrets.get("DATAFORSEO_LOGIN", "")
        password = st.secrets.get("DATAFORSEO_PASSWORD", "")
        if login and password:
            return str(login), str(password)
    except Exception:
        pass
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


# ── Auto-poll loops ───────────────────────────────────────────────────────────

def _run_collect_loop_tournament(login: str, password: str) -> None:
    """
    One poll cycle: collect results, sleep 2 min, then st.rerun().
    Rerunning re-renders the whole page (tables included) with fresh state.
    """
    _bar         = st.progress(0.0, text="Checking DataForSEO for ready tasks…")
    _status_line = st.empty()

    _summary = collect_results(login, password)

    if _summary["complete"] or load_state().get("status") == "complete":
        _bar.progress(1.0, text="Tournament complete!")
        _status_line.empty()
        st.rerun()
        return

    _bar.progress(
        1.0,
        text=f"Collected {_summary['collected']}, "
             f"{_summary['rounds_advanced']} round(s) advanced, "
             f"{_summary['errors']} error(s)",
    )
    _status_line.caption("Next check in 120s…")
    time.sleep(120)
    st.rerun()


def _run_collect_loop_manual(bracket_key: str, login: str, password: str) -> None:
    """One poll cycle for a manual bracket — collect, sleep 2 min, rerun."""
    _bar         = st.progress(0.0, text="Checking DataForSEO for ready tasks…")
    _status_line = st.empty()

    _sum = collect_manual_bracket(bracket_key, login, password)

    if _sum["complete"]:
        _bar.progress(1.0, text="Bracket complete!")
        _status_line.empty()
        st.rerun()
        return

    _bar.progress(
        1.0,
        text=f"Collected {_sum['collected']}, "
             f"{_sum['rounds_advanced']} round(s) advanced, "
             f"{_sum['errors']} error(s)",
    )
    _status_line.caption("Next check in 120s…")
    time.sleep(120)
    st.rerun()


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

    _using_secrets = bool(_creds_from_secrets())
    with st.expander("🔑 DataForSEO Credentials", expanded=not bool(login)):
        if _using_secrets:
            st.info("Credentials loaded from Streamlit secrets (`DATAFORSEO_LOGIN` / `DATAFORSEO_PASSWORD`).")
        else:
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
                _run_collect_loop_tournament(login, password)

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
            fin_tag = " (final round)" if rdata.get("is_final") else ""
            if bracket_data.get("finalists"):
                st.success(f"**{label}**: finalists found — {', '.join(bracket_data['finalists'])}")
            elif not bracket_data.get("pool"):
                st.info(f"**{label}**: no games entered")
            else:
                st.info(f"**{label}**: Round {rnum}{fin_tag} — {done}/{total} tasks complete"
                        + (f", {byes} bye(s)" if byes else ""))

        _bracket_status(_s_bracket,  "Steam")
        _bracket_status(_ns_bracket, "Non-Steam")

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
                        _rows.append({
                            "Group":    _ti + 1,
                            "Keywords": ", ".join(_t.get("keywords", [])),
                            "Scores":   ", ".join(f"{k}: {v:.1f}" for k, v in _scores.items()) if _scores else "pending",
                            "Winner":   _t.get("winner") or "—",
                            "Status":   _t.get("status", "pending"),
                        })
                    if _rows:
                        st.dataframe(pd.DataFrame(_rows), use_container_width=True, hide_index=True)

        # Reset must be checked BEFORE the loop so a click during sleep is caught on the next rerun
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
            return  # don't start the loop while confirmation dialog is open

        # Auto-resume polling — mirrors tab_steam.py / tab_nonsteam.py pattern
        _run_collect_loop_tournament(login, password)
        return

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

        st.info("Anchor pool is available — select your anchor in the Steam or Non-Steam tab.")

        if st.button("🗑 Reset Tournament", key="reset_complete_btn"):
            save_state({"pingback_url": PINGBACK_URL, "status": "idle",
                        "steam": {"rounds": {}, "current_round": 1, "pool": [],
                                  "finalists": [], "all_bye_games": []},
                        "non_steam": {"rounds": {}, "current_round": 1, "pool": [],
                                      "finalists": [], "all_bye_games": []},
                        "anchor_pool": [], "selected_anchors": []})
            st.rerun()

    st.divider()

    # ── Manual brackets ───────────────────────────────────────────────────────
    _m_state = load_manual_state()

    def _render_manual_bracket(bracket_key: str, label: str, games: list[str]) -> None:
        st.subheader(label)
        _b = _m_state[bracket_key]
        _bstatus = _manual_bracket_status(_b)

        if _bstatus == "idle":
            if not games:
                st.warning(f"No {label.split()[1]} games loaded.")
                return
            st.caption(f"{len(games)} games entered")
            with st.expander("Games entering tournament", expanded=False):
                st.write(", ".join(games))
            if st.button(f"▶ Start {label.split()[1]} Tournament", key=f"start_manual_{bracket_key}"):
                with st.spinner("Submitting round 1 tasks…"):
                    start_manual_bracket(bracket_key, games, login, password)
                st.rerun()

        elif _bstatus == "running":
            rnum  = _b.get("current_round", 1)
            rdata = _b.get("rounds", {}).get(str(rnum), {})
            tasks = rdata.get("tasks", [])
            done  = sum(1 for t in tasks if t["status"] in ("complete", "failed"))
            total = len(tasks)
            byes  = len(rdata.get("bye_games", []))
            fin_tag = " (final round)" if rdata.get("is_final") else ""
            st.info(
                f"Round {rnum}{fin_tag} — {done}/{total} tasks complete"
                + (f", {byes} bye(s)" if byes else "")
            )
            with st.expander("Bracket rounds", expanded=False):
                _render_bracket_rounds(_b)
            # Reset must be checked BEFORE the loop so a click during sleep is caught on the next rerun
            if st.button("🗑 Reset", key=f"reset_manual_{bracket_key}"):
                _m_state[bracket_key] = {"rounds": {}, "current_round": 1,
                                         "pool": [], "finalists": [], "all_bye_games": []}
                _m_state["grand_final"] = {"steam_champion": None, "nonsteam_champion": None,
                                           "task_id": None, "keywords": [], "cleaned_keywords": [],
                                           "scores": {}, "winner": None, "status": "idle"}
                save_manual_state(_m_state)
                st.rerun()
            # Auto-resume polling — mirrors auto-tournament pattern
            _run_collect_loop_manual(bracket_key, login, password)
            return

        elif _bstatus == "complete":
            finalists = _b.get("finalists", [])
            champion  = finalists[0] if finalists else None
            if champion:
                st.success(f"🏆 Champion: **{champion}**")
            with st.expander("Bracket rounds", expanded=False):
                _render_bracket_rounds(_b)
            if st.button("🗑 Reset", key=f"reset_manual_{bracket_key}_done"):
                _m_state[bracket_key] = {"rounds": {}, "current_round": 1,
                                         "pool": [], "finalists": [], "all_bye_games": []}
                _m_state["grand_final"] = {"steam_champion": None, "nonsteam_champion": None,
                                           "task_id": None, "keywords": [], "cleaned_keywords": [],
                                           "scores": {}, "winner": None, "status": "idle"}
                save_manual_state(_m_state)
                st.rerun()

    steam_games    = _get_steam_games(top_n, appended_since=appended_since)
    nonsteam_games = _get_nonsteam_games(top_n, appended_since=appended_since)

    _render_manual_bracket("steam",     "🚀 Steam Bracket",     steam_games)
    st.divider()
    _render_manual_bracket("non_steam", "📽️ Non-Steam Bracket", nonsteam_games)
    st.divider()

    # ── Grand Final ───────────────────────────────────────────────────────────
    st.subheader("⚡ Grand Final")

    _m_state2    = load_manual_state()   # reload after bracket renders may have saved
    _gf          = _m_state2.get("grand_final", {})
    _gf_status   = _gf.get("status", "idle")
    _s_champ     = (_m_state2["steam"].get("finalists") or [None])[0]
    _ns_champ    = (_m_state2["non_steam"].get("finalists") or [None])[0]

    if not _s_champ or not _ns_champ:
        st.info("Complete both the Steam and Non-Steam brackets first to unlock the Grand Final.")
    else:
        st.caption(f"**Steam champion:** {_s_champ} vs **Non-Steam champion:** {_ns_champ}")

        if _gf_status == "idle":
            if st.button("▶ Submit Grand Final", key="submit_grand_final", width="stretch"):
                with st.spinner("Submitting Grand Final task…"):
                    submit_grand_final(_s_champ, _ns_champ, login, password)
                st.rerun()

        elif _gf_status == "pending":
            st.info("Grand Final task submitted — click Collect to fetch the result.")
            _gc1, _gc2 = st.columns(2)
            with _gc1:
                if st.button("🔄 Collect Grand Final", key="collect_grand_final"):
                    with st.spinner("Polling DataForSEO tasks_ready…"):
                        _gf_res = collect_grand_final(login, password)
                    if _gf_res["complete"]:
                        st.rerun()
                    else:
                        st.info("Result not ready yet — try again in a moment.")
            with _gc2:
                if st.button("🗑 Reset Grand Final", key="reset_gf_pending"):
                    _m_state2["grand_final"] = {"steam_champion": None, "nonsteam_champion": None,
                                                "task_id": None, "keywords": [], "cleaned_keywords": [],
                                                "scores": {}, "winner": None, "status": "idle"}
                    save_manual_state(_m_state2)
                    st.rerun()

        elif _gf_status in ("complete", "failed"):
            _winner = _gf.get("winner")
            if _winner:
                st.balloons()
                st.success(f"🥇 Overall Winner: **{_winner}**")
                _scores = _gf.get("scores", {})
                _f1, _f2 = st.columns(2)
                with _f1:
                    _slabel = "🚀 Steam" + (" 🥇" if _s_champ == _winner else "")
                    st.metric(_slabel, _s_champ, f"{_scores.get(_s_champ, 0.0):.1f} pts")
                with _f2:
                    _nslabel = "📽️ Non-Steam" + (" 🥇" if _ns_champ == _winner else "")
                    st.metric(_nslabel, _ns_champ, f"{_scores.get(_ns_champ, 0.0):.1f} pts")
            else:
                st.warning("Grand Final task failed — no scores returned.")
            if st.button("🗑 Reset Grand Final", key="reset_gf_done"):
                _m_state2["grand_final"] = {"steam_champion": None, "nonsteam_champion": None,
                                            "task_id": None, "keywords": [], "cleaned_keywords": [],
                                            "scores": {}, "winner": None, "status": "idle"}
                save_manual_state(_m_state2)
                st.rerun()
