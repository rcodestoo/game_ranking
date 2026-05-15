"""
Trends Tournament tab.

Runs a multi-round DataForSEO Google Trends tournament for Steam and/or Non-Steam games,
then compares the two champions in a cross-final.
All searches are scoped to category 41 (Computer & Video Games), worldwide, past month.
"""

import math
import time
import pandas as pd
import streamlit as st

from calculation.trends_tournament import (
    run_tournament, run_cross_final, ANCHOR, TOURNAMENT_GROUP_SIZE,
)
from calculation.dataforseo_trends import load_credentials, save_credentials
from app.thread_state import _trends_thread_state
from pipelines.trends_pipeline import run_trends_pipeline


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_steam_games(n: int | None = None) -> list[str]:
    df = st.session_state.get("df_steam")
    if df is None or df.empty:
        return []
    if "priority_score" in df.columns:
        df = df.sort_values("priority_score", ascending=False)
    if "Name" not in df.columns:
        return []
    series = df["Name"].dropna().astype(str)
    return series.tolist() if n is None else series.head(n).tolist()


def _get_nonsteam_games(n: int | None = None) -> list[str]:
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
    series = df[col].dropna().astype(str)
    return series.tolist() if n is None else series.head(n).tolist()


def _results_to_df(results: list[dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df = df[~df["score"].isna()].copy()
    df["score"] = df["score"].round(2)
    df["Result"] = df.apply(
        lambda r: "🏆 Champion" if r["champion"] else ("✅ Advanced" if not r["eliminated"] else "❌ Eliminated"),
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

    # ── Auto Trends Tournament ────────────────────────────────────────────────
    st.subheader("Auto Trends Tournament")

    _anchor = st.session_state.get("trends_anchor")
    if _anchor:
        st.success(f"🏆 Current anchor: **{_anchor}**")

    if _trends_thread_state["running"]:
        st.info(f"🔄 {_trends_thread_state.get('progress', 'Running...')}")
        time.sleep(3)
        st.rerun()
    else:
        if st.button("▶ Run Trends Tournament on Current Games", key="auto_trends_run"):
            df_steam    = st.session_state.get("df_steam")
            df_nonsteam = st.session_state.get("df_nonsteam")
            if df_steam is not None and df_nonsteam is not None:
                run_trends_pipeline(df_steam, df_nonsteam)
                st.rerun()
            else:
                st.warning("Load Steam and Non-Steam data first.")

    _auto_results = st.session_state.get("tournament_results_auto")
    if _auto_results:
        with st.expander("📋 Last Auto-Run Bracket Results", expanded=False):
            df_auto = _results_to_df(_auto_results)
            if not df_auto.empty:
                st.dataframe(df_auto, use_container_width=True, hide_index=True,
                             column_config={"Score": st.column_config.NumberColumn(format="%.2f")})

    st.divider()

    # ── Config ────────────────────────────────────────────────────────────────
    with st.expander("⚙️ Settings", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            limit_games = st.checkbox(
                "Limit to top N games per source", value=False,
                help="By default all games are entered. Check to restrict to the top N by priority score.",
            )
            top_n = None
            if limit_games:
                top_n = st.slider(
                    "Top N games per source", min_value=8, max_value=256, value=32, step=8,
                )
        with c2:
            st.caption(f"**Anchor term:** {ANCHOR}")
            st.caption("**Category:** Computer & Video Games (41) · **Location:** Worldwide · **Timeframe:** past month")

    st.divider()

    # ── Steam bracket ─────────────────────────────────────────────────────────
    st.subheader("🚀 Steam Bracket")
    steam_games = _get_steam_games(top_n)

    if not steam_games:
        st.warning("No Steam games loaded.")
    else:
        st.caption(f"{len(steam_games)} games entered")
        with st.expander("Games entering tournament", expanded=False):
            st.write(", ".join(steam_games))

        if st.button("▶ Run Steam Tournament", key="run_steam_tournament", use_container_width=True):
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
            st.dataframe(df_res, use_container_width=True, hide_index=True,
                         column_config={"Score": st.column_config.NumberColumn(format="%.2f")})

    st.divider()

    # ── Non-Steam bracket ─────────────────────────────────────────────────────
    st.subheader("📽️ Non-Steam Bracket")
    nonsteam_games = _get_nonsteam_games(top_n)

    if not nonsteam_games:
        st.warning("No Non-Steam games loaded.")
    else:
        st.caption(f"{len(nonsteam_games)} games entered")
        with st.expander("Games entering tournament", expanded=False):
            st.write(", ".join(nonsteam_games))

        if st.button("▶ Run Non-Steam Tournament", key="run_nonsteam_tournament", use_container_width=True):
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
            st.dataframe(df_res, use_container_width=True, hide_index=True,
                         column_config={"Score": st.column_config.NumberColumn(format="%.2f")})

    st.divider()

    # ── Cross-final ───────────────────────────────────────────────────────────
    st.subheader("⚡ Grand Final")

    steam_champ = st.session_state.get("tournament_steam_champion")
    ns_champ    = st.session_state.get("tournament_nonsteam_champion")

    if not steam_champ or not ns_champ:
        st.info("Run both the Steam and Non-Steam tournaments first to unlock the Grand Final.")
    else:
        st.caption(f"**Steam champion:** {steam_champ} vs **Non-Steam champion:** {ns_champ}")

        if st.button("▶ Run Grand Final", key="run_grand_final", use_container_width=True):
            st.session_state.pop("tournament_final_result", None)
            with st.spinner(f"Comparing {steam_champ} vs {ns_champ}…"):
                result = run_cross_final(steam_champ, ns_champ, login=login, password=password)
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
