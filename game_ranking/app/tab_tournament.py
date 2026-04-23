"""
Trends Tournament tab.

Runs a multi-round Google Trends tournament for Steam and/or Non-Steam games,
then compares the two champions in a cross-final.
"""

import datetime as dt
import pandas as pd
import streamlit as st

from calculation.trends_tournament import (
    run_tournament, run_cross_final,
    GAMES_PER_GROUP, ANCHOR,
)
from calculation.scraper import build_pytrends


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_steam_games(n: int) -> list[str]:
    df = st.session_state.get("df_steam")
    if df is None or df.empty:
        return []
    col = "Name"
    if col not in df.columns:
        return []
    if "priority_score" in df.columns:
        df = df.sort_values("priority_score", ascending=False)
    return df[col].dropna().astype(str).head(n).tolist()


def _get_nonsteam_games(n: int) -> list[str]:
    df = st.session_state.get("df_nonsteam")
    if df is None or df.empty:
        return []
    col = "Game Title"
    if col not in df.columns:
        return []
    # Filter same way as the non-steam tab (only ranked games)
    df = df[
        (df.get("SteamStatus", pd.Series(dtype=str)).fillna("Needs Verification") != "PC Game (on Steam)") &
        (df.get("SteamStatus", pd.Series(dtype=str)).fillna("Needs Verification") != "Needs Verification") &
        (df.get("Category", pd.Series(dtype=str)).str.strip().str.lower() == "main game")
    ].copy()
    if "priority_score" in df.columns:
        df = df.sort_values("priority_score", ascending=False)
    return df[col].dropna().astype(str).head(n).tolist()


def _results_to_df(results: list[dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df = df[~df["score"].isna()].copy()   # drop bye rows from display
    df["score"] = df["score"].round(2)
    df["Result"] = df.apply(
        lambda r: "🏆 Champion" if r["champion"] else ("✅ Advanced" if not r["eliminated"] else "❌ Eliminated"),
        axis=1
    )
    return df[["round", "group", "game", "score", "Result"]].rename(columns={
        "round": "Round", "group": "Group", "game": "Game", "score": "Score (norm.)"
    })


def _champion_from_results(results: list[dict]) -> str | None:
    for r in results:
        if r.get("champion"):
            return r["game"]
    # Fallback: last non-eliminated game
    for r in reversed(results):
        if not r["eliminated"]:
            return r["game"]
    return None


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.header("🏆 Trends Tournament")
    st.caption(
        "Compares games head-to-head using Google Trends (anchor: *Minecraft*). "
        "Groups of 8 per round — top scorer advances. "
        "Steam and Non-Steam run separately, then their champions meet in the final."
    )

    # ── Config ────────────────────────────────────────────────────────────────
    with st.expander("⚙️ Settings", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider(
                "Top N games per source", min_value=8, max_value=64, value=16, step=8,
                help="How many top-ranked games to pull from each tab into the tournament"
            )
        with c2:
            st.caption(f"**Groups per round:** {top_n // GAMES_PER_GROUP} (groups of {GAMES_PER_GROUP})")
            st.caption(f"**Anchor term:** {ANCHOR}")

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

            results: list[dict] = []
            pool = list(steam_games)
            round_num = 1
            pytrends = build_pytrends()

            total_groups = sum(
                max(1, len(pool[i:i + GAMES_PER_GROUP]))
                for i in range(0, len(pool), GAMES_PER_GROUP)
            )
            groups_done = 0

            from calculation.trends_tournament import compare_group, CALL_SLEEP
            import time

            while len(pool) > 1:
                groups = [pool[i:i + GAMES_PER_GROUP] for i in range(0, len(pool), GAMES_PER_GROUP)]
                next_pool: list[str] = []

                for g_idx, group in enumerate(groups):
                    if len(group) == 1:
                        results.append({"game": group[0], "score": None, "round": round_num,
                                        "group": g_idx + 1, "eliminated": False, "champion": False})
                        next_pool.append(group[0])
                        groups_done += 1
                        bar.progress(min(groups_done / max(total_groups, 1), 0.99))
                        continue

                    preview = ", ".join(group[:3]) + ("…" if len(group) > 3 else "")
                    progress_text.caption(f"Round {round_num} · Group {g_idx + 1}/{len(groups)}: {preview}")

                    scores = compare_group(group, pytrends=pytrends, sleep_s=CALL_SLEEP)
                    winner = max(scores, key=scores.get) if scores else group[0]
                    next_pool.append(winner)

                    for game in group:
                        results.append({"game": game, "score": scores.get(game, 0.0),
                                        "round": round_num, "group": g_idx + 1,
                                        "eliminated": game != winner, "champion": False})

                    groups_done += 1
                    bar.progress(min(groups_done / max(total_groups, 1), 0.99))
                    if g_idx < len(groups) - 1:
                        time.sleep(CALL_SLEEP)

                pool = next_pool
                round_num += 1

            if pool:
                champion = pool[0]
                for r in reversed(results):
                    if r["game"] == champion and not r["eliminated"]:
                        r["champion"] = True
                        break

            bar.progress(1.0)
            progress_text.empty()

            st.session_state.tournament_steam_results = results
            st.session_state.tournament_steam_champion = _champion_from_results(results)
            st.rerun()

    # Display Steam results
    if "tournament_steam_results" in st.session_state:
        champ = st.session_state.get("tournament_steam_champion")
        if champ:
            st.success(f"🏆 Steam Champion: **{champ}**")
        df_res = _results_to_df(st.session_state.tournament_steam_results)
        if not df_res.empty:
            st.dataframe(df_res, use_container_width=True, hide_index=True,
                         column_config={"Score (norm.)": st.column_config.NumberColumn(format="%.2f")})

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

            results: list[dict] = []
            pool = list(nonsteam_games)
            round_num = 1
            pytrends = build_pytrends()

            total_groups = sum(
                max(1, len(pool[i:i + GAMES_PER_GROUP]))
                for i in range(0, len(pool), GAMES_PER_GROUP)
            )
            groups_done = 0

            from calculation.trends_tournament import compare_group, CALL_SLEEP
            import time

            while len(pool) > 1:
                groups = [pool[i:i + GAMES_PER_GROUP] for i in range(0, len(pool), GAMES_PER_GROUP)]
                next_pool: list[str] = []

                for g_idx, group in enumerate(groups):
                    if len(group) == 1:
                        results.append({"game": group[0], "score": None, "round": round_num,
                                        "group": g_idx + 1, "eliminated": False, "champion": False})
                        next_pool.append(group[0])
                        groups_done += 1
                        bar.progress(min(groups_done / max(total_groups, 1), 0.99))
                        continue

                    preview = ", ".join(group[:3]) + ("…" if len(group) > 3 else "")
                    progress_text.caption(f"Round {round_num} · Group {g_idx + 1}/{len(groups)}: {preview}")

                    scores = compare_group(group, pytrends=pytrends, sleep_s=CALL_SLEEP)
                    winner = max(scores, key=scores.get) if scores else group[0]
                    next_pool.append(winner)

                    for game in group:
                        results.append({"game": game, "score": scores.get(game, 0.0),
                                        "round": round_num, "group": g_idx + 1,
                                        "eliminated": game != winner, "champion": False})

                    groups_done += 1
                    bar.progress(min(groups_done / max(total_groups, 1), 0.99))
                    if g_idx < len(groups) - 1:
                        time.sleep(CALL_SLEEP)

                pool = next_pool
                round_num += 1

            if pool:
                champion = pool[0]
                for r in reversed(results):
                    if r["game"] == champion and not r["eliminated"]:
                        r["champion"] = True
                        break

            bar.progress(1.0)
            progress_text.empty()

            st.session_state.tournament_nonsteam_results = results
            st.session_state.tournament_nonsteam_champion = _champion_from_results(results)
            st.rerun()

    # Display Non-Steam results
    if "tournament_nonsteam_results" in st.session_state:
        champ = st.session_state.get("tournament_nonsteam_champion")
        if champ:
            st.success(f"🏆 Non-Steam Champion: **{champ}**")
        df_res = _results_to_df(st.session_state.tournament_nonsteam_results)
        if not df_res.empty:
            st.dataframe(df_res, use_container_width=True, hide_index=True,
                         column_config={"Score (norm.)": st.column_config.NumberColumn(format="%.2f")})

    st.divider()

    # ── Cross-final ───────────────────────────────────────────────────────────
    st.subheader("⚡ Grand Final")

    steam_champ = st.session_state.get("tournament_steam_champion")
    ns_champ = st.session_state.get("tournament_nonsteam_champion")

    if not steam_champ or not ns_champ:
        st.info("Run both the Steam and Non-Steam tournaments first to unlock the Grand Final.")
    else:
        st.caption(f"**Steam champion:** {steam_champ} vs **Non-Steam champion:** {ns_champ}")

        if st.button("▶ Run Grand Final", key="run_grand_final", use_container_width=True):
            st.session_state.pop("tournament_final_result", None)
            with st.spinner(f"Comparing {steam_champ} vs {ns_champ}…"):
                pytrends = build_pytrends()
                result = run_cross_final(steam_champ, ns_champ, pytrends=pytrends)
            st.session_state.tournament_final_result = result
            st.rerun()

    if "tournament_final_result" in st.session_state:
        res = st.session_state.tournament_final_result
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
