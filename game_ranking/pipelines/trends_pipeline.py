"""
Background trends pipeline.

Phase 1a — Steam tournament (separate bracket, sequential rounds).
Phase 1b — Non-Steam tournament (separate bracket, sequential rounds).
Phase 2  — Cross-final: Steam champion vs Non-Steam champion.
           Anchor = runner-up (2nd most popular game).

Scoring all games against the anchor is a separate flow (see tab_steam, tab_nonsteam, tab_inventory).
"""

import json
import time
import threading
import traceback
from datetime import datetime

import pandas as pd

from calculation.trends_tournament import (
    run_tournament,
    run_cross_final,
    get_runner_up,
    get_runner_up_from_bracket,
    strip_edition_suffix,
    CALL_SLEEP,
    GAMES_CATEGORY,
)
from calculation.dataforseo_trends import (
    fetch_comparison,
    load_credentials,
)
from app.thread_state import _trends_thread_state
from config import TOURNAMENT_ANCHOR_FILE


# ── Anchor persistence ────────────────────────────────────────────────────────

def save_tournament_anchor(anchor: str) -> None:
    """Persist the tournament runner-up (anchor) + timestamp to cache/tournament_anchor.json."""
    data = {"anchor": anchor, "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    TOURNAMENT_ANCHOR_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_tournament_anchor() -> dict | None:
    """Return {"anchor": str, "run_at": str} from cache, or None if not found."""
    if TOURNAMENT_ANCHOR_FILE.exists():
        try:
            return json.loads(TOURNAMENT_ANCHOR_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


# ── Pipeline entry point ──────────────────────────────────────────────────────

def run_trends_pipeline(steam_df, nonsteam_df, appended_since=None, top_n=None):
    """Spawn background thread. Called after CSV load or manually from the tournament tab.

    top_n: if set, each bracket is limited to the top N games by priority score.
    """
    if _trends_thread_state["running"]:
        return

    login, password = load_credentials()
    if not login or not password:
        _trends_thread_state["result"] = {"error": "DataForSEO credentials not configured."}
        return

    steam_names    = _all_games(steam_df,    "Final Priority Score", "Name",       appended_since=appended_since, top_n=top_n)
    nonsteam_names = _all_games(nonsteam_df, "priority_score",       "Game Title", appended_since=appended_since, top_n=top_n)

    if not steam_names and not nonsteam_names:
        _trends_thread_state["result"] = {
            "error": (
                "No games found for the current date filter "
                f"(appended_since={appended_since}). "
                "Try checking 'Include all games' in Settings."
            )
        }
        return

    _trends_thread_state.update({"running": True, "result": None, "progress": "Starting..."})
    threading.Thread(
        target=_worker,
        args=(steam_names, nonsteam_names, login, password),
        daemon=True,
    ).start()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _all_games(df, score_col, name_col, appended_since=None, top_n=None):
    if df is None or name_col not in df.columns:
        return []
    if score_col in df.columns:
        df = df.sort_values(score_col, ascending=False)
    if appended_since is not None and "date_appended" in df.columns:
        da = pd.to_datetime(df["date_appended"], errors="coerce")
        df = df[da >= pd.Timestamp(appended_since)]
    names = list(df[name_col].dropna().unique())
    if top_n is not None:
        names = names[:top_n]
    return names


def _extract_champion(results: list[dict]) -> str | None:
    """Extract champion from run_tournament() results."""
    for r in results:
        if r.get("champion"):
            return r["game"]
    for r in reversed(results):
        if not r.get("eliminated"):
            return r["game"]
    return None


# ── Background worker ─────────────────────────────────────────────────────────

def _worker(steam_names, nonsteam_names, login, password):
    def _progress(msg):
        _trends_thread_state["progress"] = msg

    try:
        # ── Phase 1a: Steam Tournament ────────────────────────────────────────
        steam_champion, steam_results = None, []

        if len(steam_names) >= 2:
            _progress("Phase 1a (Steam): Round 1...")
            steam_results = run_tournament(
                steam_names,
                login=login,
                password=password,
                progress_callback=lambda msg: _progress(f"Phase 1a (Steam): {msg}"),
                label="Steam-Auto",
            )
            steam_champion = _extract_champion(steam_results) or steam_names[0]

        elif len(steam_names) == 1:
            steam_champion = steam_names[0]
            _progress("Phase 1a (Steam): 1 game — fetching score vs Minecraft...")
            raw = fetch_comparison(
                [strip_edition_suffix(steam_names[0]), "Minecraft"],
                login, password, GAMES_CATEGORY,
            )
            _score = raw.get(strip_edition_suffix(steam_names[0]), 0.0)
            steam_results = [{
                "game": steam_names[0], "score": _score, "round": 1,
                "group": 1, "eliminated": False, "champion": True,
                "api_failed": _score == 0.0,
            }]
            time.sleep(CALL_SLEEP)

        # ── Phase 1b: Non-Steam Tournament ───────────────────────────────────
        nonsteam_champion, nonsteam_results = None, []

        if len(nonsteam_names) >= 2:
            _progress("Phase 1b (Non-Steam): Round 1...")
            nonsteam_results = run_tournament(
                nonsteam_names,
                login=login,
                password=password,
                progress_callback=lambda msg: _progress(f"Phase 1b (Non-Steam): {msg}"),
                label="NonSteam-Auto",
            )
            nonsteam_champion = _extract_champion(nonsteam_results) or nonsteam_names[0]

        elif len(nonsteam_names) == 1:
            nonsteam_champion = nonsteam_names[0]
            _progress("Phase 1b (Non-Steam): 1 game — fetching score vs Minecraft...")
            raw = fetch_comparison(
                [strip_edition_suffix(nonsteam_names[0]), "Minecraft"],
                login, password, GAMES_CATEGORY,
            )
            _score = raw.get(strip_edition_suffix(nonsteam_names[0]), 0.0)
            nonsteam_results = [{
                "game": nonsteam_names[0], "score": _score, "round": 1,
                "group": 1, "eliminated": False, "champion": True,
                "api_failed": _score == 0.0,
            }]
            time.sleep(CALL_SLEEP)

        # ── Phase 2: Cross-Final → anchor = runner-up ─────────────────────────
        cross_final_result, anchor = None, None

        if steam_champion and nonsteam_champion:
            _progress("Phase 2: Cross-Final...")
            cross_final_result = run_cross_final(
                steam_champion, nonsteam_champion,
                login=login, password=password, category_code=GAMES_CATEGORY,
            )
            anchor = get_runner_up(cross_final_result)
        elif steam_champion:
            anchor = get_runner_up_from_bracket(steam_results, steam_champion)
        elif nonsteam_champion:
            anchor = get_runner_up_from_bracket(nonsteam_results, nonsteam_champion)

        anchor = anchor or "Minecraft"
        save_tournament_anchor(anchor)
        _progress(f"Done. Anchor: {anchor}")

        _trends_thread_state["result"] = {
            "anchor":                      anchor,
            "steam_tournament_results":    steam_results,
            "nonsteam_tournament_results": nonsteam_results,
            "cross_final_result":          cross_final_result,
            "tournament_results":          steam_results + nonsteam_results,  # backward compat
        }

    except Exception as e:
        _trends_thread_state["result"] = {"error": f"{e}\n{traceback.format_exc()}"}
    finally:
        _trends_thread_state["running"] = False
