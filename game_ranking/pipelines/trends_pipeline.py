"""
Background trends pipeline: runs a tournament to find the anchor champion,
then scores all games against that anchor on a consistent scale.
"""

import time
import threading

from calculation.trends_tournament import run_tournament, compare_group, BATCH_SIZE, CALL_SLEEP
from calculation.scraper import build_pytrends
from app.thread_state import _trends_thread_state

TOURNAMENT_TOP_N = 50   # games per tab fed into tournament (keeps API calls manageable)


def run_trends_pipeline(steam_df, nonsteam_df):
    """Spawn background thread. Called after CSV load or manually from the tournament tab."""
    if _trends_thread_state["running"]:
        return
    steam_names = _top_n(steam_df, "Final Priority Score", "Name")
    nonsteam_names = _top_n(nonsteam_df, "priority_score", "Game Title")
    _trends_thread_state.update({"running": True, "result": None, "progress": "Starting..."})
    threading.Thread(target=_worker, args=(steam_names, nonsteam_names), daemon=True).start()


def _top_n(df, score_col, name_col):
    if df is None or name_col not in df.columns:
        return []
    if score_col in df.columns:
        df = df.sort_values(score_col, ascending=False)
    return list(df[name_col].dropna().unique()[:TOURNAMENT_TOP_N])


def _worker(steam_names, nonsteam_names):
    try:
        pytrends = build_pytrends()
        pool = list(dict.fromkeys(steam_names + nonsteam_names))   # deduplicated, order preserved

        # --- Tournament: groups of 5, no anchor ---
        results = run_tournament(
            pool,
            pytrends=pytrends,
            progress_callback=lambda msg: _trends_thread_state.update({"progress": msg}),
        )
        anchor = next((r["game"] for r in results if r.get("champion")), pool[0] if pool else "Minecraft")

        # --- Scoring: all games vs anchor ---
        all_games = list(dict.fromkeys(steam_names + nonsteam_names))
        scores = {}
        batches = [all_games[i:i + BATCH_SIZE] for i in range(0, len(all_games), BATCH_SIZE)]
        for i, batch in enumerate(batches):
            _trends_thread_state["progress"] = (
                f"Scoring {min((i + 1) * BATCH_SIZE, len(all_games))}/{len(all_games)}"
                f" vs '{anchor}'"
            )
            scores.update(compare_group(batch, anchor=anchor, pytrends=pytrends))
            if i < len(batches) - 1:
                time.sleep(CALL_SLEEP)

        _trends_thread_state["result"] = {
            "scores": scores,
            "anchor": anchor,
            "tournament_results": results,
        }
    except Exception as e:
        _trends_thread_state["result"] = {"error": str(e)}
    finally:
        _trends_thread_state["running"] = False
