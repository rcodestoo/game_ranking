"""
Background trends pipeline: runs a tournament to find the anchor champion,
then scores all games against that anchor on a consistent scale.
Uses DataForSEO Google Trends (replaces pytrends).
"""

import json
import time
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd

from calculation.trends_tournament import run_tournament, compare_group, BATCH_SIZE, CALL_SLEEP, MAX_PARALLEL_CALLS
from calculation.dataforseo_trends import load_credentials
from app.thread_state import _trends_thread_state
from config import TOURNAMENT_ANCHOR_FILE


def save_tournament_anchor(anchor: str) -> None:
    """Persist the tournament champion + timestamp to cache/tournament_anchor.json."""
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

def run_trends_pipeline(steam_df, nonsteam_df, appended_since=None):
    """Spawn background thread. Called after CSV load or manually from the tournament tab."""
    if _trends_thread_state["running"]:
        return

    login, password = load_credentials()
    if not login or not password:
        _trends_thread_state["result"] = {"error": "DataForSEO credentials not configured."}
        return

    steam_names    = _all_games(steam_df,    "Final Priority Score", "Name",       appended_since=appended_since)
    nonsteam_names = _all_games(nonsteam_df, "priority_score",       "Game Title", appended_since=appended_since)
    _trends_thread_state.update({"running": True, "result": None, "progress": "Starting..."})
    threading.Thread(
        target=_worker,
        args=(steam_names, nonsteam_names, login, password),
        daemon=True,
    ).start()


def _all_games(df, score_col, name_col, appended_since=None):
    if df is None or name_col not in df.columns:
        return []
    if score_col in df.columns:
        df = df.sort_values(score_col, ascending=False)
    if appended_since is not None and "date_appended" in df.columns:
        da = pd.to_datetime(df["date_appended"], errors="coerce")
        df = df[da >= pd.Timestamp(appended_since)]
    return list(df[name_col].dropna().unique())


def _worker(steam_names, nonsteam_names, login, password):
    try:
        pool = list(dict.fromkeys(steam_names + nonsteam_names))   # deduplicated, order preserved

        # --- Tournament: groups of 5, no anchor ---
        results = run_tournament(
            pool,
            login=login,
            password=password,
            progress_callback=lambda msg: _trends_thread_state.update({"progress": msg}),
        )
        anchor = next(
            (r["game"] for r in results if r.get("champion")),
            pool[0] if pool else "Minecraft",
        )
        save_tournament_anchor(anchor)

        # --- Scoring: all games vs anchor, 4 games + anchor per batch (parallel) ---
        all_games = list(dict.fromkeys(steam_names + nonsteam_names))
        batches = [all_games[i:i + BATCH_SIZE] for i in range(0, len(all_games), BATCH_SIZE)]

        def _score_batch(batch):
            result = compare_group(batch, login=login, password=password, anchor=anchor)
            time.sleep(CALL_SLEEP)   # pace each worker; same rate as before, just parallel
            return result

        scores = {}
        completed = 0
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_CALLS) as executor:
            futures = {executor.submit(_score_batch, batch): batch for batch in batches}
            for future in as_completed(futures):
                scores.update(future.result())
                completed += 1
                _trends_thread_state["progress"] = (
                    f"Scoring {min(completed * BATCH_SIZE, len(all_games))}/{len(all_games)}"
                    f" vs '{anchor}'"
                )

        _trends_thread_state["result"] = {
            "scores":            scores,
            "anchor":            anchor,
            "tournament_results": results,
        }
    except Exception as e:
        _trends_thread_state["result"] = {"error": f"{e}\n{traceback.format_exc()}"}
    finally:
        _trends_thread_state["running"] = False
