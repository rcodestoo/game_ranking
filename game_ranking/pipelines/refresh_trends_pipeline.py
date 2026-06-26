"""
Refresh Trends pipeline — async batch-POST pattern for the Refresh Trends buttons.

Flow:
  submit_refresh()   → build one [anchor, game] task per game → bulk POST → save state
  collect_refresh()  → poll tasks_ready → fetch results → normalize → return scores

Each task contains exactly 2 keywords: [cleaned_anchor, cleaned_game].
Normalization: score = (game_raw / anchor_raw) * 100  (anchor = 100 reference).
"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from config import (
    REFRESH_TRENDS_STATE_FILE,
    TOURNAMENT_STATE_FILE,
    TRENDS_CACHE_FILE,
)
from calculation.dataforseo_trends import (
    post_tasks_bulk,
    fetch_tasks_ready,
    fetch_task_result,
    GAMES_CATEGORY,
)
from calculation.trends_tournament import strip_edition_suffix

log = logging.getLogger(__name__)

_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_MAX_TASKS_PER_POST = 100


# ── State helpers ─────────────────────────────────────────────────────────────

def _empty_state() -> dict:
    return {
        "status":          "idle",
        "submitted_at":    None,
        "anchor":          None,
        "anchor_cleaned":  None,
        "tasks":           [],
        "source":          None,
        "collected_count": 0,
        "error_count":     0,
    }


def _resolve_state_file(state_file) -> Path:
    return Path(state_file) if state_file is not None else REFRESH_TRENDS_STATE_FILE


def load_state(state_file=None) -> dict:
    """Return refresh state from file, or a fresh idle state if missing/corrupt."""
    f = _resolve_state_file(state_file)
    if f.exists():
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            pass
    return _empty_state()


def save_state(state: dict, state_file=None) -> None:
    """Atomically write state to the state file."""
    f = _resolve_state_file(state_file)
    f.parent.mkdir(parents=True, exist_ok=True)
    tmp = f.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(f)


def reset_state(state_file=None) -> None:
    """Reset state file to idle (discard any pending tasks)."""
    save_state(_empty_state(), state_file=state_file)


def save_refresh_anchor(anchor: str, state_file=None) -> None:
    """Persist the selected anchor into the refresh state file (independent of tournament)."""
    state = load_state(state_file=state_file)
    state["anchor"] = anchor
    state["anchor_cleaned"] = strip_edition_suffix(anchor)
    save_state(state, state_file=state_file)


def load_refresh_anchor(state_file=None) -> str | None:
    """Return the last-saved refresh anchor, or None if never set."""
    return load_state(state_file=state_file).get("anchor")


# ── Anchor pool ───────────────────────────────────────────────────────────────

def load_anchor_pool() -> list[str]:
    """
    Build the anchor pool from tournament state.

    Uses the dedicated anchor_pool field when the tournament is complete.
    Otherwise builds from finalists + bye games already in the state.
    Always appends the currently saved anchor (tournament_anchor.json) as a
    fallback so the selectbox always has at least one option.
    """
    from pipelines.trends_pipeline import load_tournament_anchor

    seen: set[str] = set()
    pool: list[str] = []

    def _add(game: str) -> None:
        if game and game not in seen:
            seen.add(game)
            pool.append(game)

    if TOURNAMENT_STATE_FILE.exists():
        try:
            data = json.loads(TOURNAMENT_STATE_FILE.read_text(encoding="utf-8"))
            # Prefer the fully assembled anchor_pool when available
            assembled = data.get("anchor_pool", [])
            if assembled:
                for g in assembled:
                    _add(g)
            else:
                # Tournament incomplete — build from whatever is available so far
                for bracket in ("steam", "non_steam"):
                    b = data.get(bracket, {})
                    for g in b.get("finalists", []):
                        _add(g)
                for bracket in ("steam", "non_steam"):
                    b = data.get(bracket, {})
                    for g in b.get("all_bye_games", []):
                        _add(g)
        except Exception:
            pass

    # Always include the currently saved anchor so the selectbox is never empty
    anchor_info = load_tournament_anchor()
    if anchor_info and anchor_info.get("anchor"):
        _add(anchor_info["anchor"])

    return pool


# ── Date range ────────────────────────────────────────────────────────────────

def _date_range() -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=30)).isoformat(), today.isoformat()


# ── Submit ────────────────────────────────────────────────────────────────────

def submit_refresh(
    games: list[str],
    anchor: str,
    login: str,
    password: str,
    source: str = "refresh_all",
    state_file=None,
) -> dict:
    """
    Build one DataForSEO task per game (keywords=[cleaned_anchor, cleaned_game]),
    batch-POST all tasks at once, persist state. Returns the new state dict.

    Overwrites any previous refresh state — callers should not call this while
    a previous run has pending tasks unless they intend to discard it.
    """
    cleaned_anchor = strip_edition_suffix(anchor)
    date_from, date_to = _date_range()

    task_payloads: list[dict] = []
    task_metas:    list[dict] = []

    for game in games:
        cleaned_game = strip_edition_suffix(game)
        task_payloads.append({
            "keywords":      [cleaned_anchor, cleaned_game],
            "category_code": GAMES_CATEGORY,
            "date_from":     date_from,
            "date_to":       date_to,
            "type":          "web",
            "item_types":    ["google_trends_graph"],
        })
        task_metas.append({"game": game, "cleaned_game": cleaned_game})

    # Chunk into MAX_TASKS_PER_POST=100 per POST call
    all_task_ids: list[str | None] = []
    for chunk_start in range(0, len(task_payloads), _MAX_TASKS_PER_POST):
        chunk = task_payloads[chunk_start:chunk_start + _MAX_TASKS_PER_POST]
        ids = post_tasks_bulk(chunk, login, password)
        # Pad with None if post returned fewer IDs than tasks
        while len(ids) < len(chunk):
            ids.append(None)
        all_task_ids.extend(ids)

    tasks = []
    for meta, task_id in zip(task_metas, all_task_ids):
        tasks.append({
            "game":             meta["game"],
            "cleaned_game":     meta["cleaned_game"],
            "task_id":          task_id,
            "status":           "pending" if task_id else "failed",
            "raw_game_score":   None,
            "raw_anchor_score": None,
            "normalized_score": None,
        })
        if task_id:
            log.info("Submitted refresh task %s for %s", task_id, meta["game"])
        else:
            log.warning("Refresh task submission failed for %s", meta["game"])

    state = {
        "status":          "submitted",
        "submitted_at":    datetime.now().strftime(_DATE_FMT),
        "anchor":          anchor,
        "anchor_cleaned":  cleaned_anchor,
        "tasks":           tasks,
        "source":          source,
        "collected_count": 0,
        "error_count":     0,
    }
    save_state(state, state_file=state_file)
    return state


# ── Collect ───────────────────────────────────────────────────────────────────

def collect_refresh(
    login: str,
    password: str,
    on_task_complete=None,
    state_file=None,
) -> dict:
    """
    Poll DataForSEO tasks_ready, fetch completed tasks, normalize scores.

    Args:
      on_task_complete: optional callable(game, score, n_done, n_total, failed)
        called immediately after each task result is processed. Use this to
        drive real-time progress UI in the caller (e.g. Streamlit widgets).

    Returns:
      {
        "checked":   int,           # tasks from tasks_ready that were ours
        "collected": int,           # tasks successfully parsed (anchor_raw > 0)
        "errors":    int,           # tasks with all-zero scores
        "complete":  bool,          # True when no pending tasks remain
        "scores":    {game: int},   # normalized scores for collected tasks
      }

    Caller is responsible for applying scores to session_state and writing CSV.
    """
    state = load_state(state_file=state_file)
    if state["status"] not in ("submitted", "collecting"):
        return {"checked": 0, "collected": 0, "errors": 0, "complete": False, "scores": {}}

    state["status"] = "collecting"

    # Build pending map: task_id → task index
    pending_map: dict[str, int] = {
        t["task_id"]: i
        for i, t in enumerate(state["tasks"])
        if t["status"] == "pending" and t.get("task_id")
    }

    if not pending_map:
        state["status"] = "complete"
        save_state(state, state_file=state_file)
        return {
            "checked": 0, "collected": 0, "errors": 0,
            "complete": True, "scores": _extract_scores(state),
        }

    ready_ids = fetch_tasks_ready(login, password)
    our_ready = set(pending_map.keys()) & ready_ids

    checked   = len(our_ready)
    collected = 0
    errors    = 0
    scores:   dict[str, int] = {}
    n_total   = len(our_ready)
    n_done    = 0

    anchor_cleaned = state["anchor_cleaned"]

    for task_id in list(our_ready):
        task_idx = pending_map[task_id]
        task = state["tasks"][task_idx]

        # kw_list must match the submission order: [anchor, game]
        kw_list = [anchor_cleaned, task["cleaned_game"]]
        raw = fetch_task_result(task_id, kw_list, login, password)

        anchor_raw = raw.get(anchor_cleaned, 0.0)
        game_raw   = raw.get(task["cleaned_game"], 0.0)

        if anchor_raw > 0:
            normalized = round(game_raw / anchor_raw * 100, 2)
        else:
            normalized = 0.0

        task["raw_anchor_score"] = anchor_raw
        task["raw_game_score"]   = game_raw
        task["normalized_score"] = normalized
        # Mark "complete" if at least one score is non-zero, else "failed"
        failed = not (anchor_raw > 0 or game_raw > 0)
        task["status"] = "failed" if failed else "complete"

        scores[task["game"]] = int(normalized)
        n_done += 1

        if not failed:
            collected += 1
            log.info("Collected refresh task %s: %s → %.1f", task_id, task["game"], normalized)
        else:
            errors += 1
            log.warning("Refresh task %s all-zero scores for %s", task_id, task["game"])

        if on_task_complete is not None:
            try:
                on_task_complete(task["game"], int(normalized), n_done, n_total, failed)
            except Exception:
                pass  # never let a UI callback crash the pipeline

    state["collected_count"] = state.get("collected_count", 0) + collected
    state["error_count"]     = state.get("error_count", 0) + errors

    still_pending = any(t["status"] == "pending" for t in state["tasks"])
    if not still_pending:
        state["status"] = "complete"

    save_state(state, state_file=state_file)

    return {
        "checked":   checked,
        "collected": collected,
        "errors":    errors,
        "complete":  state["status"] == "complete",
        "scores":    scores,
    }


def _extract_scores(state: dict) -> dict[str, int]:
    """Extract {game: int_score} from all completed tasks in state."""
    return {
        t["game"]: int(t["normalized_score"] or 0)
        for t in state["tasks"]
        if t["status"] == "complete" and t.get("normalized_score") is not None
    }


# ── CSV write ─────────────────────────────────────────────────────────────────

def write_scores_to_csv(
    scores: dict[str, int],
    existing_trends: dict,
    games_fetched: list[str],
    cached_ts: dict,
) -> None:
    """
    Merge new scores into existing_trends and write to TRENDS_CACHE_FILE.
    Format matches the existing CSV: game_name, trends_score, fetched_at.
    Existing entries for games not in `scores` are preserved with their original timestamps.
    """
    now = datetime.now().strftime(_DATE_FMT)
    merged = dict(existing_trends)
    merged.update(scores)
    rows = [
        {
            "game_name":    g,
            "trends_score": s,
            "fetched_at":   now if g in games_fetched else cached_ts.get(g, now),
        }
        for g, s in merged.items()
    ]
    try:
        pd.DataFrame(rows).to_csv(TRENDS_CACHE_FILE, index=False)
    except Exception as e:
        log.error("Failed to write trends cache CSV: %s", e)
        raise
