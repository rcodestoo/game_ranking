"""
Tournament state manager.

Owns cache/tournament_state.json — pure load/save/mutate, no API calls.

State schema:
{
  "pingback_url": "https://...",
  "status": "idle|running|complete",
  "steam": {
    "rounds": {
      "1": {
        "is_final": false,
        "tasks": [
          {
            "task_id": "abc123",
            "keywords": ["Game A", ...],
            "cleaned_keywords": ["game a", ...],
            "scores": {},
            "winner": null,
            "status": "pending|complete|failed"
          }
        ],
        "bye_games": ["Lone Game X"]
      }
    },
    "current_round": 1,
    "pool": [...],          # games entering the current round
    "finalists": [],
    "all_bye_games": []
  },
  "non_steam": { ...same... },
  "anchor_pool": [],
  "selected_anchors": []
}
"""

import json
import tempfile
import os
from pathlib import Path

from config import TOURNAMENT_STATE_FILE, MANUAL_TOURNAMENT_STATE_FILE

PINGBACK_URL = "https://gameranking-research-ags.streamlit.app/"
BRACKETS = ("steam", "non_steam")


# ── Template helpers ──────────────────────────────────────────────────────────

def _empty_bracket() -> dict:
    return {
        "rounds":       {},
        "current_round": 1,
        "pool":         [],
        "finalists":    [],
        "all_bye_games": [],
    }


def _empty_state() -> dict:
    return {
        "pingback_url":    PINGBACK_URL,
        "status":          "idle",
        "steam":           _empty_bracket(),
        "non_steam":       _empty_bracket(),
        "anchor_pool":     [],
        "selected_anchors": [],
    }


# ── Persistence ───────────────────────────────────────────────────────────────

def load_state() -> dict:
    """Return current state from file, or a fresh idle state if missing/corrupt."""
    if TOURNAMENT_STATE_FILE.exists():
        try:
            return json.loads(TOURNAMENT_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return _empty_state()


def save_state(state: dict) -> None:
    """Atomically write state to the state file."""
    TOURNAMENT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = TOURNAMENT_STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(TOURNAMENT_STATE_FILE)


def reset_state(steam_games: list[str], nonsteam_games: list[str], pingback_url: str = PINGBACK_URL) -> dict:
    """Build a fresh state from the given game lists, save and return it."""
    state = _empty_state()
    state["pingback_url"] = pingback_url
    state["steam"]["pool"] = list(steam_games)
    state["non_steam"]["pool"] = list(nonsteam_games)
    save_state(state)
    return state


# ── Round queries ─────────────────────────────────────────────────────────────

def _current_round_data(state: dict, bracket: str) -> dict | None:
    """Return the current round dict for a bracket, or None if not started."""
    rnum = str(state[bracket]["current_round"])
    return state[bracket]["rounds"].get(rnum)


def is_round_complete(state: dict, bracket: str) -> bool:
    """True when every task in the current round has status complete or failed."""
    rdata = _current_round_data(state, bracket)
    if rdata is None:
        return False
    tasks = rdata.get("tasks", [])
    if not tasks:
        # Round has only bye games — trivially complete
        return True
    return all(t["status"] in ("complete", "failed") for t in tasks)


def get_pending_task_ids(state: dict) -> dict[str, tuple[str, int, int]]:
    """
    Return {task_id: (bracket, round_num_int, task_idx)} for every pending task
    across both brackets (all rounds, not just current).
    """
    pending: dict[str, tuple[str, int, int]] = {}
    for bracket in BRACKETS:
        for rnum_str, rdata in state[bracket]["rounds"].items():
            rnum = int(rnum_str)
            for idx, task in enumerate(rdata.get("tasks", [])):
                if task["status"] == "pending" and task.get("task_id"):
                    pending[task["task_id"]] = (bracket, rnum, idx)
    return pending


# ── Result updates ────────────────────────────────────────────────────────────

def update_task_result(
    state: dict,
    bracket: str,
    round_num: int,
    task_idx: int,
    scores: dict[str, float],
    winner: str | None,
) -> None:
    """Mutate state in place: record scores + winner for a completed task."""
    task = state[bracket]["rounds"][str(round_num)]["tasks"][task_idx]
    task["scores"] = scores
    task["winner"] = winner
    task["status"] = "complete" if any(v > 0 for v in scores.values()) else "failed"


# ── Round advancement ─────────────────────────────────────────────────────────

def advance_bracket(state: dict, bracket: str) -> None:
    """
    Called after a round completes. Computes the next pool from this round's
    task winners + bye_games, then either:
      - pool == 0 or 1 → marks sole finalist(s), bracket done
      - pool <= 5      → marks next round as final (top-2 finalists extracted from scores)
      - pool > 5       → normal next round
    Mutates state in place but does NOT save.
    """
    rnum = state[bracket]["current_round"]
    rdata = state[bracket]["rounds"][str(rnum)]

    # Collect winners from tasks (respecting order)
    winners: list[str] = []
    for task in rdata.get("tasks", []):
        w = task.get("winner")
        if w:
            winners.append(w)
        elif task["status"] == "failed" and task["keywords"]:
            # Fallback: advance first keyword so tournament isn't stuck
            winners.append(task["keywords"][0])

    bye_games = rdata.get("bye_games", [])
    new_pool = winners + bye_games

    if len(new_pool) == 0:
        # Nothing to do — bracket has no games
        state[bracket]["finalists"] = []
        return

    if len(new_pool) == 1:
        state[bracket]["finalists"] = new_pool
        return

    # Advance to next round
    next_rnum = rnum + 1
    state[bracket]["current_round"] = next_rnum
    state[bracket]["pool"] = new_pool
    state[bracket]["rounds"][str(next_rnum)] = {
        "is_final": len(new_pool) <= 5,
        "tasks":     [],
        "bye_games": [],
    }


def extract_finalists_from_final_round(state: dict, bracket: str) -> list[str]:
    """
    Called after a round marked is_final=True completes.
    Aggregates all scores across the round's tasks (there should be exactly 1
    for a final round, but handle multiple just in case).
    Returns top 2 games by score.
    """
    rnum = str(state[bracket]["current_round"])
    rdata = state[bracket]["rounds"][rnum]
    all_scores: dict[str, float] = {}
    for task in rdata.get("tasks", []):
        for kw, sc in task.get("scores", {}).items():
            all_scores[kw] = max(all_scores.get(kw, 0.0), sc)

    if not all_scores:
        # All-zero / failed — fall back to first 2 keywords in pool order
        pool = state[bracket]["pool"]
        return pool[:2]

    ranked = sorted(all_scores, key=all_scores.get, reverse=True)
    return ranked[:2]


# ── Anchor pool assembly ──────────────────────────────────────────────────────

# ── Manual tournament state ───────────────────────────────────────────────────

def _empty_grand_final() -> dict:
    return {
        "steam_champion":    None,
        "nonsteam_champion": None,
        "task_id":           None,
        "keywords":          [],
        "cleaned_keywords":  [],
        "scores":            {},
        "winner":            None,
        "status":            "idle",
    }


def _empty_manual_state() -> dict:
    return {
        "pingback_url": PINGBACK_URL,
        "steam":        _empty_bracket(),
        "non_steam":    _empty_bracket(),
        "grand_final":  _empty_grand_final(),
    }


def load_manual_state() -> dict:
    """Return manual tournament state from file, or a fresh empty state."""
    if MANUAL_TOURNAMENT_STATE_FILE.exists():
        try:
            return json.loads(MANUAL_TOURNAMENT_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return _empty_manual_state()


def save_manual_state(state: dict) -> None:
    """Atomically write manual state to file."""
    MANUAL_TOURNAMENT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANUAL_TOURNAMENT_STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(MANUAL_TOURNAMENT_STATE_FILE)


def reset_manual_bracket(state: dict, bracket: str, games: list[str]) -> None:
    """Reset a single bracket in the manual state dict (mutates in place)."""
    state[bracket] = _empty_bracket()
    state[bracket]["pool"] = list(games)
    state["grand_final"] = _empty_grand_final()


def get_manual_pending_task_ids(state: dict, bracket: str) -> dict[str, tuple[str, int, int]]:
    """Return {task_id: (bracket, round_num_int, task_idx)} for pending tasks in one bracket."""
    pending: dict[str, tuple[str, int, int]] = {}
    for rnum_str, rdata in state[bracket]["rounds"].items():
        rnum = int(rnum_str)
        for idx, task in enumerate(rdata.get("tasks", [])):
            if task["status"] == "pending" and task.get("task_id"):
                pending[task["task_id"]] = (bracket, rnum, idx)
    return pending


# ── Anchor pool assembly ──────────────────────────────────────────────────────

def assemble_anchor_pool(state: dict) -> list[str]:
    """
    Build anchor pool: Steam finalists + Non-Steam finalists + all bye-games
    (deduplicated, order preserved). Saves to state["anchor_pool"].
    """
    seen: set[str] = set()
    pool: list[str] = []

    def _add(game: str) -> None:
        if game not in seen:
            seen.add(game)
            pool.append(game)

    for bracket in BRACKETS:
        for g in state[bracket].get("finalists", []):
            _add(g)

    for bracket in BRACKETS:
        for g in state[bracket].get("all_bye_games", []):
            _add(g)

    state["anchor_pool"] = pool
    return pool
