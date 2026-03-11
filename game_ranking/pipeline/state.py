"""
Manages scraper_state.json — tracks last_run_date and processed app IDs per scraper.
Lives at: default_files/scraper_state.json
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

STATE_FILE = Path(__file__).resolve().parent.parent / "default_files" / "scraper_state.json"

DEFAULT_STATE = {
    "steam": {
        "last_run_date": None,       # ISO date string of last successful run
        "window_start": None,        # Start of the date window used in last run
        "window_end": None,          # End of the date window used in last run
    },
    "non_steam": {
        "last_run_date": None,
        "window_start": None,
        "window_end": None,
    }
}


def load_state() -> dict:
    """Load state from disk. Returns default state if file doesn't exist."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            # Ensure all keys exist (handles partial state files)
            for scraper in DEFAULT_STATE:
                if scraper not in state:
                    state[scraper] = DEFAULT_STATE[scraper].copy()
                for key in DEFAULT_STATE[scraper]:
                    if key not in state[scraper]:
                        state[scraper][key] = DEFAULT_STATE[scraper][key]
            return state
        except (json.JSONDecodeError, Exception):
            return {k: v.copy() for k, v in DEFAULT_STATE.items()}
    return {k: v.copy() for k, v in DEFAULT_STATE.items()}


def save_state(state: dict):
    """Save state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_next_window(scraper: str, window_days: int = 14) -> tuple[str, str]:
    """
    Calculate the next date window to scrape.

    If never run before: returns (today - window_days, today).
    If previously run: returns (last window_end, last window_end + window_days).

    Returns:
        (start_date, end_date) as ISO strings "YYYY-MM-DD"
    """
    state = load_state()
    scraper_state = state.get(scraper, {})
    last_end = scraper_state.get("window_end")

    if last_end:
        start = datetime.fromisoformat(last_end)
    else:
        # First run — look back window_days from today
        start = datetime.now() - timedelta(days=window_days)

    end = start + timedelta(days=window_days)

    # Don't scrape into the future
    today = datetime.now()
    if end > today:
        end = today

    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def mark_run_complete(scraper: str, window_start: str, window_end: str):
    """Update state after a successful scraper run."""
    state = load_state()
    state[scraper]["last_run_date"] = datetime.now().isoformat()
    state[scraper]["window_start"] = window_start
    state[scraper]["window_end"] = window_end
    save_state(state)


def get_last_run_info(scraper: str) -> dict:
    """Get last run info for display in Streamlit."""
    state = load_state()
    return state.get(scraper, DEFAULT_STATE.get(scraper, {}))
