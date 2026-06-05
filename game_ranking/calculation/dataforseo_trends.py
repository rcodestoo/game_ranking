"""
DataForSEO Google Trends client.

Compares game names within category 41 (Computer & Video Games), worldwide, past month.
Max 5 keywords per request (Google Trends hard limit).
Auth: HTTP Basic (login + password from DataForSEO dashboard).
"""

import json
import logging
import time
import requests
from pathlib import Path

log = logging.getLogger(__name__)

BASE_URL       = "https://api.dataforseo.com/v3"
GAMES_CATEGORY = 41   # Computer & Video Games
MAX_KEYWORDS   = 5

CREDS_FILE = Path(__file__).parent.parent / "cache" / "dataforseo_creds.json"

def _date_range() -> tuple[str, str]:
    from datetime import date, timedelta
    today = date.today()
    return (today - timedelta(days=30)).isoformat(), today.isoformat()


# ── Credentials ───────────────────────────────────────────────────────────────

def load_credentials() -> tuple[str, str]:
    """Return (login, password) from cache/dataforseo_creds.json, or ('', '')."""
    if CREDS_FILE.exists():
        try:
            data = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
            return data.get("login", ""), data.get("password", "")
        except Exception:
            pass
    return "", ""


def save_credentials(login: str, password: str) -> None:
    """Persist credentials to cache/dataforseo_creds.json."""
    CREDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CREDS_FILE.write_text(
        json.dumps({"login": login, "password": password}, indent=2),
        encoding="utf-8",
    )


# ── Core fetch ────────────────────────────────────────────────────────────────

def fetch_comparison(
    games: list[str],
    login: str,
    password: str,
    category_code: int = GAMES_CATEGORY,
) -> dict[str, float]:
    """
    Compare up to 5 game names via DataForSEO Google Trends (live endpoint).
    Worldwide, past 30 days, category 41 (Computer & Video Games).

    Returns {game_name: mean_interest_score} where scores are 0-100 relative
    to each other within the batch (same semantics as Google Trends explore).
    Returns 0.0 for every game on any error.
    """
    kw_list = [g for g in games[:MAX_KEYWORDS] if g and g.strip()]
    if not kw_list:
        log.warning("DataForSEO: no valid keywords after filtering empty entries")
        return {g: 0.0 for g in games[:MAX_KEYWORDS]}

    date_from, date_to = _date_range()
    payload = [{
        "keywords":      kw_list,
        "category_code": category_code,
        "date_from":     date_from,
        "date_to":       date_to,
        "type":          "web",
        "item_types":    ["google_trends_graph"],
    }]

    resp_data = None
    for attempt in range(3):
        try:
            resp = requests.post(
                f"{BASE_URL}/keywords_data/google_trends/explore/live",
                json=payload,
                auth=(login, password),
                timeout=150,
            )
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning("DataForSEO rate limited (429), waiting %ds before retry", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            resp_data = resp.json()
            break
        except Exception as e:
            log.warning("DataForSEO attempt %d failed: %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                log.error("DataForSEO request failed after 3 attempts: %s", e)
                return {g: 0.0 for g in kw_list}

    if resp_data is None:
        return {g: 0.0 for g in kw_list}

    # ── Parse response ────────────────────────────────────────────────────────
    tasks = resp_data.get("tasks", [])
    if not tasks:
        log.warning("DataForSEO: empty tasks array")
        return {g: 0.0 for g in kw_list}

    task = tasks[0]
    status_code = task.get("status_code")
    if status_code != 20000:
        log.warning("DataForSEO task error %s: %s", status_code, task.get("status_message"))
        return {g: 0.0 for g in kw_list}

    result = (task.get("result") or [None])[0]
    if not result:
        log.warning("DataForSEO: null result")
        return {g: 0.0 for g in kw_list}

    items = result.get("items") or []
    graph_item = next((i for i in items if i.get("type") == "google_trends_graph"), None)
    if not graph_item:
        log.warning("DataForSEO: no google_trends_graph in items")
        return {g: 0.0 for g in kw_list}

    # Use pre-computed averages if present (indexed by keyword position)
    averages = graph_item.get("averages")
    if averages and len(averages) == len(kw_list):
        return {kw_list[i]: float(averages[i]) for i in range(len(kw_list))}

    # Fallback: compute mean from daily data points
    trend_points = graph_item.get("data") or []
    sums   = [0.0] * len(kw_list)
    counts = [0]   * len(kw_list)
    for point in trend_points:
        values = point.get("values", [])
        for i, v in enumerate(values):
            if i < len(kw_list) and v is not None:
                sums[i]   += float(v)
                counts[i] += 1

    return {
        kw_list[i]: round(sums[i] / counts[i], 2) if counts[i] > 0 else 0.0
        for i in range(len(kw_list))
    }
