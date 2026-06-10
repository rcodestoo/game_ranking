"""
DataForSEO Google Trends client.

Compares game names within category 41 (Computer & Video Games), worldwide, past month.
Max 5 keywords per request (Google Trends hard limit).
Auth: HTTP Basic (login + password from DataForSEO dashboard).

Uses the task-based endpoint (task_post → task_get).
This delegates Google Trends rate-limiting to DataForSEO's internal queue.
"""

import json
import logging
import random
import time
import requests
from pathlib import Path

log = logging.getLogger(__name__)

BASE_URL       = "https://api.dataforseo.com/v3"
GAMES_CATEGORY = 41   # Computer & Video Games
MAX_KEYWORDS   = 5
MAX_TASKS_PER_POST = 100  # DataForSEO hard limit — >100 tasks per POST returns error 40006

TASK_POST_URL   = f"{BASE_URL}/keywords_data/google_trends/explore/task_post"
TASK_GET_URL    = f"{BASE_URL}/keywords_data/google_trends/explore/task_get"
TASKS_READY_URL = f"{BASE_URL}/keywords_data/google_trends/explore/tasks_ready"
POLL_INTERVAL = 10   # seconds between task_get polls
POLL_TIMEOUT  = 120  # total seconds to wait for a task to complete (2 min — fail fast so progress updates appear promptly)
POST_RETRIES  = 3    # attempts to submit the task

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


# ── Task helpers ──────────────────────────────────────────────────────────────

def _is_dns_error(e: Exception) -> bool:
    s = str(e)
    return "getaddrinfo failed" in s or "Errno 11001" in s


def _post_task(
    payload: list[dict],
    login: str,
    password: str,
) -> str | None:
    """
    Submit a task to DataForSEO's task_post endpoint.
    Returns the task_id string on success, None on failure.
    """
    for attempt in range(POST_RETRIES):
        try:
            resp = requests.post(
                TASK_POST_URL,
                json=payload,
                auth=(login, password),
                timeout=30,
            )
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning("DataForSEO rate limited (429), waiting %ds before retry", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            candidate = resp.json()
        except Exception as e:
            if _is_dns_error(e):
                log.error("DataForSEO DNS resolution failed — check network connectivity")
                return None
            log.warning("DataForSEO task_post attempt %d failed: %s", attempt + 1, e)
            if attempt < POST_RETRIES - 1:
                time.sleep(5 * (attempt + 1) + random.uniform(0, 2))
                continue
            log.error("DataForSEO task_post failed after %d attempts", POST_RETRIES)
            return None

        tasks = candidate.get("tasks", [])
        if not tasks:
            log.warning("DataForSEO task_post: empty tasks in response")
            return None

        task = tasks[0]
        status_code = task.get("status_code")
        task_id = task.get("id")

        if status_code in (20100, 20000) and task_id:
            return task_id

        log.warning(
            "DataForSEO task_post attempt %d unexpected status %s: %s",
            attempt + 1, status_code, task.get("status_message"),
        )
        if attempt < POST_RETRIES - 1:
            time.sleep(5 * (attempt + 1) + random.uniform(0, 2))

    return None


def _poll_task(
    task_id: str,
    login: str,
    password: str,
) -> dict | None:
    """
    Poll task_get/{task_id} until the task completes or POLL_TIMEOUT elapses.
    Returns the completed tasks[0] dict on success, None on failure or timeout.
    """
    # Status codes that mean "still working — keep polling"
    _IN_PROGRESS = {20100, 20200, 40600, 40601, 40602}
    # Status codes that mean the task failed and polling is pointless
    _TERMINAL_ERRORS = {40101, 40400}

    deadline = time.monotonic() + POLL_TIMEOUT
    poll_count = 0

    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL)
        poll_count += 1

        try:
            resp = requests.get(
                f"{TASK_GET_URL}/{task_id}",
                auth=(login, password),
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            if _is_dns_error(e):
                log.error("DataForSEO DNS resolution failed during polling")
                return None
            log.warning("DataForSEO poll %d failed: %s — retrying", poll_count, e)
            continue

        tasks = data.get("tasks", [])
        if not tasks:
            log.warning("DataForSEO poll %d: empty tasks array", poll_count)
            continue

        task = tasks[0]
        status_code = task.get("status_code")

        if status_code == 20000:
            return task

        if status_code in _IN_PROGRESS:
            continue

        if status_code in _TERMINAL_ERRORS or (status_code and status_code >= 50000):
            log.error(
                "DataForSEO task %s failed with status %s: %s",
                task_id, status_code, task.get("status_message"),
            )
            return None

        log.warning(
            "DataForSEO task %s unexpected status %s: %s — continuing to poll",
            task_id, status_code, task.get("status_message"),
        )

    log.error(
        "DataForSEO task %s timed out after %ds (%d polls)",
        task_id, POLL_TIMEOUT, poll_count,
    )
    return None


# ── Bulk task submission ──────────────────────────────────────────────────────

def post_tasks_bulk(
    task_payloads: list[dict],
    login: str,
    password: str,
) -> list[str | None]:
    """
    Submit up to MAX_TASKS_PER_POST task objects in a single POST call.

    Per DataForSEO docs: send all tasks as a JSON array (max 100 per call).
    Returns a list of task_id strings aligned by index (None for failed tasks).
    Caller must chunk if len(task_payloads) > MAX_TASKS_PER_POST.
    """
    if not task_payloads:
        return []
    chunk = task_payloads[:MAX_TASKS_PER_POST]
    task_ids: list[str | None] = []

    for attempt in range(POST_RETRIES):
        try:
            resp = requests.post(
                TASK_POST_URL,
                json=chunk,
                auth=(login, password),
                timeout=30,
            )
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning("DataForSEO rate limited (429), waiting %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            if _is_dns_error(e):
                log.error("DataForSEO DNS error in post_tasks_bulk")
                return []
            log.warning("post_tasks_bulk attempt %d: %s", attempt + 1, e)
            if attempt < POST_RETRIES - 1:
                time.sleep(5 * (attempt + 1) + random.uniform(0, 2))
                continue
            return []

        for task in data.get("tasks", []):
            tid = task.get("id")
            sc  = task.get("status_code")
            if sc in (20100, 20000) and tid:
                task_ids.append(tid)
            else:
                log.warning("post_tasks_bulk task error %s: %s", sc, task.get("status_message"))
                task_ids.append(None)
        break

    return task_ids


def fetch_tasks_ready(login: str, password: str) -> set[str]:
    """
    GET /v3/keywords_data/google_trends/explore/tasks_ready.
    Returns the set of task_id strings DataForSEO has completed.
    Returns empty set on any error.
    """
    try:
        resp = requests.get(TASKS_READY_URL, auth=(login, password), timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning("fetch_tasks_ready failed: %s", e)
        return set()

    ready_ids: set[str] = set()
    for task in data.get("tasks", []):
        for item in (task.get("result") or []):
            tid = item.get("id")
            if tid:
                ready_ids.add(tid)
    return ready_ids


def fetch_task_result(
    task_id: str,
    kw_list: list[str],
    login: str,
    password: str,
) -> dict[str, float]:
    """
    Fetch and parse a single completed task result.
    Task must already appear in tasks_ready — the first _poll_task call
    will return status 20000 immediately without looping.
    Returns {keyword: score} or all-zeros on failure.
    """
    task = _poll_task(task_id, login, password)
    if task is None:
        return {g: 0.0 for g in kw_list}
    parsed = _parse_task(task, kw_list)
    return parsed if parsed is not None else {g: 0.0 for g in kw_list}


# ── Core fetch ────────────────────────────────────────────────────────────────

def _parse_task(task: dict, kw_list: list[str]) -> dict[str, float] | None:
    """
    Extract {keyword: score} from a completed DataForSEO task dict.
    Returns None if the task result is missing or malformed.
    """
    status_code = task.get("status_code")
    if status_code != 20000:
        log.warning("DataForSEO task error %s: %s", status_code, task.get("status_message"))
        return None

    result = (task.get("result") or [None])[0]
    if not result:
        log.warning("DataForSEO: null result")
        return None

    items = result.get("items") or []
    graph_item = next((i for i in items if i.get("type") == "google_trends_graph"), None)
    if not graph_item:
        log.warning("DataForSEO: no google_trends_graph in items")
        return None

    averages = graph_item.get("averages")
    if averages and len(averages) == len(kw_list):
        return {kw_list[i]: float(averages[i]) for i in range(len(kw_list))}

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


def fetch_comparison(
    games: list[str],
    login: str,
    password: str,
    category_code: int = GAMES_CATEGORY,
) -> dict[str, float]:
    """
    Compare up to 5 game names via DataForSEO Google Trends (task queue).
    Worldwide, past 30 days, category 41 (Computer & Video Games).

    Returns {game_name: mean_interest_score} (0-100). Returns 0.0 on error.
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

    log.info("DataForSEO task queue: submitting for %s", kw_list)

    task_id = _post_task(payload, login, password)
    if task_id is None:
        log.error("DataForSEO task queue: failed to submit task for %s", kw_list)
        return {g: 0.0 for g in kw_list}

    log.info("DataForSEO task queue: task %s submitted, polling...", task_id)
    task = _poll_task(task_id, login, password)
    if task is None:
        log.error("DataForSEO task queue: task %s did not complete successfully", task_id)
        return {g: 0.0 for g in kw_list}

    parsed = _parse_task(task, kw_list)
    if parsed is not None:
        score_str = ", ".join(f"{k}={v:.1f}" for k, v in parsed.items())
        log.info("DataForSEO task queue: task %s complete — %s", task_id, score_str)
        return parsed
    log.error("DataForSEO task queue: task %s parse failed", task_id)
    return {g: 0.0 for g in kw_list}
