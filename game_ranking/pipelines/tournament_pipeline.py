"""
Tournament pipeline — orchestration layer.

Handles API calls and drives state transitions via tournament_state.py.

Flow:
  start_tournament()  → reset state → submit_round() for both brackets
  collect_results()   → poll tasks_ready → fetch results → advance/submit next rounds
                        → when both brackets have finalists → assemble anchor pool

Pingback URL is set on every task. Result collection uses polling
(Streamlit cannot receive inbound POST requests).
"""

import logging
from datetime import date, timedelta

from calculation.dataforseo_trends import (
    post_tasks_bulk,
    fetch_tasks_ready,
    fetch_task_result,
    check_task,
    GAMES_CATEGORY,
)
from pipelines.trends_cache import (
    load_trends_cache,
    save_trends_cache,
    lookup_cached_scores,
    write_cached_scores,
)
from calculation.trends_tournament import strip_edition_suffix, TOURNAMENT_GROUP_SIZE
from pipelines.tournament_state import (
    load_state,
    save_state,
    reset_state,
    is_round_complete,
    get_pending_task_ids,
    update_task_result,
    advance_bracket,
    extract_finalists_from_final_round,
    assemble_anchor_pool,
    load_manual_state,
    save_manual_state,
    reset_manual_bracket,
    get_manual_pending_task_ids,
    PINGBACK_URL,
    BRACKETS,
)

log = logging.getLogger(__name__)

_GROUP_SIZE = TOURNAMENT_GROUP_SIZE  # 5


def _date_range() -> tuple[str, str]:
    today = date.today()
    return (today - timedelta(days=30)).isoformat(), today.isoformat()


# ── Round submission ──────────────────────────────────────────────────────────

def submit_round(
    state: dict,
    bracket: str,
    login: str,
    password: str,
    save_fn=None,
    cache: dict | None = None,
) -> dict:
    """
    Submit the current round for one bracket.

    Splits state[bracket]["pool"] into groups of 5:
      - Group of 1 → bye (auto-advance, no API call)
      - Group of 2–5 → one DataForSEO task (or cache hit — no API call)

    All non-bye, non-cached tasks are batch-POSTed in one call (up to 100 per POST).
    Task IDs are written back into state. State is saved before returning.
    """
    pool = state[bracket]["pool"]
    if not pool:
        return state

    rnum = state[bracket]["current_round"]
    rnum_str = str(rnum)

    # Ensure round dict exists
    if rnum_str not in state[bracket]["rounds"]:
        state[bracket]["rounds"][rnum_str] = {
            "is_final": len(pool) <= _GROUP_SIZE,
            "tasks":     [],
            "bye_games": [],
        }

    rdata = state[bracket]["rounds"][rnum_str]
    date_from, date_to = _date_range()
    pingback_url = state.get("pingback_url", PINGBACK_URL)

    # Build task dicts for non-bye groups; handle byes immediately
    task_dicts:  list[dict] = []
    task_metas:  list[dict] = []  # parallel list: original + cleaned keywords

    groups = [pool[i:i + _GROUP_SIZE] for i in range(0, len(pool), _GROUP_SIZE)]

    for group in groups:
        if len(group) == 1:
            # True bye — auto-advance, no task
            bye_game = group[0]
            rdata["bye_games"].append(bye_game)
            if bye_game not in state[bracket]["all_bye_games"]:
                state[bracket]["all_bye_games"].append(bye_game)
            log.info("[%s] Round %d: bye → %s", bracket, rnum, bye_game)
            continue

        cleaned = [strip_edition_suffix(g) for g in group]

        # Check cache before submitting to DataForSEO
        if cache is not None:
            cached_scores = lookup_cached_scores(cleaned, cache)
            if cached_scores is not None:
                scores_orig = {orig: cached_scores.get(clean, 0.0)
                               for orig, clean in zip(group, cleaned)}
                winner = max(scores_orig, key=scores_orig.get) if any(v > 0 for v in scores_orig.values()) else None
                rdata["tasks"].append({
                    "task_id":          None,
                    "keywords":         group,
                    "cleaned_keywords": cleaned,
                    "scores":           scores_orig,
                    "winner":           winner,
                    "status":           "cached",
                })
                log.info("[%s] Round %d: cache hit for %s — winner: %s", bracket, rnum, group, winner)
                continue

        task_dicts.append({
            "keywords":      cleaned,
            "category_code": GAMES_CATEGORY,
            "date_from":     date_from,
            "date_to":       date_to,
            "type":          "web",
            "item_types":    ["google_trends_graph"],
            "pingback_url":  pingback_url,
        })
        task_metas.append({"keywords": group, "cleaned_keywords": cleaned})

    # Batch-POST in chunks of 100
    task_ids: list[str | None] = []
    for chunk_start in range(0, len(task_dicts), 100):
        chunk = task_dicts[chunk_start:chunk_start + 100]
        ids = post_tasks_bulk(chunk, login, password)
        # Pad with None if post returned fewer IDs than tasks
        while len(ids) < len(chunk):
            ids.append(None)
        task_ids.extend(ids)

    # Write task records into state
    for meta, task_id in zip(task_metas, task_ids):
        rdata["tasks"].append({
            "task_id":         task_id,
            "keywords":        meta["keywords"],
            "cleaned_keywords": meta["cleaned_keywords"],
            "scores":          {},
            "winner":          None,
            "status":          "pending" if task_id else "failed",
        })
        if task_id:
            log.info("[%s] Round %d: submitted task %s for %s", bracket, rnum, task_id, meta["keywords"])
        else:
            log.warning("[%s] Round %d: task submission failed for %s", bracket, rnum, meta["keywords"])

    # If ALL groups were byes (very unlikely but possible), the round is already done
    (save_fn or save_state)(state)
    return state


# ── Result collection ─────────────────────────────────────────────────────────

def collect_results(login: str, password: str) -> dict:
    """
    Poll DataForSEO tasks_ready, fetch completed tasks, update state.
    Advances complete rounds and submits the next round automatically.
    Assembles the anchor pool when both brackets have finalists.

    Returns:
      {
        "checked":        int,   # tasks from tasks_ready that were ours
        "collected":      int,   # tasks successfully parsed
        "rounds_advanced": int,  # rounds that completed and advanced
        "errors":         int,   # tasks with all-zero scores
        "complete":       bool,  # True if tournament is fully done
      }
    """
    state = load_state()
    if state["status"] not in ("running",):
        return {"checked": 0, "collected": 0, "rounds_advanced": 0, "errors": 0, "complete": state["status"] == "complete"}

    cache = load_trends_cache()
    pending_map = get_pending_task_ids(state)
    if not pending_map:
        # No pending tasks — check if both brackets are actually done
        _check_completion(state, login, password, cache=cache)
        save_state(state)
        return {"checked": 0, "collected": 0, "rounds_advanced": 0, "errors": 0, "complete": state["status"] == "complete"}

    ready_ids = fetch_tasks_ready(login, password)
    our_ready = set(pending_map.keys()) & ready_ids

    if len(ready_ids) >= 1000:
        missing_from_ready = set(pending_map.keys()) - our_ready
        if missing_from_ready:
            log.info("tasks_ready saturated (1000 cap); checking %d pending tasks directly", len(missing_from_ready))
            for tid in missing_from_ready:
                if check_task(tid, login, password) is not None:
                    our_ready.add(tid)

    checked = len(our_ready)
    collected = 0
    errors = 0

    for task_id in list(our_ready):
        bracket, rnum, task_idx = pending_map[task_id]
        rdata = state[bracket]["rounds"][str(rnum)]
        task = rdata["tasks"][task_idx]
        cleaned_kws = task["cleaned_keywords"]
        orig_kws    = task["keywords"]

        scores_raw = fetch_task_result(task_id, cleaned_kws, login, password)

        # Map cleaned → original names
        scores_orig = {orig: scores_raw.get(clean, 0.0) for orig, clean in zip(orig_kws, cleaned_kws)}
        winner = max(scores_orig, key=scores_orig.get) if any(v > 0 for v in scores_orig.values()) else None

        update_task_result(state, bracket, rnum, task_idx, scores_orig, winner)

        if any(v > 0 for v in scores_orig.values()):
            collected += 1
            log.info("[%s] Task %s collected — winner: %s", bracket, task_id, winner)
            write_cached_scores(cleaned_kws, scores_raw, cache)
        else:
            errors += 1
            log.warning("[%s] Task %s all-zero scores", bracket, task_id)

    rounds_advanced = _check_completion(state, login, password, cache=cache)
    save_trends_cache(cache)
    save_state(state)

    return {
        "checked":        checked,
        "collected":      collected,
        "rounds_advanced": rounds_advanced,
        "errors":         errors,
        "complete":       state["status"] == "complete",
    }


def _check_completion(state: dict, login: str, password: str, cache: dict | None = None) -> int:
    """
    For each bracket: if its current round is complete, advance it.
    If both brackets have finalists, assemble anchor pool and mark complete.
    Returns the number of rounds that were advanced.
    Mutates state in place (caller must save).
    """
    rounds_advanced = 0

    for bracket in BRACKETS:
        if state[bracket].get("finalists"):
            continue  # already done

        rnum_str = str(state[bracket]["current_round"])
        rdata = state[bracket]["rounds"].get(rnum_str, {})

        if not is_round_complete(state, bracket):
            continue

        # Round complete — extract finalists or advance
        if rdata.get("is_final"):
            finalists = extract_finalists_from_final_round(state, bracket)
            state[bracket]["finalists"] = finalists
            log.info("[%s] Final round complete — finalists: %s", bracket, finalists)
        else:
            advance_bracket(state, bracket)
            rounds_advanced += 1
            # Submit next round if bracket not yet done
            if not state[bracket].get("finalists"):
                submit_round(state, bracket, login, password, cache=cache)
                # Note: submit_round calls save_state internally; that's OK — it's idempotent
                rounds_advanced += 1  # count the submission as another advance

    # Check if tournament is done
    both_done = all(bool(state[b].get("finalists")) or not state[b]["pool"] for b in BRACKETS)
    if both_done and state["status"] == "running":
        assemble_anchor_pool(state)
        state["status"] = "complete"
        log.info("Tournament complete — anchor pool: %s", state["anchor_pool"])

    return rounds_advanced


# ── Manual bracket pipeline ───────────────────────────────────────────────────

def start_manual_bracket(bracket: str, games: list[str], login: str, password: str) -> dict:
    """
    Reset one bracket in manual_tournament_state.json and submit round 1.
    Also resets the grand final (champion may change).
    Returns the updated manual state dict.
    """
    state = load_manual_state()
    reset_manual_bracket(state, bracket, games)  # also resets grand_final
    save_manual_state(state)

    if not games:
        return state
    if len(games) == 1:
        state[bracket]["finalists"] = games[:]
        save_manual_state(state)
        return state

    cache = load_trends_cache()
    state[bracket]["rounds"]["1"] = {
        "is_final": len(games) <= _GROUP_SIZE,
        "tasks":     [],
        "bye_games": [],
    }
    submit_round(state, bracket, login, password, save_fn=save_manual_state, cache=cache)
    save_trends_cache(cache)
    return state


def collect_manual_bracket(bracket: str, login: str, password: str) -> dict:
    """
    Poll tasks_ready for one manual bracket, fetch completed tasks, advance rounds.
    Returns a summary dict identical in shape to collect_results().
    """
    state = load_manual_state()
    cache = load_trends_cache()
    pending_map = get_manual_pending_task_ids(state, bracket)

    if not pending_map:
        rounds_advanced = _advance_manual_if_complete(state, bracket, login, password, cache=cache)
        save_manual_state(state)
        return {"checked": 0, "collected": 0, "rounds_advanced": rounds_advanced,
                "errors": 0, "complete": bool(state[bracket].get("finalists"))}

    ready_ids = fetch_tasks_ready(login, password)
    our_ready = set(pending_map.keys()) & ready_ids

    if len(ready_ids) >= 1000:
        missing_from_ready = set(pending_map.keys()) - our_ready
        if missing_from_ready:
            log.info("tasks_ready saturated (1000 cap); checking %d pending tasks directly", len(missing_from_ready))
            for tid in missing_from_ready:
                if check_task(tid, login, password) is not None:
                    our_ready.add(tid)

    checked = len(our_ready)
    collected = 0
    errors = 0

    for task_id in list(our_ready):
        _brk, rnum, task_idx = pending_map[task_id]
        rdata   = state[bracket]["rounds"][str(rnum)]
        task    = rdata["tasks"][task_idx]
        cleaned_kws = task["cleaned_keywords"]
        orig_kws    = task["keywords"]

        scores_raw  = fetch_task_result(task_id, cleaned_kws, login, password)
        scores_orig = {orig: scores_raw.get(clean, 0.0)
                       for orig, clean in zip(orig_kws, cleaned_kws)}
        winner = max(scores_orig, key=scores_orig.get) if any(v > 0 for v in scores_orig.values()) else None

        update_task_result(state, bracket, rnum, task_idx, scores_orig, winner)

        if any(v > 0 for v in scores_orig.values()):
            collected += 1
            log.info("[manual/%s] Task %s collected — winner: %s", bracket, task_id, winner)
            write_cached_scores(cleaned_kws, scores_raw, cache)
        else:
            errors += 1
            log.warning("[manual/%s] Task %s all-zero scores", bracket, task_id)

    rounds_advanced = _advance_manual_if_complete(state, bracket, login, password, cache=cache)
    save_trends_cache(cache)
    save_manual_state(state)

    return {
        "checked":         checked,
        "collected":       collected,
        "rounds_advanced": rounds_advanced,
        "errors":          errors,
        "complete":        bool(state[bracket].get("finalists")),
    }


def _advance_manual_if_complete(
    state: dict, bracket: str, login: str, password: str, cache: dict | None = None
) -> int:
    """Advance the bracket if its current round is done. Returns rounds advanced."""
    if state[bracket].get("finalists"):
        return 0
    rnum_str = str(state[bracket]["current_round"])
    rdata = state[bracket]["rounds"].get(rnum_str, {})
    if not is_round_complete(state, bracket):
        return 0
    if rdata.get("is_final"):
        finalists = extract_finalists_from_final_round(state, bracket)
        state[bracket]["finalists"] = finalists
        log.info("[manual/%s] Final round complete — finalists: %s", bracket, finalists)
        return 1
    else:
        advance_bracket(state, bracket)
        if not state[bracket].get("finalists"):
            submit_round(state, bracket, login, password, save_fn=save_manual_state, cache=cache)
        return 2


def submit_grand_final(steam_champ: str, ns_champ: str, login: str, password: str) -> dict:
    """Submit a 2-game Grand Final task. Returns the updated manual state dict."""
    state = load_manual_state()
    keywords = [steam_champ, ns_champ]
    cleaned  = [strip_edition_suffix(g) for g in keywords]
    date_from, date_to = _date_range()

    task_ids = post_tasks_bulk([{
        "keywords":      cleaned,
        "category_code": GAMES_CATEGORY,
        "date_from":     date_from,
        "date_to":       date_to,
        "type":          "web",
        "item_types":    ["google_trends_graph"],
        "pingback_url":  state.get("pingback_url", PINGBACK_URL),
    }], login, password)

    task_id = task_ids[0] if task_ids else None
    state["grand_final"] = {
        "steam_champion":    steam_champ,
        "nonsteam_champion": ns_champ,
        "task_id":           task_id,
        "keywords":          keywords,
        "cleaned_keywords":  cleaned,
        "scores":            {},
        "winner":            None,
        "status":            "pending" if task_id else "failed",
    }
    save_manual_state(state)
    log.info("[grand_final] Submitted task %s for %s vs %s", task_id, steam_champ, ns_champ)
    return state


def collect_grand_final(login: str, password: str) -> dict:
    """
    Poll tasks_ready for the Grand Final task and fetch result if ready.
    Returns {"complete": bool, "winner": str|None, "scores": dict}.
    """
    state = load_manual_state()
    gf      = state.get("grand_final", {})
    task_id = gf.get("task_id")

    if not task_id or gf.get("status") != "pending":
        return {"complete": gf.get("status") == "complete", "winner": gf.get("winner"), "scores": gf.get("scores", {})}

    ready_ids = fetch_tasks_ready(login, password)
    if task_id not in ready_ids:
        # tasks_ready may be saturated (>1000 queued) — try direct task_get
        if check_task(task_id, login, password) is None:
            return {"complete": False, "winner": None, "scores": {}}

    kws_orig  = gf["keywords"]
    kws_clean = gf["cleaned_keywords"]
    scores_raw  = fetch_task_result(task_id, kws_clean, login, password)
    scores_orig = {orig: scores_raw.get(clean, 0.0) for orig, clean in zip(kws_orig, kws_clean)}
    winner = max(scores_orig, key=scores_orig.get) if any(v > 0 for v in scores_orig.values()) else None

    gf["scores"] = scores_orig
    gf["winner"] = winner
    gf["status"] = "complete" if winner else "failed"
    state["grand_final"] = gf
    save_manual_state(state)
    log.info("[grand_final] Complete — winner: %s", winner)
    return {"complete": gf["status"] == "complete", "winner": winner, "scores": scores_orig}


# ── Tournament entry point ────────────────────────────────────────────────────

def start_tournament(
    steam_games: list[str],
    nonsteam_games: list[str],
    login: str,
    password: str,
    pingback_url: str = PINGBACK_URL,
) -> dict:
    """
    Reset tournament state and submit round 1 for both brackets.
    Returns the updated state dict.
    """
    state = reset_state(steam_games, nonsteam_games, pingback_url)
    cache = load_trends_cache()

    for bracket, games in (("steam", steam_games), ("non_steam", nonsteam_games)):
        if not games:
            continue
        if len(games) == 1:
            # Sole game — instant finalist, no task needed
            state[bracket]["finalists"] = games[:]
            log.info("[%s] Single game — instant finalist: %s", bracket, games[0])
        else:
            # Mark round 1
            state[bracket]["rounds"]["1"] = {
                "is_final": len(games) <= _GROUP_SIZE,
                "tasks":     [],
                "bye_games": [],
            }
            submit_round(state, bracket, login, password, cache=cache)

    save_trends_cache(cache)

    # Handle case where both brackets finished immediately (e.g., 1 game each)
    both_done = all(bool(state[b].get("finalists")) or not state[b]["pool"] for b in BRACKETS)
    if both_done:
        assemble_anchor_pool(state)
        state["status"] = "complete"
    else:
        state["status"] = "running"

    save_state(state)
    return state
