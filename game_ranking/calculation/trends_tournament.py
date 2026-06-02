"""
Google Trends tournament engine — powered by DataForSEO.

Compares games head-to-head using DataForSEO's Google Trends API
(category 41 = Computer & Video Games, worldwide, past month).
Up to 5 keywords per request (Google Trends hard limit).

Tournament flow:
  1. Split games into groups of TOURNAMENT_GROUP_SIZE (5).
  2. Within each group, call DataForSEO → highest mean score advances.
  3. Repeat until one game remains (the champion).
  4. Optional anchor-based scoring: all games vs champion, 4 games + anchor per batch.
  5. Optional cross-final: Steam champion vs Non-Steam champion.
"""

import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from calculation.dataforseo_trends import fetch_comparison, GAMES_CATEGORY

log = logging.getLogger(__name__)

# Matches common edition/remaster suffixes so Google Trends gets a cleaner query.
# Handles colon, dash, or space separators before the suffix.
# Examples: "Game: Deluxe Edition" → "Game", "Dark Souls: Remastered" → "Dark Souls"
_EDITION_RE = re.compile(
    r'(?:\s*[-:]\s*|\s+)'
    r'(?:'
    r'(?:Deluxe|Standard|Complete|Ultimate|Gold|Enhanced|Anniversary|'
    r'Definitive|Special|Digital|Premium|Collectors?\'?|Extended|'
    r'Legendary|Game\s+of\s+the\s+Year|GOTY)\s+Edition'
    r"|Director'?s?\s+Cut"
    r'|Remastered'
    r')\s*$',
    re.IGNORECASE,
)


def strip_edition_suffix(name: str) -> str:
    """Remove edition/remaster suffixes for cleaner Google Trends queries."""
    return _EDITION_RE.sub('', name).strip()

ANCHOR               = "Minecraft"   # stable reference for anchor-based scoring
GAMES_PER_GROUP      = 8            # games per manual tournament group (UI brackets)
TOURNAMENT_GROUP_SIZE = 5           # games per auto-tournament group (= DataForSEO max)
BATCH_SIZE           = 4            # games per anchor-scoring batch (4 + anchor = 5)
CALL_SLEEP           = 2.0          # seconds each worker sleeps after its API call
MAX_PARALLEL_CALLS   = 3            # concurrent DataForSEO requests (~3× speedup)


# ── Low-level: single batch, no anchor ───────────────────────────────────────

def compare_group_direct(
    games: list[str],
    login: str,
    password: str,
    category_code: int = GAMES_CATEGORY,
) -> dict[str, float]:
    """
    Compare up to TOURNAMENT_GROUP_SIZE games in a single DataForSEO call.
    Returns {game: mean_score}. Highest scorer wins.
    """
    return fetch_comparison(games[:TOURNAMENT_GROUP_SIZE], login, password, category_code)


# ── Mid-level: anchor-based, multiple batches ─────────────────────────────────

def compare_group(
    games: list[str],
    login: str,
    password: str,
    anchor: str = ANCHOR,
    category_code: int = GAMES_CATEGORY,
    sleep_s: float = CALL_SLEEP,
) -> dict[str, float]:
    """
    Compare up to GAMES_PER_GROUP games using anchor-based normalisation.
    Splits into batches of BATCH_SIZE (4 games + anchor = 5 per call).
    All batches share the same anchor so scores are directly comparable.
    Returns {game: normalised_score} where anchor = 100.
    """
    scores: dict[str, float] = {}
    batches = [games[i:i + BATCH_SIZE] for i in range(0, len(games), BATCH_SIZE)]

    for i, batch in enumerate(batches):
        raw = fetch_comparison(batch + [anchor], login, password, category_code)
        anchor_val = raw.get(anchor, 1.0) or 1.0
        for g in batch:
            scores[g] = round(raw.get(g, 0.0) / anchor_val * 100, 2)
        if i < len(batches) - 1:
            time.sleep(sleep_s)

    return scores


# ── Parallel worker for a single group ───────────────────────────────────────

def _run_group_worker(args: tuple) -> tuple:
    """
    Thread worker: fetch scores for one group, then sleep to pace API calls.
    Returns (g_idx, group, scores_or_None).  scores=None means a bye.
    """
    g_idx, group, login, password, category_code, sleep_s = args
    if len(group) == 1:
        return g_idx, group, None
    scores = compare_group_direct(group, login, password, category_code)
    time.sleep(sleep_s)   # each worker paces itself — same rate as before, just parallel
    return g_idx, group, scores


# ── High-level: full tournament ───────────────────────────────────────────────

def run_tournament(
    games: list[str],
    login: str,
    password: str,
    category_code: int = GAMES_CATEGORY,
    sleep_s: float = CALL_SLEEP,
    progress_callback=None,
) -> list[dict]:
    """
    Run a multi-round DataForSEO Trends tournament.

    Each round: games split into groups of TOURNAMENT_GROUP_SIZE (5).
    Highest scorer in each group advances. Rounds continue until one champion remains.
    Single-game groups get an automatic bye.

    Groups within each round are processed in parallel (MAX_PARALLEL_CALLS workers).
    Each worker sleeps sleep_s after its call to maintain per-request pacing.

    progress_callback(msg: str) is called as groups complete each round.

    Returns a flat list of result dicts:
      game        – game name
      score       – mean trends score for this match (None for byes)
      round       – round number (1 = first round)
      group       – group number within the round
      eliminated  – True if knocked out in this round
      champion    – True only for the overall tournament winner
    """
    results: list[dict] = []
    pool = list(games)
    round_num = 1

    while len(pool) > 1:
        groups = [pool[i:i + TOURNAMENT_GROUP_SIZE] for i in range(0, len(pool), TOURNAMENT_GROUP_SIZE)]

        worker_args = [
            (g_idx, group, login, password, category_code, sleep_s)
            for g_idx, group in enumerate(groups)
        ]

        # Process all groups in this round in parallel
        group_results: dict[int, tuple] = {}
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_CALLS) as executor:
            futures = {
                executor.submit(_run_group_worker, args): args[0]
                for args in worker_args
            }
            for future in as_completed(futures):
                g_idx, group, scores = future.result()
                group_results[g_idx] = (group, scores)
                if progress_callback:
                    done = len(group_results)
                    progress_callback(
                        f"Round {round_num}: {done}/{len(groups)} groups done"
                    )

        # Reconstruct next pool in original group order
        next_pool: list[str] = []
        for g_idx in range(len(groups)):
            group, scores = group_results[g_idx]
            if scores is None:  # bye
                results.append({
                    "game": group[0], "score": None,
                    "round": round_num, "group": g_idx + 1,
                    "eliminated": False, "champion": False,
                })
                next_pool.append(group[0])
            else:
                winner = max(scores, key=scores.get) if scores else group[0]
                next_pool.append(winner)
                for game in group:
                    results.append({
                        "game":       game,
                        "score":      scores.get(game, 0.0),
                        "round":      round_num,
                        "group":      g_idx + 1,
                        "eliminated": game != winner,
                        "champion":   False,
                    })

        pool = next_pool
        round_num += 1

    if pool:
        champion = pool[0]
        for r in reversed(results):
            if r["game"] == champion and not r["eliminated"]:
                r["champion"] = True
                break

    return results


# ── Cross-final: Steam champion vs Non-Steam champion ────────────────────────

def run_cross_final(
    steam_champion: str,
    nonsteam_champion: str,
    login: str,
    password: str,
    anchor: str = ANCHOR,
    category_code: int = GAMES_CATEGORY,
) -> dict:
    """
    Direct comparison between the Steam and Non-Steam tournament winners
    using anchor-based normalisation for a fair cross-comparison.

    Returns:
      winner            – name of the overall winner
      steam_champion    – Steam entrant
      nonsteam_champion – Non-Steam entrant
      steam_score       – normalised score
      nonsteam_score    – normalised score
    """
    scores = compare_group(
        [steam_champion, nonsteam_champion],
        login=login,
        password=password,
        anchor=anchor,
        category_code=category_code,
    )
    s  = scores.get(steam_champion, 0.0)
    ns = scores.get(nonsteam_champion, 0.0)
    return {
        "winner":            steam_champion if s >= ns else nonsteam_champion,
        "steam_champion":    steam_champion,
        "nonsteam_champion": nonsteam_champion,
        "steam_score":       s,
        "nonsteam_score":    ns,
    }
