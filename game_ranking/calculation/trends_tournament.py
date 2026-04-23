"""
Google Trends tournament engine.

Uses pytrends anchor-based comparison to rank games relative to each other.
pytrends caps comparisons at 5 keywords per request; we use batches of 4 games
+ 1 fixed anchor so all batches share a common scale.

Tournament flow:
  1. Split games into groups of `games_per_group` (default 8).
  2. Within each group, run anchor-based comparison → top scorer advances.
  3. Repeat until one game remains (the champion).
  4. Optional cross-final: Steam champion vs Non-Steam champion.
"""

import time
import pandas as pd
from calculation.scraper import build_pytrends, fetch_with_retry, TIMEFRAME, CAT_GAMES

ANCHOR = "Minecraft"        # stable reference term shared across all batches
GAMES_PER_GROUP = 8         # games per tournament group
BATCH_SIZE = 4              # games per pytrends call (4 + anchor = 5, the max)
CALL_SLEEP = 2.0            # seconds between pytrends calls


# ── Low-level: single pytrends call ──────────────────────────────────────────

def _score_batch(games: list[str], anchor: str, pytrends) -> dict[str, float]:
    """
    Call pytrends with `games` + `anchor` and return mean interest for each game,
    normalised so that anchor = 100. Returns {game: score}, score = 0 on failure.
    """
    kw_list = games + [anchor]
    try:
        df = fetch_with_retry(pytrends, kw_list, timeframe=TIMEFRAME, cat=CAT_GAMES)
        if df.empty:
            return {g: 0.0 for g in games}
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        anchor_mean = float(df[anchor].mean()) if anchor in df.columns else 1.0
        if anchor_mean == 0:
            anchor_mean = 1.0

        result = {}
        for game in games:
            if game in df.columns:
                result[game] = round(float(df[game].mean()) / anchor_mean * 100, 2)
            else:
                result[game] = 0.0
        return result
    except Exception:
        return {g: 0.0 for g in games}


# ── Mid-level: compare a group of up to GAMES_PER_GROUP games ────────────────

def compare_group(
    games: list[str],
    anchor: str = ANCHOR,
    pytrends=None,
    sleep_s: float = CALL_SLEEP,
) -> dict[str, float]:
    """
    Compare up to GAMES_PER_GROUP games using anchor-based normalisation.
    Splits into batches of BATCH_SIZE; all batches share the same anchor so
    scores are directly comparable. Returns {game: normalised_score}.
    """
    if pytrends is None:
        pytrends = build_pytrends()

    scores: dict[str, float] = {}
    batches = [games[i:i + BATCH_SIZE] for i in range(0, len(games), BATCH_SIZE)]

    for i, batch in enumerate(batches):
        scores.update(_score_batch(batch, anchor, pytrends))
        if i < len(batches) - 1:
            time.sleep(sleep_s)

    return scores


# ── High-level: full tournament ───────────────────────────────────────────────

def run_tournament(
    games: list[str],
    games_per_group: int = GAMES_PER_GROUP,
    anchor: str = ANCHOR,
    pytrends=None,
    sleep_s: float = CALL_SLEEP,
    progress_callback=None,
) -> list[dict]:
    """
    Run a multi-round Google Trends tournament.

    Each round: games are split into groups of `games_per_group`; the highest
    scorer in each group advances. Rounds continue until one champion remains.

    progress_callback(msg: str) is called before each group comparison so the
    caller can update a progress bar.

    Returns a flat list of result dicts:
      game        – game name
      score       – normalised trends score for this match (None for byes)
      round       – round number (1 = first round)
      group       – group number within the round
      eliminated  – True if knocked out in this round
      champion    – True only for the overall tournament winner
    """
    if pytrends is None:
        pytrends = build_pytrends()

    results: list[dict] = []
    pool = list(games)
    round_num = 1

    while len(pool) > 1:
        groups = [pool[i:i + games_per_group] for i in range(0, len(pool), games_per_group)]
        next_pool: list[str] = []

        for g_idx, group in enumerate(groups):
            # Single-game group → automatic bye
            if len(group) == 1:
                results.append({
                    "game": group[0], "score": None,
                    "round": round_num, "group": g_idx + 1,
                    "eliminated": False, "champion": False,
                })
                next_pool.append(group[0])
                continue

            if progress_callback:
                preview = ", ".join(group[:3]) + ("…" if len(group) > 3 else "")
                progress_callback(f"Round {round_num} · Group {g_idx + 1}/{len(groups)}: {preview}")

            scores = compare_group(group, anchor=anchor, pytrends=pytrends, sleep_s=sleep_s)
            winner = max(scores, key=scores.get) if scores else group[0]
            next_pool.append(winner)

            for game in group:
                results.append({
                    "game": game,
                    "score": scores.get(game, 0.0),
                    "round": round_num,
                    "group": g_idx + 1,
                    "eliminated": game != winner,
                    "champion": False,
                })

            if g_idx < len(groups) - 1:
                time.sleep(sleep_s)

        pool = next_pool
        round_num += 1

    # Mark champion
    if pool:
        champion = pool[0]
        # Find the last result entry for the champion and flag it
        for r in reversed(results):
            if r["game"] == champion and not r["eliminated"]:
                r["champion"] = True
                break

    return results


# ── Cross-final: Steam champion vs Non-Steam champion ────────────────────────

def run_cross_final(
    steam_champion: str,
    nonsteam_champion: str,
    anchor: str = ANCHOR,
    pytrends=None,
) -> dict:
    """
    Direct comparison between the Steam and Non-Steam tournament winners.
    Returns:
      winner           – name of the overall winner
      steam_champion   – Steam entrant
      nonsteam_champion – Non-Steam entrant
      steam_score      – normalised score
      nonsteam_score   – normalised score
    """
    if pytrends is None:
        pytrends = build_pytrends()

    scores = compare_group(
        [steam_champion, nonsteam_champion],
        anchor=anchor,
        pytrends=pytrends,
    )
    s = scores.get(steam_champion, 0.0)
    ns = scores.get(nonsteam_champion, 0.0)
    return {
        "winner": steam_champion if s >= ns else nonsteam_champion,
        "steam_champion": steam_champion,
        "nonsteam_champion": nonsteam_champion,
        "steam_score": s,
        "nonsteam_score": ns,
    }
