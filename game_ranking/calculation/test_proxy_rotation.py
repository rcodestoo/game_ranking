"""
Live rotation test: fetch Google Trends data for N games, rotating through
all 7 configured proxies in sequence. Confirms each proxy can successfully
pull genre-scoped (cat=8) trends from Google.

Usage:
    python -m game_ranking.calculation.test_proxy_rotation
"""

import sys
import os
import time
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import game_ranking.calculation.scraper as scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# One game per proxy — enough to exercise every proxy in the list exactly once
GAMES = [
    "Minecraft",
    "Elden Ring",
    "Stardew Valley",
    "Hollow Knight",
    "Terraria",
    "Hades",
    "Celeste",
]

INTER_GAME_DELAY = 5  # seconds between fetches


def run():
    proxies = list(scraper.PROXIES)
    n = len(proxies)

    if len(GAMES) < n:
        print(f"WARNING: only {len(GAMES)} games for {n} proxies — some proxies won't be tested.")

    print(f"\n{'='*60}")
    print(f"PROXY ROTATION TEST  ({n} proxies, {len(GAMES)} games)")
    print(f"{'='*60}\n")

    results = []

    for i, game in enumerate(GAMES):
        proxy_idx = i % n
        proxy = proxies[proxy_idx]

        # Point scraper at exactly this one proxy
        scraper.set_proxies([proxy])
        print(f"[{i+1}/{len(GAMES)}] '{game}'  ->  proxy {proxy_idx+1}/{n}: {proxy}")

        try:
            pytrends = scraper.get_shared_pytrends()
            df = scraper.fetch_with_retry(
                pytrends, [game],
                timeframe=scraper.TIMEFRAME,
                cat=scraper.CAT_GAMES,
            )
            if df.empty:
                status = "EMPTY"
                score = None
            else:
                if "isPartial" in df.columns:
                    df = df.drop(columns=["isPartial"])
                col = game if game in df.columns else df.columns[0]
                score = int(df[col].iloc[-1])
                status = "OK"
            print(f"         Result: {status}  (last score = {score})\n")
        except Exception as e:
            status = "FAIL"
            score = None
            print(f"         Result: FAIL -- {e}\n")

        results.append((proxy_idx + 1, proxy, game, status, score))

        if i < len(GAMES) - 1:
            time.sleep(INTER_GAME_DELAY)

    # Restore full proxy list when done
    scraper.set_proxies(list(scraper.PROXIES))

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    ok = sum(1 for *_, s, __ in results if s == "OK")
    for p_num, proxy, game, status, score in results:
        tag = "OK " if status == "OK" else "---"
        host = proxy.split("@")[-1]
        print(f"  [{tag}] proxy {p_num} ({host})  |  {game}  |  score={score}")
    print(f"\n{ok}/{len(GAMES)} fetches succeeded across {n} proxies.")

    if ok == 0:
        sys.exit(1)


if __name__ == "__main__":
    run()
