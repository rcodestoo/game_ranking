"""
steam_players.py
----------------
Two data-fetch modes:

1. Peak CCU via SteamSpy (fetch_player_data)
   Fetches peak concurrent player counts for a list of game names.
   App IDs are resolved via Steam Store search and cached locally.
   Cache: game_ranking/cache/steam_appid_cache.json

2. Daily concurrent player snapshots (fetch_player_counts_if_needed)
   Calls the Steam ISteamUserStats API once per day for Steam games
   in the inventory and appends results to a history CSV.
   History: game_ranking/cache/player_counts_history.csv
"""

import time
import json
import difflib
import requests
import pandas as pd
from datetime import datetime, timedelta
from config import CACHE_DIR

CACHE_FILE    = CACHE_DIR / "steam_appid_cache.json"
HISTORY_FILE  = CACHE_DIR / "player_counts_history.csv"
CCU_TTL_HOURS = 24
MIN_STEAMSPY_INTERVAL  = 0.25   # seconds between SteamSpy requests (≤4 req/s)
MIN_STORE_INTERVAL     = 0.5    # seconds between Steam Store search requests
CONCURRENT_PLAYERS_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
HISTORY_COLUMNS        = ["date", "game_name", "steam_appid", "player_count"]

_last_request_time: float = 0.0


def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def _throttle(interval: float) -> None:
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < interval:
        time.sleep(interval - elapsed)
    _last_request_time = time.time()


def search_steam_appid(game_name: str, cache: dict) -> int | None:
    """
    Resolve a game name to a Steam App ID.
    Checks the local cache first; only hits the Steam Store API if not cached.
    Returns the App ID (int) or None if no match found.
    """
    entry = cache.get(game_name, {})
    if "appid" in entry:
        return entry["appid"]

    _throttle(MIN_STORE_INTERVAL)
    try:
        resp = requests.get(
            "https://store.steampowered.com/api/storesearch/",
            params={"term": game_name, "cc": "us", "l": "en"},
            timeout=10,
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
    except Exception:
        return None

    if not items:
        cache.setdefault(game_name, {})["appid"] = None
        return None

    names = [item["name"] for item in items]
    matches = difflib.get_close_matches(game_name, names, n=1, cutoff=0.4)
    if matches:
        matched_name = matches[0]
        appid = next(item["id"] for item in items if item["name"] == matched_name)
    else:
        appid = items[0]["id"]

    cache.setdefault(game_name, {})["appid"] = appid
    return appid


def get_steamspy_peak_ccu(appid: int, max_retries: int = 3) -> dict | None:
    """
    Fetch peak CCU and related stats from SteamSpy.
    Returns a dict with keys: peak_ccu, avg_2weeks_hrs — or None on failure.
    """
    url = "https://steamspy.com/api.php"
    for attempt in range(max_retries):
        _throttle(MIN_STEAMSPY_INTERVAL)
        try:
            resp = requests.get(
                url,
                params={"request": "appdetails", "appid": appid},
                timeout=15,
            )
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(2 ** (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            avg_2wk_mins = data.get("average_2weeks") or 0
            return {
                "peak_ccu":       data.get("ccu"),
                "avg_2weeks_hrs": round(avg_2wk_mins / 60, 1) if avg_2wk_mins else None,
            }
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
    return None


def fetch_player_data(game_names: list, progress_callback=None) -> pd.DataFrame:
    """
    Fetch peak CCU data for a list of game names.

    progress_callback(i, total, current_name) is called before each game is
    processed and once more at the end with i == total.

    Returns a DataFrame with columns:
        Game Name | App ID | Peak CCU | Avg Playtime (2wk hrs) | Peak CCU Numeric
    """
    cache = _load_cache()
    now   = datetime.utcnow()
    rows  = []

    for i, name in enumerate(game_names):
        if progress_callback:
            progress_callback(i, len(game_names), name)

        entry = cache.get(name, {})

        ccu_fetched_at = entry.get("ccu_fetched_at")
        ccu_fresh = (
            ccu_fetched_at is not None
            and (now - datetime.fromisoformat(ccu_fetched_at)) < timedelta(hours=CCU_TTL_HOURS)
            and "peak_ccu" in entry
        )

        appid = search_steam_appid(name, cache)

        if appid is None:
            rows.append({
                "Game Name":              name,
                "App ID":                 "N/A",
                "Peak CCU":               "N/A",
                "Peak CCU Numeric":       0,
                "Avg Playtime (2wk hrs)": "N/A",
            })
            _save_cache(cache)
            continue

        if not ccu_fresh:
            ccu_data = get_steamspy_peak_ccu(appid)
            if ccu_data:
                entry.update({
                    "ccu_fetched_at":  now.isoformat(),
                    "peak_ccu":        ccu_data["peak_ccu"],
                    "avg_2weeks_hrs":  ccu_data["avg_2weeks_hrs"],
                })
                cache[name] = entry
                _save_cache(cache)
        else:
            ccu_data = {
                "peak_ccu":       entry.get("peak_ccu"),
                "avg_2weeks_hrs": entry.get("avg_2weeks_hrs"),
            }

        if ccu_data:
            peak    = ccu_data.get("peak_ccu")
            avg_2wk = ccu_data.get("avg_2weeks_hrs")
            rows.append({
                "Game Name":              name,
                "App ID":                 appid,
                "Peak CCU":               f"{peak:,}" if isinstance(peak, int) else "N/A",
                "Peak CCU Numeric":       peak if isinstance(peak, int) else 0,
                "Avg Playtime (2wk hrs)": avg_2wk if avg_2wk is not None else "N/A",
            })
        else:
            rows.append({
                "Game Name":              name,
                "App ID":                 appid,
                "Peak CCU":               "N/A",
                "Peak CCU Numeric":       0,
                "Avg Playtime (2wk hrs)": "N/A",
            })

    if progress_callback:
        progress_callback(len(game_names), len(game_names), "Done")

    _save_cache(cache)
    return pd.DataFrame(rows, columns=[
        "Game Name", "App ID", "Peak CCU", "Peak CCU Numeric", "Avg Playtime (2wk hrs)",
    ])


def fetch_player_counts_if_needed(inventory_df: pd.DataFrame, force: bool = False) -> pd.DataFrame:
    """
    Fetch current concurrent player counts for Steam games and append to
    player_counts_history.csv. Runs at most once per UTC hour.

    Returns the full history DataFrame (all dates).
    """
    current_hour = datetime.utcnow().strftime("%Y-%m-%d %H:00")

    if HISTORY_FILE.exists():
        history = pd.read_csv(HISTORY_FILE, dtype={"steam_appid": "Int64"})
    else:
        history = pd.DataFrame(columns=HISTORY_COLUMNS)

    if not force and not history.empty and (history["date"] == current_hour).any():
        return history

    if force and not history.empty:
        history = history[history["date"] != current_hour]

    required_cols = {"steam_appid", "Game Name"}
    if not required_cols.issubset(inventory_df.columns):
        return history

    try:
        has_id = inventory_df["steam_appid"].notna()
        games_to_fetch = inventory_df[has_id][["Game Name", "steam_appid"]].drop_duplicates()
    except Exception:
        return history

    new_rows = []
    for _, row in games_to_fetch.iterrows():
        appid = int(row["steam_appid"])
        try:
            _throttle(MIN_STORE_INTERVAL)
            resp = requests.get(
                CONCURRENT_PLAYERS_URL,
                params={"appid": appid},
                timeout=10,
            )
            resp.raise_for_status()
            response_data = resp.json().get("response", {})
            if response_data.get("result") == 1:
                new_rows.append({
                    "date":         current_hour,
                    "game_name":    row["Game Name"],
                    "steam_appid":  appid,
                    "player_count": response_data["player_count"],
                })
        except Exception:
            continue

    if new_rows:
        new_df  = pd.DataFrame(new_rows)
        history = pd.concat([history, new_df], ignore_index=True)
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        history.to_csv(HISTORY_FILE, index=False)

    return history


def resolve_inventory_appids(inventory_df: pd.DataFrame) -> tuple:
    """
    Populate steam_appid for all PC (Steam) platform games in the inventory
    that are missing it. Checks the local AppID cache first.

    Returns (updated_df, n_resolved).
    """
    df = inventory_df.copy()
    if "steam_appid" not in df.columns:
        df["steam_appid"] = pd.NA

    steam_mask   = df["Platform"].str.contains("Steam", case=False, na=False)
    missing_mask = df["steam_appid"].isna()
    to_resolve   = df[steam_mask & missing_mask]

    if to_resolve.empty:
        return df, 0

    cache      = _load_cache()
    n_resolved = 0

    for idx, row in to_resolve.iterrows():
        name = str(row.get("Game Name", "")).strip()
        if not name:
            continue
        appid = search_steam_appid(name, cache)
        if appid is not None:
            df.at[idx, "steam_appid"] = int(appid)
            n_resolved += 1

    _save_cache(cache)
    return df, n_resolved
