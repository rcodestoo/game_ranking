from pathlib import Path

BASE_DIR  = Path(__file__).resolve().parent
RAW_DIR   = BASE_DIR / 'raw'
DATA_DIR  = BASE_DIR / 'data'
CACHE_DIR = BASE_DIR / 'cache'

CSV_STEAM        = RAW_DIR  / 'raw_steam.csv'
CSV_NON_STEAM    = RAW_DIR  / 'raw_non_steam.csv'
DEV_LIST         = DATA_DIR / 'developer_list.xlsx'
GENRE_LIST       = DATA_DIR / 'genre_list.xlsx'
INVENTORY_FILE   = DATA_DIR / 'team_reviews_game_inventory.csv'
TRENDS_CACHE_FILE        = CACHE_DIR / 'nonsteam_trends_cache.csv'
INVENTORY_TRENDS_HISTORY_FILE = CACHE_DIR / 'inventory_trends_history.csv'
STEAMSPY_CACHE_FILE      = CACHE_DIR / 'steamspy_cache.csv'
TOURNAMENT_ANCHOR_FILE   = CACHE_DIR / 'tournament_anchor.json'
TOURNAMENT_STATE_FILE        = CACHE_DIR / 'tournament_state.json'
MANUAL_TOURNAMENT_STATE_FILE = CACHE_DIR / 'manual_tournament_state.json'
REFRESH_TRENDS_STATE_FILE         = CACHE_DIR / 'refresh_trends_state.json'
REFRESH_TRENDS_STATE_FILE_STEAM   = CACHE_DIR / 'refresh_trends_state_steam.json'
REFRESH_TRENDS_STATE_FILE_NONSTEAM = CACHE_DIR / 'refresh_trends_state_nonsteam.json'


def get_latest_steam_csv() -> "Path":
    """Return the most recently dated raw_steam_YYYY-MM-DD.csv, falling back to CSV_STEAM."""
    candidates = sorted(RAW_DIR.glob("raw_steam_????-??-??.csv"))
    return candidates[-1] if candidates else CSV_STEAM


def get_latest_nonsteam_csv() -> "Path":
    """Return the most recently dated raw_non_steam_YYYY-MM-DD.csv, falling back to CSV_NON_STEAM."""
    candidates = sorted(RAW_DIR.glob("raw_non_steam_????-??-??.csv"))
    return candidates[-1] if candidates else CSV_NON_STEAM
