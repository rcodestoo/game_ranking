from pathlib import Path

# BASE_DIR is the folder containing THIS script
BASE_DIR = Path(__file__).resolve().parent

# Define directories using forward slashes (works on both) or as parts
RAW_DIR = BASE_DIR / 'default_files'
DATA_DIR = BASE_DIR / 'src' / 'data'

# Define files
CSV_STEAM = RAW_DIR / 'raw_steam.csv'
CSV_NON_STEAM = RAW_DIR / 'raw_non_steam_2026-03-09.csv' #'raw_non_steam.csv'
DEV_LIST = DATA_DIR / 'developer_list.xlsx'
GENRE_LIST = DATA_DIR / 'genre_list.xlsx'
INVENTORY_FILE = DATA_DIR / 'team_reviews_game_inventory.csv'
TRENDS_CACHE_FILE = DATA_DIR / "nonsteam_trends_cache.csv"
STEAMSPY_CACHE_FILE = RAW_DIR / "steamspy_cache.csv"


def get_latest_steam_csv() -> "Path":
    """Return the most recently dated raw_steam_YYYY-MM-DD.csv, falling back to CSV_STEAM."""
    candidates = sorted(RAW_DIR.glob("raw_steam_????-??-??.csv"))
    return candidates[-1] if candidates else CSV_STEAM


def get_latest_nonsteam_csv() -> "Path":
    """Return the most recently dated raw_non_steam_YYYY-MM-DD.csv, falling back to CSV_NON_STEAM."""
    candidates = sorted(RAW_DIR.glob("raw_non_steam_????-??-??.csv"))
    return candidates[-1] if candidates else CSV_NON_STEAM