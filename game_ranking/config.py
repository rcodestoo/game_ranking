from pathlib import Path

# BASE_DIR is the folder containing THIS script
BASE_DIR = Path(__file__).resolve().parent

# Define directories using forward slashes (works on both) or as parts
RAW_DIR = BASE_DIR / 'raw_files'
DATA_DIR = BASE_DIR / 'src' / 'data'

# Define files
CSV_STEAM = RAW_DIR / 'raw_steam.csv'
CSV_NON_STEAM = RAW_DIR / 'raw_steam.csv'
DEV_LIST = DATA_DIR / 'developer_list.xlsx'
GENRE_LIST = DATA_DIR / 'genre_list.xlsx'