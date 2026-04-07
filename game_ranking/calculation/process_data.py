import pandas as pd
import math
import requests
from config import DEV_LIST, GENRE_LIST, INVENTORY_FILE
from calculation.scraper import fetch_game_trends

# LOAD DEVELOPER AND GENRE LIST
developer_list = pd.read_excel(DEV_LIST)
genre_list = pd.read_excel(GENRE_LIST)
inventory = pd.read_csv(INVENTORY_FILE, index_col=0)


def load_data(steam_report=None, non_steam_report=None, steam_df=None, nonsteam_df=None,
              developer_list=developer_list, genre_list=genre_list, inventory=inventory):
    """
    Load data from files or use provided DataFrames.

    Args:
        steam_report: Path to Steam CSV (used if steam_df is None)
        non_steam_report: Path to Non-Steam CSV (used if nonsteam_df is None)
        steam_df: Pre-loaded Steam DataFrame (takes precedence)
        nonsteam_df: Pre-loaded Non-Steam DataFrame (takes precedence)
    """
    if steam_df is None:
        steam_df = pd.read_csv(steam_report)
    if nonsteam_df is None:
        nonsteam_df = pd.read_csv(non_steam_report, encoding='utf-8-sig')
    return steam_df, nonsteam_df, developer_list, genre_list, inventory


def clean_dev_genre_list(df):
    df['Developers'] = df['Developers'].astype(str)
    df['Genres'] = df['Genres'].astype(str)

    # Replace common suffixes to avoid splitting issues
    df['Developers'] = df['Developers'].str.replace(r',\s*inc\.?', ' Inc.', case=False, regex=True)
    df['Developers'] = df['Developers'].str.replace(r',\s*ltd\.?', ' Ltd.', case=False, regex=True)
    df['Developers'] = df['Developers'].str.replace(r',\s*llc\.?', ' LLC.', case=False, regex=True)

    # Split the string at the comma to create a list
    df['Developers'] = df['Developers'].str.split(',')
    df['Genres'] = df['Genres'].str.split(',')
    return df


def flagging(df):
    extra_cols = [c for c in ["date_appended"] if c in df.columns]
    df_calculation = df[["Name", "ReleaseDate", "Developers", "Genres", "FollowerCount"] + extra_cols].copy()

    for index, row in df_calculation.iterrows():
        genres = row['Genres']
        indie_flag = False
        for genre in genres:
            if genre.strip().lower() == 'indie':
                indie_flag = True
                break
        df_calculation.loc[index, 'Is_Indie'] = indie_flag

        developers = row['Developers']
        df_calculation.loc[index, 'Has_Multiple_Developers'] = len(developers) > 1

        df_calculation.loc[index, 'Has_Multiple_Genres'] = len(genres) > 1

    return df_calculation


def calculate_hybrid_score(value, min_value, max_value):
    """
    Normalise any count-based signal to a 1–5 scale using a hybrid
    linear/logarithmic approach: 0.5 * linear_norm + 0.5 * log_norm.
    Used for Steam follower counts, Non-Steam adjusted views, etc.
    """
    linear_norm = ((value - min_value) / (max_value - min_value)) * (5 - 1) + 1
    log_norm = ((math.log(value) - math.log(min_value)) /
                (math.log(max_value) - math.log(min_value))) * (5 - 1) + 1
    return 0.5 * linear_norm + 0.5 * log_norm


def calculate_trends_weighted_points(trends_score: float) -> float:
    """Normalise a 0–100 Google Trends score to a 1–5 scale (linear)."""
    return (trends_score / 100) * (5 - 1) + 1


def calculate_developer_weighted_points(developers, developer_list=developer_list):
    missing_devs = []
    dev_points = []
    for developer in developers:
        weighted_point = developer_list.loc[
            developer_list['Developer Name'].str.strip().str.lower() == developer.strip().lower(),
            'Total Hybrid Weighted Points'
        ]
        if not weighted_point.empty:
            dev_points.append(weighted_point.iloc[0].item())
        else:
            dev_points.append(1)
            missing_devs.append(developer.strip())
    avg_weighted_point = sum(dev_points) / len(dev_points) if dev_points else 1
    return avg_weighted_point, missing_devs


def calculate_google_trends_points(game_name: str) -> int:
    return fetch_game_trends(game_name)


def populate_appids():
    """
    Cross-reference inventory game names against raw_steam.csv to populate
    the steam_appid column. Only fills Steam platform rows where steam_appid
    is missing. Saves the updated inventory back to CSV.
    """
    from config import get_latest_steam_csv

    inv = pd.read_csv(INVENTORY_FILE, index_col=0)

    if 'steam_appid' not in inv.columns:
        inv['steam_appid'] = pd.NA

    steam_mask = inv['Platform'].str.contains('Steam', case=False, na=False)
    missing_mask = inv['steam_appid'].isna()
    to_match = inv[steam_mask & missing_mask]

    if to_match.empty:
        return

    raw_steam = pd.read_csv(get_latest_steam_csv())
    name_to_appid = {
        str(row['Name']).strip().lower(): row['AppId']
        for _, row in raw_steam.iterrows()
    }

    matched = False
    for idx, row in to_match.iterrows():
        game_name = str(row['Game Name']).strip()
        appid = name_to_appid.get(game_name.lower())
        if appid is not None:
            inv.at[idx, 'steam_appid'] = int(appid)
            matched = True

    if matched:
        inv.to_csv(INVENTORY_FILE, index=True)
