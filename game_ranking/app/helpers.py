"""
Shared helper functions used across app modules.
"""

import datetime as dt
import streamlit as st
import pandas as pd

from config import get_latest_steam_csv, get_latest_nonsteam_csv
from calculation.process_data import load_data, clean_dev_genre_list, flagging


_TRENDS_TS_FMT = "%Y-%m-%d %H:%M:%S"
TRENDS_TTL_HOURS = 24


def filter_stale_trends_games(games: list, cache_timestamps: dict) -> list:
    """
    Return only the games from `games` that are missing from the cache or whose
    cached timestamp is older than TRENDS_TTL_HOURS. Pass a dict of
    {game_name: fetched_at_str} built from the trends cache CSV.
    """
    cutoff = dt.datetime.now() - dt.timedelta(hours=TRENDS_TTL_HOURS)
    stale = []
    for game in games:
        ts_str = cache_timestamps.get(game)
        if not ts_str:
            stale.append(game)
            continue
        try:
            if dt.datetime.strptime(ts_str, _TRENDS_TS_FMT) < cutoff:
                stale.append(game)
        except Exception:
            stale.append(game)
    return stale


def load_trends_cache_timestamps(trends_cache_file) -> dict:
    """Return {game_name: fetched_at_str} from the trends cache CSV."""
    try:
        df = pd.read_csv(trends_cache_file)
        if "fetched_at" in df.columns and "game_name" in df.columns:
            return dict(zip(df["game_name"], df["fetched_at"]))
    except Exception:
        pass
    return {}


def highlight_new_rows(df: pd.DataFrame):
    """Return a styled dataframe with today's newly appended rows highlighted yellow."""
    today = dt.date.today().isoformat()

    def _row_style(row):
        if str(row.get("date_appended", "")).startswith(today):
            return ["background-color: #fff59d; color: #000000"] * len(row)
        return [""] * len(row)

    if "date_appended" in df.columns:
        return df.style.apply(_row_style, axis=1)
    return df


def load_defaults():
    """Load default CSV files into session state and clean/flag the Steam DataFrame."""
    df_steam, df_nonsteam, dev_list, genre_list, inventory = load_data(
        get_latest_steam_csv(), get_latest_nonsteam_csv()
    )
    df_steam = clean_dev_genre_list(df_steam)
    df_steam = flagging(df_steam)
    st.session_state.df_steam = df_steam
    st.session_state.steam_source = "default file"
    st.session_state.steam_cleaned = True
    st.session_state.df_nonsteam = df_nonsteam
    st.session_state.nonsteam_source = "default file"
    st.session_state.nonsteam_cleaned = True
    st.session_state.dev_list = dev_list
    st.session_state.genre_list = genre_list
    st.session_state.uploaded_steam_bytes = None
    st.session_state.uploaded_steam_name = None
    st.session_state.uploaded_nonsteam_bytes = None
    st.session_state.uploaded_nonsteam_name = None


def reload_steam_from_csv():
    """Re-read the latest raw_steam_YYYY-MM-DD.csv from disk and update session state."""
    try:
        latest = get_latest_steam_csv()
        tmp = pd.read_csv(latest)
        tmp = clean_dev_genre_list(tmp)
        tmp = flagging(tmp)
        st.session_state.df_steam = tmp
        st.session_state.steam_source = latest.name
        st.session_state.steam_cleaned = True
    except Exception as e:
        st.error(f"Failed to reload Steam CSV: {e}")


def reload_nonsteam_from_csv():
    """Re-read the latest raw_non_steam_YYYY-MM-DD.csv from disk and update session state."""
    try:
        latest = get_latest_nonsteam_csv()
        tmp = pd.read_csv(latest, encoding='utf-8-sig')
        st.session_state.df_nonsteam = tmp
        st.session_state.nonsteam_source = latest.name
        st.session_state.nonsteam_cleaned = True
    except Exception as e:
        st.error(f"Failed to reload Non-Steam CSV: {e}")


def format_last_run(info: dict) -> str:
    last = info.get("last_run_date")
    if not last:
        return "Never run"
    try:
        ts = dt.datetime.fromisoformat(last)
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return last


def format_next_window(scraper: str) -> str:
    from pipelines.state import get_next_window
    start, end = get_next_window(scraper, window_days=14)
    return f"{start} → {end}"
