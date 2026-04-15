"""
Main Streamlit entry point.
Sets up page config, sidebar, shared session state, and renders tabs.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime as dt
import io

import pandas as pd
import streamlit as st

from config import INVENTORY_FILE, TRENDS_CACHE_FILE, get_latest_steam_csv, get_latest_nonsteam_csv
from calculation.process_data import load_data, clean_dev_genre_list, flagging, populate_appids
from calculation.steam_players import fetch_player_counts_if_needed, resolve_inventory_appids
from pipelines.steam_pipeline import append_from_uploaded_steam_csv
from pipelines.nonsteam_pipeline import append_from_uploaded_nonsteam_csv
from app.helpers import load_defaults, reload_steam_from_csv, reload_nonsteam_from_csv
from app import tab_steam, tab_nonsteam, tab_inventory


# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="AGS - Game Ranking Tool", layout="wide")

st.title("🎮 AGS Game Ranking Tool")
st.caption("Review priority scores for Steam and Non-Steam games. Upload a CSV or use the defaults — adjust weights and filters as needed.")

# ── Session state initialisation ──────────────────────────────────────────────
_SESSION_DEFAULTS = {
    "df_steam":               None,
    "df_nonsteam":            None,
    "steam_cleaned":          False,
    "nonsteam_cleaned":       False,
    "uploaded_steam_bytes":   None,
    "uploaded_steam_name":    None,
    "uploaded_nonsteam_bytes": None,
    "uploaded_nonsteam_name": None,
    # "steam_scraper_log":      [],   # scraper disabled
    # "nonsteam_scraper_log":   [],  # scraper disabled
}
for _key, _default in _SESSION_DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default

if "dev_list" not in st.session_state:
    try:
        _, _, st.session_state.dev_list, st.session_state.genre_list, _ = load_data(
            get_latest_steam_csv(), get_latest_nonsteam_csv()
        )
    except Exception:
        pass

# ── SIDEBAR: FILE UPLOADS ──────────────────────────────────────────────────────
st.sidebar.header("📁 Data")

uploaded_steam    = st.sidebar.file_uploader("Upload Steam CSV",     type="csv", key="steam_upload")
uploaded_nonsteam = st.sidebar.file_uploader("Upload Non-Steam CSV", type="csv", key="nonsteam_upload")

if uploaded_steam and uploaded_steam.name != st.session_state.uploaded_steam_name:
    st.session_state.uploaded_steam_bytes = uploaded_steam.getvalue()
    st.session_state.uploaded_steam_name  = uploaded_steam.name

if uploaded_nonsteam and uploaded_nonsteam.name != st.session_state.uploaded_nonsteam_name:
    st.session_state.uploaded_nonsteam_bytes = uploaded_nonsteam.getvalue()
    st.session_state.uploaded_nonsteam_name  = uploaded_nonsteam.name

# Preview and load — Steam
if st.session_state.uploaded_steam_bytes:
    with st.sidebar.expander("👀 Preview Steam File"):
        preview_steam = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
        st.dataframe(preview_steam.head(3), width='stretch')
        st.caption(f"Rows: {len(preview_steam)}, Columns: {len(preview_steam.columns)}")

    if st.sidebar.button("📥 Load Steam Data", key="load_steam_btn"):
        try:
            steam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_steam_bytes))
            steam_required_cols = ['Name', 'FollowerCount', 'Developers', 'Genres', 'ReleaseDate']
            steam_missing = [col for col in steam_required_cols if col not in steam_df_upload.columns]
            if steam_missing:
                st.sidebar.error(f"Missing columns: {', '.join(steam_missing)}")
            else:
                n_updated, n_new = append_from_uploaded_steam_csv(steam_df_upload)
                reload_steam_from_csv()
                st.sidebar.success(f"✅ Saved: {n_new} new, {n_updated} updated")
        except Exception as e:
            st.sidebar.error(f"Error loading file: {e}")
else:
    st.sidebar.caption("Using default Steam CSV.")

# Preview and load — Non-Steam
if st.session_state.uploaded_nonsteam_bytes:
    with st.sidebar.expander("👀 Preview Non-Steam File"):
        preview_nonsteam = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
        st.dataframe(preview_nonsteam.head(3), width='stretch')
        st.caption(f"Rows: {len(preview_nonsteam)}, Columns: {len(preview_nonsteam.columns)}")

    if st.sidebar.button("📥 Load Non-Steam Data", key="load_nonsteam_btn"):
        try:
            nonsteam_df_upload = pd.read_csv(io.BytesIO(st.session_state.uploaded_nonsteam_bytes))
            nonsteam_required_cols = ['Game Title', 'Developers', 'SteamStatus', 'YouTube Views']
            nonsteam_missing = [col for col in nonsteam_required_cols if col not in nonsteam_df_upload.columns]
            if nonsteam_missing:
                st.sidebar.error(f"Missing columns: {', '.join(nonsteam_missing)}")
            else:
                n_updated, n_new = append_from_uploaded_nonsteam_csv(nonsteam_df_upload)
                reload_nonsteam_from_csv()
                st.sidebar.success(f"✅ Saved: {n_new} new, {n_updated} updated")
        except Exception as e:
            st.sidebar.error(f"Error loading file: {e}")
else:
    st.sidebar.caption("Using default Non-Steam CSV.")

st.sidebar.divider()
if st.sidebar.button("🔄 Reset to Defaults", use_container_width=True):
    load_defaults()
    st.rerun()

# Load defaults if not already loaded
try:
    if st.session_state.df_steam is None or st.session_state.df_nonsteam is None:
        load_defaults()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Retrieve data from session state
df_steam  = st.session_state.df_steam.copy()
df_nonsteam = st.session_state.df_nonsteam.copy()

# ── Global date range (shared across all tabs) ────────────────────────────────
def _min_date(series: pd.Series) -> dt.date:
    parsed = pd.to_datetime(series, errors='coerce').dropna()
    return parsed.min().date() if len(parsed) else dt.date(2000, 1, 1)

def _max_date(series: pd.Series) -> dt.date:
    parsed = pd.to_datetime(series, errors='coerce').dropna()
    return parsed.max().date() if len(parsed) else dt.date.today()

_steam_min = _min_date(df_steam.get('ReleaseDate',            pd.Series(dtype=str)))
_steam_max = _max_date(df_steam.get('ReleaseDate',            pd.Series(dtype=str)))
_ns_min    = _min_date(df_nonsteam.get('YouTube ReleaseDate', pd.Series(dtype=str)))
_ns_max    = _max_date(df_nonsteam.get('YouTube ReleaseDate', pd.Series(dtype=str)))
_inv_min   = _min_date(
    st.session_state.game_data.get('Date Purchased', pd.Series(dtype=str))
    if 'game_data' in st.session_state else pd.Series(dtype=str)
)
_inv_max   = _max_date(
    st.session_state.game_data.get('Date Purchased', pd.Series(dtype=str))
    if 'game_data' in st.session_state else pd.Series(dtype=str)
)
GLOBAL_DATE_MIN = min(_steam_min, _ns_min, _inv_min)
GLOBAL_DATE_MAX = max(_steam_max, _ns_max, _inv_max)

# ── Pre-render resets (must run before ANY widget is instantiated) ────────────
if (st.session_state.get("steam_reset_filters") or
        st.session_state.get("ns_reset_filters") or
        st.session_state.get("inv_reset_filters")):
    for _k in ("steam_start_date", "ns_start_date", "inv_start_date"):
        st.session_state[_k] = GLOBAL_DATE_MIN
    for _k in ("steam_end_date", "ns_end_date", "inv_end_date"):
        st.session_state[_k] = GLOBAL_DATE_MAX
    st.session_state["inv_status_quick_filter"] = None

# Clamp stale session-state date values to the computed range
for _key in ("steam_start_date", "ns_start_date", "inv_start_date"):
    if _key in st.session_state and isinstance(st.session_state[_key], dt.date):
        st.session_state[_key] = max(st.session_state[_key], GLOBAL_DATE_MIN)
for _key in ("steam_end_date", "ns_end_date", "inv_end_date"):
    if _key in st.session_state and isinstance(st.session_state[_key], dt.date):
        st.session_state[_key] = min(st.session_state[_key], GLOBAL_DATE_MAX)

# ── Inventory AppID population + hourly player count fetch ────────────────────
populate_appids()
_inv_for_fetch = pd.read_csv(INVENTORY_FILE, index_col=0)
_inv_for_fetch, _n_appids_resolved = resolve_inventory_appids(_inv_for_fetch)
if _n_appids_resolved > 0:
    _inv_for_fetch.to_csv(INVENTORY_FILE, index=True)
    if "game_data" in st.session_state:
        st.session_state.game_data = pd.read_csv(INVENTORY_FILE, index_col=0)
st.session_state.player_count_history = fetch_player_counts_if_needed(_inv_for_fetch)

# ── Load cached trends scores ─────────────────────────────────────────────────
if "nonsteam_trends" not in st.session_state:
    if TRENDS_CACHE_FILE.exists():
        _tc = pd.read_csv(TRENDS_CACHE_FILE)
        st.session_state.nonsteam_trends = dict(zip(_tc["game_name"], _tc["trends_score"]))
        if "fetched_at" in _tc.columns:
            _fetched_vals = _tc["fetched_at"].dropna()
            st.session_state.trends_last_fetched_at = str(_fetched_vals.iloc[-1]) if len(_fetched_vals) else None
        else:
            st.session_state.trends_last_fetched_at = None
    else:
        st.session_state.nonsteam_trends = {}
        st.session_state.trends_last_fetched_at = None

# ── TABS ───────────────────────────────────────────────────────────────────────
_tab_steam, _tab_nonsteam, _tab_inventory = st.tabs(
    ["🚀 Steam Report", "📽️ Non-Steam Report", "🎮 Game Inventory"]
)

with _tab_steam:
    tab_steam.render(GLOBAL_DATE_MIN, GLOBAL_DATE_MAX)

with _tab_nonsteam:
    tab_nonsteam.render(df_steam, GLOBAL_DATE_MIN, GLOBAL_DATE_MAX)

with _tab_inventory:
    tab_inventory.render(GLOBAL_DATE_MIN, GLOBAL_DATE_MAX)
