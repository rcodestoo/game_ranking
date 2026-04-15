"""
Non-Steam Pipeline
==================
Runs the non-steam script.py scraper headlessly (no input() prompts),
then appends newly scraped games to raw/raw_non_steam_YYYY-MM-DD.csv.

Three stages:
  1. collect_followers()           — scrapes Steam community groups for follower counts
  2. collect_igdb_data_for_games() — fetches IGDB genres/themes/keywords + YouTube stats
  3. export_to_csv()               — merges all data → combined JSON → final CSV

All file paths and tunable parameters are passed in from the Streamlit UI.
The interactive input() call inside export_to_csv is monkey-patched away.
"""

import ast
import builtins
import importlib.util
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from config import RAW_DIR, CACHE_DIR, get_latest_nonsteam_csv
from pipelines.state import get_next_window, mark_run_complete

logger = logging.getLogger(__name__)

# ── Default paths (relative to the scraper repo) ──────────────────────────────
_SCRAPER_REPO = Path("C:/Users/Rasika/Desktop/AGS/repos/SteamCommunityGroupScraper")

SCRIPT_PATH_DEFAULT       = _SCRAPER_REPO / "script.py"
GAMES_JSON_DEFAULT        = _SCRAPER_REPO / "games.json"
FOLLOWER_JSON_DEFAULT     = _SCRAPER_REPO / "follower_counts_output.json"
IGDB_DETAILS_JSON_DEFAULT = _SCRAPER_REPO / "igdb_game_details.json"
COMBINED_JSON_DEFAULT     = _SCRAPER_REPO / "data_full_combined.json"

TEMP_EXPORT_CSV  = CACHE_DIR / "_nonsteam_temp_export.csv"
MAX_GAMES_DEFAULT     = 100
MIN_FOLLOWERS_DEFAULT = 0


# ── Module loader ──────────────────────────────────────────────────────────────

def _load_script_module(script_path: Path):
    """Dynamically load script.py as a module without executing its main()."""
    if not script_path.exists():
        raise FileNotFoundError(
            f"Non-steam scraper not found at {script_path}. "
            "Check the Script Path setting on the Non-Steam tab."
        )
    spec = importlib.util.spec_from_file_location("nonsteam_script", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── input() patcher ───────────────────────────────────────────────────────────

def _make_fake_input(answers: dict):
    """Build a fake input() function that returns fixed answers by prompt substring."""
    def fake_input(prompt=""):
        prompt_lower = str(prompt).lower()
        for key, value in answers.items():
            if key in prompt_lower:
                logger.info(f"[auto-input] '{prompt}' -> '{value}'")
                return str(value)
        logger.warning(f"[auto-input] Unmatched prompt: '{prompt}' -- returning empty string")
        return ""
    return fake_input


def _patch_module_input(module, answers: dict):
    """
    Patch input() both on the module's __builtins__ AND on the real builtins module.
    The builtins patch is stored so it can be restored after the pipeline run.
    """
    fake = _make_fake_input(answers)

    builtins_dict = {k: getattr(builtins, k) for k in dir(builtins) if not k.startswith("_")}
    builtins_dict["input"] = fake
    module.__builtins__ = builtins_dict

    builtins.input = fake


# ── stdout capture ────────────────────────────────────────────────────────────

class _StreamCapture:
    """Redirect stdout from the script module to the status_callback."""
    def __init__(self, callback):
        self.callback = callback
        self._buf = ""

    def write(self, text):
        self._buf += text
        if "\n" in text:
            lines = self._buf.split("\n")
            for line in lines[:-1]:
                if line.strip():
                    self.callback(f"  {line.strip()}")
            self._buf = lines[-1]

    def flush(self):
        pass


# ── Main pipeline entry point ─────────────────────────────────────────────────

def run_nonsteam_scraper(
    script_path: str = None,
    games_json: str = None,
    follower_json: str = None,
    igdb_details_json: str = None,
    combined_json: str = None,
    max_games: int = None,
    min_followers: int = None,
    start_date: str = None,
    end_date: str = None,
    status_callback=None,
) -> dict:
    """
    Run all three non-steam scraper stages headlessly.

    Returns dict: success, new_rows, window_start, window_end, error
    """

    def log(msg: str):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    script_path       = Path(script_path)       if script_path       else SCRIPT_PATH_DEFAULT
    games_json        = Path(games_json)        if games_json        else GAMES_JSON_DEFAULT
    follower_json     = Path(follower_json)     if follower_json     else FOLLOWER_JSON_DEFAULT
    igdb_details_json = Path(igdb_details_json) if igdb_details_json else IGDB_DETAILS_JSON_DEFAULT
    combined_json     = Path(combined_json)     if combined_json     else COMBINED_JSON_DEFAULT
    max_games         = max_games     if max_games     is not None else MAX_GAMES_DEFAULT
    min_followers     = min_followers if min_followers is not None else MIN_FOLLOWERS_DEFAULT

    if start_date is None or end_date is None:
        start_date, end_date = get_next_window("non_steam", window_days=14)
    log(f"Scrape window: {start_date} -> {end_date}")

    if not games_json.exists():
        msg = f"games.json not found at: {games_json}"
        log(f"ERROR: {msg}")
        return {"success": False, "new_rows": 0, "window_start": start_date,
                "window_end": end_date, "error": msg}

    _real_input = builtins.input

    try:
        log("Loading scraper module...")
        script = _load_script_module(script_path)

        _patch_module_input(script, {
            "minimum number of followers": str(min_followers),
            "export this monthly report":  "n",
            "export this report":          "n",
            "choice":                      "n",
            "youtube":                     "",
            "enter your choice":           "8",
        })

        capture = _StreamCapture(log)

        # STAGE 1 — Collect Steam follower counts
        log(f"Stage 1/3: Collecting follower counts "
            f"(processing up to {max_games if max_games else 'all'} games)...")
        old_stdout = sys.stdout
        sys.stdout = capture
        try:
            script.collect_followers(
                json_file=str(games_json),
                output_file=str(follower_json),
                max_games=max_games if max_games else None,
            )
        finally:
            sys.stdout = old_stdout
        log("Stage 1 complete.")

        # STAGE 2 — Fetch IGDB genres/themes/keywords + YouTube stats
        log("Stage 2/3: Fetching IGDB + YouTube data (skips already-fetched games)...")
        old_stdout = sys.stdout
        sys.stdout = capture
        try:
            script.collect_igdb_data_for_games(
                input_games_json_path=str(games_json),
                output_igdb_details_path=str(igdb_details_json),
            )
        finally:
            sys.stdout = old_stdout
        log("Stage 2 complete.")

        # STAGE 3 — Export combined data to CSV
        log("Stage 3/3: Exporting combined data to CSV...")
        old_stdout = sys.stdout
        sys.stdout = capture
        try:
            success = script.export_to_csv(
                games_json_file=str(games_json),
                follower_counts_file=str(follower_json),
                combined_json_file=str(combined_json),
                csv_file=str(TEMP_EXPORT_CSV),
                igdb_details_file=str(igdb_details_json),
                pop_steam_tags=False,
            )
        finally:
            sys.stdout = old_stdout

        if not success:
            return {"success": False, "new_rows": 0, "window_start": start_date,
                    "window_end": end_date, "error": "export_to_csv returned False"}
        log("Stage 3 complete.")

        new_rows = _append_to_raw_nonsteam(log)
        TEMP_EXPORT_CSV.unlink(missing_ok=True)

        mark_run_complete("non_steam", start_date, end_date)
        log(f"Pipeline complete -- {new_rows} new games appended.")
        return {"success": True, "new_rows": new_rows, "window_start": start_date,
                "window_end": end_date, "error": None}

    except Exception as e:
        log(f"Pipeline error: {e}")
        import traceback
        log(traceback.format_exc())
        return {"success": False, "new_rows": 0, "window_start": start_date,
                "window_end": end_date, "error": str(e)}

    finally:
        builtins.input = _real_input


# ── Canonical column schema ────────────────────────────────────────────────────

NONSTEAM_COLUMNS = [
    "Game Title",
    "Category",
    "Release Date",
    "Developers",
    "Publishers",
    "Platforms",
    "Genres",
    "Themes",
    "Keywords",
    "YouTube URL",
    "YouTube Views",
    "YouTube Likes",
    "YouTube ReleaseDate",
    "SteamStatus",
    "date_appended",
]

_NONSTEAM_RENAME = {
    "Name":                "Game Title",
    "name":                "Game Title",
    "game_title":          "Game Title",
    "ReleaseDate":         "Release Date",
    "release_date":        "Release Date",
    "developers":          "Developers",
    "publishers":          "Publishers",
    "platforms":           "Platforms",
    "genres":              "Genres",
    "themes":              "Themes",
    "keywords":            "Keywords",
    "YoutubeURL":          "YouTube URL",
    "youtube_url":         "YouTube URL",
    "YoutubeViewCount":    "YouTube Views",
    "youtube_views":       "YouTube Views",
    "YoutubeLikeCount":    "YouTube Likes",
    "youtube_likes":       "YouTube Likes",
    "YoutubeReleaseDate":  "YouTube ReleaseDate",
    "youtube_releasedate": "YouTube ReleaseDate",
    "steam_status":        "SteamStatus",
}

_STEAM_API        = "https://store.steampowered.com/api/appdetails"
_STEAM_SEARCH_API = "https://store.steampowered.com/api/storesearch/"
_PC_PLATFORM_KEYWORDS = ("pc (microsoft windows)", "windows")


def _extract_platforms_from_release_info(release_info_val) -> str:
    try:
        info = ast.literal_eval(str(release_info_val))
        plats = list(info.get("original_platforms", []))
        for port in info.get("ports", []):
            plats.append(port.get("platform", ""))
        return ", ".join(set(p for p in plats if p))
    except Exception:
        return ""


def _detect_steam_status(app_id, platforms_str: str = "") -> str:
    try:
        resp = requests.get(
            _STEAM_API,
            params={"appids": int(app_id), "filters": "basic"},
            timeout=5,
        )
        if resp.json().get(str(int(app_id)), {}).get("success"):
            return "PC Game (on Steam)"
    except Exception:
        pass
    plats_lower = str(platforms_str).lower()
    if any(kw in plats_lower for kw in _PC_PLATFORM_KEYWORDS):
        return "Non-Steam PC Game"
    return "Console / Other"


def _search_steam_app_id(name: str):
    try:
        resp = requests.get(
            _STEAM_SEARCH_API,
            params={"term": name, "l": "english", "cc": "US"},
            timeout=5,
        )
        resp.raise_for_status()
        for item in resp.json().get("items", []):
            if item.get("name", "").strip().lower() == name.strip().lower():
                return item["id"]
    except Exception:
        pass
    return None


def _fill_steam_status(df: pd.DataFrame, log=None) -> pd.DataFrame:
    if "AppId" not in df.columns:
        return df

    if "SteamStatus" not in df.columns:
        df["SteamStatus"] = None

    rows_to_check = df[df["AppId"].notna()]
    if rows_to_check.empty:
        return df

    if log:
        log(f"Detecting SteamStatus for {len(rows_to_check)} games via Steam store API...")

    for idx, row in rows_to_check.iterrows():
        platforms_str = _extract_platforms_from_release_info(row.get("ReleaseInfo", ""))
        try:
            status = _detect_steam_status(row["AppId"], platforms_str)
        except Exception:
            status = "Console / Other"
        time.sleep(0.5)

        if status != "PC Game (on Steam)":
            name = str(row.get("Name", "")).strip()
            try:
                store_id = _search_steam_app_id(name)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    if log:
                        log("Rate limited by Steam. Waiting 10s...")
                    time.sleep(10)
                    store_id = _search_steam_app_id(name)
                else:
                    store_id = None
            if store_id is not None:
                status = _detect_steam_status(store_id, platforms_str)
            time.sleep(0.5)

        df.at[idx, "SteamStatus"] = status

    return df


def verify_single_game_steam_status(game_title: str, platforms_str: str = "") -> str:
    """Check a single game by name against the Steam store. Returns status string."""
    app_id = _search_steam_app_id(game_title)
    if app_id is not None:
        return _detect_steam_status(app_id, platforms_str)
    plats_lower = platforms_str.lower()
    if any(kw in plats_lower for kw in _PC_PLATFORM_KEYWORDS):
        return "Non-Steam PC Game"
    return "Console / Other"


def backfill_steam_status(log=None) -> int:
    """
    Re-check every game in the latest non-steam CSV against the Steam store.
    Writes results back to the same CSV file. Returns number of rows processed.
    """
    source_path = get_latest_nonsteam_csv()
    if not source_path.exists():
        if log:
            log("No non-steam CSV found.")
        return 0

    df = pd.read_csv(source_path, encoding='utf-8-sig')
    df = _normalize_nonsteam_df(df)
    n = len(df)
    if log:
        log(f"Checking {n} games against Steam store...")

    for i, (idx, row) in enumerate(df.iterrows()):
        name = str(row.get("Game Title", "")).strip()
        platforms_str = str(row.get("Platforms", ""))

        try:
            app_id = _search_steam_app_id(name)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                if log:
                    log("Rate limited by Steam. Waiting 10 seconds...")
                time.sleep(10)
                app_id = _search_steam_app_id(name)
            else:
                app_id = None

        if app_id is not None:
            status = _detect_steam_status(app_id, platforms_str)
        else:
            plats_lower = platforms_str.lower()
            if any(kw in plats_lower for kw in _PC_PLATFORM_KEYWORDS):
                status = "Non-Steam PC Game"
            else:
                status = "Console / Other"

        df.at[idx, "SteamStatus"] = status

        if log and (i + 1) % 25 == 0:
            log(f"  {i + 1}/{n} checked...")

        time.sleep(1.5)

    df.to_csv(source_path, index=False)
    if log:
        log(f"Done. SteamStatus updated for all {n} games.")
    return n


def _normalize_nonsteam_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename alias columns to canonical names and enforce NONSTEAM_COLUMNS order."""
    df = df.rename(columns=_NONSTEAM_RENAME)
    for col in NONSTEAM_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[NONSTEAM_COLUMNS]


def _normalize_release_date(series: pd.Series) -> pd.Series:
    """
    Convert date values to ISO format (YYYY-MM-DD) where parseable.
    Handles mixed D/M/YYYY and M/D/YYYY slash formats by inspecting the
    numeric parts to determine which format is unambiguous:
      - part[0] > 12  → must be D/M/YYYY  (e.g. 15/1/2026)
      - part[1] > 12  → must be M/D/YYYY  (e.g. 3/17/2026)
      - both ≤ 12     → ambiguous; defaults to D/M/YYYY
    Non-date strings like 'Coming Soon' or 'TBD' are kept as-is.
    """
    def _parse(val):
        if pd.isna(val) or str(val).strip() == "":
            return None
        s = str(val).strip()

        # Already ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS...)
        if len(s) >= 10 and s[4] == '-':
            try:
                return pd.to_datetime(s).strftime("%Y-%m-%d")
            except Exception:
                return s

        # Slash-separated: detect format from numeric parts
        if '/' in s:
            parts = s.split('/')
            if len(parts) == 3:
                try:
                    p1, p2 = int(parts[0]), int(parts[1])
                    if p1 > 12:
                        dayfirst = True   # e.g. 15/1/2026 → day=15
                    elif p2 > 12:
                        dayfirst = False  # e.g. 3/17/2026 → month=3
                    else:
                        dayfirst = True   # ambiguous → default D/M
                    return pd.to_datetime(s, dayfirst=dayfirst).strftime("%Y-%m-%d")
                except Exception:
                    return s

        # Fallback for any other format pandas can parse (e.g. 'Jan 15, 2026')
        try:
            return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
        except Exception:
            return s  # 'Coming Soon', 'TBD', 'N/A', etc.

    return series.apply(_parse)


def _append_to_raw_nonsteam(log) -> int:
    """
    Read the temp export CSV and append only new rows (by Game Title) to a
    timestamped raw_non_steam_YYYY-MM-DD.csv. Returns number of new rows added.
    """
    try:
        new_df = pd.read_csv(TEMP_EXPORT_CSV, encoding="utf-8-sig")
    except Exception as e:
        log(f"Could not read temp export: {e}")
        return 0

    if new_df.empty:
        log("Temp export was empty -- no games found")
        return 0

    new_df = _fill_steam_status(new_df, log)

    new_df["date_appended"] = None
    new_df = _normalize_nonsteam_df(new_df)
    new_df["Release Date"] = _normalize_release_date(new_df["Release Date"])
    new_df["YouTube ReleaseDate"] = _normalize_release_date(new_df["YouTube ReleaseDate"])

    no_video = (
        new_df["YouTube URL"].isna() |
        (new_df["YouTube URL"].astype(str).str.strip() == "")
    )
    new_df.loc[no_video, "YouTube URL"] = "No YouTube video found"

    source_path = get_latest_nonsteam_csv()
    out_path = RAW_DIR / f"raw_non_steam_{date.today()}.csv"

    if source_path.exists():
        existing_df = pd.read_csv(source_path, encoding='utf-8-sig')
        existing_df = _normalize_nonsteam_df(existing_df)

        existing_names = set(
            existing_df["Game Title"].astype(str).str.strip().str.lower()
        )
        new_only = new_df[
            ~new_df["Game Title"].astype(str).str.strip().str.lower().isin(existing_names)
        ].copy()

        if new_only.empty:
            log("No new unique games -- CSV already up to date")
            return 0

        new_only["date_appended"] = date.today().isoformat()

        combined = pd.concat([existing_df, new_only], ignore_index=True)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_path, index=False)
        log(f"Saved {len(new_only)} new rows → {out_path.name}")
        return len(new_only)
    else:
        new_df["date_appended"] = date.today().isoformat()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_csv(out_path, index=False)
        log(f"Created {out_path.name} with {len(new_df)} rows")
        return len(new_df)


def append_from_uploaded_nonsteam_csv(uploaded_df: pd.DataFrame) -> tuple:
    """
    Merge an externally uploaded non-steam DataFrame into the persistent CSV.
    - Rows whose Game Title already exists are OVERWRITTEN.
    - New rows are APPENDED.
    Returns (n_updated, n_new).
    """
    today = date.today().isoformat()

    uploaded_df = uploaded_df.copy()
    uploaded_df = _normalize_nonsteam_df(uploaded_df)
    uploaded_df["Release Date"] = _normalize_release_date(uploaded_df["Release Date"])
    uploaded_df["YouTube ReleaseDate"] = _normalize_release_date(uploaded_df["YouTube ReleaseDate"])

    no_video = (
        uploaded_df["YouTube URL"].isna() |
        (uploaded_df["YouTube URL"].astype(str).str.strip() == "")
    )
    uploaded_df.loc[no_video, "YouTube URL"] = "No YouTube video found"

    uploaded_df["date_appended"] = today

    upload_names = set(uploaded_df["Game Title"].astype(str).str.strip().str.lower())

    source_path = get_latest_nonsteam_csv()
    out_path = RAW_DIR / f"raw_non_steam_{date.today()}.csv"

    if source_path.exists():
        existing_df = pd.read_csv(source_path, encoding='utf-8-sig')
        existing_df = _normalize_nonsteam_df(existing_df)

        existing_names = set(existing_df["Game Title"].astype(str).str.strip().str.lower())
        n_updated = len(upload_names & existing_names)
        n_new = len(upload_names - existing_names)

        kept = existing_df[
            ~existing_df["Game Title"].astype(str).str.strip().str.lower().isin(upload_names)
        ]
        combined = pd.concat([kept, uploaded_df], ignore_index=True)
    else:
        n_updated, n_new = 0, len(uploaded_df)
        combined = uploaded_df

    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_path, index=False)
    return n_updated, n_new
