"""
Non-Steam Pipeline
==================
Runs the non-steam script.py scraper headlessly (no input() prompts),
then appends newly scraped games to default_files/raw_non_steam.csv.

Three stages:
  1. collect_followers()           — scrapes Steam community groups for follower counts
  2. collect_igdb_data_for_games() — fetches IGDB genres/themes/keywords + YouTube stats
  3. export_to_csv()               — merges all data → combined JSON → final CSV

All file paths and tunable parameters are passed in from the Streamlit UI.
The interactive input() call inside export_to_csv is monkey-patched away.
"""

import builtins
import importlib.util
import json
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from config import RAW_DIR, get_latest_nonsteam_csv
from pipeline.state import get_next_window, mark_run_complete

logger = logging.getLogger(__name__)

# ── Default paths (relative to the scraper repo) ──────────────────────────────
_SCRAPER_REPO = Path("C:/Users/Rasika/Desktop/AGS/repos/SteamCommunityGroupScraper")

SCRIPT_PATH_DEFAULT       = _SCRAPER_REPO / "script.py"
GAMES_JSON_DEFAULT        = _SCRAPER_REPO / "games.json"
FOLLOWER_JSON_DEFAULT     = _SCRAPER_REPO / "follower_counts_output.json"
IGDB_DETAILS_JSON_DEFAULT = _SCRAPER_REPO / "igdb_game_details.json"
COMBINED_JSON_DEFAULT     = _SCRAPER_REPO / "data_full_combined.json"

# Output CSV lives in the AGS project
_AGS_ROOT        = Path(__file__).resolve().parent.parent
RAW_NONSTEAM_CSV = _AGS_ROOT / "default_files" / "raw_non_steam.csv"
TEMP_EXPORT_CSV  = Path("C:/Users/Rasika/Desktop/AGS/repos/game_ranking/default_files/_nonsteam_temp_export.csv")
MAX_GAMES_DEFAULT     = 100  # Stage 1 cap — avoids very long runs
MIN_FOLLOWERS_DEFAULT = 0    # export all games; Streamlit filters later


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
    The module-level patch handles calls resolved via the module namespace;
    the builtins-level patch handles calls in functions that resolve input() directly
    from the built-in scope (e.g. export_to_csv in script.py).
    The builtins patch is stored so it can be restored after the pipeline run.
    """
    fake = _make_fake_input(answers)

    # Patch the module's own builtins dict
    builtins_dict = {k: getattr(builtins, k) for k in dir(builtins) if not k.startswith("_")}
    builtins_dict["input"] = fake
    module.__builtins__ = builtins_dict

    # Also patch the real builtins module so ALL code in the process sees the fake
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
    # File paths (all optional — fall back to defaults above)
    script_path: str = None,
    games_json: str = None,
    follower_json: str = None,
    igdb_details_json: str = None,
    combined_json: str = None,
    # Stage 1 config
    max_games: int = None,
    # Stage 3 config
    min_followers: int = None,
    # Date window (optional override)
    start_date: str = None,
    end_date: str = None,
    # Streamlit callback
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

    # ── Resolve paths and settings ─────────────────────────────────────────────
    script_path       = Path(script_path)       if script_path       else SCRIPT_PATH_DEFAULT
    games_json        = Path(games_json)        if games_json        else GAMES_JSON_DEFAULT
    follower_json     = Path(follower_json)     if follower_json     else FOLLOWER_JSON_DEFAULT
    igdb_details_json = Path(igdb_details_json) if igdb_details_json else IGDB_DETAILS_JSON_DEFAULT
    combined_json     = Path(combined_json)     if combined_json     else COMBINED_JSON_DEFAULT
    max_games         = max_games     if max_games     is not None else MAX_GAMES_DEFAULT
    min_followers     = min_followers if min_followers is not None else MIN_FOLLOWERS_DEFAULT

    # ── Date window ────────────────────────────────────────────────────────────
    if start_date is None or end_date is None:
        start_date, end_date = get_next_window("non_steam", window_days=14)
    log(f"Scrape window: {start_date} -> {end_date}")

    # ── Preflight ──────────────────────────────────────────────────────────────
    if not games_json.exists():
        msg = f"games.json not found at: {games_json}"
        log(f"ERROR: {msg}")
        return {"success": False, "new_rows": 0, "window_start": start_date,
                "window_end": end_date, "error": msg}

    # Save the real input() so we can restore it after the pipeline
    _real_input = builtins.input

    try:
        log("Loading scraper module...")
        script = _load_script_module(script_path)

        # Patch away all interactive input() calls — both on the module and globally,
        # because export_to_csv resolves input() from the built-in scope directly.
        _patch_module_input(script, {
            "minimum number of followers": str(min_followers),
            "export this monthly report":  "n",
            "export this report":          "n",
            "choice":                      "n",
            "youtube":                     "",
            "enter your choice":           "8",
        })

        capture = _StreamCapture(log)

        # ══════════════════════════════════════════════════════════════════════
        # STAGE 1 — Collect Steam follower counts
        # ══════════════════════════════════════════════════════════════════════
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

        # ══════════════════════════════════════════════════════════════════════
        # STAGE 2 — Fetch IGDB genres/themes/keywords + YouTube stats
        # ══════════════════════════════════════════════════════════════════════
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

        # ══════════════════════════════════════════════════════════════════════
        # STAGE 3 — Export combined data to CSV
        # ══════════════════════════════════════════════════════════════════════
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

        # ── Append to master CSV ───────────────────────────────────────────────
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
        # Always restore the real input() regardless of success or failure
        builtins.input = _real_input


# ── Canonical column schema ────────────────────────────────────────────────────

# Authoritative column order for all non-steam CSV files.
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

# Known scraper column aliases → canonical names
# Covers both the camelCase names from export_to_csv and any lowercase variants
_NONSTEAM_RENAME = {
    # Game title
    "Name":                "Game Title",
    "name":                "Game Title",
    "game_title":          "Game Title",
    # Release date
    "ReleaseDate":         "Release Date",
    "release_date":        "Release Date",
    # Developers / publishers / platforms (lowercase only; Title-case already matches)
    "developers":          "Developers",
    "publishers":          "Publishers",
    "platforms":           "Platforms",
    "genres":              "Genres",
    "themes":              "Themes",
    "keywords":            "Keywords",
    # YouTube fields — scraper uses camelCase
    "YoutubeURL":          "YouTube URL",
    "youtube_url":         "YouTube URL",
    "YoutubeViewCount":    "YouTube Views",
    "youtube_views":       "YouTube Views",
    "YoutubeLikeCount":    "YouTube Likes",
    "youtube_likes":       "YouTube Likes",
    "YoutubeReleaseDate":  "YouTube ReleaseDate",
    "youtube_releasedate": "YouTube ReleaseDate",
    # Steam status
    "steam_status":        "SteamStatus",
}


def _normalize_nonsteam_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename alias columns to canonical names and enforce NONSTEAM_COLUMNS order."""
    df = df.rename(columns=_NONSTEAM_RENAME)
    for col in NONSTEAM_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[NONSTEAM_COLUMNS]


def _normalize_release_date(series: pd.Series) -> pd.Series:
    """
    Convert Release Date values to ISO format (YYYY-MM-DD) where parseable.
    Non-date strings like 'Coming Soon' or 'TBD' are kept as-is so the UI
    can display why the date is missing.
    """
    def _parse(val):
        if pd.isna(val) or str(val).strip() == "":
            return None
        s = str(val).strip()
        try:
            return pd.to_datetime(s, dayfirst=False).strftime("%Y-%m-%d")
        except Exception:
            return s  # preserve 'Coming Soon', 'TBD', 'Q4 2026', etc.
    return series.apply(_parse)


# ── CSV append helper ──────────────────────────────────────────────────────────

def _append_to_raw_nonsteam(log) -> int:
    """
    Read the temp export CSV and append only new rows (by Game Title) to a
    timestamped raw_non_steam_YYYY-MM-DD.csv. Returns number of new rows added.
    """
    try:
        # The scraper writes with encoding='utf-8-sig'; match it here to avoid
        # Windows cp1252 misreads that corrupt multibyte game names and shift columns.
        new_df = pd.read_csv(TEMP_EXPORT_CSV, encoding="utf-8-sig")
    except Exception as e:
        log(f"Could not read temp export: {e}")
        return 0

    if new_df.empty:
        log("Temp export was empty -- no games found")
        return 0

    # Normalise to canonical column names before any further processing
    new_df["date_appended"] = None  # placeholder; stamped only on truly new rows below
    new_df = _normalize_nonsteam_df(new_df)

    # Fix 1: normalise Release Date to ISO format; keep non-date strings as-is
    new_df["Release Date"] = _normalize_release_date(new_df["Release Date"])

    # Fix 2: mark rows with no YouTube video found
    no_video = (
        new_df["YouTube URL"].isna() |
        (new_df["YouTube URL"].astype(str).str.strip() == "")
    )
    new_df.loc[no_video, "YouTube URL"] = "No YouTube video found"

    source_path = get_latest_nonsteam_csv()
    out_path = RAW_DIR / f"raw_non_steam_{date.today()}.csv"

    if source_path.exists():
        existing_df = pd.read_csv(source_path)
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

        # Stamp only the newly appended rows
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
    - ALL uploaded rows get date_appended = today.
    Returns (n_updated, n_new).
    """
    today = date.today().isoformat()

    uploaded_df = uploaded_df.copy()
    uploaded_df = _normalize_nonsteam_df(uploaded_df)
    uploaded_df["Release Date"] = _normalize_release_date(uploaded_df["Release Date"])

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
        existing_df = pd.read_csv(source_path)
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