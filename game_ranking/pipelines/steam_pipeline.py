"""
Steam Pipeline
==============
Triggers the C# Gawk-3000 scraper via `dotnet run` in non-interactive mode,
then appends newly scraped games to raw/raw_steam_YYYY-MM-DD.csv.

The scraper is fed via stdin so it never blocks waiting for Console.ReadLine().
Menu flow automated:
  3 → Export cache to CSV (with fixed options: all fields, date range from state)
"""

import subprocess
import csv
import io
import logging
import os
import tempfile
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from config import CSV_STEAM, BASE_DIR, RAW_DIR, CACHE_DIR, get_latest_steam_csv
from pipelines.state import get_next_window, mark_run_complete, load_state

logger = logging.getLogger(__name__)

SCRAPER_DIR  = BASE_DIR.parent / "Release-Gawk-3000" / "Gawk-3000"
RAW_STEAM_CSV = CSV_STEAM
TEMP_EXPORT   = CACHE_DIR / "_steam_temp_export.csv"


def _build_stdin_for_export(start_date: str, end_date: str) -> str:
    """
    Build the stdin string that answers the C# program's Console.ReadLine() prompts
    for menu option 3 (Export cache to CSV).
    """
    lines = [
        "3",
        str(TEMP_EXPORT),
        "y",   # Include genres
        "y",   # Include categories
        "y",   # Include publishers
        "y",   # Include developers
        "y",   # Filter by date
        start_date,
        end_date,
        "",    # Press any key
        "6",   # Exit
    ]
    return "\n".join(lines) + "\n"


def run_steam_scraper(start_date=None, end_date=None, status_callback=None) -> dict:
    """
    Run the Gawk-3000 C# scraper non-interactively.

    Args:
        start_date: optional start date string (YYYY-MM-DD). If None, uses get_next_window()
        end_date: optional end date string (YYYY-MM-DD). If None, uses get_next_window()
        status_callback: optional callable(str) for streaming status to Streamlit

    Returns:
        dict with keys: success (bool), new_rows (int), window_start, window_end, error (str|None)
    """
    def log(msg: str):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    if start_date is None or end_date is None:
        start_date, end_date = get_next_window("steam", window_days=14)

    log(f"🗓️ Steam scrape window: {start_date} → {end_date}")

    stdin_payload = _build_stdin_for_export(start_date, end_date)

    try:
        log("🚀 Starting dotnet run...")
        result = subprocess.run(
            ["dotnet", "run", "--project", str(SCRAPER_DIR)],
            input=stdin_payload,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(SCRAPER_DIR),
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout or "Unknown error"
            log(f"❌ dotnet run failed (exit {result.returncode}): {error_msg[:500]}")
            return {"success": False, "new_rows": 0, "window_start": start_date,
                    "window_end": end_date, "error": error_msg[:500]}

        log("✅ dotnet run completed")

        if not TEMP_EXPORT.exists():
            return {"success": False, "new_rows": 0, "window_start": start_date,
                    "window_end": end_date, "error": "Temp export CSV not created by scraper"}

        new_rows = _append_to_raw_steam(start_date, end_date, log)
        TEMP_EXPORT.unlink(missing_ok=True)

        mark_run_complete("steam", start_date, end_date)
        log(f"✅ Steam pipeline complete. {new_rows} new games appended.")
        return {"success": True, "new_rows": new_rows, "window_start": start_date,
                "window_end": end_date, "error": None}

    except subprocess.TimeoutExpired:
        msg = "Scraper timed out after 5 minutes"
        log(f"❌ {msg}")
        return {"success": False, "new_rows": 0, "window_start": start_date,
                "window_end": end_date, "error": msg}
    except FileNotFoundError:
        msg = ("dotnet not found. Make sure the .NET SDK is installed and "
               "the Gawk-3000 project path is set correctly in pipelines/steam_pipeline.py")
        log(f"❌ {msg}")
        return {"success": False, "new_rows": 0, "window_start": start_date,
                "window_end": end_date, "error": msg}
    except Exception as e:
        log(f"❌ Unexpected error: {e}")
        return {"success": False, "new_rows": 0, "window_start": start_date,
                "window_end": end_date, "error": str(e)}


def _normalize_release_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the ReleaseDate column to dd-mm-yyyy format."""
    if "ReleaseDate" not in df.columns:
        return df
    parsed = pd.to_datetime(df["ReleaseDate"], errors="coerce", dayfirst=True)
    mask = parsed.notna()
    df.loc[mask, "ReleaseDate"] = parsed[mask].dt.strftime("%d-%m-%Y")
    return df


def _append_to_raw_steam(start_date: str, end_date: str, log) -> int:
    """
    Read the temp export CSV and append only new rows (by AppId) to a timestamped
    raw_steam_YYYY-MM-DD.csv. Returns number of new rows added.
    """
    try:
        new_df = pd.read_csv(TEMP_EXPORT)
    except Exception as e:
        log(f"⚠️ Could not read temp export: {e}")
        return 0

    if new_df.empty:
        log("⚠️ Temp export was empty — no new games in this date window")
        return 0

    new_df = _normalize_release_dates(new_df)

    if "AppId" not in new_df.columns and "appid" in new_df.columns.str.lower().tolist():
        new_df = new_df.rename(columns={c: "AppId" for c in new_df.columns if c.lower() == "appid"})

    source_path = get_latest_steam_csv()
    out_path = RAW_DIR / f"raw_steam_{date.today()}.csv"

    if source_path.exists():
        existing_df = pd.read_csv(source_path)
        existing_ids = set(existing_df["AppId"].astype(str))
        new_only = new_df[~new_df["AppId"].astype(str).isin(existing_ids)].copy()

        if new_only.empty:
            log("ℹ️ No new unique AppIds found — CSV already up to date")
            return 0

        new_only["date_appended"] = date.today().isoformat()

        all_cols = existing_df.columns.tolist()
        if "date_appended" not in all_cols:
            all_cols.append("date_appended")
        for col in all_cols:
            if col not in new_only.columns:
                new_only[col] = None
        for col in all_cols:
            if col not in existing_df.columns:
                existing_df[col] = None
        new_only = new_only[all_cols]
        existing_df = existing_df[all_cols]

        combined = pd.concat([existing_df, new_only], ignore_index=True)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_path, index=False)
        log(f"📝 Saved {len(new_only)} new rows → {out_path.name}")
        return len(new_only)
    else:
        new_df["date_appended"] = date.today().isoformat()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_csv(out_path, index=False)
        log(f"📝 Created {out_path.name} with {len(new_df)} rows")
        return len(new_df)


def append_from_uploaded_steam_csv(uploaded_df: pd.DataFrame) -> tuple:
    """
    Merge an externally uploaded steam DataFrame into the persistent CSV.
    - Rows whose AppId already exists are OVERWRITTEN.
    - New rows are APPENDED.
    - ALL uploaded rows get date_appended = today.
    Returns (n_updated, n_new).
    """
    today = date.today().isoformat()

    uploaded_df = uploaded_df.copy()

    for col in list(uploaded_df.columns):
        if col.lower() == "appid" and col != "AppId":
            uploaded_df = uploaded_df.rename(columns={col: "AppId"})

    uploaded_df = _normalize_release_dates(uploaded_df)
    uploaded_df["date_appended"] = today
    upload_ids = set(uploaded_df["AppId"].astype(str))

    source_path = get_latest_steam_csv()
    out_path = RAW_DIR / f"raw_steam_{date.today()}.csv"

    if source_path.exists():
        existing_df = pd.read_csv(source_path)
        existing_ids = set(existing_df["AppId"].astype(str))
        n_updated = len(upload_ids & existing_ids)
        n_new = len(upload_ids - existing_ids)

        kept = existing_df[~existing_df["AppId"].astype(str).isin(upload_ids)].copy()

        all_cols = existing_df.columns.tolist()
        if "date_appended" not in all_cols:
            all_cols.append("date_appended")
        for col in all_cols:
            if col not in uploaded_df.columns:
                uploaded_df[col] = None
            if col not in kept.columns:
                kept[col] = None
        uploaded_df = uploaded_df[all_cols]
        kept = kept[all_cols]

        combined = pd.concat([kept, uploaded_df], ignore_index=True)
    else:
        n_updated, n_new = 0, len(uploaded_df)
        combined = uploaded_df

    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_path, index=False)
    return n_updated, n_new
