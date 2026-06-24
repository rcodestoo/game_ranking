"""
append_csv.py -Validate and append new Steam/Non-Steam CSV data to the master lists.

Usage (run from repo root):
    python game_ranking/scripts/append_csv.py <steam_file> <nonsteam_file>

Example:
    python game_ranking/scripts/append_csv.py ^
        "game_ranking/raw/export_full_combined_filtered (4).csv" ^
        "game_ranking/raw/Categorized_Game_List_2026-06-01_to_2026-07-01 (1).csv"

The script:
  - Validates both files match the canonical schema
  - Warns on any ReleaseDate / Release Date values that couldn't be parsed
  - Appends new rows and overwrites existing ones (by AppId / Game Title)
  - Writes output to raw_steam_YYYY-MM-DD.csv / raw_non_steam_YYYY-MM-DD.csv
  - Logs INFO to stdout and WARNING+ to game_ranking/logs/append_YYYY-MM-DD.log
"""

import logging
import os
import re
import sys
from datetime import date
from pathlib import Path

# ── sys.path / cwd setup ─────────────────────────────────────────────────────
# Pipeline internals use bare imports like `from pipelines.normalizer import ...`
# so game_ranking/ must be on sys.path and the cwd.
SCRIPT_DIR = Path(__file__).resolve().parent        # game_ranking/scripts/
GAME_RANKING_DIR = SCRIPT_DIR.parent                # game_ranking/

_LAUNCH_DIR = Path.cwd()  # save original cwd before we change it

sys.path.insert(0, str(GAME_RANKING_DIR))
os.chdir(str(GAME_RANKING_DIR))

# ── Deferred imports (need sys.path set first) ────────────────────────────────
import pandas as pd  # noqa: E402
from pipelines.normalizer import _normalize_steam_release_dates  # noqa: E402
from pipelines.nonsteam_pipeline import (  # noqa: E402
    _normalize_release_date,
    append_from_uploaded_nonsteam_csv,
)
from pipelines.steam_pipeline import append_from_uploaded_steam_csv  # noqa: E402

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = GAME_RANKING_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"append_{date.today()}.log"

_fmt = "%(asctime)s [%(levelname)-7s] %(message)s"
_datefmt = "%H:%M:%S"

import io as _io
_stdout_handler = logging.StreamHandler(_io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True))
_stdout_handler.setLevel(logging.INFO)
_stdout_handler.setFormatter(logging.Formatter(_fmt, _datefmt))

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setLevel(logging.WARNING)
_file_handler.setFormatter(logging.Formatter(_fmt, _datefmt))

logging.basicConfig(level=logging.DEBUG, handlers=[_stdout_handler, _file_handler])
log = logging.getLogger(__name__)

# ── Canonical schemas ─────────────────────────────────────────────────────────
STEAM_COLUMNS = [
    "AppId", "Name", "FollowerCount", "Genres", "Categories",
    "IGDB_Genres", "IGDB_Themes", "IGDB_Keywords",
    "ExactMatch", "ScrapedName", "ReleaseDate", "ParsedDate",
    "YoutubeURL", "YoutubeViewCount", "YoutubeLikeCount",
    "ScrapingError", "Developers", "IsFullyProcessed", "Publishers", "ReleaseInfo",
]

NONSTEAM_COLUMNS = [
    "Game Title", "Category", "Release Date", "Developers", "Publishers",
    "Platforms", "Genres", "Themes", "Keywords",
    "YouTube URL", "YouTube Views", "YouTube Likes", "YouTube ReleaseDate",
    "SteamStatus",
]

# Non-date strings that are acceptable and should not trigger a warning
_KNOWN_NON_DATE_RE = re.compile(
    r"^(coming soon|tbd|tba|early access|q[1-4]\s*\d{4}|\d{4})$",
    re.IGNORECASE,
)
_STEAM_DATE_RE = re.compile(r"^\d{2}-\d{2}-\d{4}$")
_NS_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _validate_schema(df: pd.DataFrame, required_cols: list, label: str) -> bool:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log.error("%s: missing required columns: %s", label, missing)
        return False
    return True


def _check_steam_dates(df: pd.DataFrame) -> int:
    """Pre-check Steam ReleaseDate values. Returns count of warnings."""
    normalized, _ = _normalize_steam_release_dates(df["ReleaseDate"])
    warn_count = 0
    for idx, (orig, norm) in enumerate(zip(df["ReleaseDate"], normalized)):
        if pd.isna(orig) or str(orig).strip() == "":
            continue
        s = str(norm).strip()
        if not _STEAM_DATE_RE.match(s) and not _KNOWN_NON_DATE_RE.match(s):
            name = df.at[idx, "Name"] if "Name" in df.columns else f"row {idx + 2}"
            log.warning(
                "Steam: unparseable ReleaseDate -game='%s' | raw='%s' | after normalization='%s'",
                name, orig, norm,
            )
            warn_count += 1
    return warn_count


def _check_ns_dates(df: pd.DataFrame) -> int:
    """Pre-check Non-Steam Release Date values. Returns count of warnings."""
    normalized = _normalize_release_date(df["Release Date"])
    warn_count = 0
    for idx, (orig, norm) in enumerate(zip(df["Release Date"], normalized)):
        if pd.isna(orig) or str(orig).strip() == "":
            continue
        s = str(norm).strip()
        if not _NS_DATE_RE.match(s) and not _KNOWN_NON_DATE_RE.match(s):
            title = df.at[idx, "Game Title"] if "Game Title" in df.columns else f"row {idx + 2}"
            log.warning(
                "Non-Steam: unparseable Release Date -game='%s' | raw='%s' | after normalization='%s'",
                title, orig, norm,
            )
            warn_count += 1
    return warn_count


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) != 3:
        print(__doc__)
        print("Error: expected exactly 2 arguments -<steam_file> <nonsteam_file>")
        sys.exit(1)

    steam_path = (_LAUNCH_DIR / sys.argv[1]).resolve()
    ns_path = (_LAUNCH_DIR / sys.argv[2]).resolve()

    log.info("Warnings and errors will also be written to: %s", LOG_FILE)

    # ── Steam ─────────────────────────────────────────────────────────────────
    log.info("-" * 60)
    log.info("STEAM: %s", steam_path.name)

    if not steam_path.exists():
        log.error("Steam: file not found: %s", steam_path)
        sys.exit(1)

    steam_df = pd.read_csv(steam_path, encoding="utf-8-sig")
    log.info("Steam: loaded %d rows", len(steam_df))

    if not _validate_schema(steam_df, STEAM_COLUMNS, "Steam"):
        sys.exit(1)
    log.info("Steam: schema OK (all 20 columns present)")

    date_warns = _check_steam_dates(steam_df)
    if date_warns:
        log.warning("Steam: %d row(s) with unparseable ReleaseDate -review warnings above", date_warns)
    else:
        log.info("Steam: all ReleaseDate values parseable")

    n_updated, n_new = append_from_uploaded_steam_csv(steam_df)
    log.info(
        "Steam: append complete - %d updated, %d new -> raw_steam_%s.csv",
        n_updated, n_new, date.today(),
    )

    # ── Non-Steam ─────────────────────────────────────────────────────────────
    log.info("-" * 60)
    log.info("NON-STEAM: %s", ns_path.name)

    if not ns_path.exists():
        log.error("Non-Steam: file not found: %s", ns_path)
        sys.exit(1)

    ns_df = pd.read_csv(ns_path, encoding="utf-8-sig")
    log.info("Non-Steam: loaded %d rows", len(ns_df))

    if not _validate_schema(ns_df, NONSTEAM_COLUMNS, "Non-Steam"):
        sys.exit(1)
    log.info("Non-Steam: schema OK (all 14 columns present)")

    date_warns = _check_ns_dates(ns_df)
    if date_warns:
        log.warning("Non-Steam: %d row(s) with unparseable Release Date -review warnings above", date_warns)
    else:
        log.info("Non-Steam: all Release Date values parseable")

    n_updated, n_new = append_from_uploaded_nonsteam_csv(ns_df)
    log.info(
        "Non-Steam: append complete - %d updated, %d new -> raw_non_steam_%s.csv",
        n_updated, n_new, date.today(),
    )

    log.info("-" * 60)
    log.info("All done. Review %s for any warnings.", LOG_FILE)


if __name__ == "__main__":
    main()
