"""
Upload normalization helpers.

Provides encoding-safe CSV reading and per-upload normalization for Steam
and Non-Steam files so the Load handlers in main.py never crash on
encoding issues or unusual date formats.
"""

import io
import re

import pandas as pd

_ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]


def read_csv_auto_encoding(raw_bytes: bytes, **kwargs) -> tuple[pd.DataFrame, str]:
    """
    Try reading a CSV with UTF-8, utf-8-sig, cp1252, then latin-1.
    Returns (DataFrame, encoding_used).
    Raises ValueError if all encodings fail.
    """
    last_exc = None
    for enc in _ENCODINGS:
        try:
            df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, **kwargs)
            return df, enc
        except (UnicodeDecodeError, UnicodeError):
            last_exc = last_exc  # keep trying
        except Exception as e:
            last_exc = e
            break
    raise ValueError(f"Could not decode CSV with any supported encoding: {last_exc}")


def _normalize_steam_release_dates(series: pd.Series) -> tuple[pd.Series, int]:
    """
    Per-cell date normalization for Steam ReleaseDate values.
    Handles:
      - "22 Apr, 2026"   (DD Mon, YYYY)
      - "Apr 23, 2026"   (Mon DD, YYYY)
      - "April 2026"     (Month YYYY — no day, mapped to 1st)
      - ISO / dd-mm-yyyy and anything else pandas can parse
      - Non-date strings like "Coming Soon", "TBD" — kept as-is
    Output format: "%d-%m-%Y" (consistent with existing stored data).
    Returns (normalized_series, count_of_values_changed).
    """
    _MONTH_NAMES = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    _MONTH_YEAR_RE = re.compile(
        r"^([A-Za-z]+)\s+(\d{4})$"
    )

    def _parse(val):
        if pd.isna(val) or str(val).strip() == "":
            return val
        s = str(val).strip()

        # "Month YYYY" — no day present
        m = _MONTH_YEAR_RE.match(s)
        if m:
            month_str, year_str = m.group(1).lower(), m.group(2)
            month_num = _MONTH_NAMES.get(month_str)
            if month_num:
                try:
                    return pd.Timestamp(int(year_str), month_num, 1).strftime("%d-%m-%Y")
                except Exception:
                    pass

        # Try pandas per-cell (handles "22 Apr, 2026", "Apr 23, 2026", ISO, etc.)
        try:
            return pd.to_datetime(s, dayfirst=True).strftime("%d-%m-%Y")
        except Exception:
            return s  # "Coming Soon", "TBD", etc.

    original = series.copy()
    normalized = series.apply(_parse)
    changed = int((normalized != original).sum())
    return normalized, changed


def prepare_steam_upload(raw_bytes: bytes) -> tuple[pd.DataFrame, list[str]]:
    """
    Full normalization for a Steam CSV upload.
    Returns (df, warnings) where warnings is a list of human-readable strings
    describing what was auto-fixed.
    """
    warnings: list[str] = []

    df, enc = read_csv_auto_encoding(raw_bytes)
    if enc != "utf-8":
        warnings.append(f"ℹ️ Encoding: detected {enc} → converted to UTF-8")

    # Normalise AppId column name variants
    appid_variants = [c for c in df.columns if c.lower() == "appid" and c != "AppId"]
    if appid_variants:
        df = df.rename(columns={appid_variants[0]: "AppId"})

    # Normalise ReleaseDate
    if "ReleaseDate" in df.columns:
        df["ReleaseDate"], n_fixed = _normalize_steam_release_dates(df["ReleaseDate"])
        if n_fixed:
            warnings.append(f"ℹ️ Normalized {n_fixed} ReleaseDate value(s) to dd-mm-yyyy format")

    return df, warnings


def prepare_nonsteam_upload(raw_bytes: bytes) -> tuple[pd.DataFrame, list[str]]:
    """
    Full normalization for a Non-Steam CSV upload.
    Returns (df, warnings).
    Date normalization and column alignment happen downstream in
    append_from_uploaded_nonsteam_csv() — no duplication.
    """
    warnings: list[str] = []

    df, enc = read_csv_auto_encoding(raw_bytes)
    if enc != "utf-8":
        warnings.append(f"ℹ️ Encoding: detected {enc} → converted to UTF-8")

    return df, warnings
