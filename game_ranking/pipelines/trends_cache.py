"""
Keyword-level Google Trends results cache.

Stores DataForSEO comparison results so that re-running a tournament with the
same games skips API calls for groups already compared within the past 30 days.

Cache file: cache/trends_results_cache.json
Cache key:  "|".join(sorted(cleaned_keywords))
TTL:        30 days — matches the DataForSEO "past 30 days" query window, so
            a cached result is never older than the data it represents.
"""

import json
import logging
from datetime import date, timedelta

from config import CACHE_DIR

log = logging.getLogger(__name__)

_CACHE_FILE  = CACHE_DIR / "trends_results_cache.json"
_TTL_DAYS    = 30


def _cache_key(cleaned_kws: list[str]) -> str:
    return "|".join(sorted(cleaned_kws))


# ── Load / save ───────────────────────────────────────────────────────────────

def load_trends_cache() -> dict:
    """Load cache from disk. Returns empty dict on missing or corrupt file."""
    if _CACHE_FILE.exists():
        try:
            return json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("trends_results_cache.json corrupt — starting fresh: %s", e)
    return {}


def save_trends_cache(cache: dict) -> None:
    """Atomically write cache to disk (tmp-rename pattern)."""
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CACHE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(_CACHE_FILE)


# ── Lookup / write ────────────────────────────────────────────────────────────

def lookup_cached_scores(cleaned_kws: list[str], cache: dict) -> dict[str, float] | None:
    """
    Return cached scores for cleaned_kws if present and within TTL, else None.
    Scores are keyed by cleaned keyword name (same as what DataForSEO returns).
    """
    key   = _cache_key(cleaned_kws)
    entry = cache.get(key)
    if not entry:
        return None
    try:
        fetched = date.fromisoformat(entry["fetched_date"])
    except (KeyError, ValueError):
        return None
    if date.today() - fetched > timedelta(days=_TTL_DAYS):
        return None
    scores = entry.get("scores", {})
    return {kw: float(scores.get(kw, 0.0)) for kw in cleaned_kws}


def write_cached_scores(
    cleaned_kws: list[str],
    scores: dict[str, float],
    cache: dict,
) -> None:
    """
    Write a result into the in-memory cache dict.
    Caller is responsible for calling save_trends_cache() afterwards.
    Skips entries where all scores are zero (API-error results).
    """
    if not any(v > 0 for v in scores.values()):
        return
    key = _cache_key(cleaned_kws)
    cache[key] = {
        "keywords":     list(cleaned_kws),
        "scores":       {kw: scores.get(kw, 0.0) for kw in cleaned_kws},
        "fetched_date": date.today().isoformat(),
    }
