import time
import random
import logging
import requests
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

TIMEFRAME    = "today 1-m"
GEO          = ""                      # Worldwide (empty string = worldwide in pytrends)
CAT_GAMES    = 8              # Google Trends "Games" category
MAX_RETRIES  = 5
BACKOFF_BASE = 60            # seconds
INTER_REQUEST_DELAY = (8, 15) # seconds, randomized between calls

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Module-level shared session — reused across all fetch_game_trends calls
_shared_pytrends: TrendReq | None = None

# ── User-Agent rotation ───────────────────────────────────────────────────────
_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
_ua_index: int = 0


def _next_user_agent() -> str:
    global _ua_index
    ua = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
    _ua_index += 1
    return ua


# ── Proxy list ────────────────────────────────────────────────────────────────
# Add your residential proxy URLs here (one per entry).
# Format: "http://username:password@host:port"
# Leave the list empty to use a direct connection.
# On a rate-limit hit the session automatically rotates to the next proxy.
PROXIES: list[str] = [
    # Decodo rotating residential proxy — single gateway, IPs rotate server-side
    "http://splvkkx9bh:3sCF_nzgr3tsaH94wC@gate.decodo.com:7000",
]

_proxies: list[str] = list(PROXIES)   # runtime copy (can be overridden via set_proxies())
_proxy_index: int = 0


def set_proxies(proxies: list[str]) -> None:
    """Configure the proxy list used for Google Trends requests."""
    global _proxies, _proxy_index, _shared_pytrends
    _proxies = list(proxies)
    _proxy_index = 0
    _shared_pytrends = None   # force rebuild with new proxy on next call
    log.info("Proxy list updated: %d proxies configured.", len(_proxies))


def _current_proxy() -> str | None:
    return _proxies[_proxy_index] if _proxies else None


def _build_pytrends(proxy: str | None = None) -> TrendReq:
    kwargs: dict = dict(
        hl="en-US",
        tz=0,
        timeout=(10, 30),
        retries=0,
        backoff_factor=0.0,
        requests_args={
            "headers": {"User-Agent": _next_user_agent()},
        },
    )
    if proxy:
        kwargs["proxies"] = [proxy]
        log.info("pytrends session using proxy: %s", proxy)
    return TrendReq(**kwargs)


def get_shared_pytrends() -> TrendReq:
    global _shared_pytrends
    if _shared_pytrends is None:
        _shared_pytrends = _build_pytrends(_current_proxy())
    return _shared_pytrends


def reset_shared_pytrends() -> None:
    """Rotate to the next proxy (if any) and force a new session on next call."""
    global _shared_pytrends, _proxy_index
    _shared_pytrends = None
    if _proxies:
        _proxy_index = (_proxy_index + 1) % len(_proxies)
        log.info("Rotated to proxy index %d: %s", _proxy_index, _proxies[_proxy_index])


def fetch_with_retry(pytrends: TrendReq, keywords: list[str], timeframe: str = TIMEFRAME, cat: int = 0) -> pd.DataFrame:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload(kw_list=keywords, timeframe=timeframe, geo=GEO, cat=cat)
            df = pytrends.interest_over_time()
            if df.empty:
                log.warning("Empty response for keywords: %s", keywords)
            return df
        except (TooManyRequestsError, ResponseError):
            wait = BACKOFF_BASE * attempt
            log.warning("Rate limited (attempt %d/%d). Waiting %ds...", attempt, MAX_RETRIES, wait)
            reset_shared_pytrends()
            pytrends = get_shared_pytrends()   # pick up the rotated proxy
            time.sleep(wait)
        except requests.exceptions.Timeout:
            wait = BACKOFF_BASE * attempt
            log.warning("Timeout (attempt %d/%d). Waiting %ds...", attempt, MAX_RETRIES, wait)
            reset_shared_pytrends()
            pytrends = get_shared_pytrends()   # pick up the rotated proxy
            time.sleep(wait)
        except Exception as e:
            wait = BACKOFF_BASE * attempt
            log.error("Unexpected error on attempt %d: %s", attempt, e)
            reset_shared_pytrends()
            pytrends = get_shared_pytrends()
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts.")


def fetch_game_trends(game_name: str) -> int:
    """
    Fetch the past month's worldwide Google Trends interest for the given
    game name, scoped to the Games category (cat=8). Returns the most
    recent weekly score (0-100). Returns 0 on any failure.

    Reuses a shared TrendReq session across calls and sleeps a random
    delay between requests to avoid rate limiting.
    """
    pytrends = get_shared_pytrends()
    try:
        df = fetch_with_retry(pytrends, [game_name], timeframe=TIMEFRAME, cat=CAT_GAMES)
        if df.empty:
            return 0
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        if df.empty:
            return 0
        col = game_name if game_name in df.columns else df.columns[0]
        score = int(df[col].iloc[-1])
    except Exception as e:
        log.error("fetch_game_trends failed for '%s': %s", game_name, e)
        return 0

    delay = random.uniform(*INTER_REQUEST_DELAY)
    log.debug("Sleeping %.1fs before next request", delay)
    time.sleep(delay)

    reset_shared_pytrends()   # rotate proxy for every game

    return score
