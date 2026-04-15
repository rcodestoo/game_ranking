import time
import logging
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

TIMEFRAME    = "today 1-m"
GEO          = ""             # Worldwide
MAX_RETRIES  = 5
BACKOFF_BASE = 60             # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def build_pytrends() -> TrendReq:
    return TrendReq(
        hl="en-US",
        tz=0,
        timeout=(10, 30),
        retries=2,
        backoff_factor=0.5,
    )


def resolve_game_topic(pytrends: TrendReq, game_name: str) -> str:
    """
    Call get_suggestions() and return the topic mid (/m/xxxxx) for the
    first suggestion whose 'type' field contains the word 'game'.
    Falls back to the literal game_name if nothing game-related is found.
    """
    try:
        suggestions = pytrends.suggestions(keyword=game_name)
        for s in suggestions:
            if "game" in s.get("type", "").lower():
                log.info("Resolved '%s' → topic '%s' (%s)", game_name, s["title"], s["mid"])
                return s["mid"]
        log.warning("No game-type suggestion for '%s', using literal keyword.", game_name)
    except Exception as e:
        log.warning("suggestions() failed for '%s': %s", game_name, e)
    return game_name


def fetch_with_retry(pytrends: TrendReq, keywords: list[str], timeframe: str = TIMEFRAME) -> pd.DataFrame:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload(kw_list=keywords, timeframe=timeframe, geo=GEO, gprop="")
            df = pytrends.interest_over_time()
            if df.empty:
                log.warning("Empty response for keywords: %s", keywords)
            return df
        except TooManyRequestsError:
            wait = BACKOFF_BASE * attempt
            log.warning("Rate limited (attempt %d/%d). Waiting %ds...", attempt, MAX_RETRIES, wait)
            time.sleep(wait)
        except Exception as e:
            log.error("Unexpected error on attempt %d: %s", attempt, e)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_BASE)
    raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts.")


def fetch_game_trends(game_name: str) -> int:
    """
    Resolve a game title to its Google Trends topic ID, fetch the past
    month's interest-over-time, and return the most recent score (0-100).
    Returns 0 on any failure.
    """
    pytrends = build_pytrends()
    topic = resolve_game_topic(pytrends, game_name)
    time.sleep(1)
    try:
        df = fetch_with_retry(pytrends, [topic], timeframe=TIMEFRAME)
        if df.empty:
            return 0
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        if df.empty:
            return 0
        col = topic if topic in df.columns else df.columns[0]
        return int(df[col].iloc[-1])
    except Exception as e:
        log.error("fetch_game_trends failed for '%s': %s", game_name, e)
        return 0
