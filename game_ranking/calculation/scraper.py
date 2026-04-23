import time
import logging
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

TIMEFRAME    = "today 1-m"
GEO          = ""                      # Worldwide (empty string = worldwide in pytrends)
CAT_GAMES    = 8              # Google Trends "Games" category
MAX_RETRIES  = 5
BACKOFF_BASE = 60            # seconds

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
        requests_args={
            "headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        },
        # proxies=['http://193.23.194.147:3128'] #, 'http://bfruwyxf:1frbwt481p0o@198.23.239.134:6540']
    )


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
            time.sleep(wait)
        except Exception as e:
            log.error("Unexpected error on attempt %d: %s", attempt, e)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_BASE)
    raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts.")


def fetch_game_trends(game_name: str) -> int:
    """
    Fetch the past month's worldwide Google Trends interest for the given
    game name, scoped to the Games category (cat=8). Returns the most
    recent weekly score (0-100). Returns 0 on any failure.
    """
    pytrends = build_pytrends()
    try:
        df = fetch_with_retry(pytrends, [game_name], timeframe=TIMEFRAME, cat=CAT_GAMES)
        if df.empty:
            return 0
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        if df.empty:
            return 0
        col = game_name if game_name in df.columns else df.columns[0]
        return int(df[col].iloc[-1])
    except Exception as e:
        log.error("fetch_game_trends failed for '%s': %s", game_name, e)
        return 0
