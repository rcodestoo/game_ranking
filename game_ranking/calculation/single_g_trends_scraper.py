import time
import logging
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

TIMEFRAME    = "today 1-m"
GEO          = ""                      # Worldwide (empty string = worldwide in pytrends)
CAT_GAMES    = 8              # Google Trends "Games" category
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


def fetch_with_retry(pytrends: TrendReq, keywords: list[str], timeframe: str = TIMEFRAME, cat: int = 0) -> pd.DataFrame:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload(kw_list=keywords, timeframe=timeframe, geo=GEO, cat=cat)
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


if __name__ == "__main__":
    keyword = input("Enter game keyword: ").strip()
    pytrends = build_pytrends()
    df = fetch_with_retry(pytrends, [keyword], timeframe=TIMEFRAME, cat=CAT_GAMES)
    if df.empty:
        print("No data returned.")
    else:
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        print(df.to_string())
        col = keyword if keyword in df.columns else df.columns[0]
        print(f"\nMost recent score: {df[col].iloc[-1]}")
