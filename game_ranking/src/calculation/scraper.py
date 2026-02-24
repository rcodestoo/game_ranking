import time
import logging
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
from config import GENRE_LIST

GENRES = pd.read_excel(GENRE_LIST)["Genre"].tolist()
    

def scrape_google_trends(keywords, timeframe='today 12-m'):
    # Set up Pytrends connection
    pytrends = TrendReq(hl='en-US', tz=360)

    # Build payload with specified keywords and timeframe
    pytrends.build_payload(keywords, cat=0, timeframe=timeframe) #, geo='', gprop=''

    # Retrieve interest over time data
    interest_over_time_df = pytrends.interest_over_time()

    return interest_over_time_df


# Anchor term: a stable, well-known keyword used in every batch so scores
# can be rescaled to be comparable across batches.
ANCHOR = "video game"

TIMEFRAME = "today 12-m"   # Past 12 months
GEO = ""                    # Worldwide (use e.g. "US" for a specific country)
BATCH_SIZE = 4              # Max 4 genres + 1 anchor = 5 keywords per request
OUTPUT_FILE = "gaming_genre_trends.csv"

# Retry / rate-limit settings
MAX_RETRIES = 5
BACKOFF_BASE = 60           # seconds — pytrends can be aggressive with 429s

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_pytrends() -> TrendReq:
    """Create a pytrends session with sensible defaults."""
    return TrendReq(
        hl="en-US",
        tz=0,
        timeout=(10, 30),
        retries=2,
        backoff_factor=0.5,
    )


def fetch_with_retry(pytrends: TrendReq, keywords: list[str]) -> pd.DataFrame:
    """
    Build payload and fetch interest_over_time with exponential back-off
    on rate-limit errors.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            pytrends.build_payload(
                kw_list=keywords,
                timeframe=TIMEFRAME,
                geo=GEO,
                gprop="",
            )
            df = pytrends.interest_over_time()
            if df.empty:
                log.warning("Empty response for keywords: %s", keywords)
            return df
        except TooManyRequestsError:
            wait = BACKOFF_BASE * attempt
            log.warning(
                "Rate limited (attempt %d/%d). Waiting %ds before retry...",
                attempt, MAX_RETRIES, wait,
            )
            time.sleep(wait)
        except Exception as e:
            log.error("Unexpected error on attempt %d: %s", attempt, e)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_BASE)
    raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts.")


def fetch_anchor_baseline(pytrends: TrendReq) -> pd.Series:
    """
    Fetch the anchor term alone to get its absolute trend line.
    This is used as the denominator for cross-batch normalization.
    """
    log.info("Fetching anchor baseline: '%s'", ANCHOR)
    df = fetch_with_retry(pytrends, [ANCHOR])
    if df.empty or ANCHOR not in df.columns:
        raise ValueError("Could not fetch anchor term baseline.")
    return df[ANCHOR]


def fetch_batch(
    pytrends: TrendReq,
    genres: list[str],
    anchor_baseline: pd.Series,
) -> pd.DataFrame:
    """
    Fetch a batch of genres alongside the anchor term, then rescale so
    that the genre scores are relative to the anchor's true baseline.

    Rescaling formula:
        genre_rescaled = genre_raw * (anchor_baseline / anchor_in_batch)
    This maps each genre onto the same absolute scale as the anchor baseline.
    """
    keywords = genres + [ANCHOR]
    log.info("Fetching batch: %s", genres)

    df = fetch_with_retry(pytrends, keywords)

    if df.empty:
        log.warning("No data returned for batch: %s", genres)
        return pd.DataFrame()

    # Drop the 'isPartial' column if present
    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    # Align anchor baseline index with this batch
    anchor_in_batch = df[ANCHOR]
    baseline_aligned = anchor_baseline.reindex(df.index).fillna(method="ffill").fillna(1)

    # Avoid division by zero
    scale_factor = baseline_aligned / anchor_in_batch.replace(0, 1)

    result = pd.DataFrame(index=df.index)
    for genre in genres:
        if genre in df.columns:
            result[genre] = (df[genre] * scale_factor).round(2)
        else:
            log.warning("Genre '%s' missing from response.", genre)
            result[genre] = None

    return result


def google_trends() -> None:
    log.info("Starting Gaming Genre Trends scraper")
    log.info("Genres: %s", GENRES)
    log.info("Timeframe: %s | Geo: %s", TIMEFRAME, GEO or "Worldwide")

    pytrends = build_pytrends()

    # Step 1: Get anchor baseline
    anchor_baseline = fetch_anchor_baseline(pytrends)
    time.sleep(5)  # Polite pause before next request

    # Step 2: Fetch genres in batches
    all_results: list[pd.DataFrame] = []
    batches = [GENRES[i : i + BATCH_SIZE] for i in range(0, len(GENRES), BATCH_SIZE)]

    for idx, batch in enumerate(batches, start=1):
        log.info("Processing batch %d/%d", idx, len(batches))
        batch_df = fetch_batch(pytrends, batch, anchor_baseline)
        if not batch_df.empty:
            all_results.append(batch_df)
        if idx < len(batches):
            pause = 15
            log.info("Pausing %ds between batches to avoid rate limiting...", pause)
            time.sleep(pause)

    if not all_results:
        log.error("No data collected. Exiting.")
        return

    # Step 3: Merge all batches on date index
    combined = pd.concat(all_results, axis=1)
    combined.index.name = "date"
    combined = combined.sort_index()

    # Step 4: Add a summary row — average interest per genre
    avg_row = combined.mean().rename("AVERAGE_12M").to_frame().T
    avg_row.index.name = "date"

    # Step 5: Write CSV
    trend_data = pd.concat([avg_row, combined], axis=0)
    trend_data.to_csv(OUTPUT_FILE)
    log.info("Data written to '%s'", OUTPUT_FILE)

    # Step 6: Print a quick summary
    print("\n=== 12-Month Average Interest by Genre (normalized) ===")
    summary = combined.mean().sort_values(ascending=False)
    for genre, score in summary.items():
        print(f"  {genre:<20} {score:>6.1f}")
    print(f"\nFull data saved to: {OUTPUT_FILE}")

    return trend_data