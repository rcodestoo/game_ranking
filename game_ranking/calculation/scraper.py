import time
import logging
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
from config import GENRE_LIST, CACHE_DIR

GENRES = pd.read_excel(GENRE_LIST)["Genre"].tolist()

# Anchor term: a stable, well-known keyword used in every batch so scores
# can be rescaled to be comparable across batches.
ANCHOR = "video game"

TIMEFRAME   = "today 12-m"   # 
GEO         = ""              # Worldwide
BATCH_SIZE  = 4              # Max 4 genres + 1 anchor = 5 keywords per request
OUTPUT_FILE = str(CACHE_DIR / "gaming_genre_trends.csv")

MAX_RETRIES  = 5
BACKOFF_BASE = 60  # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def scrape_google_trends(keywords, timeframe='today 12-m'):
    pytrends = TrendReq(hl='en-US', tz=360)
    pytrends.build_payload(keywords, cat=0, timeframe=timeframe)
    return pytrends.interest_over_time()


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


def fetch_game_trends(game_name: str) -> int:
    """
    Resolve a game title to its Google Trends topic ID, fetch 12-month
    interest-over-time, and return the most recent weekly interest score (0-100).
    Returns 0 on any failure.
    """
    pytrends = build_pytrends()
    topic = resolve_game_topic(pytrends, game_name)
    time.sleep(1)
    try:
        df = fetch_with_retry(pytrends, [topic], timeframe="now 1-d")
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


def fetch_anchor_baseline(pytrends: TrendReq) -> pd.Series:
    log.info("Fetching anchor baseline: '%s'", ANCHOR)
    df = fetch_with_retry(pytrends, [ANCHOR])
    if df.empty or ANCHOR not in df.columns:
        raise ValueError("Could not fetch anchor term baseline.")
    return df[ANCHOR]


def fetch_batch(pytrends: TrendReq, genres: list[str], anchor_baseline: pd.Series) -> pd.DataFrame:
    """
    Fetch a batch of genres alongside the anchor term, then rescale so
    that the genre scores are relative to the anchor's true baseline.
    """
    keywords = genres + [ANCHOR]
    log.info("Fetching batch: %s", genres)

    df = fetch_with_retry(pytrends, keywords)

    if df.empty:
        log.warning("No data returned for batch: %s", genres)
        return pd.DataFrame()

    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    anchor_in_batch = df[ANCHOR]
    baseline_aligned = anchor_baseline.reindex(df.index).fillna(method="ffill").fillna(1)
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

    anchor_baseline = fetch_anchor_baseline(pytrends)
    time.sleep(5)

    all_results: list[pd.DataFrame] = []
    batches = [GENRES[i: i + BATCH_SIZE] for i in range(0, len(GENRES), BATCH_SIZE)]

    for idx, batch in enumerate(batches, start=1):
        log.info("Processing batch %d/%d", idx, len(batches))
        batch_df = fetch_batch(pytrends, batch, anchor_baseline)
        if not batch_df.empty:
            all_results.append(batch_df)
        if idx < len(batches):
            pause = 15
            log.info("Pausing %ds between batches...", pause)
            time.sleep(pause)

    if not all_results:
        log.error("No data collected. Exiting.")
        return

    combined = pd.concat(all_results, axis=1)
    combined.index.name = "date"
    combined = combined.sort_index()

    avg_row = combined.mean().rename("AVERAGE_12M").to_frame().T
    avg_row.index.name = "date"

    trend_data = pd.concat([avg_row, combined], axis=0)
    trend_data.to_csv(OUTPUT_FILE)
    log.info("Data written to '%s'", OUTPUT_FILE)

    print("\n=== 12-Month Average Interest by Genre (normalized) ===")
    summary = combined.mean().sort_values(ascending=False)
    for genre, score in summary.items():
        print(f"  {genre:<20} {score:>6.1f}")
    print(f"\nFull data saved to: {OUTPUT_FILE}")

    return trend_data
