"""
Global configuration and constants for the HN data pipeline.

All tuneable values live here — nothing is hard-coded in the source modules.
Update this file to change behaviour without touching any pipeline logic.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Filesystem paths
# ---------------------------------------------------------------------------

BASE_DIR: Path = Path(__file__).parent
DATA_DIR: Path = BASE_DIR / "data"
OUTPUT_DIR: Path = DATA_DIR / "output"

RAW_CSV: Path = OUTPUT_DIR / "raw.csv"
CLEAN_CSV: Path = OUTPUT_DIR / "clean.csv"
TRANSFORMED_CSV: Path = OUTPUT_DIR / "transformed.csv"

# ---------------------------------------------------------------------------
# Scraper settings
# ---------------------------------------------------------------------------

HN_BASE_URL: str = "https://news.ycombinator.com/news"

#: Default number of HN pages to scrape (30 stories per page).
DEFAULT_PAGES: int = 3

#: Seconds before a single HTTP request is considered timed out.
REQUEST_TIMEOUT: int = 10

#: Total automatic retries for transient network errors.
MAX_RETRIES: int = 3

#: Exponential back-off multiplier applied between successive retries.
RETRY_BACKOFF: float = 1.5

#: Polite delay (seconds) between page requests — do not set to 0 in production.
RATE_LIMIT_DELAY: float = 2.0

REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; HN-DataPipeline/1.0; "
        "+https://github.com/michaelwilda/python-data-pipeline)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# Pipeline / transformation settings
# ---------------------------------------------------------------------------

#: Stories with a score below this value are dropped during cleaning.
MIN_SCORE_THRESHOLD: int = 1

#: Number of records returned by top-N queries.
TOP_N_RECORDS: int = 10

# ---------------------------------------------------------------------------
# Chart settings
# ---------------------------------------------------------------------------

CHART_DPI: int = 150
CHART_STYLE: str = "seaborn-v0_8-whitegrid"
FIGURE_SIZE: tuple[int, int] = (12, 6)

TOP_DOMAINS_CHART: Path = OUTPUT_DIR / "top_domains.png"
SCORE_HISTOGRAM_CHART: Path = OUTPUT_DIR / "score_distribution.png"
SCORE_VS_COMMENTS_CHART: Path = OUTPUT_DIR / "score_vs_comments.png"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
