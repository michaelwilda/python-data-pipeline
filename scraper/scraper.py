"""
Hacker News scraper.

Fetches story listings from news.ycombinator.com, parses the HTML with
BeautifulSoup, and returns the results as a :class:`pandas.DataFrame`.

Ethical scraping notes
----------------------
* A descriptive ``User-Agent`` header identifies this client transparently.
* A configurable ``RATE_LIMIT_DELAY`` pause is observed between every request.
* Only publicly accessible pages are scraped; no authentication is attempted.
* HN's robots.txt disallows only ``/x?fnid=*``; the ``/news`` endpoint is
  unrestricted (https://news.ycombinator.com/robots.txt).
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    HN_BASE_URL,
    MAX_RETRIES,
    OUTPUT_DIR,
    RAW_CSV,
    RATE_LIMIT_DELAY,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF,
)

logger = logging.getLogger(__name__)


@dataclass
class Story:
    """A single Hacker News story record.

    Attributes:
        story_id: HN's internal numeric identifier.
        rank: Position on the listing page (1-based, global across pages).
        title: Headline text.
        url: Destination URL (absolute; HN self-posts become item?id= links).
        domain: Registered domain extracted from *url* (e.g. ``"github.com"``).
        score: Cumulative upvote points at scrape time.
        author: HN username of the submitter.
        comments: Number of comments at scrape time.
        age: Human-readable age string returned by HN (e.g. ``"3 hours ago"``).
        scraped_at: ISO-8601 UTC timestamp when this record was created.
    """

    story_id: str
    rank: int
    title: str
    url: str
    domain: str
    score: int
    author: str
    comments: int
    age: str
    scraped_at: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
    )

    def to_dict(self) -> dict:
        """Return the story as a plain dictionary suitable for DataFrame construction."""
        return asdict(self)


class HNScraper:
    """Scrapes story listings from Hacker News.

    Uses a :class:`requests.Session` with automatic retries, custom headers,
    and a configurable rate-limit delay to behave as a polite HTTP client.

    Args:
        pages: Number of HN listing pages to scrape (30 stories per page).
        delay: Seconds to wait between consecutive page requests.

    Example::

        scraper = HNScraper(pages=3)
        df = scraper.scrape()
        scraper.save(df)
    """

    def __init__(self, pages: int = 3, delay: float = RATE_LIMIT_DELAY) -> None:
        self.pages = pages
        self.delay = delay
        self._session: requests.Session = self._build_session()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        """Scrape *self.pages* pages and return all stories as a DataFrame.

        Returns:
            A :class:`pandas.DataFrame` with one row per story and columns
            matching the fields of :class:`Story`.
        """
        stories: list[Story] = []
        for page_num in range(1, self.pages + 1):
            logger.info("Scraping page %d / %d ...", page_num, self.pages)
            page_stories = self._scrape_page(page_num)
            stories.extend(page_stories)
            logger.info(
                "  -> collected %d stories (running total: %d)",
                len(page_stories),
                len(stories),
            )
            if page_num < self.pages:
                logger.debug("Sleeping %.1f s (rate-limit delay).", self.delay)
                time.sleep(self.delay)

        if not stories:
            logger.warning("No stories were collected. Check network connectivity.")
            return pd.DataFrame()

        df = pd.DataFrame([s.to_dict() for s in stories])
        logger.info("Scraping complete — %d total stories.", len(df))
        return df

    def save(self, df: pd.DataFrame, path: Path = RAW_CSV) -> None:
        """Persist *df* as a CSV file.

        Args:
            df: The DataFrame to save.
            path: Destination path (defaults to :data:`config.RAW_CSV`).
        """
        from pathlib import Path as _Path
        _Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info("Raw data saved -> %s  (%d rows)", path, len(df))

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _scrape_page(self, page_num: int) -> list[Story]:
        """Fetch and parse a single HN listing page.

        Args:
            page_num: The 1-based page number to fetch.

        Returns:
            A list of :class:`Story` objects parsed from the page HTML.
        """
        url = f"{HN_BASE_URL}?p={page_num}"
        html = self._fetch(url)
        if html is None:
            logger.warning("Skipping page %d — no HTML returned.", page_num)
            return []
        return self._parse(html, page_offset=(page_num - 1) * 30)

    def _fetch(self, url: str) -> Optional[str]:
        """Perform a GET request and return the response body.

        Args:
            url: The URL to fetch.

        Returns:
            Response text on success, or ``None`` on any error.
        """
        try:
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            logger.debug("GET %s -> HTTP %d", url, resp.status_code)
            return resp.text
        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error for %s: %s", url, exc)
        except requests.exceptions.ConnectionError as exc:
            logger.error("Connection error for %s: %s", url, exc)
        except requests.exceptions.Timeout:
            logger.error("Request timed out: %s", url)
        except requests.exceptions.RequestException as exc:
            logger.error("Unexpected request error for %s: %s", url, exc)
        return None

    def _parse(self, html: str, page_offset: int = 0) -> list[Story]:
        """Parse raw HTML into a list of :class:`Story` objects.

        Args:
            html: Raw HTML string from an HN listing page.
            page_offset: Number of stories already collected from previous
                pages; used as a rank fallback when no rank tag is found.

        Returns:
            Parsed stories in listing order.
        """
        soup = BeautifulSoup(html, "html.parser")
        stories: list[Story] = []

        # HN renders stories as pairs of <tr> rows:
        #   Row 1 (.athing): rank + title + link
        #   Row 2 (.subtext): score, author, age, comments
        title_rows = soup.find_all("tr", class_="athing")
        for title_row in title_rows:
            story = self._parse_story(title_row, page_offset)
            if story is not None:
                stories.append(story)
        return stories

    def _parse_story(self, title_row: Tag, page_offset: int) -> Optional[Story]:
        """Extract one :class:`Story` from its title ``<tr>`` element.

        Args:
            title_row: The ``<tr class="athing">`` BeautifulSoup tag.
            page_offset: Global rank offset used when the rank span is absent.

        Returns:
            A populated :class:`Story`, or ``None`` if required fields are missing.
        """
        try:
            story_id: str = str(title_row.get("id", ""))

            # --- Rank ---
            rank_tag = title_row.find("span", class_="rank")
            rank_text = rank_tag.get_text(strip=True).rstrip(".") if rank_tag else ""
            rank = int(rank_text) if rank_text.isdigit() else page_offset + 1

            # --- Title & URL ---
            titleline = title_row.find("span", class_="titleline")
            if titleline is None:
                return None
            link_tag = titleline.find("a")
            if link_tag is None:
                return None
            title: str = link_tag.get_text(strip=True)
            raw_href: str = link_tag.get("href", "")
            url = (
                urljoin("https://news.ycombinator.com/", raw_href)
                if raw_href.startswith("item?")
                else raw_href
            )

            # --- Domain (HN provides it directly; fall back to URL parsing) ---
            sitestr = titleline.find("span", class_="sitestr")
            domain: str = (
                sitestr.get_text(strip=True) if sitestr else domain_from_url(url)
            )

            # --- Subtext row: score, author, age, comments ---
            score, author, age, comments = 0, "", "", 0
            subtext_row = title_row.find_next_sibling("tr")
            if subtext_row:
                subtext = subtext_row.find("td", class_="subtext")
                if subtext:
                    score = self._parse_score(subtext)
                    author = self._parse_author(subtext)
                    age = self._parse_age(subtext)
                    comments = self._parse_comments(subtext)

            return Story(
                story_id=story_id,
                rank=rank,
                title=title,
                url=url,
                domain=domain,
                score=score,
                author=author,
                comments=comments,
                age=age,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse story row: %s", exc)
            return None

    @staticmethod
    def _parse_score(subtext: Tag) -> int:
        """Extract the integer score from a subtext tag."""
        score_tag = subtext.find("span", class_="score")
        if score_tag:
            text = score_tag.get_text(strip=True).split()[0]
            return int(text) if text.isdigit() else 0
        return 0

    @staticmethod
    def _parse_author(subtext: Tag) -> str:
        """Extract the author username from a subtext tag."""
        author_tag = subtext.find("a", class_="hnuser")
        return author_tag.get_text(strip=True) if author_tag else ""

    @staticmethod
    def _parse_age(subtext: Tag) -> str:
        """Extract the human-readable age string from a subtext tag."""
        age_tag = subtext.find("span", class_="age")
        return age_tag.get_text(strip=True) if age_tag else ""

    @staticmethod
    def _parse_comments(subtext: Tag) -> int:
        """Extract the comment count from a subtext tag.

        HN renders comments as the last anchor element, e.g. ``'87 comments'``
        or ``'discuss'`` (meaning 0 comments for new posts).
        """
        for link in reversed(subtext.find_all("a")):
            text = link.get_text(strip=True)
            if "comment" in text:
                parts = text.split()
                return int(parts[0]) if parts and parts[0].isdigit() else 0
        return 0

    @staticmethod
    def _build_session() -> requests.Session:
        """Create a :class:`requests.Session` with retry logic and headers."""
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session


def domain_from_url(url: str) -> str:
    """Extract a bare domain from *url*, stripping a leading ``www.``.

    Args:
        url: Any URL string.

    Returns:
        The netloc with a leading ``www.`` removed, or an empty string when
        *url* has no netloc (e.g. relative paths, empty strings).

    Examples::

        >>> domain_from_url("https://www.example.com/path")
        'example.com'
        >>> domain_from_url("https://github.com/user/repo")
        'github.com'
        >>> domain_from_url("not-a-url")
        ''
    """
    netloc = urlparse(url).netloc
    return netloc.removeprefix("www.")
