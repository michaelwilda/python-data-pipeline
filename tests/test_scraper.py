"""
Unit tests for scraper/scraper.py.

HTTP calls are fully mocked with :mod:`unittest.mock` — no live network
requests are made during the test run.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from scraper.scraper import HNScraper, Story, domain_from_url

# ---------------------------------------------------------------------------
# Sample HTML fixtures
# ---------------------------------------------------------------------------

SAMPLE_HTML = """
<html><body>
<table>
  <tr class="athing" id="12345678">
    <td class="title"><span class="rank">1.</span></td>
    <td class="votelinks"></td>
    <td class="title">
      <span class="titleline">
        <a href="https://example.com/article">An Amazing Article</a>
        <span class="sitebit comhead">(<span class="sitestr">example.com</span>)</span>
      </span>
    </td>
  </tr>
  <tr>
    <td colspan="2"></td>
    <td class="subtext">
      <span class="score" id="score_12345678">342 points</span>
      by <a href="user?id=johndoe" class="hnuser">johndoe</a>
      <span class="age"><a href="item?id=12345678">5 hours ago</a></span>
      | <a href="item?id=12345678">87&nbsp;comments</a>
    </td>
  </tr>
  <tr class="athing" id="98765432">
    <td class="title"><span class="rank">2.</span></td>
    <td class="votelinks"></td>
    <td class="title">
      <span class="titleline">
        <a href="https://github.com/user/repo">Cool Open Source Project</a>
        <span class="sitebit comhead">(<span class="sitestr">github.com</span>)</span>
      </span>
    </td>
  </tr>
  <tr>
    <td colspan="2"></td>
    <td class="subtext">
      <span class="score" id="score_98765432">512 points</span>
      by <a href="user?id=janedev" class="hnuser">janedev</a>
      <span class="age"><a href="item?id=98765432">7 hours ago</a></span>
      | <a href="item?id=98765432">203&nbsp;comments</a>
    </td>
  </tr>
</table>
</body></html>
"""

ASK_HN_HTML = """
<html><body>
<table>
  <tr class="athing" id="11111111">
    <td class="title"><span class="rank">1.</span></td>
    <td class="votelinks"></td>
    <td class="title">
      <span class="titleline">
        <a href="item?id=11111111">Ask HN: What is your favourite tool?</a>
      </span>
    </td>
  </tr>
  <tr>
    <td colspan="2"></td>
    <td class="subtext">
      <span class="score" id="score_11111111">45 points</span>
      by <a href="user?id=asker" class="hnuser">asker</a>
      <span class="age"><a href="item?id=11111111">1 hour ago</a></span>
      | <a href="item?id=11111111">discuss</a>
    </td>
  </tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def scraper() -> HNScraper:
    """Return a single-page scraper with no delay."""
    return HNScraper(pages=1, delay=0)


# ---------------------------------------------------------------------------
# Story dataclass
# ---------------------------------------------------------------------------


class TestStory:
    def test_to_dict_contains_all_fields(self) -> None:
        s = Story(
            story_id="1", rank=1, title="Test", url="https://test.com",
            domain="test.com", score=100, author="user", comments=10, age="1h",
        )
        d = s.to_dict()
        assert d["story_id"] == "1"
        assert d["score"] == 100
        assert "scraped_at" in d

    def test_scraped_at_auto_populated(self) -> None:
        s = Story("1", 1, "T", "http://x.com", "x.com", 0, "u", 0, "now")
        assert s.scraped_at != ""

    def test_scraped_at_is_iso_format(self) -> None:
        s = Story("1", 1, "T", "http://x.com", "x.com", 0, "u", 0, "now")
        import re
        assert re.match(r"\d{4}-\d{2}-\d{2}T", s.scraped_at)


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


class TestParsing:
    def test_parse_returns_correct_count(self, scraper: HNScraper) -> None:
        assert len(scraper._parse(SAMPLE_HTML)) == 2

    def test_parse_extracts_title(self, scraper: HNScraper) -> None:
        stories = scraper._parse(SAMPLE_HTML)
        assert stories[0].title == "An Amazing Article"
        assert stories[1].title == "Cool Open Source Project"

    def test_parse_extracts_url(self, scraper: HNScraper) -> None:
        stories = scraper._parse(SAMPLE_HTML)
        assert stories[0].url == "https://example.com/article"

    def test_parse_extracts_domain_from_sitestr(self, scraper: HNScraper) -> None:
        stories = scraper._parse(SAMPLE_HTML)
        assert stories[0].domain == "example.com"
        assert stories[1].domain == "github.com"

    def test_parse_extracts_score(self, scraper: HNScraper) -> None:
        stories = scraper._parse(SAMPLE_HTML)
        assert stories[0].score == 342
        assert stories[1].score == 512

    def test_parse_extracts_author(self, scraper: HNScraper) -> None:
        stories = scraper._parse(SAMPLE_HTML)
        assert stories[0].author == "johndoe"
        assert stories[1].author == "janedev"

    def test_parse_extracts_comments(self, scraper: HNScraper) -> None:
        stories = scraper._parse(SAMPLE_HTML)
        assert stories[0].comments == 87
        assert stories[1].comments == 203

    def test_parse_extracts_rank(self, scraper: HNScraper) -> None:
        stories = scraper._parse(SAMPLE_HTML)
        assert stories[0].rank == 1
        assert stories[1].rank == 2

    def test_parse_ask_hn_uses_absolute_url(self, scraper: HNScraper) -> None:
        stories = scraper._parse(ASK_HN_HTML)
        assert len(stories) == 1
        assert "news.ycombinator.com" in stories[0].url

    def test_parse_discuss_yields_zero_comments(self, scraper: HNScraper) -> None:
        stories = scraper._parse(ASK_HN_HTML)
        assert stories[0].comments == 0

    def test_parse_empty_html_returns_empty_list(self, scraper: HNScraper) -> None:
        assert scraper._parse("<html><body></body></html>") == []


# ---------------------------------------------------------------------------
# HTTP fetching (mocked)
# ---------------------------------------------------------------------------


class TestFetching:
    def test_returns_html_on_200(self, scraper: HNScraper) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()
        with patch.object(scraper._session, "get", return_value=mock_resp):
            assert scraper._fetch("https://example.com") == SAMPLE_HTML

    def test_returns_none_on_http_error(self, scraper: HNScraper) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("403")
        with patch.object(scraper._session, "get", return_value=mock_resp):
            assert scraper._fetch("https://example.com") is None

    def test_returns_none_on_timeout(self, scraper: HNScraper) -> None:
        with patch.object(scraper._session, "get",
                          side_effect=requests.exceptions.Timeout()):
            assert scraper._fetch("https://example.com") is None

    def test_returns_none_on_connection_error(self, scraper: HNScraper) -> None:
        with patch.object(scraper._session, "get",
                          side_effect=requests.exceptions.ConnectionError()):
            assert scraper._fetch("https://example.com") is None

    def test_returns_none_on_generic_request_exception(self, scraper: HNScraper) -> None:
        with patch.object(scraper._session, "get",
                          side_effect=requests.exceptions.RequestException("boom")):
            assert scraper._fetch("https://example.com") is None


# ---------------------------------------------------------------------------
# Full scrape flow (mocked)
# ---------------------------------------------------------------------------


class TestScrapeFlow:
    def test_scrape_returns_dataframe_with_stories(self, scraper: HNScraper) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()
        with patch.object(scraper._session, "get", return_value=mock_resp):
            df = scraper.scrape()
        assert len(df) == 2
        assert set(["title", "score", "author", "comments"]).issubset(df.columns)

    def test_scrape_returns_empty_dataframe_on_network_failure(self,
                                                               scraper: HNScraper) -> None:
        with patch.object(scraper._session, "get",
                          side_effect=requests.exceptions.ConnectionError()):
            df = scraper.scrape()
        assert len(df) == 0

    def test_save_creates_csv(self, scraper: HNScraper, tmp_path) -> None:
        import pandas as pd
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        out = tmp_path / "raw.csv"
        scraper.save(df, path=out)
        assert out.exists()
        assert pd.read_csv(out).shape == (2, 2)


# ---------------------------------------------------------------------------
# domain_from_url helper
# ---------------------------------------------------------------------------


class TestDomainFromUrl:
    def test_strips_www_prefix(self) -> None:
        assert domain_from_url("https://www.example.com/path") == "example.com"

    def test_no_www_unchanged(self) -> None:
        assert domain_from_url("https://github.com/user/repo") == "github.com"

    def test_empty_string_returns_empty(self) -> None:
        assert domain_from_url("") == ""

    def test_relative_url_returns_empty(self) -> None:
        assert domain_from_url("not-a-url") == ""

    def test_subdomain_other_than_www_preserved(self) -> None:
        assert domain_from_url("https://api.github.com/v3") == "api.github.com"
