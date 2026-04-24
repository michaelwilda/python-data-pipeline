"""
Unit tests for pipeline/cleaner.py (DataCleaner).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline.cleaner import DataCleaner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_raw_df(**overrides) -> pd.DataFrame:
    """Return a minimal valid raw DataFrame; columns may be overridden."""
    base: dict = {
        "story_id": ["1", "2", "3"],
        "rank": ["1", "2", "3"],
        "title": ["Story A", "Story B", "Story C"],
        "url": ["https://a.com", "https://b.com", "https://c.com"],
        "domain": ["a.com", "b.com", "c.com"],
        "score": ["100", "50", "200"],
        "author": ["alice", "bob", "carol"],
        "comments": ["10", "5", "30"],
        "age": ["1 hour ago", "2 hours ago", "3 hours ago"],
        "scraped_at": ["2024-01-15T10:00:00Z"] * 3,
    }
    base.update(overrides)
    return pd.DataFrame(base)


@pytest.fixture()
def cleaner(tmp_path: Path) -> DataCleaner:
    """DataCleaner pointing at a dummy (irrelevant for direct-df tests) path."""
    dummy = tmp_path / "raw.csv"
    dummy.write_text("")
    return DataCleaner(raw_path=dummy)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDataCleaner:
    def test_clean_valid_data_passes_through(self, cleaner: DataCleaner) -> None:
        result = cleaner.clean(_make_raw_df())
        assert len(result) == 3

    def test_removes_duplicate_story_ids(self, cleaner: DataCleaner) -> None:
        df = _make_raw_df(
            story_id=["1", "1", "2"],
            title=["A", "A dup", "B"],
            url=["https://a.com", "https://a2.com", "https://b.com"],
        )
        assert len(cleaner.clean(df)) == 2

    def test_strips_whitespace_from_title(self, cleaner: DataCleaner) -> None:
        df = _make_raw_df(title=["  Story A  ", "  B  ", "C  "])
        result = cleaner.clean(df)
        assert result["title"].iloc[0] == "Story A"

    def test_coerces_score_to_int(self, cleaner: DataCleaner) -> None:
        result = cleaner.clean(_make_raw_df())
        assert result["score"].dtype == int

    def test_coerces_comments_to_int(self, cleaner: DataCleaner) -> None:
        result = cleaner.clean(_make_raw_df())
        assert result["comments"].dtype == int

    def test_fills_missing_score_with_zero(self, cleaner: DataCleaner) -> None:
        # Score of None -> 0 -> then filtered by min_score, so we check no NaN
        df = _make_raw_df(score=["10", None, "50"])
        result = cleaner.clean(df)
        assert result["score"].isna().sum() == 0

    def test_drops_rows_with_empty_title(self, cleaner: DataCleaner) -> None:
        df = _make_raw_df(
            story_id=["1", "2", "3"],
            title=["Valid Title", "", "Another"],
        )
        assert len(cleaner.clean(df)) == 2

    def test_drops_rows_with_empty_url(self, cleaner: DataCleaner) -> None:
        df = _make_raw_df(
            story_id=["1", "2", "3"],
            url=["https://a.com", "", "https://c.com"],
        )
        assert len(cleaner.clean(df)) == 2

    def test_filters_zero_score(self, cleaner: DataCleaner) -> None:
        df = _make_raw_df(score=["0", "5", "10"])
        result = cleaner.clean(df)
        assert 0 not in result["score"].values

    def test_parses_scraped_at_to_datetime(self, cleaner: DataCleaner) -> None:
        result = cleaner.clean(_make_raw_df())
        assert pd.api.types.is_datetime64_any_dtype(result["scraped_at"])

    def test_adds_missing_column_with_empty_string(self, cleaner: DataCleaner) -> None:
        df = _make_raw_df().drop(columns=["domain"])
        result = cleaner.clean(df)
        assert "domain" in result.columns

    def test_output_has_reset_integer_index(self, cleaner: DataCleaner) -> None:
        result = cleaner.clean(_make_raw_df())
        assert list(result.index) == list(range(len(result)))

    def test_load_raises_file_not_found(self, tmp_path: Path) -> None:
        c = DataCleaner(raw_path=tmp_path / "nonexistent.csv")
        with pytest.raises(FileNotFoundError, match="Run the scraper first"):
            c.load()

    def test_save_creates_csv_file(self, cleaner: DataCleaner, tmp_path: Path) -> None:
        out = tmp_path / "clean.csv"
        cleaner.save(_make_raw_df(), path=out)
        assert out.exists()
        assert len(pd.read_csv(out)) == 3

    def test_stringified_nan_in_title_drops_row(self, cleaner: DataCleaner) -> None:
        df = _make_raw_df(title=["nan", "Good Title", "Another"])
        result = cleaner.clean(df)
        assert "nan" not in result["title"].values
