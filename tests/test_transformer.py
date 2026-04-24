"""
Unit tests for pipeline/transformer.py (DataTransformer).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline.transformer import DataTransformer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_clean_df() -> pd.DataFrame:
    """Return a representative clean DataFrame for transformation tests."""
    return pd.DataFrame({
        "story_id": ["1", "2", "3", "4", "5"],
        "rank": [1, 2, 3, 4, 5],
        "title": ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"],
        "url": [
            "https://github.com/a",
            "https://github.com/b",
            "https://www.bbc.com/news/tech",
            "https://medium.com/article",
            "https://github.com/c",
        ],
        "domain": ["github.com", "github.com", "www.bbc.com", "medium.com", "github.com"],
        "score": [500, 120, 80, 310, 45],
        "author": ["alice", "bob", "carol", "dave", "eve"],
        "comments": [50, 20, 0, 100, 5],
        "age": ["1 hour ago"] * 5,
        "scraped_at": pd.to_datetime(["2024-01-15T10:00:00Z"] * 5, utc=True),
    })


@pytest.fixture()
def transformer(tmp_path: Path) -> DataTransformer:
    """DataTransformer pointing at a dummy (irrelevant for direct-df tests) path."""
    dummy = tmp_path / "clean.csv"
    dummy.write_text("")
    return DataTransformer(clean_path=dummy)


# ---------------------------------------------------------------------------
# Tests: derived columns
# ---------------------------------------------------------------------------


class TestDerivedColumns:
    def test_adds_domain_clean(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert "domain_clean" in df.columns

    def test_domain_clean_strips_www(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert "www.bbc.com" not in df["domain_clean"].values
        assert "bbc.com" in df["domain_clean"].values

    def test_domain_clean_is_lowercase(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert all(v == v.lower() for v in df["domain_clean"].dropna())

    def test_adds_score_per_comment(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert "score_per_comment" in df.columns

    def test_score_per_comment_zero_when_no_comments(self,
                                                     transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        gamma = df[df["title"] == "Gamma"]
        assert gamma["score_per_comment"].iloc[0] == 0.0

    def test_score_per_comment_correct_value(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        alpha = df[df["title"] == "Alpha"]
        assert alpha["score_per_comment"].iloc[0] == 10.0  # 500 / 50

    def test_adds_engagement_tier(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert "engagement_tier" in df.columns
        assert set(df["engagement_tier"].unique()).issubset({"High", "Medium", "Low"})

    def test_adds_score_rank(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert "score_rank" in df.columns
        top = df.loc[df["score"] == 500, "score_rank"].iloc[0]
        assert top == 1

    def test_adds_domain_story_count(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert "domain_story_count" in df.columns
        github_count = df.loc[df["domain_clean"] == "github.com", "domain_story_count"].iloc[0]
        assert github_count == 3


# ---------------------------------------------------------------------------
# Tests: aggregation helpers
# ---------------------------------------------------------------------------


class TestAggregations:
    def test_top_n_returns_correct_count(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        top2 = transformer.top_n_by_score(df, n=2)
        assert len(top2) == 2

    def test_top_n_is_sorted_descending(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        top3 = transformer.top_n_by_score(df, n=3)
        assert top3.iloc[0]["score"] >= top3.iloc[1]["score"]

    def test_domain_counts_sorted_descending(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        counts = transformer.domain_counts(df)
        assert counts["story_count"].is_monotonic_decreasing

    def test_domain_counts_has_correct_columns(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        counts = transformer.domain_counts(df)
        assert {"domain_clean", "story_count"}.issubset(counts.columns)

    def test_domain_counts_github_is_first(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        counts = transformer.domain_counts(df)
        assert counts.iloc[0]["domain_clean"] == "github.com"
        assert counts.iloc[0]["story_count"] == 3


# ---------------------------------------------------------------------------
# Tests: summary stats
# ---------------------------------------------------------------------------


class TestSummaryStats:
    def test_stats_contain_all_expected_keys(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        stats = transformer.summary_stats(df)
        expected = {
            "total_stories", "unique_authors", "unique_domains",
            "avg_score", "median_score", "max_score", "avg_comments", "top_domain",
        }
        assert expected == set(stats.keys())

    def test_total_stories_correct(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert transformer.summary_stats(df)["total_stories"] == 5

    def test_max_score_correct(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert transformer.summary_stats(df)["max_score"] == 500

    def test_unique_authors_correct(self, transformer: DataTransformer) -> None:
        df = transformer.transform(_make_clean_df())
        assert transformer.summary_stats(df)["unique_authors"] == 5


# ---------------------------------------------------------------------------
# Tests: I/O
# ---------------------------------------------------------------------------


class TestIO:
    def test_load_raises_file_not_found(self, tmp_path: Path) -> None:
        t = DataTransformer(clean_path=tmp_path / "nonexistent.csv")
        with pytest.raises(FileNotFoundError):
            t.load()

    def test_save_creates_csv_file(self, transformer: DataTransformer,
                                   tmp_path: Path) -> None:
        df = transformer.transform(_make_clean_df())
        out = tmp_path / "transformed.csv"
        transformer.save(df, path=out)
        assert out.exists()
        loaded = pd.read_csv(out)
        assert len(loaded) == 5
