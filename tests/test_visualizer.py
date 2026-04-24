"""
Unit tests for visualizer/charts.py (ChartGenerator).

All charts are written to a pytest ``tmp_path`` directory so no files are
created inside the repository during the test run.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from visualizer.charts import ChartGenerator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_transformed_df() -> pd.DataFrame:
    """Return a minimal transformed DataFrame suitable for all chart methods."""
    return pd.DataFrame({
        "story_id": ["1", "2", "3", "4", "5", "6"],
        "title": ["A", "B", "C", "D", "E", "F"],
        "domain": [
            "github.com", "github.com", "bbc.com",
            "medium.com", "github.com", "arxiv.org",
        ],
        "domain_clean": [
            "github.com", "github.com", "bbc.com",
            "medium.com", "github.com", "arxiv.org",
        ],
        "score": [500, 120, 80, 310, 45, 200],
        "comments": [50, 20, 0, 100, 5, 30],
        "engagement_tier": ["High", "Medium", "Low", "High", "Low", "Medium"],
        "score_per_comment": [10.0, 6.0, 0.0, 3.1, 9.0, 6.67],
        "score_rank": [1, 4, 5, 2, 6, 3],
        "domain_story_count": [3, 3, 1, 1, 3, 1],
        "author": ["a", "b", "c", "d", "e", "f"],
        "rank": [1, 2, 3, 4, 5, 6],
    })


@pytest.fixture()
def gen() -> ChartGenerator:
    return ChartGenerator()


# ---------------------------------------------------------------------------
# Individual chart methods
# ---------------------------------------------------------------------------


class TestPlotTopDomains:
    def test_creates_png_file(self, gen: ChartGenerator, tmp_path: Path) -> None:
        out = tmp_path / "top_domains.png"
        result = gen.plot_top_domains(_make_transformed_df(), output_path=out)
        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_returns_path_object(self, gen: ChartGenerator, tmp_path: Path) -> None:
        out = tmp_path / "top_domains.png"
        result = gen.plot_top_domains(_make_transformed_df(), output_path=out)
        assert isinstance(result, Path)


class TestPlotScoreDistribution:
    def test_creates_png_file(self, gen: ChartGenerator, tmp_path: Path) -> None:
        out = tmp_path / "score_dist.png"
        result = gen.plot_score_distribution(_make_transformed_df(), output_path=out)
        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_returns_path_object(self, gen: ChartGenerator, tmp_path: Path) -> None:
        out = tmp_path / "score_dist.png"
        result = gen.plot_score_distribution(_make_transformed_df(), output_path=out)
        assert isinstance(result, Path)


class TestPlotScoreVsComments:
    def test_creates_png_file(self, gen: ChartGenerator, tmp_path: Path) -> None:
        out = tmp_path / "scatter.png"
        result = gen.plot_score_vs_comments(_make_transformed_df(), output_path=out)
        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    def test_handles_missing_tier(self, gen: ChartGenerator, tmp_path: Path) -> None:
        df = _make_transformed_df()
        df["engagement_tier"] = "High"  # only one tier — scatter still renders
        out = tmp_path / "scatter_mono.png"
        result = gen.plot_score_vs_comments(df, output_path=out)
        assert out.exists()


# ---------------------------------------------------------------------------
# generate_all
# ---------------------------------------------------------------------------


class TestGenerateAll:
    def test_returns_three_paths(self, gen: ChartGenerator, tmp_path: Path) -> None:
        df = _make_transformed_df()
        with (
            patch.object(gen, "plot_top_domains", return_value=tmp_path / "a.png"),
            patch.object(gen, "plot_score_distribution", return_value=tmp_path / "b.png"),
            patch.object(gen, "plot_score_vs_comments", return_value=tmp_path / "c.png"),
            patch("visualizer.charts.OUTPUT_DIR", tmp_path),
        ):
            paths = gen.generate_all(df)
        assert len(paths) == 3

    def test_calls_all_chart_methods(self, gen: ChartGenerator, tmp_path: Path) -> None:
        df = _make_transformed_df()
        with (
            patch.object(gen, "plot_top_domains", return_value=tmp_path / "a.png") as m1,
            patch.object(gen, "plot_score_distribution",
                         return_value=tmp_path / "b.png") as m2,
            patch.object(gen, "plot_score_vs_comments",
                         return_value=tmp_path / "c.png") as m3,
            patch("visualizer.charts.OUTPUT_DIR", tmp_path),
        ):
            gen.generate_all(df)
        m1.assert_called_once()
        m2.assert_called_once()
        m3.assert_called_once()


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_raises_when_file_missing(self, tmp_path: Path) -> None:
        g = ChartGenerator(transformed_path=tmp_path / "nonexistent.csv")
        with pytest.raises(FileNotFoundError):
            g.load()

    def test_load_reads_csv(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "transformed.csv"
        _make_transformed_df().to_csv(csv_path, index=False)
        g = ChartGenerator(transformed_path=csv_path)
        df = g.load()
        assert len(df) == 6
