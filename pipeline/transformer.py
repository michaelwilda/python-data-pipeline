"""
Data transformation module.

Operates on the clean DataFrame produced by :class:`~pipeline.cleaner.DataCleaner`
and produces an enriched, analysis-ready DataFrame plus summary statistics.

Transformation steps
--------------------
1. ``domain_clean``       — lower-cased, ``www.``-stripped domain.
2. ``score_per_comment``  — score / comments ratio (0 when no comments).
3. ``engagement_tier``    — categorical label: ``"High"``, ``"Medium"``, or ``"Low"``
                            based on score percentile.
4. ``score_rank``         — global rank by score (1 = highest score).
5. ``domain_story_count`` — number of stories from the same domain.
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from config import CLEAN_CSV, TOP_N_RECORDS, TRANSFORMED_CSV

logger = logging.getLogger(__name__)


class DataTransformer:
    """Enriches and aggregates clean Hacker News data.

    Args:
        clean_path: Path to the clean CSV. Defaults to :data:`config.CLEAN_CSV`.

    Example::

        transformer = DataTransformer()
        df = transformer.transform()
        stats = transformer.summary_stats(df)
        transformer.save(df)
    """

    def __init__(self, clean_path: Path = CLEAN_CSV) -> None:
        self.clean_path = Path(clean_path)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load(self) -> pd.DataFrame:
        """Load the cleaned CSV into a DataFrame.

        Returns:
            A :class:`pandas.DataFrame` with cleaned HN data.

        Raises:
            FileNotFoundError: If :attr:`clean_path` does not exist.
        """
        if not self.clean_path.exists():
            raise FileNotFoundError(
                f"Clean CSV not found at {self.clean_path}. "
                "Run the cleaner first (--clean)."
            )
        df = pd.read_csv(self.clean_path)
        logger.info("Loaded clean data: %d rows from %s", len(df), self.clean_path)
        return df

    def transform(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        """Apply all transformation steps to *df*.

        Args:
            df: Cleaned DataFrame. If ``None``, :meth:`load` is called
                automatically.

        Returns:
            An enriched :class:`pandas.DataFrame` with derived columns.
        """
        if df is None:
            df = self.load()

        df = (
            df.pipe(self._add_domain_clean)
              .pipe(self._add_score_per_comment)
              .pipe(self._add_engagement_tier)
              .pipe(self._add_score_rank)
              .pipe(self._add_domain_story_count)
        )
        logger.info(
            "Transformation complete: %d rows, %d columns.", len(df), len(df.columns)
        )
        return df

    def top_n_by_score(
        self, df: pd.DataFrame, n: int = TOP_N_RECORDS
    ) -> pd.DataFrame:
        """Return the top *n* stories sorted by score descending.

        Args:
            df: Transformed DataFrame.
            n: Number of stories to return.

        Returns:
            A subset :class:`pandas.DataFrame` of the highest-scoring stories.
        """
        return df.nlargest(n, "score").reset_index(drop=True)

    def domain_counts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a frequency table of stories per domain.

        Args:
            df: Transformed DataFrame.

        Returns:
            A :class:`pandas.DataFrame` with columns ``domain_clean`` and
            ``story_count``, sorted by count descending.
        """
        return (
            df.groupby("domain_clean")
            .size()
            .reset_index(name="story_count")
            .sort_values("story_count", ascending=False)
            .reset_index(drop=True)
        )

    def summary_stats(self, df: pd.DataFrame) -> dict:
        """Compute high-level summary statistics over the dataset.

        Args:
            df: Transformed DataFrame.

        Returns:
            A dictionary with keys: ``total_stories``, ``unique_authors``,
            ``unique_domains``, ``avg_score``, ``median_score``, ``max_score``,
            ``avg_comments``, and ``top_domain``.
        """
        stats: dict = {
            "total_stories": int(len(df)),
            "unique_authors": int(df["author"].nunique()),
            "unique_domains": int(df["domain_clean"].nunique()),
            "avg_score": round(float(df["score"].mean()), 2),
            "median_score": float(df["score"].median()),
            "max_score": int(df["score"].max()),
            "avg_comments": round(float(df["comments"].mean()), 2),
            "top_domain": str(df["domain_clean"].mode().iloc[0]) if len(df) > 0 else "",
        }
        logger.info("Summary stats: %s", stats)
        return stats

    def save(self, df: pd.DataFrame, path: Path = TRANSFORMED_CSV) -> None:
        """Persist the transformed DataFrame to a CSV file.

        Args:
            df: The transformed DataFrame to save.
            path: Destination path. Defaults to :data:`config.TRANSFORMED_CSV`.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info("Transformed data saved -> %s  (%d rows)", path, len(df))

    # ------------------------------------------------------------------
    # Transformation steps
    # ------------------------------------------------------------------

    @staticmethod
    def _add_domain_clean(df: pd.DataFrame) -> pd.DataFrame:
        """Add ``domain_clean``: lower-cased domain with leading ``www.`` removed."""
        df = df.copy()
        df["domain_clean"] = (
            df["domain"]
            .astype(str)
            .str.lower()
            .str.strip()
            .str.replace(r"^www\.", "", regex=True)
        )
        # Fallback: extract from URL for rows where domain is empty/null-like
        mask = df["domain_clean"].isin(["", "nan", "none"])
        if mask.any():
            df.loc[mask, "domain_clean"] = df.loc[mask, "url"].apply(
                lambda u: urlparse(str(u)).netloc.removeprefix("www.")
            )
        return df

    @staticmethod
    def _add_score_per_comment(df: pd.DataFrame) -> pd.DataFrame:
        """Add ``score_per_comment``: score / comments (0.0 when comments == 0)."""
        df = df.copy()
        df["score_per_comment"] = df.apply(
            lambda r: round(r["score"] / r["comments"], 2)
            if r["comments"] > 0
            else 0.0,
            axis=1,
        )
        return df

    @staticmethod
    def _add_engagement_tier(df: pd.DataFrame) -> pd.DataFrame:
        """Add ``engagement_tier``: ``"High"``, ``"Medium"``, or ``"Low"``
        based on 33rd/66th score percentiles.
        """
        df = df.copy()
        p33 = df["score"].quantile(0.33)
        p66 = df["score"].quantile(0.66)

        def _tier(score: int) -> str:
            if score >= p66:
                return "High"
            if score >= p33:
                return "Medium"
            return "Low"

        df["engagement_tier"] = df["score"].apply(_tier)
        return df

    @staticmethod
    def _add_score_rank(df: pd.DataFrame) -> pd.DataFrame:
        """Add ``score_rank``: global rank by score, 1 = highest."""
        df = df.copy()
        df["score_rank"] = df["score"].rank(method="min", ascending=False).astype(int)
        return df

    @staticmethod
    def _add_domain_story_count(df: pd.DataFrame) -> pd.DataFrame:
        """Add ``domain_story_count``: total stories from each domain in the dataset."""
        df = df.copy()
        df["domain_story_count"] = (
            df.groupby("domain_clean")["domain_clean"].transform("count")
        )
        return df
