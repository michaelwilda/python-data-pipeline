"""
Data cleaning module.

Reads the raw CSV produced by the scraper and returns a clean, normalised
:class:`pandas.DataFrame` ready for transformation.

Cleaning steps
--------------
1. Ensure all expected columns are present (add empty ones if missing).
2. Drop exact duplicate rows (keyed on ``story_id``).
3. Strip whitespace from all string columns; replace stringified nulls with ``""``.
4. Coerce ``score``, ``comments``, and ``rank`` to integers; fill NaN with 0.
5. Drop rows whose ``title``, ``url``, or ``story_id`` are empty.
6. Filter out stories whose ``score`` is below :data:`config.MIN_SCORE_THRESHOLD`.
7. Parse ``scraped_at`` to :class:`pandas.Timestamp`.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from config import CLEAN_CSV, MIN_SCORE_THRESHOLD, RAW_CSV

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS: list[str] = [
    "story_id", "rank", "title", "url", "domain",
    "score", "author", "comments", "age", "scraped_at",
]
_STRING_COLUMNS: list[str] = ["story_id", "title", "url", "domain", "author", "age"]
_INT_COLUMNS: list[str] = ["score", "comments", "rank"]


class DataCleaner:
    """Cleans and normalises raw Hacker News story data.

    Args:
        raw_path: Path to the raw CSV file. Defaults to :data:`config.RAW_CSV`.

    Example::

        cleaner = DataCleaner()
        df = cleaner.clean()
        cleaner.save(df)
    """

    def __init__(self, raw_path: Path = RAW_CSV) -> None:
        self.raw_path = Path(raw_path)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load(self) -> pd.DataFrame:
        """Load the raw CSV into a DataFrame (all columns as strings).

        Returns:
            A :class:`pandas.DataFrame` with raw, uncleaned data.

        Raises:
            FileNotFoundError: If :attr:`raw_path` does not exist.
        """
        if not self.raw_path.exists():
            raise FileNotFoundError(
                f"Raw CSV not found at {self.raw_path}. "
                "Run the scraper first (--scrape)."
            )
        df = pd.read_csv(self.raw_path, dtype=str)
        logger.info("Loaded raw data: %d rows from %s", len(df), self.raw_path)
        return df

    def clean(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Run all cleaning steps and return the sanitised DataFrame.

        Args:
            df: Raw DataFrame to clean. If ``None``, :meth:`load` is called
                automatically.

        Returns:
            A cleaned :class:`pandas.DataFrame` with a reset integer index.
        """
        if df is None:
            df = self.load()

        original_len = len(df)
        df = (
            df.pipe(self._ensure_columns)
              .pipe(self._drop_duplicates)
              .pipe(self._strip_strings)
              .pipe(self._coerce_integers)
              .pipe(self._drop_invalid_rows)
              .pipe(self._filter_by_score)
              .pipe(self._parse_timestamps)
        )
        logger.info(
            "Cleaning complete: %d -> %d rows (dropped %d).",
            original_len, len(df), original_len - len(df),
        )
        return df.reset_index(drop=True)

    def save(self, df: pd.DataFrame, path: Path = CLEAN_CSV) -> None:
        """Persist the cleaned DataFrame to a CSV file.

        Args:
            df: The cleaned DataFrame to save.
            path: Destination path. Defaults to :data:`config.CLEAN_CSV`.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info("Clean data saved -> %s  (%d rows)", path, len(df))

    # ------------------------------------------------------------------
    # Cleaning steps — each accepts and returns a DataFrame
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Add any expected columns that are absent, filled with empty string."""
        for col in _REQUIRED_COLUMNS:
            if col not in df.columns:
                logger.warning("Expected column '%s' not found — adding empty.", col)
                df[col] = ""
        return df

    @staticmethod
    def _drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows with duplicate ``story_id`` values, keeping the first."""
        before = len(df)
        df = df.drop_duplicates(subset=["story_id"]).copy()
        if (dropped := before - len(df)):
            logger.debug("Dropped %d duplicate story_id rows.", dropped)
        return df

    @staticmethod
    def _strip_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Strip whitespace and normalise null-like strings to empty string."""
        null_like = {"nan", "None", "<NA>", "NaN"}
        for col in _STRING_COLUMNS:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .replace(dict.fromkeys(null_like, ""))
                )
        return df

    @staticmethod
    def _coerce_integers(df: pd.DataFrame) -> pd.DataFrame:
        """Coerce numeric columns to int, filling unparseable values with 0."""
        for col in _INT_COLUMNS:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .fillna(0)
                    .astype(int)
                )
        return df

    @staticmethod
    def _drop_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows with empty ``title``, ``url``, or ``story_id``."""
        before = len(df)
        mask = (
            (df["title"].str.len() > 0)
            & (df["url"].str.len() > 0)
            & (df["story_id"].str.len() > 0)
        )
        df = df[mask]
        if (dropped := before - len(df)):
            logger.debug("Dropped %d rows with empty required fields.", dropped)
        return df

    @staticmethod
    def _filter_by_score(df: pd.DataFrame) -> pd.DataFrame:
        """Remove stories whose score is below :data:`config.MIN_SCORE_THRESHOLD`."""
        before = len(df)
        df = df[df["score"] >= MIN_SCORE_THRESHOLD]
        if (dropped := before - len(df)):
            logger.debug(
                "Dropped %d rows below MIN_SCORE_THRESHOLD=%d.",
                dropped, MIN_SCORE_THRESHOLD,
            )
        return df

    @staticmethod
    def _parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        """Parse the ``scraped_at`` column to :class:`pandas.Timestamp`."""
        if "scraped_at" in df.columns:
            df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
        return df
