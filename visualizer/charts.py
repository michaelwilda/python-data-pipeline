"""
Chart generation module.

Reads the transformed DataFrame and produces three publication-quality charts
saved as PNG files under ``data/output/``:

1. **Top Domains Bar Chart**      — Horizontal bar chart of the top-N domains
   by story count, with value labels on each bar.
2. **Score Distribution Histogram** — Frequency histogram with mean and median
   reference lines overlaid.
3. **Score vs. Comments Scatter**  — Scatter plot coloured by engagement tier
   (High / Medium / Low) to surface story interaction patterns.

Note: ``matplotlib.use("Agg")`` is called before importing ``pyplot`` to ensure
the module works in headless/CI environments without a display server.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # must precede pyplot import; safe for headless environments
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as ticker  # noqa: E402
import pandas as pd  # noqa: E402

from config import (  # noqa: E402
    CHART_DPI,
    CHART_STYLE,
    FIGURE_SIZE,
    OUTPUT_DIR,
    SCORE_HISTOGRAM_CHART,
    SCORE_VS_COMMENTS_CHART,
    TOP_DOMAINS_CHART,
    TOP_N_RECORDS,
    TRANSFORMED_CSV,
)

logger = logging.getLogger(__name__)

_PALETTE: dict[str, str] = {
    "primary": "#3B82F6",
    "secondary": "#10B981",
    "High": "#EF4444",
    "Medium": "#F59E0B",
    "Low": "#6B7280",
}


class ChartGenerator:
    """Generates and saves charts from the transformed HN dataset.

    Args:
        transformed_path: Path to the transformed CSV. Defaults to
            :data:`config.TRANSFORMED_CSV`.

    Example::

        gen = ChartGenerator()
        df = gen.load()
        gen.plot_top_domains(df)
        gen.plot_score_distribution(df)
        gen.plot_score_vs_comments(df)
    """

    def __init__(self, transformed_path: Path | None = None) -> None:
        self.transformed_path = Path(transformed_path or TRANSFORMED_CSV)
        try:
            plt.style.use(CHART_STYLE)
        except OSError:
            logger.warning("Chart style '%s' unavailable; using default.", CHART_STYLE)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load(self) -> pd.DataFrame:
        """Load the transformed CSV into a DataFrame.

        Returns:
            A :class:`pandas.DataFrame` ready for charting.

        Raises:
            FileNotFoundError: If the transformed CSV does not exist.
        """
        if not self.transformed_path.exists():
            raise FileNotFoundError(
                f"Transformed CSV not found at {self.transformed_path}. "
                "Run --transform first."
            )
        df = pd.read_csv(self.transformed_path)
        logger.info("Loaded transformed data: %d rows", len(df))
        return df

    def generate_all(self, df: pd.DataFrame | None = None) -> list[Path]:
        """Generate all three charts and return a list of their output paths.

        Args:
            df: Transformed DataFrame. Loaded from disk if ``None``.

        Returns:
            A list of three :class:`pathlib.Path` objects for the saved images.
        """
        if df is None:
            df = self.load()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        paths = [
            self.plot_top_domains(df),
            self.plot_score_distribution(df),
            self.plot_score_vs_comments(df),
        ]
        logger.info("All %d charts generated.", len(paths))
        return paths

    def plot_top_domains(
        self,
        df: pd.DataFrame,
        n: int = TOP_N_RECORDS,
        output_path: Path = TOP_DOMAINS_CHART,
    ) -> Path:
        """Horizontal bar chart of the top-N domains by story count.

        Args:
            df: Transformed DataFrame with a ``domain_clean`` column.
            n: Number of domains to display.
            output_path: Destination PNG path.

        Returns:
            The resolved :class:`pathlib.Path` of the saved image.
        """
        counts = (
            df.groupby("domain_clean")
            .size()
            .nlargest(n)
            .sort_values(ascending=True)  # ascending so largest bar is at top
        )

        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        bars = ax.barh(
            counts.index, counts.values,
            color=_PALETTE["primary"], edgecolor="white", linewidth=0.5,
        )
        for bar in bars:
            w = bar.get_width()
            ax.text(
                w + 0.1, bar.get_y() + bar.get_height() / 2,
                str(int(w)), va="center", ha="left", fontsize=9,
            )
        ax.set_xlabel("Number of Stories", fontsize=12)
        ax.set_title(f"Top {n} Domains by Story Count", fontsize=14, fontweight="bold")
        ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()

        output_path = Path(output_path)
        fig.savefig(output_path, dpi=CHART_DPI, bbox_inches="tight")
        plt.close(fig)
        logger.info("Chart saved -> %s", output_path)
        return output_path

    def plot_score_distribution(
        self,
        df: pd.DataFrame,
        output_path: Path = SCORE_HISTOGRAM_CHART,
    ) -> Path:
        """Histogram of story score distribution with mean/median lines.

        Args:
            df: Transformed DataFrame with a ``score`` column.
            output_path: Destination PNG path.

        Returns:
            The resolved :class:`pathlib.Path` of the saved image.
        """
        scores = df["score"].dropna()
        n_bins = min(40, max(10, len(scores) // 5))

        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        ax.hist(scores, bins=n_bins, color=_PALETTE["secondary"],
                edgecolor="white", linewidth=0.5)

        mean_val, median_val = scores.mean(), scores.median()
        ax.axvline(
            mean_val, color="#EF4444", linestyle="--", linewidth=1.5,
            label=f"Mean: {mean_val:.0f}",
        )
        ax.axvline(
            median_val, color="#6B7280", linestyle=":", linewidth=1.5,
            label=f"Median: {median_val:.0f}",
        )
        ax.set_xlabel("Score (points)", fontsize=12)
        ax.set_ylabel("Number of Stories", fontsize=12)
        ax.set_title("Distribution of Hacker News Story Scores", fontsize=14, fontweight="bold")
        ax.legend(fontsize=10)
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()

        output_path = Path(output_path)
        fig.savefig(output_path, dpi=CHART_DPI, bbox_inches="tight")
        plt.close(fig)
        logger.info("Chart saved -> %s", output_path)
        return output_path

    def plot_score_vs_comments(
        self,
        df: pd.DataFrame,
        output_path: Path = SCORE_VS_COMMENTS_CHART,
    ) -> Path:
        """Scatter plot of score vs. comment count, coloured by engagement tier.

        Args:
            df: Transformed DataFrame with ``score``, ``comments``, and
                ``engagement_tier`` columns.
            output_path: Destination PNG path.

        Returns:
            The resolved :class:`pathlib.Path` of the saved image.
        """
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)

        for tier in ["High", "Medium", "Low"]:
            subset = df[df["engagement_tier"] == tier]
            ax.scatter(
                subset["comments"], subset["score"],
                c=_PALETTE.get(tier, "#999999"),
                label=tier, alpha=0.65, edgecolors="white", linewidths=0.3, s=40,
            )

        ax.set_xlabel("Number of Comments", fontsize=12)
        ax.set_ylabel("Score (points)", fontsize=12)
        ax.set_title(
            "Score vs. Comments — coloured by Engagement Tier",
            fontsize=14, fontweight="bold",
        )
        ax.legend(title="Engagement Tier", fontsize=10)
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()

        output_path = Path(output_path)
        fig.savefig(output_path, dpi=CHART_DPI, bbox_inches="tight")
        plt.close(fig)
        logger.info("Chart saved -> %s", output_path)
        return output_path
