#!/usr/bin/env python3
"""
HN Data Pipeline — CLI entry point.

Usage examples::

    # Full pipeline (scrape -> clean -> transform -> visualize)
    python main.py --all

    # Scrape only (5 pages)
    python main.py --scrape --pages 5

    # Clean previously scraped data
    python main.py --clean

    # Transform (enrich) previously cleaned data
    python main.py --transform

    # Generate charts from previously transformed data
    python main.py --visualize

    # Full pipeline with debug logging
    python main.py --all --verbose
"""

from __future__ import annotations

import argparse
import logging
import sys

from config import DEFAULT_PAGES, LOG_DATE_FORMAT, LOG_FORMAT


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def configure_logging(verbose: bool = False) -> None:
    """Configure root logger with a consistent format.

    Args:
        verbose: If ``True``, sets the log level to ``DEBUG``; otherwise ``INFO``.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------


def run_scrape(pages: int) -> None:
    """Execute the scraping stage and persist raw.csv.

    Args:
        pages: Number of HN listing pages to fetch.
    """
    from scraper import HNScraper
    logger = logging.getLogger(__name__)
    logger.info("=== STAGE 1: Scrape (%d page(s)) ===", pages)
    scraper = HNScraper(pages=pages)
    df = scraper.scrape()
    scraper.save(df)


def run_clean() -> None:
    """Execute the cleaning stage: raw.csv -> clean.csv."""
    from pipeline import DataCleaner
    logger = logging.getLogger(__name__)
    logger.info("=== STAGE 2: Clean ===")
    cleaner = DataCleaner()
    df = cleaner.clean()
    cleaner.save(df)


def run_transform() -> None:
    """Execute the transformation stage: clean.csv -> transformed.csv."""
    from pipeline import DataTransformer
    logger = logging.getLogger(__name__)
    logger.info("=== STAGE 3: Transform ===")
    transformer = DataTransformer()
    df = transformer.transform()
    stats = transformer.summary_stats(df)
    logger.info("--- Summary Statistics ---")
    for key, value in stats.items():
        logger.info("  %-22s %s", key + ":", value)
    transformer.save(df)


def run_visualize() -> None:
    """Execute the visualization stage: generate and save all charts."""
    from visualizer import ChartGenerator
    logger = logging.getLogger(__name__)
    logger.info("=== STAGE 4: Visualize ===")
    gen = ChartGenerator()
    for path in gen.generate_all():
        logger.info("  Chart saved -> %s", path)


# ---------------------------------------------------------------------------
# CLI parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Construct and return the argument parser."""
    parser = argparse.ArgumentParser(
        prog="hn-pipeline",
        description="Hacker News data pipeline: scrape -> clean -> transform -> visualize.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--scrape", action="store_true",
        help="Fetch stories from HN and save data/output/raw.csv.",
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Clean raw.csv and produce clean.csv.",
    )
    parser.add_argument(
        "--transform", action="store_true",
        help="Enrich clean.csv and produce transformed.csv.",
    )
    parser.add_argument(
        "--visualize", action="store_true",
        help="Generate charts from transformed.csv into data/output/.",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run the full pipeline: scrape -> clean -> transform -> visualize.",
    )
    parser.add_argument(
        "--pages", type=int, default=DEFAULT_PAGES, metavar="N",
        help=f"Number of HN pages to scrape (default: {DEFAULT_PAGES}).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, configure logging, and run the requested pipeline stages.

    Args:
        argv: Argument list. Defaults to ``sys.argv[1:]`` when ``None``.

    Returns:
        Exit code: ``0`` on success, ``1`` on error.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)

    if not any([args.scrape, args.clean, args.transform, args.visualize, args.all]):
        parser.print_help()
        return 0

    try:
        if args.all or args.scrape:
            run_scrape(pages=args.pages)
        if args.all or args.clean:
            run_clean()
        if args.all or args.transform:
            run_transform()
        if args.all or args.visualize:
            run_visualize()
        logger.info("Pipeline finished successfully.")
        return 0
    except FileNotFoundError as exc:
        logger.error("File not found: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected pipeline error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
