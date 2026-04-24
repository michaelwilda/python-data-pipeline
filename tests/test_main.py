"""
Unit tests for main.py (CLI entry point).

All pipeline stage functions are mocked so no real I/O or network calls occur.
"""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

import main as main_module
from main import main, configure_logging, _build_parser


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    def test_sets_info_by_default(self) -> None:
        import logging
        configure_logging(verbose=False)
        assert logging.getLogger().level <= logging.INFO

    def test_sets_debug_when_verbose(self) -> None:
        import logging
        configure_logging(verbose=True)
        assert logging.getLogger().level <= logging.DEBUG


# ---------------------------------------------------------------------------
# _build_parser
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_default_pages(self) -> None:
        from config import DEFAULT_PAGES
        parser = _build_parser()
        args = parser.parse_args([])
        assert args.pages == DEFAULT_PAGES

    def test_pages_flag(self) -> None:
        args = _build_parser().parse_args(["--pages", "7"])
        assert args.pages == 7

    def test_all_flag(self) -> None:
        args = _build_parser().parse_args(["--all"])
        assert args.all is True

    def test_verbose_flag(self) -> None:
        args = _build_parser().parse_args(["-v"])
        assert args.verbose is True


# ---------------------------------------------------------------------------
# main() — happy-path
# ---------------------------------------------------------------------------


class TestMainHappyPath:
    def test_no_flags_returns_zero(self) -> None:
        assert main([]) == 0

    def test_scrape_flag_calls_run_scrape(self) -> None:
        with patch("main.run_scrape") as mock_scrape:
            result = main(["--scrape", "--pages", "2"])
        assert result == 0
        mock_scrape.assert_called_once_with(pages=2)

    def test_clean_flag_calls_run_clean(self) -> None:
        with patch("main.run_clean") as mock_clean:
            result = main(["--clean"])
        assert result == 0
        mock_clean.assert_called_once()

    def test_transform_flag_calls_run_transform(self) -> None:
        with patch("main.run_transform") as mock_transform:
            result = main(["--transform"])
        assert result == 0
        mock_transform.assert_called_once()

    def test_visualize_flag_calls_run_visualize(self) -> None:
        with patch("main.run_visualize") as mock_viz:
            result = main(["--visualize"])
        assert result == 0
        mock_viz.assert_called_once()

    def test_all_flag_calls_all_stages(self) -> None:
        with (
            patch("main.run_scrape") as ms,
            patch("main.run_clean") as mc,
            patch("main.run_transform") as mt,
            patch("main.run_visualize") as mv,
        ):
            result = main(["--all", "--pages", "1"])
        assert result == 0
        ms.assert_called_once_with(pages=1)
        mc.assert_called_once()
        mt.assert_called_once()
        mv.assert_called_once()


# ---------------------------------------------------------------------------
# main() — error handling
# ---------------------------------------------------------------------------


class TestMainErrorHandling:
    def test_file_not_found_returns_one(self) -> None:
        with patch("main.run_clean", side_effect=FileNotFoundError("missing")):
            assert main(["--clean"]) == 1

    def test_unexpected_exception_returns_one(self) -> None:
        with patch("main.run_transform", side_effect=RuntimeError("boom")):
            assert main(["--transform"]) == 1
