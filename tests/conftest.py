"""
Pytest configuration.

Ensures the project root is on ``sys.path`` regardless of how pytest is
invoked, so that ``import config``, ``import scraper``, etc. always resolve.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Insert repo root at position 0 so project imports take priority over any
# installed packages with the same name.
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
