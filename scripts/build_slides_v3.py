"""Backward-compatible wrapper delegating slide generation to scripts/build_slides.py."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from build_slides import main  # noqa: E402

if __name__ == "__main__":
    main()
