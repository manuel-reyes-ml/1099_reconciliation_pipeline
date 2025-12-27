from __future__ import annotations

import sys
from pathlib import Path

# tests/ is one level under the repo root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))