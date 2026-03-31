from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TEST_DB = ROOT / "tgg_test.db"
os.environ.setdefault("DATABASE_URL", f"sqlite:///{TEST_DB}")

if TEST_DB.exists():
    TEST_DB.unlink()
