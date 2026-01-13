from __future__ import annotations

import sys
from pathlib import Path


# Ensure imports resolve to the canonical src/ tree when running pytest from the repo root.
_repo_root = Path(__file__).resolve().parent
_src_path = _repo_root / "src"
if _src_path.exists():
    sys.path.insert(0, str(_src_path))
