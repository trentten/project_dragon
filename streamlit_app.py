from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repo's src/ is on the path when running this wrapper.
_repo_root = Path(__file__).resolve().parent
_src_path = _repo_root / "src"
if _src_path.exists():
    sys.path.insert(0, str(_src_path))

from project_dragon.streamlit_app import main as _main

# Export for Streamlit and CLI
main = _main

if __name__ == "__main__":
    _main()
