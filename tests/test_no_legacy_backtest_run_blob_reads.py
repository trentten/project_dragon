from __future__ import annotations

import re
from pathlib import Path


def test_no_legacy_backtest_run_blob_reads_outside_inspector_and_storage() -> None:
    """Guardrail: legacy blob columns should not be read from backtest_runs outside allowed files.

    Allowed:
    - src/project_dragon/storage.py (migration/backfill + compatibility)
    - examples/legacy_blob_inspector.py (explicit audit/backfill tool)

    Disallowed:
    - Any other Python file containing SQL that references backtest_runs + legacy blob columns.
    """

    repo_root = Path(__file__).resolve().parents[1]

    allowed = {
        (repo_root / "src" / "project_dragon" / "storage.py").resolve(),
        (repo_root / "examples" / "legacy_blob_inspector.py").resolve(),
        Path(__file__).resolve(),
    }

    legacy_cols = ("config_json", "metrics_json", "metadata_json", "trades_json")

    # Heuristic patterns: fail only when the blob column is referenced from backtest_runs.
    #
    # We avoid false positives such as `d.config_json` (details-table alias) by:
    # - matching unqualified `config_json` only when NOT preceded by a dot
    # - matching qualified access only for `r.<col>` or `backtest_runs.<col>`
    patterns = [
        # qualified: r.config_json / backtest_runs.config_json
        re.compile(
            r"\b(?:r|backtest_runs)\.(?:" + "|".join(legacy_cols) + r")\b",
            re.IGNORECASE,
        ),
        # unqualified near FROM backtest_runs, but not preceded by '.'
        re.compile(
            r"FROM\s+backtest_runs\b[\s\S]{0,400}(?<!\.)\b(?:" + "|".join(legacy_cols) + r")\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?<!\.)\b(?:" + "|".join(legacy_cols) + r")\b[\s\S]{0,400}FROM\s+backtest_runs\b",
            re.IGNORECASE,
        ),
    ]

    offenders: list[str] = []

    for path in repo_root.rglob("*.py"):
        resolved = path.resolve()
        if resolved in allowed:
            continue
        # Ignore venv/pycache-like paths if present.
        if any(part in {"__pycache__", ".venv", "venv"} for part in resolved.parts):
            continue

        text = path.read_text(encoding="utf-8", errors="ignore")
        if "backtest_runs" not in text:
            continue

        for pat in patterns:
            if pat.search(text):
                offenders.append(str(path.relative_to(repo_root)))
                break

    assert not offenders, (
        "Legacy blob columns from backtest_runs must only be accessed via "
        "get_backtest_run_details() (or inspector/storage). Offending files:\n- "
        + "\n- ".join(offenders)
    )
