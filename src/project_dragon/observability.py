from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

logger = logging.getLogger("project_dragon.profile")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    raw = str(raw).strip().lower()
    return raw in {"1", "true", "t", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def profiling_enabled() -> bool:
    return _env_flag("DRAGON_PROFILE", False) or _env_flag("DRAGON_DEBUG_TIMING", False)


def slow_threshold_ms() -> float:
    return max(0.0, _env_float("DRAGON_SLOW_MS", 100.0))


def _safe_meta(meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not meta:
        return {}
    out: Dict[str, Any] = {}
    for k, v in meta.items():
        key = str(k)
        if any(t in key.lower() for t in ("json", "payload", "secret", "key", "token")):
            continue
        if isinstance(v, (int, float, bool)) or v is None:
            out[key] = v
        elif isinstance(v, str):
            out[key] = (v[:120] + "…") if len(v) > 120 else v
        else:
            out[key] = str(v)
    return out


@contextmanager
def profile_span(
    name: str,
    *,
    meta: Optional[Dict[str, Any]] = None,
    slow_ms: Optional[float] = None,
) -> Iterator[None]:
    """Time a block and emit a slow-span log when enabled.

    Enabled via `DRAGON_PROFILE=1` (or `DRAGON_DEBUG_TIMING=1`).
    Slow threshold via `DRAGON_SLOW_MS` (default 100ms).
    """

    if not profiling_enabled():
        yield
        return

    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        threshold = slow_threshold_ms() if slow_ms is None else float(slow_ms)
        if elapsed_ms >= threshold:
            m = _safe_meta(meta)
            if m:
                logger.info("SLOW span=%s ms=%.2f meta=%s", name, elapsed_ms, m)
            else:
                logger.info("SLOW span=%s ms=%.2f", name, elapsed_ms)
