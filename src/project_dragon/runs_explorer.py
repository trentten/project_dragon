from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Tuple


# Optional per-strategy defaults for which config keys are most useful to compare.
# Keys must match `flatten_config(...)` output keys.
STRATEGY_CONFIG_KEYS: Dict[str, List[str]] = {
    # Project Dragon v1 primary strategy.
    # Keep this list intentionally small and high-signal.
    "dragon_dca_atr": [
        # Default compare keys (requested): indicator intervals
        "trend.ma_interval_min",
        "rsi.interval_min",
        "bbands.interval_min",
        "macd.interval_min",
    ],
}


_DENY_KEYS_EXACT = {
    "_runtime",
    "runtime",
    "health",
    "debug",
    "logs",
    "events",
    "secrets",
    "credentials",
    "exchange_credentials",
}

_DENY_KEYS_SUBSTR = (
    "api_key",
    "apikey",
    "secret",
    "password",
    "passphrase",
    "private",
    "token",
)


def _should_skip_key(key: str) -> bool:
    k = (key or "").strip().lower()
    if not k:
        return True
    if k in _DENY_KEYS_EXACT:
        return True
    return any(s in k for s in _DENY_KEYS_SUBSTR)


def flatten_config(config: Mapping[str, Any] | None, strategy: str | None = None) -> Dict[str, Any]:
    """Flatten a nested config dict into dotted-path keys.

    Rules:
    - Dict keys become dotted paths: `tp.mode`.
    - Lists become numeric indices: `dca.levels.0.mult`.
    - Skips runtime/health/secrets-like keys.
    - Skips very large strings and caps list traversal.

    This is strategy-agnostic (strategy is accepted for future tuning).
    """

    if not isinstance(config, Mapping):
        return {}

    out: Dict[str, Any] = {}

    def walk(obj: Any, prefix: str) -> None:
        if isinstance(obj, Mapping):
            for k_raw, v in obj.items():
                k = str(k_raw)
                if _should_skip_key(k):
                    continue
                p = f"{prefix}.{k}" if prefix else k
                walk(v, p)
            return

        if isinstance(obj, list):
            # Avoid exploding huge arrays.
            max_n = 10
            n = min(len(obj), max_n)
            for i in range(n):
                p = f"{prefix}.{i}" if prefix else str(i)
                walk(obj[i], p)
            if len(obj) > max_n:
                out[f"{prefix}.__len__" if prefix else "__len__"] = len(obj)
            return

        # Scalars
        if isinstance(obj, str) and len(obj) > 300:
            return

        # Only emit leaf keys.
        if prefix:
            out[prefix] = obj

    walk(dict(config), "")
    return out


def _normalize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        # Stable bucketing for floats.
        return round(v, 10)
    if isinstance(v, str):
        return v.strip()
    # Fallback to repr for non-JSON scalars.
    try:
        return repr(v)
    except Exception:
        return str(v)


def compute_config_diff_flags(
    flattened_configs: Iterable[Mapping[str, Any]],
    keys: Iterable[str],
) -> Tuple[Dict[str, Any], List[Dict[str, bool]]]:
    """Compute per-row diff flags vs the mode for each key.

    Returns:
    - `modes`: key -> most common normalized value
    - `flags`: list aligned to `flattened_configs`, each is key -> differs_from_mode
    """

    cfgs = [dict(c) for c in flattened_configs]
    key_list = [str(k) for k in keys]

    modes: Dict[str, Any] = {}
    for k in key_list:
        c = Counter(_normalize_value(row.get(k)) for row in cfgs)
        if not c:
            modes[k] = None
            continue
        modes[k] = c.most_common(1)[0][0]

    flags: List[Dict[str, bool]] = []
    for row in cfgs:
        f: Dict[str, bool] = {}
        for k in key_list:
            f[k] = _normalize_value(row.get(k)) != modes.get(k)
        flags.append(f)

    return modes, flags


def top_config_keys_by_frequency(
    flattened_configs: Iterable[Mapping[str, Any]],
    *,
    max_keys: int = 25,
) -> List[str]:
    """Heuristic: pick keys that appear most often across configs."""

    counter: Counter[str] = Counter()
    for row in flattened_configs:
        try:
            for k in row.keys():
                counter[str(k)] += 1
        except Exception:
            continue

    # Prefer frequent keys; tie-break by name for stability.
    ordered = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    return [k for k, _ in ordered[: max(1, int(max_keys))]]
