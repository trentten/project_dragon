from __future__ import annotations

import base64
import hashlib
import json
import math
import mimetypes
import os
import re
import sqlite3
import threading
import requests
import uuid
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, TYPE_CHECKING, Tuple
from uuid import uuid4

from project_dragon.observability import profile_span

if TYPE_CHECKING:
    from project_dragon.domain import BacktestResult, Candle

_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "backtests.sqlite"
_db_path = _DB_PATH

_DB_INIT_LOCK = threading.Lock()
_DB_INITIALIZED_FOR: Optional[Path] = None
_DB_INITIALIZED_VERSION: Optional[int] = None


DEFAULT_USER_EMAIL = "admin@local"

# Sentinel used for optional update helpers where None means "set NULL".
_UNSET: object = object()


SQLITE_BUSY_TIMEOUT_MS = 5000


def _ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def _migration_0001_core_schema(conn: sqlite3.Connection) -> None:
    """Initial schema + additive upgrades.

    This migration is intentionally idempotent (CREATE IF NOT EXISTS + safe ALTERs).
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_runs (
            id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            config_json TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            metadata_json TEXT DEFAULT '{}',
            start_time TEXT,
            end_time TEXT,
            net_profit REAL,
            net_return_pct REAL,
            max_drawdown_pct REAL,
            sharpe REAL,
            sortino REAL,
            win_rate REAL,
            profit_factor REAL,
            cpc_index REAL,
            common_sense_ratio REAL,
            trades_json TEXT,
            sweep_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            sweep_id INTEGER,
            run_id INTEGER,
            bot_id INTEGER,
            progress REAL DEFAULT 0.0,
            message TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            error_text TEXT DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            exchange_id TEXT,
            symbol TEXT,
            timeframe TEXT,
            status TEXT,
            desired_status TEXT,
            config_json TEXT NOT NULL,
            heartbeat_at TEXT,
            heartbeat_msg TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_run_map (
            bot_id INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_id, run_id)
        )
        """
    )

    # Ensure additive columns / indices exist.
    _ensure_metadata_column(conn)
    _ensure_schema(conn)

    # Useful indexes for list views and common filters.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_created_at ON backtest_runs(created_at)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol_tf_created ON backtest_runs(symbol, timeframe, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_created_at ON backtest_runs(sweep_id, created_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sweeps_created_at ON sweeps(created_at)")

    # Optional multi-user performance index for sweeps list.
    # (Safe even if the column doesn't exist yet; `_ensure_schema` will add it.)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sweeps_user_created ON sweeps(user_id, created_at)")
    except sqlite3.OperationalError:
        pass


def _migration_0002_backtest_run_details(conn: sqlite3.Connection) -> None:
    """Create backtest_run_details table for large JSON blobs."""

    # Base table (older DBs may already have this with fewer columns).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_run_details (
            run_id TEXT PRIMARY KEY,
            config_json TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            trades_json TEXT,
            metadata_json TEXT
        )
        """
    )

    cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_run_details)").fetchall()}
    if "metadata_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}' ")
        except sqlite3.OperationalError:
            pass
    if "created_at" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN created_at TEXT")
        except sqlite3.OperationalError:
            pass
    if "updated_at" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN updated_at TEXT")
        except sqlite3.OperationalError:
            pass

    # Chart artifacts (avoid re-running backtests just to render charts)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_run_details)").fetchall()}
    if "equity_curve_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN equity_curve_json TEXT")
        except sqlite3.OperationalError:
            pass
    if "equity_timestamps_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN equity_timestamps_json TEXT")
        except sqlite3.OperationalError:
            pass
    if "extra_series_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN extra_series_json TEXT")
        except sqlite3.OperationalError:
            pass

    # Candle + run-context artifacts (render + analysis without network/re-run)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_run_details)").fetchall()}
    if "candles_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN candles_json TEXT")
        except sqlite3.OperationalError:
            pass
    if "params_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN params_json TEXT")
        except sqlite3.OperationalError:
            pass
    if "run_context_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN run_context_json TEXT")
        except sqlite3.OperationalError:
            pass
    if "computed_metrics_json" not in cols:
        try:
            conn.execute("ALTER TABLE backtest_run_details ADD COLUMN computed_metrics_json TEXT")
        except sqlite3.OperationalError:
            pass

    cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_run_details)").fetchall()}
    if "updated_at" in cols:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_run_details_updated_at ON backtest_run_details(updated_at)"
        )


def _migration_0003_exchange_state_snapshots(conn: sqlite3.Connection) -> None:
    # bot_state_snapshots additions
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(bot_state_snapshots)").fetchall()}
    except Exception:
        cols = set()
    if "exchange_state" not in cols:
        try:
            conn.execute("ALTER TABLE bot_state_snapshots ADD COLUMN exchange_state TEXT")
        except sqlite3.OperationalError:
            pass
    if "last_exchange_error_at" not in cols:
        try:
            conn.execute("ALTER TABLE bot_state_snapshots ADD COLUMN last_exchange_error_at TEXT")
        except sqlite3.OperationalError:
            pass

    # account_state_snapshots additions
    try:
        cols2 = {row[1] for row in conn.execute("PRAGMA table_info(account_state_snapshots)").fetchall()}
    except Exception:
        cols2 = set()
    if "exchange_state" not in cols2:
        try:
            conn.execute("ALTER TABLE account_state_snapshots ADD COLUMN exchange_state TEXT")
        except sqlite3.OperationalError:
            pass
    if "last_exchange_error_at" not in cols2:
        try:
            conn.execute("ALTER TABLE account_state_snapshots ADD COLUMN last_exchange_error_at TEXT")
        except sqlite3.OperationalError:
            pass


def _migration_0004_backtest_run_details_artifacts(conn: sqlite3.Connection) -> None:
    """Ensure `backtest_run_details` has artifact columns.

    Rationale:
    - Earlier DBs may have applied migration v2 before artifact columns existed.
    - SQLite migrations are version-gated, so adding columns inside v2 later does
      not update already-migrated DBs.
    - This migration is additive and idempotent.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_run_details (
            run_id TEXT PRIMARY KEY,
            config_json TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            trades_json TEXT,
            metadata_json TEXT
        )
        """
    )

    cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_run_details)").fetchall()}

    def _add_col(name: str, ddl: str) -> None:
        if name in cols:
            return
        try:
            conn.execute(ddl)
            cols.add(name)
        except sqlite3.OperationalError:
            return

    _add_col("metadata_json", "ALTER TABLE backtest_run_details ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}' ")
    _add_col("created_at", "ALTER TABLE backtest_run_details ADD COLUMN created_at TEXT")
    _add_col("updated_at", "ALTER TABLE backtest_run_details ADD COLUMN updated_at TEXT")

    # Chart artifacts
    _add_col("equity_curve_json", "ALTER TABLE backtest_run_details ADD COLUMN equity_curve_json TEXT")
    _add_col("equity_timestamps_json", "ALTER TABLE backtest_run_details ADD COLUMN equity_timestamps_json TEXT")
    _add_col("extra_series_json", "ALTER TABLE backtest_run_details ADD COLUMN extra_series_json TEXT")

    # Candle + run-context artifacts
    _add_col("candles_json", "ALTER TABLE backtest_run_details ADD COLUMN candles_json TEXT")
    _add_col("params_json", "ALTER TABLE backtest_run_details ADD COLUMN params_json TEXT")
    _add_col("run_context_json", "ALTER TABLE backtest_run_details ADD COLUMN run_context_json TEXT")
    _add_col("computed_metrics_json", "ALTER TABLE backtest_run_details ADD COLUMN computed_metrics_json TEXT")

    if "updated_at" in cols:
        try:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_backtest_run_details_updated_at ON backtest_run_details(updated_at)"
            )
        except sqlite3.OperationalError:
            pass


_MIGRATIONS: list[tuple[int, str, Any]] = [
    (1, "core_schema", _migration_0001_core_schema),
    (2, "backtest_run_details", _migration_0002_backtest_run_details),
    (3, "exchange_state_snapshots", _migration_0003_exchange_state_snapshots),
    (4, "backtest_run_details_artifacts", _migration_0004_backtest_run_details_artifacts),
]


def apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply any pending schema migrations."""

    _ensure_schema_migrations(conn)
    applied = {
        int(r[0])
        for r in conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    }
    for version, name, fn in _MIGRATIONS:
        if int(version) in applied:
            continue
        conn.execute("BEGIN")
        try:
            fn(conn)
            conn.execute(
                "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                (int(version), str(name), now_utc_iso()),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _apply_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    """Apply per-connection SQLite pragmas for better multi-process behavior.

    WAL is persisted per-DB, but setting it here is safe and idempotent.
    busy_timeout is per-connection and prevents transient 'database is locked' failures.
    """

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    try:
        # Needed for ON DELETE CASCADE and general FK integrity.
        conn.execute("PRAGMA foreign_keys=ON;")
    except Exception:
        pass
    try:
        # Good tradeoff for SQLite durability vs throughput in this workload.
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    try:
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_MS)};")
    except Exception:
        pass


def get_sqlite_pragmas(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Return key pragma values for debugging/verification."""

    out: Dict[str, Any] = {}
    try:
        out["journal_mode"] = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    except Exception:
        out["journal_mode"] = None
    try:
        out["busy_timeout"] = conn.execute("PRAGMA busy_timeout;").fetchone()[0]
    except Exception:
        out["busy_timeout"] = None
    try:
        out["foreign_keys"] = conn.execute("PRAGMA foreign_keys;").fetchone()[0]
    except Exception:
        out["foreign_keys"] = None
    try:
        out["synchronous"] = conn.execute("PRAGMA synchronous;").fetchone()[0]
    except Exception:
        out["synchronous"] = None
    return out


def open_db_connection(path: Optional[Path] = None) -> sqlite3.Connection:
    """Open a configured SQLite connection to the Project Dragon DB."""

    db_path = init_db(path)
    # timeout is another guardrail; busy_timeout is still set explicitly.
    conn = sqlite3.connect(db_path, timeout=max(1.0, SQLITE_BUSY_TIMEOUT_MS / 1000.0))
    conn.row_factory = sqlite3.Row
    _apply_sqlite_pragmas(conn)
    return conn


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_bytes_to_data_uri(file_bytes: bytes, filename: str) -> str:
    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        mime_type = "image/svg+xml" if filename.lower().endswith(".svg") else "image/png"
    b64 = base64.b64encode(file_bytes).decode("ascii")
    return f"data:{mime_type};base64,{b64}"


def _parse_exchange_symbol_assets(exchange_symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """Best-effort parse base/quote from common symbol formats.

    This is intentionally heuristic; it is used only for icon lookup and metadata display.
    """

    s = (exchange_symbol or "").strip()
    if not s:
        return None, None

    # Common ccxt format: BTC/USDT or BTC/USDT:USDT
    if "/" in s:
        left, right = s.split("/", 1)
        base = left.strip().upper() or None
        quote = right.split(":", 1)[0].strip().upper() or None
        return base, quote

    # Common WooX perps format: PERP_BTC_USDT
    m = re.match(r"^(?:PERP|SPOT)_(?P<base>[A-Z0-9]+)_(?P<quote>[A-Z0-9]+)$", s.strip().upper())
    if m:
        return (m.group("base") or "").strip() or None, (m.group("quote") or "").strip() or None

    # Simple dash format: BTC-USDT
    if "-" in s:
        parts = [p.strip().upper() for p in s.split("-") if p.strip()]
        if len(parts) >= 2:
            return parts[0], parts[1]

    return s.strip().upper(), None


def upsert_symbol(
    conn: sqlite3.Connection,
    *,
    exchange_symbol: str,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
    market_type: Optional[str] = None,
    icon_uri: Optional[str] = None,
) -> None:
    exchange_symbol = (exchange_symbol or "").strip()
    if not exchange_symbol:
        return

    if base_asset is None or quote_asset is None:
        base_guess, quote_guess = _parse_exchange_symbol_assets(exchange_symbol)
        base_asset = base_asset or base_guess
        quote_asset = quote_asset or quote_guess

    market_type_norm = (str(market_type).strip().lower() if market_type is not None else None)
    if market_type_norm in {"perp", "perps", "futures"}:
        market_type_norm = "perps"
    elif market_type_norm == "spot":
        market_type_norm = "spot"
    elif market_type_norm is not None and not market_type_norm:
        market_type_norm = None

    conn.execute(
        """
        INSERT INTO symbols (
            exchange_symbol, base_asset, quote_asset, market_type, icon_uri, updated_at
        ) VALUES (
            :exchange_symbol, :base_asset, :quote_asset, :market_type, :icon_uri, :updated_at
        )
        ON CONFLICT(exchange_symbol) DO UPDATE SET
            base_asset = COALESCE(excluded.base_asset, symbols.base_asset),
            quote_asset = COALESCE(excluded.quote_asset, symbols.quote_asset),
            market_type = COALESCE(excluded.market_type, symbols.market_type),
            icon_uri = COALESCE(excluded.icon_uri, symbols.icon_uri),
            updated_at = excluded.updated_at
        """,
        {
            "exchange_symbol": exchange_symbol,
            "base_asset": (base_asset.strip().upper() if isinstance(base_asset, str) and base_asset.strip() else None),
            "quote_asset": (quote_asset.strip().upper() if isinstance(quote_asset, str) and quote_asset.strip() else None),
            "market_type": market_type_norm,
            "icon_uri": icon_uri,
            "updated_at": now_utc_iso(),
        },
    )


def ensure_symbols_for_exchange_symbols(conn: sqlite3.Connection, exchange_symbols: Iterable[str]) -> int:
    """Ensure a `symbols` row exists for each exchange_symbol (best-effort).

    Performance note:
    - Callers may invoke this on every UI rerun (e.g. Results grids).
    - Avoid rewriting existing rows (especially `updated_at`) since that turns a
      lightweight metadata check into thousands of writes.
    """

    # Normalize + de-dup while preserving order.
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in exchange_symbols:
        sym = str(raw or "").strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        normalized.append(sym)

    if not normalized:
        return 0

    # Fetch existing symbols in chunks to keep the IN clause bounded.
    existing: set[str] = set()
    chunk_size = 500
    try:
        for i in range(0, len(normalized), chunk_size):
            chunk = normalized[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            rows = conn.execute(
                f"SELECT exchange_symbol FROM symbols WHERE exchange_symbol IN ({placeholders})",
                chunk,
            ).fetchall()
            for r in rows:
                try:
                    existing.add(str(r[0]))
                except Exception:
                    continue
    except Exception:
        # If the symbols table doesn't exist yet, fall back to best-effort upserts.
        existing = set()

    missing = [sym for sym in normalized if sym not in existing]
    if not missing:
        return 0

    n = 0
    for sym in missing:
        try:
            upsert_symbol(conn, exchange_symbol=sym)
            n += 1
        except Exception:
            continue
    return n


def sync_symbol_icons_from_manifest(
    conn: sqlite3.Connection,
    *,
    manifest_path: str,
    icons_root: str,
    force: bool = False,
    max_updates: int = 500,
) -> int:
    """Sync local icon pack (manifest + files) into `symbols.icon_uri`.

    `manifest_path` must be a JSON object mapping asset ticker -> relative file path.
    Updates all rows matching `symbols.base_asset = <ticker>`.
    """
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(manifest_path)
    if not os.path.isdir(icons_root):
        raise FileNotFoundError(icons_root)

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    if not isinstance(manifest, dict):
        raise ValueError("Icon manifest must be a JSON object")

    updates = 0
    for raw_key, rel_path in manifest.items():
        if updates >= max_updates:
            break
        if not isinstance(raw_key, str) or not isinstance(rel_path, str):
            continue
        asset = raw_key.strip().upper()
        rel_path_norm = rel_path.strip().lstrip("/")
        if not asset or not rel_path_norm:
            continue
        full_path = os.path.join(icons_root, rel_path_norm)
        if not os.path.exists(full_path):
            continue

        # Skip if already set (unless force)
        if not force:
            row = conn.execute(
                "SELECT 1 FROM symbols WHERE base_asset = ? AND icon_uri IS NOT NULL AND TRIM(icon_uri) != '' LIMIT 1",
                (asset,),
            ).fetchone()
            if row:
                continue

        with open(full_path, "rb") as f:
            icon_bytes = f.read()
        icon_uri = _file_bytes_to_data_uri(icon_bytes, filename=os.path.basename(full_path))

        cur = conn.execute(
            "UPDATE symbols SET icon_uri = ?, updated_at = ? WHERE base_asset = ?",
            (icon_uri, now_utc_iso(), asset),
        )
        if cur.rowcount:
            updates += 1

    return updates


def sync_symbol_icons_from_spothq_pack(
    conn: sqlite3.Connection,
    *,
    icons_root: str,
    base_assets: Optional[Iterable[str]] = None,
    style: str = "color",
    size_preference: Optional[Iterable[str]] = None,
    force: bool = False,
    max_updates: int = 500,
) -> int:
    """Sync spothq/cryptocurrency-icons into `symbols.icon_uri`.

    This resolver does NOT require a manifest and does not perform network calls.
    It looks for files in the spothq layout:

    - <root>/svg/<style>/<ticker>.svg
    - <root>/<size>/<style>/<ticker>.png

    Rows are updated by `symbols.base_asset` (so multiple exchange_symbol rows for the
    same asset get the same icon).
    """

    from project_dragon.ui.crypto_icons import icon_bytes_to_data_uri, resolve_crypto_icon

    root = (icons_root or "").strip()
    if not root or not os.path.isdir(root):
        raise FileNotFoundError(root or icons_root)

    prefs = list(size_preference) if size_preference is not None else ["svg", "32", "128"]
    style_norm = (style or "color").strip().lower() or "color"

    assets: list[str] = []
    if base_assets is not None:
        for a in base_assets:
            s = (str(a or "").strip().upper())
            if s and s not in assets:
                assets.append(s)
    else:
        try:
            rows = conn.execute(
                "SELECT DISTINCT base_asset FROM symbols WHERE base_asset IS NOT NULL AND TRIM(base_asset) != '' ORDER BY base_asset"
            ).fetchall()
            assets = [str(r[0]).strip().upper() for r in rows or [] if str(r[0] or "").strip()]
        except Exception:
            assets = []

    updates = 0
    for asset in assets:
        if updates >= max_updates:
            break

        # Skip if already set (unless force)
        if not force:
            row = conn.execute(
                "SELECT 1 FROM symbols WHERE base_asset = ? AND icon_uri IS NOT NULL AND TRIM(icon_uri) != '' LIMIT 1",
                (asset,),
            ).fetchone()
            if row:
                continue

        resolved = resolve_crypto_icon(asset, size_preference=prefs, style=style_norm, icons_root=Path(root))
        if not resolved:
            continue
        mime, b = resolved
        icon_uri = icon_bytes_to_data_uri(mime, b)

        cur = conn.execute(
            "UPDATE symbols SET icon_uri = ?, updated_at = ? WHERE base_asset = ?",
            (icon_uri, now_utc_iso(), asset),
        )
        if cur.rowcount:
            updates += 1

    return updates


def get_current_user_email() -> str:
    """Resolve current user identity.

    Priority order:
    1) Reverse-proxy auth header (Streamlit): DRAGON_AUTH_HEADER (default X-Forwarded-User)
    2) Env var DRAGON_USER_EMAIL
    3) Fallback DEFAULT_USER_EMAIL

    Notes:
    - Header lookup is best-effort and safe to call outside Streamlit.
    - Value is normalized (strip + lower); empty falls back.
    """

    header_name = (os.environ.get("DRAGON_AUTH_HEADER") or "X-Forwarded-User").strip() or "X-Forwarded-User"

    def _extract_first_email(value: str) -> str:
        raw = (value or "").strip()
        if not raw:
            return ""
        # Handle comma-separated values by picking the first email-ish token.
        matches = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", raw, flags=re.IGNORECASE)
        if matches:
            return matches[0]
        first_part = raw.split(",", 1)[0].strip()
        if "<" in first_part and ">" in first_part:
            inner = first_part.split("<", 1)[1].split(">", 1)[0].strip()
            matches = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", inner, flags=re.IGNORECASE)
            return matches[0] if matches else inner
        # As a final fallback, take the first whitespace token that looks like an email.
        for token in first_part.replace(";", " ").split():
            if "@" in token and "." in token:
                return token.strip("<>\"' ")
        return first_part

    email = ""
    try:
        import streamlit as st  # type: ignore
        try:
            from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx  # type: ignore
        except Exception:
            get_script_run_ctx = None

        headers = {}
        try:
            if get_script_run_ctx is not None and get_script_run_ctx() is not None:
                headers = st.context.headers  # type: ignore[attr-defined]
        except Exception:
            headers = {}
        if isinstance(headers, dict):
            v = headers.get(header_name) or headers.get(header_name.lower()) or ""
            email = _extract_first_email(str(v))
    except Exception:
        # Streamlit not available or headers unavailable.
        email = ""

    if not email:
        email = (os.environ.get("DRAGON_USER_EMAIL") or "").strip()
    email = (email or "").strip().lower()
    return email or DEFAULT_USER_EMAIL


def get_or_create_user_id(email: str, conn: Optional[sqlite3.Connection] = None) -> str:
    """Ensure a user row exists and return a stable user_id.

    v0 uses the email itself as the user_id (TEXT) for migration safety.
    Accepts an optional open connection for callers that already have one.
    """

    resolved = (email or "").strip().lower() or DEFAULT_USER_EMAIL
    if conn is not None:
        return get_or_create_user(conn, resolved)
    with _get_conn() as c:
        return get_or_create_user(c, resolved)


def get_or_create_user(conn: sqlite3.Connection, email: str) -> str:
    """Ensure a user row exists and return user_id.

    For v0 we use email itself as the stable user_id.
    """

    user_id = (email or "").strip() or DEFAULT_USER_EMAIL
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO users(id, email, created_at)
            VALUES (?, ?, ?)
            """,
            (user_id, user_id, now_utc_iso()),
        )
        conn.commit()
    except sqlite3.OperationalError:
        # Schema may not be migrated yet; caller is expected to run init_db().
        pass
    return user_id


def init_db(path: Optional[Path] = None) -> Path:
    global _db_path
    global _DB_INITIALIZED_FOR
    global _DB_INITIALIZED_VERSION
    if path is not None:
        _db_path = path

    resolved = Path(_db_path).resolve()
    # Avoid running migrations/DDL on every connection open. Streamlit reruns
    # frequently and this function is called by open_db_connection().
    # However, when code updates add new migrations, a long-running Streamlit
    # process must still be able to apply them without requiring a restart.
    latest_version = 0
    try:
        latest_version = max(int(v) for (v, _name, _fn) in _MIGRATIONS)
    except Exception:
        latest_version = 0
    if (
        _DB_INITIALIZED_FOR is not None
        and _DB_INITIALIZED_FOR == resolved
        and _DB_INITIALIZED_VERSION is not None
        and int(_DB_INITIALIZED_VERSION) >= int(latest_version)
    ):
        return resolved

    with _DB_INIT_LOCK:
        if (
            _DB_INITIALIZED_FOR is not None
            and _DB_INITIALIZED_FOR == resolved
            and _DB_INITIALIZED_VERSION is not None
            and int(_DB_INITIALIZED_VERSION) >= int(latest_version)
        ):
            return resolved
        resolved.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(resolved) as conn:
            _apply_sqlite_pragmas(conn)
            apply_migrations(conn)
            # Record current max migration version present in DB.
            try:
                row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
                _DB_INITIALIZED_VERSION = int(row[0] or 0) if row else 0
            except Exception:
                _DB_INITIALIZED_VERSION = latest_version
        _DB_INITIALIZED_FOR = resolved
        return resolved


def _ensure_metadata_column(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(backtest_runs)").fetchall()
    column_names = {row[1] for row in rows}
    if "metadata_json" not in column_names:
        conn.execute("ALTER TABLE backtest_runs ADD COLUMN metadata_json TEXT DEFAULT '{}'")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    # Core identity + credentials tables.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    # Per-user settings (UI preferences, etc).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, key),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_settings_user ON user_settings(user_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS exchange_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            label TEXT,
            api_key TEXT NOT NULL,
            api_secret_enc TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_used_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exchange_credentials_user
        ON exchange_credentials(user_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exchange_credentials_user_exchange
        ON exchange_credentials(user_id, exchange_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exchange_credentials_user_id
        ON exchange_credentials(user_id, id)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candles_cache (
            exchange_id TEXT NOT NULL,
            market_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (exchange_id, market_type, symbol, timeframe, timestamp_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candles_cache_symbol_tf_ts
        ON candles_cache(exchange_id, symbol, timeframe, timestamp_ms)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sweeps (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            created_at TEXT NOT NULL,
            name TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            data_source TEXT NOT NULL,
            exchange_id TEXT,
            symbol TEXT,
            timeframe TEXT,
            range_mode TEXT NOT NULL,
            range_params_json TEXT NOT NULL,
            base_config_json TEXT NOT NULL,
            sweep_definition_json TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT
        )
        """
    )

    # Older DBs may have sweeps without user_id; add it safely.
    try:
        sweep_cols = {row[1] for row in conn.execute("PRAGMA table_info(sweeps)").fetchall()}
    except Exception:
        sweep_cols = set()
    if "user_id" not in sweep_cols:
        try:
            conn.execute("ALTER TABLE sweeps ADD COLUMN user_id TEXT")
        except sqlite3.OperationalError:
            pass

    # Multi-asset sweep support (additive columns).
    try:
        sweep_cols = {row[1] for row in conn.execute("PRAGMA table_info(sweeps)").fetchall()}
    except Exception:
        sweep_cols = set()
    if "sweep_scope" not in sweep_cols:
        try:
            conn.execute("ALTER TABLE sweeps ADD COLUMN sweep_scope TEXT NOT NULL DEFAULT 'single_asset'")
        except sqlite3.OperationalError:
            pass
    if "sweep_assets_json" not in sweep_cols:
        try:
            conn.execute("ALTER TABLE sweeps ADD COLUMN sweep_assets_json TEXT")
        except sqlite3.OperationalError:
            pass
    if "sweep_category_id" not in sweep_cols:
        try:
            conn.execute("ALTER TABLE sweeps ADD COLUMN sweep_category_id INTEGER")
        except sqlite3.OperationalError:
            pass

    # Index for per-user sweeps list (safe even if column missing).
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sweeps_user_created ON sweeps(user_id, created_at)")
    except sqlite3.OperationalError:
        pass
    columns = {row[1] for row in conn.execute("PRAGMA table_info(backtest_runs)").fetchall()}
    added_avg_position_time_s = False
    desired_columns = {
        "sweep_id": "ALTER TABLE backtest_runs ADD COLUMN sweep_id TEXT",
        "market_type": "ALTER TABLE backtest_runs ADD COLUMN market_type TEXT",
        "start_time": "ALTER TABLE backtest_runs ADD COLUMN start_time TEXT",
        "end_time": "ALTER TABLE backtest_runs ADD COLUMN end_time TEXT",
        "net_profit": "ALTER TABLE backtest_runs ADD COLUMN net_profit REAL",
        "net_return_pct": "ALTER TABLE backtest_runs ADD COLUMN net_return_pct REAL",
        "roi_pct_on_margin": "ALTER TABLE backtest_runs ADD COLUMN roi_pct_on_margin REAL",
        "max_drawdown_pct": "ALTER TABLE backtest_runs ADD COLUMN max_drawdown_pct REAL",
        "sharpe": "ALTER TABLE backtest_runs ADD COLUMN sharpe REAL",
        "sortino": "ALTER TABLE backtest_runs ADD COLUMN sortino REAL",
        "win_rate": "ALTER TABLE backtest_runs ADD COLUMN win_rate REAL",
        "profit_factor": "ALTER TABLE backtest_runs ADD COLUMN profit_factor REAL",
        "cpc_index": "ALTER TABLE backtest_runs ADD COLUMN cpc_index REAL",
        "common_sense_ratio": "ALTER TABLE backtest_runs ADD COLUMN common_sense_ratio REAL",
        "trades_json": "ALTER TABLE backtest_runs ADD COLUMN trades_json TEXT",
        "avg_position_time_s": "ALTER TABLE backtest_runs ADD COLUMN avg_position_time_s REAL",
    }
    for name, ddl in desired_columns.items():
        if name not in columns:
            try:
                conn.execute(ddl)
                if name == "avg_position_time_s":
                    added_avg_position_time_s = True
            except sqlite3.OperationalError:
                pass

    # One-time best-effort backfill for avg_position_time_s from stored trades_json.
    # Runs only when the column is first added in this DB.
    if added_avg_position_time_s:
        try:
            import math
            from datetime import timezone

            def _parse_iso_dt(v: Any) -> Optional[datetime]:
                if v is None:
                    return None
                s = str(v).strip()
                if not s:
                    return None
                try:
                    if s.endswith("Z"):
                        s2 = s[:-1] + "+00:00"
                    else:
                        s2 = s
                    dt = datetime.fromisoformat(s2)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except Exception:
                    return None

            def _compute_avg_pos_time_s_from_trades(trades_obj: Any) -> Optional[float]:
                if not isinstance(trades_obj, list) or not trades_obj:
                    return None
                net_qty = 0.0
                open_ts: Optional[datetime] = None
                durs: list[float] = []
                for t in trades_obj:
                    if not isinstance(t, dict):
                        continue
                    ts = _parse_iso_dt(t.get("timestamp"))
                    if ts is None:
                        continue
                    try:
                        qty = float(t.get("size") or 0.0)
                    except Exception:
                        qty = 0.0
                    if qty <= 0:
                        continue
                    side = str(t.get("side") or "").strip().upper()
                    prev = net_qty
                    if side == "LONG":
                        net_qty += qty
                    else:
                        net_qty -= qty
                    if prev == 0.0 and net_qty != 0.0 and open_ts is None:
                        open_ts = ts
                    if prev != 0.0 and net_qty == 0.0 and open_ts is not None:
                        try:
                            dt_s = float((ts - open_ts).total_seconds())
                        except Exception:
                            dt_s = float("nan")
                        if math.isfinite(dt_s) and dt_s >= 0:
                            durs.append(dt_s)
                        open_ts = None
                if not durs:
                    return None
                try:
                    return float(sum(durs) / len(durs))
                except Exception:
                    return None

            rows = conn.execute(
                "SELECT id, trades_json FROM backtest_runs WHERE avg_position_time_s IS NULL AND trades_json IS NOT NULL AND TRIM(trades_json) != ''"
            ).fetchall()
            for r in rows or []:
                try:
                    run_id = str(r[0])
                    raw = r[1]
                    trades_obj = json.loads(raw) if isinstance(raw, str) and raw.strip() else None
                    avg_s = _compute_avg_pos_time_s_from_trades(trades_obj)
                    if avg_s is None:
                        continue
                    conn.execute(
                        "UPDATE backtest_runs SET avg_position_time_s = ? WHERE id = ?",
                        (float(avg_s), run_id),
                    )
                except Exception:
                    continue
            conn.commit()
        except Exception:
            pass

    # Backfill market_type from existing stored config/metadata for older rows.
    # (Best-effort; no JSON1 dependency.)
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_runs)").fetchall()}
        if "market_type" in cols:
            rows = conn.execute(
                "SELECT id, market_type, metadata_json, config_json FROM backtest_runs WHERE market_type IS NULL OR TRIM(market_type) = ''"
            ).fetchall()
            for r in rows:
                try:
                    run_id = r[0]
                    meta_raw = r[2]
                    cfg_raw = r[3]
                    meta = json.loads(meta_raw) if isinstance(meta_raw, str) and meta_raw else {}
                    cfg = json.loads(cfg_raw) if isinstance(cfg_raw, str) and cfg_raw else {}
                except Exception:
                    meta = {}
                    cfg = {}
                    run_id = r[0]

                mkt = None
                if isinstance(meta, dict):
                    mkt = meta.get("market_type") or meta.get("market") or meta.get("market_type_hint")
                if not mkt and isinstance(cfg, dict):
                    ds = cfg.get("data_settings") if isinstance(cfg.get("data_settings"), dict) else None
                    if isinstance(ds, dict):
                        mkt = ds.get("market_type")
                    if not mkt:
                        mkt = cfg.get("market_type")
                mkt_norm = (str(mkt).strip().lower() if mkt is not None else "")
                if mkt_norm in {"perp", "perps", "futures"}:
                    mkt_norm = "perps"
                elif mkt_norm in {"spot"}:
                    mkt_norm = "spot"
                if mkt_norm:
                    try:
                        conn.execute("UPDATE backtest_runs SET market_type = ? WHERE id = ?", (mkt_norm, str(run_id)))
                    except Exception:
                        pass
            conn.commit()
    except Exception:
        pass

    # Index (safe even if column missing on very old DBs)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_roi_pct_on_margin ON backtest_runs(roi_pct_on_margin)")
    except sqlite3.OperationalError:
        pass

    conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_id ON backtest_runs(sweep_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_created_at ON backtest_runs(sweep_id, created_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_net_return_pct ON backtest_runs(net_return_pct)")

    # Symbols metadata + cached icons (local icon pack, stored as data URIs)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbols (
            exchange_symbol TEXT PRIMARY KEY,
            base_asset TEXT NOT NULL,
            quote_asset TEXT,
            market_type TEXT,
            icon_uri TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symbols_base_asset ON symbols(base_asset)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symbols_market_type ON symbols(market_type)")

    # --- Assets + categories (per-user grouping) -----------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            base_asset TEXT,
            quote_asset TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(exchange_id, symbol)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_exchange_status ON assets(exchange_id, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_exchange_symbol ON assets(exchange_id, symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_exchange_base ON assets(exchange_id, base_asset)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asset_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            name TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'user',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, exchange_id, name)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_asset_categories_user_exchange ON asset_categories(user_id, exchange_id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asset_category_membership (
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, exchange_id, category_id, symbol)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_asset_cat_membership_cat ON asset_category_membership(user_id, exchange_id, category_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_asset_cat_membership_symbol ON asset_category_membership(user_id, exchange_id, symbol)"
    )

    # Best-effort backfill: seed assets from existing symbols table (or runs/bots) once.
    try:
        row = conn.execute("SELECT COUNT(*) FROM assets").fetchone()
        assets_count = int(row[0] if row is not None else 0)
    except Exception:
        assets_count = 0
    if assets_count <= 0:
        try:
            _seed_assets_from_existing_tables(conn)
        except Exception:
            pass

    # Per-user shortlists for runs explorer.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_shortlists (
            user_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            shortlisted INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, run_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_shortlists_user_updated ON run_shortlists(user_id, updated_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_bot_id ON jobs(bot_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            exchange_id TEXT,
            symbol TEXT,
            timeframe TEXT,
            status TEXT,
            desired_status TEXT,
            config_json TEXT NOT NULL,
            heartbeat_at TEXT,
            heartbeat_msg TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    bot_columns = {row[1] for row in conn.execute("PRAGMA table_info(bots)").fetchall()}
    bot_new_columns = {
        "user_id": "ALTER TABLE bots ADD COLUMN user_id TEXT",
        "credentials_id": "ALTER TABLE bots ADD COLUMN credentials_id INTEGER",
        "account_id": "ALTER TABLE bots ADD COLUMN account_id INTEGER",
        "realized_pnl": "ALTER TABLE bots ADD COLUMN realized_pnl REAL DEFAULT 0",
        "unrealized_pnl": "ALTER TABLE bots ADD COLUMN unrealized_pnl REAL DEFAULT 0",
        "fees_total": "ALTER TABLE bots ADD COLUMN fees_total REAL DEFAULT 0",
        "funding_total": "ALTER TABLE bots ADD COLUMN funding_total REAL DEFAULT 0",
        "roi_pct_on_margin": "ALTER TABLE bots ADD COLUMN roi_pct_on_margin REAL",
        "dca_fills_current": "ALTER TABLE bots ADD COLUMN dca_fills_current INTEGER DEFAULT 0",
        "position_json": "ALTER TABLE bots ADD COLUMN position_json TEXT DEFAULT '{}'",
        "positions_snapshot_json": "ALTER TABLE bots ADD COLUMN positions_snapshot_json TEXT DEFAULT '{}'",
        "positions_normalized_json": "ALTER TABLE bots ADD COLUMN positions_normalized_json TEXT",
        "open_orders_json": "ALTER TABLE bots ADD COLUMN open_orders_json TEXT DEFAULT '[]'",
        "mark_price": "ALTER TABLE bots ADD COLUMN mark_price REAL",
        "desired_action": "ALTER TABLE bots ADD COLUMN desired_action TEXT",
        "blocked_actions_count": "ALTER TABLE bots ADD COLUMN blocked_actions_count INTEGER DEFAULT 0",
    }
    for name, ddl in bot_new_columns.items():
        if name not in bot_columns:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_user_id ON bots(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_credentials_id ON bots(credentials_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_user_account_id ON bots(user_id, account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_status ON bots(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_desired_status ON bots(desired_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_desired_action ON bots(desired_action)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_blocked_actions_count ON bots(blocked_actions_count)")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_roi_pct_on_margin ON bots(roi_pct_on_margin)")
    except sqlite3.OperationalError:
        pass

    # Multi-account model (trading_accounts)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            label TEXT NOT NULL,
            credential_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            risk_block_new_entries INTEGER DEFAULT 1,
            risk_max_daily_loss_usd REAL,
            risk_max_total_loss_usd REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, exchange_id, label)
        )
        """
    )
    # Additive account-risk columns for older DBs (idempotent)
    try:
        acct_cols = {row[1] for row in conn.execute("PRAGMA table_info(trading_accounts)").fetchall()}
        acct_new_columns = {
            "risk_block_new_entries": "ALTER TABLE trading_accounts ADD COLUMN risk_block_new_entries INTEGER DEFAULT 1",
            "risk_max_daily_loss_usd": "ALTER TABLE trading_accounts ADD COLUMN risk_max_daily_loss_usd REAL",
            "risk_max_total_loss_usd": "ALTER TABLE trading_accounts ADD COLUMN risk_max_total_loss_usd REAL",
        }
        for name, ddl in acct_new_columns.items():
            if name not in acct_cols:
                try:
                    conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass
    except sqlite3.OperationalError:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_accounts_user_exchange ON trading_accounts(user_id, exchange_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_accounts_credential_id ON trading_accounts(credential_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_accounts_status ON trading_accounts(status)")

    # Backfill bots.account_id for legacy bots that used bots.credentials_id.
    try:
        # Ensure bots.user_id is non-empty before backfill.
        conn.execute(
            "UPDATE bots SET user_id = COALESCE(user_id, ?) WHERE user_id IS NULL OR TRIM(user_id) = ''",
            (DEFAULT_USER_EMAIL,),
        )
        legacy_rows = conn.execute(
            """
            SELECT id,
                   COALESCE(NULLIF(TRIM(user_id), ''), ?) AS user_id,
                   COALESCE(NULLIF(TRIM(exchange_id), ''), 'woox') AS exchange_id,
                   credentials_id
            FROM bots
            WHERE (account_id IS NULL OR account_id = 0)
              AND credentials_id IS NOT NULL
            """,
            (DEFAULT_USER_EMAIL,),
        ).fetchall()
        now = now_utc_iso()
        for bot_id_val, uid_raw, ex_raw, cred_id_raw in legacy_rows:
            try:
                cred_id = int(cred_id_raw)
            except (TypeError, ValueError):
                continue
            if cred_id <= 0:
                continue
            uid = str(uid_raw or "").strip() or DEFAULT_USER_EMAIL
            ex = str(ex_raw or "woox").strip().lower() or "woox"
            # Prefer reusing any existing account for (user, exchange, credential).
            row = conn.execute(
                """
                SELECT id
                FROM trading_accounts
                WHERE user_id = ? AND exchange_id = ? AND credential_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (uid, ex, int(cred_id)),
            ).fetchone()
            if row:
                account_id = int(row[0])
            else:
                label = f"Legacy {ex.upper()} (cred #{cred_id})"
                conn.execute(
                    """
                    INSERT OR IGNORE INTO trading_accounts(
                        user_id, exchange_id, label, credential_id, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'active', ?, ?)
                    """,
                    (uid, ex, label, int(cred_id), now, now),
                )
                row2 = conn.execute(
                    """
                    SELECT id
                    FROM trading_accounts
                    WHERE user_id = ? AND exchange_id = ? AND credential_id = ?
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (uid, ex, int(cred_id)),
                ).fetchone()
                if not row2:
                    continue
                account_id = int(row2[0])

            conn.execute(
                "UPDATE bots SET account_id = ? WHERE id = ? AND (account_id IS NULL OR account_id = 0)",
                (int(account_id), int(bot_id_val)),
            )
        conn.commit()
    except sqlite3.OperationalError:
        # Older DBs may not have the columns yet; keep schema init idempotent.
        pass

    # Backfill null blocked_actions_count values
    try:
        conn.execute(
            "UPDATE bots SET blocked_actions_count = COALESCE(blocked_actions_count, 0) WHERE blocked_actions_count IS NULL"
        )
    except sqlite3.OperationalError:
        pass

    # Backfill user_id for legacy bots.
    try:
        conn.execute(
            "UPDATE bots SET user_id = COALESCE(user_id, ?) WHERE user_id IS NULL OR TRIM(user_id) = ''",
            (DEFAULT_USER_EMAIL,),
        )
    except sqlite3.OperationalError:
        pass

    # Ensure default user exists.
    try:
        get_or_create_user(conn, DEFAULT_USER_EMAIL)
    except Exception:
        pass
    job_columns = {row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    if "sweep_id" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN sweep_id INTEGER")
        except sqlite3.OperationalError:
            pass
    if "run_id" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN run_id INTEGER")
        except sqlite3.OperationalError:
            pass
    if "bot_id" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN bot_id INTEGER")
        except sqlite3.OperationalError:
            pass
    if "worker_id" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN worker_id TEXT")
        except sqlite3.OperationalError:
            pass

    # Lease-based multi-worker correctness (additive / ALTER-only).
    # These columns intentionally coexist with legacy jobs.worker_id.
    if "claimed_by" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN claimed_by TEXT")
        except sqlite3.OperationalError:
            pass
    if "claimed_at" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN claimed_at TEXT")
        except sqlite3.OperationalError:
            pass
    if "lease_expires_at" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN lease_expires_at TEXT")
        except sqlite3.OperationalError:
            pass
    if "last_lease_renew_at" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN last_lease_renew_at TEXT")
        except sqlite3.OperationalError:
            pass
    if "lease_version" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN lease_version INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
    if "stale_reclaims" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN stale_reclaims INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
    if "updated_at" not in job_columns:
        try:
            conn.execute("ALTER TABLE jobs ADD COLUMN updated_at TEXT")
        except sqlite3.OperationalError:
            pass
    # Backfill updated_at where missing
    try:
        conn.execute("UPDATE jobs SET updated_at = COALESCE(updated_at, created_at) WHERE updated_at IS NULL OR updated_at = ''")
    except sqlite3.OperationalError:
        pass

    # Backfill lease_version/stale_reclaims NULLs for legacy rows.
    try:
        conn.execute(
            "UPDATE jobs SET lease_version = COALESCE(lease_version, 0) WHERE lease_version IS NULL"
        )
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute(
            "UPDATE jobs SET stale_reclaims = COALESCE(stale_reclaims, 0) WHERE stale_reclaims IS NULL"
        )
    except sqlite3.OperationalError:
        pass

    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_worker_id ON jobs(worker_id)")

    # Lease lookup / stale reclaim helpers.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_lease_expires ON jobs(status, lease_expires_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_claimed_by_status ON jobs(claimed_by, status)")

    # bot_run_map for traceability
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_run_map (
            bot_id INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_id, run_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_run_map_run_id ON bot_run_map(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_run_map_bot_id ON bot_run_map(bot_id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            json_payload TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_events_bot_id_ts ON bot_events(bot_id, ts DESC)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            run_id TEXT,
            symbol TEXT NOT NULL,
            exchange_id TEXT,
            position_side TEXT,
            order_action TEXT,
            client_order_id TEXT,
            external_order_id TEXT,
            filled_qty REAL NOT NULL,
            avg_fill_price REAL NOT NULL,
            fee_paid REAL,
            fee_asset TEXT,
            is_reduce_only INTEGER DEFAULT 0,
            is_dca INTEGER DEFAULT 0,
            note TEXT,
            event_ts TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_fills_bot_ts ON bot_fills(bot_id, event_ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_fills_run_id ON bot_fills(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_fills_client_oid ON bot_fills(bot_id, client_order_id)")

    # Unified ledger for fills/fees/funding adjustments
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            event_ts TEXT NOT NULL,
            kind TEXT NOT NULL,
            symbol TEXT,
            side TEXT,
            position_side TEXT,
            qty REAL,
            price REAL,
            fee REAL,
            funding REAL,
            pnl REAL,
            ref_id TEXT,
            meta_json TEXT DEFAULT '{}'
        )
        """
    )
    # Unique idempotency only when ref_id is present
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_ledger_unique_ref
        ON bot_ledger(bot_id, kind, ref_id)
        WHERE ref_id IS NOT NULL
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_ledger_bot_ts ON bot_ledger(bot_id, event_ts DESC)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_positions_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            run_id TEXT,
            symbol TEXT NOT NULL,
            exchange_id TEXT,
            position_side TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            closed_at TEXT NOT NULL,
            max_size REAL,
            entry_avg_price REAL,
            exit_avg_price REAL,
            realized_pnl REAL,
            fees_paid REAL,
            num_fills INTEGER,
            num_dca_fills INTEGER,
            metadata_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_positions_closed ON bot_positions_history(bot_id, closed_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_positions_run_id ON bot_positions_history(run_id)")

    # Bot order intents (restart-safe dynamic order tracking)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_order_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            intent_key TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'parked',
            local_id INTEGER NOT NULL,
            note TEXT,
            position_side TEXT,
            activation_pct REAL,
            target_price REAL,
            qty REAL,
            reduce_only INTEGER DEFAULT 0,
            post_only INTEGER DEFAULT 0,
            client_order_id TEXT,
            external_order_id TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(bot_id, intent_key)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_order_intents_bot_status ON bot_order_intents(bot_id, status, updated_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_order_intents_bot_local ON bot_order_intents(bot_id, local_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_order_intents_client_oid ON bot_order_intents(bot_id, client_order_id)"
    )

    # Durable state snapshots (bot/account) for UI + multi-worker readiness.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_state_snapshots (
            bot_id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            worker_id TEXT,
            status TEXT,
            desired_status TEXT,
            desired_action TEXT,
            next_action TEXT,
            health_status TEXT,
            health_json TEXT,
            pos_status TEXT,
            positions_summary_json TEXT,
            open_orders_count INTEGER,
            open_intents_count INTEGER,
            account_id INTEGER,
            risk_blocked INTEGER,
            risk_reason TEXT,
            last_error TEXT,
            last_event_ts TEXT,
            last_event_level TEXT,
            last_event_type TEXT,
            last_event_message TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_state_snapshots_user_updated ON bot_state_snapshots(user_id, updated_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_state_snapshots_account_updated ON bot_state_snapshots(account_id, updated_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_state_snapshots_exchange_symbol_tf ON bot_state_snapshots(exchange_id, symbol, timeframe)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_state_snapshots (
            account_id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            worker_id TEXT,
            status TEXT,
            risk_blocked INTEGER,
            risk_reason TEXT,
            positions_summary_json TEXT,
            open_orders_count INTEGER,
            margin_ratio REAL,
            wallet_balance REAL,
            last_error TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_account_state_snapshots_user_updated ON account_state_snapshots(user_id, updated_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_account_state_snapshots_exchange_updated ON account_state_snapshots(exchange_id, updated_at DESC)"
    )


def _get_conn() -> sqlite3.Connection:
    return open_db_connection()


@dataclass
class SweepMeta:
    name: str
    strategy_name: str
    strategy_version: str
    data_source: str
    exchange_id: Optional[str]
    symbol: Optional[str]
    timeframe: Optional[str]
    range_mode: str
    range_params_json: str
    base_config_json: str
    sweep_definition_json: str
    sweep_scope: str = "single_asset"
    sweep_assets_json: Optional[str] = None
    sweep_category_id: Optional[int] = None
    user_id: Optional[str] = None
    status: str = "pending"
    error_message: Optional[str] = None
    id: Optional[str] = None


def create_sweep(meta: SweepMeta) -> str:
    sweep_id = meta.id or str(uuid.uuid4())
    payload = asdict(meta)
    payload["id"] = sweep_id
    payload["created_at"] = now_utc_iso()
    with _get_conn() as conn:
        _ensure_schema(conn)

        # Backwards-compatible: older DBs may not have sweeps.user_id.
        try:
            sweep_cols = {row[1] for row in conn.execute("PRAGMA table_info(sweeps)").fetchall()}
        except Exception:
            sweep_cols = set()

        # Refresh sweeps columns after any ALTERs.
        try:
            sweep_cols = {row[1] for row in conn.execute("PRAGMA table_info(sweeps)").fetchall()}
        except Exception:
            sweep_cols = set()

        has_user_id = "user_id" in sweep_cols
        has_scope = "sweep_scope" in sweep_cols
        has_assets = "sweep_assets_json" in sweep_cols
        has_cat = "sweep_category_id" in sweep_cols

        if has_user_id:
            payload["user_id"] = (payload.get("user_id") or "").strip() or DEFAULT_USER_EMAIL

        cols: list[str] = [
            "id",
            "created_at",
            "name",
            "strategy_name",
            "strategy_version",
            "data_source",
            "exchange_id",
            "symbol",
            "timeframe",
            "range_mode",
            "range_params_json",
            "base_config_json",
            "sweep_definition_json",
            "status",
            "error_message",
        ]
        if has_user_id:
            cols.insert(1, "user_id")
        if has_scope:
            cols.append("sweep_scope")
        if has_assets:
            cols.append("sweep_assets_json")
        if has_cat:
            cols.append("sweep_category_id")

        placeholders = ", ".join([f":{c}" for c in cols])
        cols_sql = ", ".join(cols)

        conn.execute(
            f"""
            INSERT INTO sweeps ({cols_sql})
            VALUES ({placeholders})
            """,
            payload,
        )
    return sweep_id


def _infer_exchange_id_for_symbol_seed(exchange_symbol: str) -> str:
    s = (exchange_symbol or "").strip().upper()
    if s.startswith("PERP_") or s.startswith("SPOT_"):
        return "woox"
    # CCXT default used in the app is "woo".
    return "woo"


def _seed_assets_from_existing_tables(conn: sqlite3.Connection) -> int:
    """Seed `assets` once from existing tables (best-effort).

    Priority:
    1) symbols table (if present)
    2) sweeps/bots distinct symbols with exchange_id
    3) backtest_runs metadata_json (best-effort, limited scan)
    """

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    now = now_utc_iso()
    rows: list[tuple[str, str, Optional[str], Optional[str], str, str]] = []

    try:
        sym_rows = conn.execute(
            "SELECT exchange_symbol, base_asset, quote_asset FROM symbols WHERE exchange_symbol IS NOT NULL AND TRIM(exchange_symbol) != ''"
        ).fetchall()
    except Exception:
        sym_rows = []

    for r in sym_rows or []:
        ex_sym = str(r[0] or "").strip()
        if not ex_sym:
            continue
        ex_id = _infer_exchange_id_for_symbol_seed(ex_sym)
        base = (str(r[1]).strip().upper() if r[1] is not None else None)
        quote = (str(r[2]).strip().upper() if r[2] is not None else None)
        rows.append((ex_id, ex_sym, base, quote, now, now))

    if not rows:
        try:
            sw = conn.execute(
                "SELECT DISTINCT exchange_id, symbol FROM sweeps WHERE exchange_id IS NOT NULL AND TRIM(exchange_id) != '' AND symbol IS NOT NULL AND TRIM(symbol) != ''"
            ).fetchall()
        except Exception:
            sw = []
        for r in sw or []:
            ex_id = str(r[0] or "").strip()
            sym = str(r[1] or "").strip()
            if not ex_id or not sym:
                continue
            base, quote = _parse_exchange_symbol_assets(sym)
            rows.append((ex_id, sym, base, quote, now, now))

        try:
            bt = conn.execute(
                "SELECT DISTINCT exchange_id, symbol FROM bots WHERE exchange_id IS NOT NULL AND TRIM(exchange_id) != '' AND symbol IS NOT NULL AND TRIM(symbol) != ''"
            ).fetchall()
        except Exception:
            bt = []
        for r in bt or []:
            ex_id = str(r[0] or "").strip()
            sym = str(r[1] or "").strip()
            if not ex_id or not sym:
                continue
            base, quote = _parse_exchange_symbol_assets(sym)
            rows.append((ex_id, sym, base, quote, now, now))

    if not rows:
        # Last resort: scan a limited number of runs for metadata.exchange_id.
        try:
            run_rows = conn.execute(
                "SELECT symbol, metadata_json FROM backtest_runs WHERE symbol IS NOT NULL AND TRIM(symbol) != '' ORDER BY created_at DESC LIMIT 5000"
            ).fetchall()
        except Exception:
            run_rows = []
        for r in run_rows or []:
            sym = str(r[0] or "").strip()
            if not sym:
                continue
            ex_id = ""
            try:
                meta_raw = r[1]
                meta = json.loads(meta_raw) if isinstance(meta_raw, str) and meta_raw.strip() else {}
                if isinstance(meta, dict):
                    ex_id = str(meta.get("exchange_id") or "").strip()
            except Exception:
                ex_id = ""
            if not ex_id:
                ex_id = _infer_exchange_id_for_symbol_seed(sym)
            base, quote = _parse_exchange_symbol_assets(sym)
            rows.append((ex_id, sym, base, quote, now, now))

    if not rows:
        return 0

    # Deduplicate before insert.
    dedup: dict[tuple[str, str], tuple[str, str, Optional[str], Optional[str], str, str]] = {}
    for ex_id, sym, base, quote, ca, ua in rows:
        key = (str(ex_id), str(sym))
        if key not in dedup:
            dedup[key] = (str(ex_id), str(sym), base, quote, ca, ua)

    cur = conn.executemany(
        """
        INSERT OR IGNORE INTO assets(exchange_id, symbol, base_asset, quote_asset, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        list(dedup.values()),
    )
    try:
        return int(cur.rowcount or 0)
    except Exception:
        return 0


def _upsert_asset(
    conn: sqlite3.Connection,
    *,
    exchange_id: str,
    symbol: str,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
    status: Optional[str] = None,
) -> None:
    ex_id = (exchange_id or "").strip()
    sym = (symbol or "").strip()
    if not ex_id or not sym:
        return
    if base_asset is None or quote_asset is None:
        b, q = _parse_exchange_symbol_assets(sym)
        base_asset = base_asset or b
        quote_asset = quote_asset or q
    base_norm = (base_asset.strip().upper() if isinstance(base_asset, str) and base_asset.strip() else None)
    quote_norm = (quote_asset.strip().upper() if isinstance(quote_asset, str) and quote_asset.strip() else None)
    status_norm = (str(status).strip().lower() if status is not None else None)
    if status_norm is not None and status_norm not in {"active", "disabled"}:
        status_norm = None

    conn.execute(
        """
        INSERT INTO assets(exchange_id, symbol, base_asset, quote_asset, status, created_at, updated_at)
        VALUES (:exchange_id, :symbol, :base_asset, :quote_asset, COALESCE(:status, 'active'), :created_at, :updated_at)
        ON CONFLICT(exchange_id, symbol) DO UPDATE SET
            base_asset = COALESCE(excluded.base_asset, assets.base_asset),
            quote_asset = COALESCE(excluded.quote_asset, assets.quote_asset),
            status = COALESCE(excluded.status, assets.status),
            updated_at = excluded.updated_at
        """,
        {
            "exchange_id": ex_id,
            "symbol": sym,
            "base_asset": base_norm,
            "quote_asset": quote_norm,
            "status": status_norm,
            "created_at": now_utc_iso(),
            "updated_at": now_utc_iso(),
        },
    )


def upsert_asset(
    exchange_id: str,
    symbol: str,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
) -> None:
    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            _upsert_asset(
                conn,
                exchange_id=str(exchange_id or "").strip(),
                symbol=str(symbol or "").strip(),
                base_asset=base_asset,
                quote_asset=quote_asset,
            )


def upsert_assets_bulk(exchange_id: str, rows: Iterable[tuple[str, Optional[str], Optional[str]]]) -> int:
    """Bulk upsert assets for an exchange.

    `rows` is an iterable of (symbol, base_asset, quote_asset).
    Returns the number of non-empty symbols processed.
    """

    ex_id = str(exchange_id or "").strip()
    if not ex_id:
        raise ValueError("exchange_id is required")

    now = now_utc_iso()
    payload: list[tuple[str, str, Optional[str], Optional[str], str, str]] = []
    for sym_raw, base_raw, quote_raw in (rows or []):
        sym = str(sym_raw or "").strip()
        if not sym:
            continue
        b = str(base_raw or "").strip().upper() or None
        q = str(quote_raw or "").strip().upper() or None
        payload.append((ex_id, sym, b, q, now, now))

    if not payload:
        return 0

    # Deduplicate before insert (SQLite executemany does not dedupe automatically).
    dedup: dict[tuple[str, str], tuple[str, str, Optional[str], Optional[str], str, str]] = {}
    for ex, sym, b, q, ca, ua in payload:
        key = (ex, sym)
        if key not in dedup:
            dedup[key] = (ex, sym, b, q, ca, ua)

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            conn.executemany(
                """
                INSERT INTO assets(exchange_id, symbol, base_asset, quote_asset, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(exchange_id, symbol) DO UPDATE SET
                    base_asset = COALESCE(excluded.base_asset, assets.base_asset),
                    quote_asset = COALESCE(excluded.quote_asset, assets.quote_asset),
                    updated_at = excluded.updated_at
                """,
                list(dedup.values()),
            )

    return int(len(dedup))


def list_assets(
    user_id: str,
    exchange_id: str,
    status: str = "active",
) -> List[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    st_norm = (status or "").strip().lower() or "active"
    if st_norm not in {"active", "disabled", "all"}:
        st_norm = "active"

    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row

        where = ""
        params: list[Any] = [ex_id, uid, ex_id]
        if st_norm != "all":
            where = "WHERE a.status = ?"
            params.insert(1, st_norm)

        # One query: assets + a comma-separated list of category names for this user.
        rows = conn.execute(
            f"""
            SELECT
                a.id,
                a.exchange_id,
                a.symbol,
                a.base_asset,
                a.quote_asset,
                a.status,
                a.created_at,
                a.updated_at,
                COALESCE(GROUP_CONCAT(DISTINCT c.name), '') AS categories_csv
            FROM assets a
            LEFT JOIN asset_category_membership m
                ON m.exchange_id = a.exchange_id
                AND m.symbol = a.symbol
                AND m.user_id = ?
            LEFT JOIN asset_categories c
                ON c.id = m.category_id
                AND c.user_id = m.user_id
                AND c.exchange_id = m.exchange_id
            WHERE a.exchange_id = ?
            {('AND a.status = ?' if st_norm != 'all' else '')}
            GROUP BY a.id
            ORDER BY a.base_asset ASC, a.symbol ASC
            """,
            tuple([uid, ex_id] + ([st_norm] if st_norm != "all" else [])),
        ).fetchall()

    out: list[Dict[str, Any]] = []
    for r in rows or []:
        d = dict(r)
        cats = [c.strip() for c in str(d.get("categories_csv") or "").split(",") if c.strip()]
        d["categories"] = cats
        out.append(d)
    return out


def list_categories(user_id: str, exchange_id: str) -> List[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                c.*, 
                COALESCE(mc.member_count, 0) AS member_count
            FROM asset_categories c
            LEFT JOIN (
                SELECT user_id, exchange_id, category_id, COUNT(*) AS member_count
                FROM asset_category_membership
                GROUP BY user_id, exchange_id, category_id
            ) mc
                ON mc.user_id = c.user_id
                AND mc.exchange_id = c.exchange_id
                AND mc.category_id = c.id
            WHERE c.user_id = ? AND c.exchange_id = ?
            ORDER BY c.name ASC
            """,
            (uid, ex_id),
        ).fetchall()
    return [dict(r) for r in rows or []]


def set_assets_status(exchange_id: str, symbols: Iterable[str], status: str) -> int:
    """Bulk set assets.status for an exchange.

    Returns the number of rows updated (best-effort).
    """
    ex_id = (exchange_id or "").strip()
    st_norm = (status or "").strip().lower()
    if st_norm not in {"active", "disabled"}:
        raise ValueError("status must be 'active' or 'disabled'")
    syms = [str(s or "").strip() for s in (symbols or []) if str(s or "").strip()]
    if not ex_id or not syms:
        return 0
    now = now_utc_iso()

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            updated = 0
            chunk = 300
            for i in range(0, len(syms), chunk):
                part = syms[i : i + chunk]
                placeholders = ",".join(["?"] * len(part))
                cur = conn.execute(
                    f"UPDATE assets SET status = ?, updated_at = ? WHERE exchange_id = ? AND symbol IN ({placeholders})",
                    tuple([st_norm, now, ex_id] + part),
                )
                try:
                    updated += int(cur.rowcount or 0)
                except Exception:
                    pass
            return int(updated)


def create_category(user_id: str, exchange_id: str, name: str, source: str = "user") -> int:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    nm = (name or "").strip()
    if not ex_id or not nm:
        raise ValueError("exchange_id and name are required")
    src = (source or "user").strip().lower() or "user"
    if src not in {"user", "auto"}:
        src = "user"
    now = now_utc_iso()

    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row
        with conn:
            conn.execute(
                """
                INSERT INTO asset_categories(user_id, exchange_id, name, source, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, exchange_id, name) DO UPDATE SET
                    updated_at = excluded.updated_at
                """,
                (uid, ex_id, nm, src, now, now),
            )
            row = conn.execute(
                "SELECT id FROM asset_categories WHERE user_id = ? AND exchange_id = ? AND name = ?",
                (uid, ex_id, nm),
            ).fetchone()
            return int(row[0]) if row else 0


def get_category_symbols(user_id: str, exchange_id: str, category_id: int) -> List[str]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    cid = int(category_id)
    with _get_conn() as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT symbol
            FROM asset_category_membership
            WHERE user_id = ? AND exchange_id = ? AND category_id = ?
            ORDER BY symbol ASC
            """,
            (uid, ex_id, cid),
        ).fetchall()
    return [str(r[0]) for r in rows or [] if r and str(r[0] or "").strip()]


def set_category_membership(
    user_id: str,
    exchange_id: str,
    category_id: int,
    symbols_to_add: Iterable[str],
    symbols_to_remove: Iterable[str],
) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    cid = int(category_id)
    now = now_utc_iso()

    add_rows: list[tuple[str, str, int, str, str]] = []
    for s in symbols_to_add or []:
        sym = (str(s or "").strip())
        if not sym:
            continue
        add_rows.append((uid, ex_id, cid, sym, now))

    rem_rows: list[str] = []
    for s in symbols_to_remove or []:
        sym = (str(s or "").strip())
        if not sym:
            continue
        rem_rows.append(sym)

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            if add_rows:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO asset_category_membership(user_id, exchange_id, category_id, symbol, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    add_rows,
                )
            if rem_rows:
                # Chunk to stay under SQLite parameter limits.
                chunk = 300
                for i in range(0, len(rem_rows), chunk):
                    part = rem_rows[i : i + chunk]
                    placeholders = ",".join(["?"] * len(part))
                    conn.execute(
                        f"DELETE FROM asset_category_membership WHERE user_id = ? AND exchange_id = ? AND category_id = ? AND symbol IN ({placeholders})",
                        tuple([uid, ex_id, cid] + part),
                    )
            conn.execute(
                "UPDATE asset_categories SET updated_at = ? WHERE user_id = ? AND exchange_id = ? AND id = ?",
                (now, uid, ex_id, cid),
            )


def delete_category(user_id: str, exchange_id: str, category_id: int) -> int:
    """Delete a category and all its memberships for a user/exchange."""

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    cid = int(category_id)
    if not ex_id:
        raise ValueError("exchange_id is required")

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            conn.execute(
                "DELETE FROM asset_category_membership WHERE user_id = ? AND exchange_id = ? AND category_id = ?",
                (uid, ex_id, cid),
            )
            cur = conn.execute(
                "DELETE FROM asset_categories WHERE user_id = ? AND exchange_id = ? AND id = ?",
                (uid, ex_id, cid),
            )
            return int(cur.rowcount or 0)


def seed_asset_categories_if_empty(user_id: str, exchange_id: str, *, force: bool = False) -> Dict[str, Any]:
    """One-time best-effort category seeding.

    Never overwrites user edits: when categories exist, default is a no-op.
    If `force=True`, this runs in merge mode (adds missing auto categories/memberships via INSERT OR IGNORE).
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    if not ex_id:
        raise ValueError("exchange_id is required")

    with _get_conn() as conn:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT COUNT(*) FROM asset_categories WHERE user_id = ? AND exchange_id = ?",
            (uid, ex_id),
        ).fetchone()
        existing = int(row[0] if row is not None else 0)
        if existing > 0 and not bool(force):
            return {"status": "skipped", "reason": "categories_exist", "created_categories": 0, "created_memberships": 0}

    # Create base categories regardless (source=auto).
    base_categories: list[tuple[str, str]] = [
        ("L1", "layer-1"),
        ("L2", "layer-2"),
        ("Meme", "meme-token"),
        ("Stable", "stablecoins"),
        ("DeFi", "decentralized-finance-defi"),
    ]

    created_categories = 0
    created_memberships = 0
    cat_name_to_id: dict[str, int] = {}
    for name, _slug in base_categories:
        try:
            cid = create_category(uid, ex_id, name, source="auto")
            if cid:
                cat_name_to_id[name] = cid
                created_categories += 1
        except Exception:
            continue

    # Best-effort online fetch from CoinGecko.
    ticker_map: dict[str, set[str]] = {name: set() for name, _ in base_categories}
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "project-dragon/1.0 (category-seed)"})

        for name, slug in base_categories:
            if not slug:
                continue
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "category": slug,
                "order": "market_cap_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": "false",
            }
            resp = session.get(url, params=params, timeout=8)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                sym = (str(item.get("symbol") or "").strip().upper())
                if sym:
                    ticker_map[name].add(sym)
    except Exception:
        ticker_map = {name: set() for name, _ in base_categories}

    # Map tickers -> exchange symbols using assets.base_asset.
    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row

        for cat_name, tickers in ticker_map.items():
            cid = cat_name_to_id.get(cat_name)
            if not cid or not tickers:
                continue
            tick_list = sorted(list(tickers))
            chunk = 300
            symbols: list[str] = []
            for i in range(0, len(tick_list), chunk):
                part = tick_list[i : i + chunk]
                placeholders = ",".join(["?"] * len(part))
                rows = conn.execute(
                    f"SELECT symbol FROM assets WHERE exchange_id = ? AND base_asset IN ({placeholders}) AND status = 'active'",
                    tuple([ex_id] + part),
                ).fetchall()
                for r in rows or []:
                    s = str(r[0] or "").strip()
                    if s:
                        symbols.append(s)
            if symbols:
                before = conn.execute(
                    "SELECT COUNT(*) FROM asset_category_membership WHERE user_id = ? AND exchange_id = ? AND category_id = ?",
                    (uid, ex_id, int(cid)),
                ).fetchone()
                before_n = int(before[0] if before is not None else 0)

                # Insert (ignore duplicates).
                now = now_utc_iso()
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO asset_category_membership(user_id, exchange_id, category_id, symbol, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [(uid, ex_id, int(cid), s, now) for s in sorted(set(symbols))],
                )

                after = conn.execute(
                    "SELECT COUNT(*) FROM asset_category_membership WHERE user_id = ? AND exchange_id = ? AND category_id = ?",
                    (uid, ex_id, int(cid)),
                ).fetchone()
                after_n = int(after[0] if after is not None else 0)
                created_memberships += max(0, after_n - before_n)

        conn.commit()

    return {
        "status": "ok",
        "created_categories": int(created_categories),
        "created_memberships": int(created_memberships),
    }


def get_symbols_primary_category_map(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    exchange_id: str,
    symbols: Sequence[str],
) -> Dict[str, str]:
    """Return symbol -> (first category name) mapping for a user/exchange.

    Uses a single query; suitable for result grids.
    """
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    syms = [str(s or "").strip() for s in (symbols or []) if str(s or "").strip()]
    if not ex_id or not syms:
        return {}
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row

    out: Dict[str, str] = {}
    chunk = 300
    for i in range(0, len(syms), chunk):
        part = syms[i : i + chunk]
        placeholders = ",".join(["?"] * len(part))
        rows = conn.execute(
            f"""
            SELECT m.symbol, MIN(c.name) AS cat
            FROM asset_category_membership m
            JOIN asset_categories c
                ON c.id = m.category_id
                AND c.user_id = m.user_id
                AND c.exchange_id = m.exchange_id
            WHERE m.user_id = ? AND m.exchange_id = ? AND m.symbol IN ({placeholders})
            GROUP BY m.symbol
            """,
            tuple([uid, ex_id] + part),
        ).fetchall()
        for r in rows or []:
            sym = str(r[0] or "").strip()
            cat = str(r[1] or "").strip()
            if sym and cat:
                out[sym] = cat
    return out


def update_sweep_status(sweep_id: str, status: str, error_message: Optional[str] = None) -> None:
    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            conn.execute(
                "UPDATE sweeps SET status = ?, error_message = ? WHERE id = ?",
                (status, error_message, sweep_id),
            )


def list_sweeps(limit: int = 100, offset: int = 0) -> List[Dict]:
    with _get_conn() as conn:
        _ensure_schema(conn)
        with profile_span("db.list_sweeps", meta={"limit": int(limit), "offset": int(offset)}):
            rows = conn.execute(
            """
            SELECT * FROM sweeps
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
            ).fetchall()
    return [dict(r) for r in rows]


def list_sweeps_with_run_counts(
    *,
    user_id: str,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[Dict]:
    """List sweeps with a DB-derived run_count per sweep.

    Performance: one query using a LEFT JOIN to an aggregated subquery.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    _ = filters  # reserved for future SQL WHERE clauses

    with _get_conn() as conn:
        _ensure_schema(conn)
        try:
            sweep_cols = {row[1] for row in conn.execute("PRAGMA table_info(sweeps)").fetchall()}
        except Exception:
            sweep_cols = set()

        where_clause = ""
        params: list[Any] = []
        if "user_id" in sweep_cols:
            where_clause = "WHERE (s.user_id = ? OR s.user_id IS NULL OR TRIM(s.user_id) = '')"
            params.append(uid)

        params.extend([int(limit), int(offset)])

        with profile_span(
            "db.list_sweeps_with_run_counts",
            meta={"limit": int(limit), "offset": int(offset), "user_scoped": bool(where_clause)},
        ):
            rows = conn.execute(
                f"""
                SELECT
                    s.*,
                    COALESCE(rc.run_count, 0) AS run_count
                FROM sweeps s
                LEFT JOIN (
                    SELECT sweep_id, COUNT(*) AS run_count
                    FROM backtest_runs
                    WHERE sweep_id IS NOT NULL AND TRIM(sweep_id) != ''
                    GROUP BY sweep_id
                ) rc
                    ON rc.sweep_id = s.id
                {where_clause}
                ORDER BY s.created_at DESC
                LIMIT ? OFFSET ?
                """,
                tuple(params),
            ).fetchall()

    return [dict(r) for r in rows]


def list_sweep_best_metrics(*, user_id: str) -> List[Dict[str, Any]]:
    """Compute per-sweep best metrics via SQL aggregation.

    This avoids loading all run summaries into memory (which can be slow and can
    make the Sweeps page feel like it's hanging).

    Returns rows like:
      {"sweep_id": "...", "best_roi_pct_on_margin": 12.3, "best_cpc_index": 1.8, ...}

    Only includes metrics that exist as columns in `backtest_runs`.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL

    with _get_conn() as conn:
        _ensure_schema(conn)

        try:
            run_cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_runs)").fetchall()}
        except Exception:
            run_cols = set()

        metric_cols: list[tuple[str, str]] = []
        if "net_return_pct" in run_cols:
            metric_cols.append(("best_net_return_pct", "net_return_pct"))
        if "sharpe" in run_cols:
            metric_cols.append(("best_sharpe", "sharpe"))
        if "roi_pct_on_margin" in run_cols:
            metric_cols.append(("best_roi_pct_on_margin", "roi_pct_on_margin"))
        if "cpc_index" in run_cols:
            metric_cols.append(("best_cpc_index", "cpc_index"))

        # Nothing to do if sweep_id missing or no metrics exist.
        if "sweep_id" not in run_cols or not metric_cols:
            return []

        select_bits = ["sweep_id"] + [f"MAX({src}) AS {alias}" for alias, src in metric_cols]
        where_bits: list[str] = ["sweep_id IS NOT NULL", "TRIM(COALESCE(sweep_id,'')) <> ''"]
        params: list[Any] = []
        if "user_id" in run_cols:
            where_bits.append("(user_id = ? OR user_id IS NULL OR TRIM(user_id) = '')")
            params.append(uid)

        sql = (
            "SELECT "
            + ", ".join(select_bits)
            + " FROM backtest_runs WHERE "
            + " AND ".join(where_bits)
            + " GROUP BY sweep_id"
        )

        with profile_span("db.list_sweep_best_metrics", meta={"user_id": uid}):
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_sweep_run_count(*, user_id: str, sweep_id: str) -> int:
    """Fallback helper to count runs for a given sweep.

    Prefer using the run_count already returned by list_sweeps_with_run_counts.
    """

    _ = (user_id or "").strip() or DEFAULT_USER_EMAIL
    sid = str(sweep_id or "").strip()
    if not sid:
        return 0

    with _get_conn() as conn:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM backtest_runs WHERE sweep_id = ?",
            (sid,),
        ).fetchone()
    try:
        return int(row[0] if row is not None else 0)
    except Exception:
        return 0


def get_sweep(sweep_id: str) -> Optional[Dict]:
    with _get_conn() as conn:
        _ensure_schema(conn)
        row = conn.execute("SELECT * FROM sweeps WHERE id = ?", (sweep_id,)).fetchone()
    return dict(row) if row else None


def list_runs_for_sweep(sweep_id: str) -> List[Dict]:
    with _get_conn() as conn:
        _ensure_schema(conn)
        with profile_span("db.list_runs_for_sweep", meta={"sweep_id": str(sweep_id)}):
            rows = conn.execute(
            """
            SELECT * FROM backtest_runs
            WHERE sweep_id = ?
            ORDER BY created_at ASC
            """,
            (sweep_id,),
            ).fetchall()
    return [dict(r) for r in rows]


def _canonicalize_json_for_hash(raw: Any) -> str:
    """Return a stable string representation for hash-based comparisons.

    Used for deduplication. This is intentionally tolerant of legacy / malformed
    payloads: when JSON parsing fails, it falls back to a normalized string.
    """

    if raw is None:
        return "null"

    # Most callers store JSON strings in SQLite.
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return ""
        try:
            obj = json.loads(s)
        except Exception:
            return s
    else:
        obj = raw

    try:
        # Ensure deterministic formatting independent of dict insertion order.
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return ""


def dedupe_sweep_runs(sweep_id: str) -> Dict[str, Any]:
    """Remove duplicate runs within a sweep.

    Duplicates are defined as runs that share the same sweep_id and the same
    canonicalized config payload, *scoped by* (strategy_name, strategy_version,
    symbol, timeframe) to avoid deleting legitimate multi-asset sweeps.

    Returns a small summary dict suitable for logging/UI.
    """

    sid = str(sweep_id or "").strip()
    if not sid:
        return {"sweep_id": sid, "total": 0, "deduped_groups": 0, "removed": 0}

    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row

        with profile_span("db.dedupe_sweep_runs", meta={"sweep_id": sid}):
            rows = conn.execute(
                """
                SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version, config_json
                FROM backtest_runs
                WHERE sweep_id = ?
                ORDER BY created_at ASC
                """,
                (sid,),
            ).fetchall()

            total = len(rows or [])
            if total <= 1:
                return {"sweep_id": sid, "total": total, "deduped_groups": 0, "removed": 0}

            groups: Dict[str, List[sqlite3.Row]] = {}
            for r in rows:
                cfg_canon = _canonicalize_json_for_hash(r["config_json"])
                cfg_hash = hashlib.sha256(cfg_canon.encode("utf-8", errors="ignore")).hexdigest()
                key = "|".join(
                    [
                        str(r["strategy_name"] or ""),
                        str(r["strategy_version"] or ""),
                        str(r["symbol"] or ""),
                        str(r["timeframe"] or ""),
                        cfg_hash,
                    ]
                )
                groups.setdefault(key, []).append(r)

            to_remove: List[str] = []
            deduped_groups = 0
            for _k, bucket in groups.items():
                if len(bucket) <= 1:
                    continue

                deduped_groups += 1
                bucket_ids = [str(b["id"]) for b in bucket]

                # Prefer keeping a run that is already referenced by a live bot.
                keep_id: Optional[str] = None
                try:
                    placeholders = ",".join(["?"] * len(bucket_ids))
                    ref = conn.execute(
                        f"SELECT run_id FROM bot_run_map WHERE run_id IN ({placeholders}) LIMIT 1",
                        tuple(bucket_ids),
                    ).fetchone()
                    if ref:
                        keep_id = str(ref[0])
                except sqlite3.OperationalError:
                    keep_id = None

                if not keep_id:
                    keep_id = str(bucket[0]["id"])  # earliest created_at

                for rid in bucket_ids:
                    if rid != keep_id:
                        to_remove.append(rid)

            removed = 0
            if to_remove:
                placeholders = ",".join(["?"] * len(to_remove))
                with conn:
                    try:
                        conn.execute(
                            f"DELETE FROM backtest_run_details WHERE run_id IN ({placeholders})",
                            tuple(to_remove),
                        )
                    except sqlite3.OperationalError:
                        # Legacy DB without details table.
                        pass
                    cur = conn.execute(
                        f"DELETE FROM backtest_runs WHERE id IN ({placeholders})",
                        tuple(to_remove),
                    )
                    removed = int(cur.rowcount or 0)

            return {
                "sweep_id": sid,
                "total": total,
                "deduped_groups": deduped_groups,
                "removed": removed,
            }


def get_setting(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT value_json FROM app_settings WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except (TypeError, json.JSONDecodeError):
        return default


def get_user_setting(conn: sqlite3.Connection, user_id: str, key: str, default: Any = None) -> Any:
    _ensure_schema(conn)
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = conn.execute(
        "SELECT value_json FROM user_settings WHERE user_id = ? AND key = ?",
        (uid, str(key)),
    ).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except (TypeError, json.JSONDecodeError):
        return default


def set_user_setting(conn: sqlite3.Connection, user_id: str, key: str, value: Any) -> None:
    _ensure_schema(conn)
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    payload = json.dumps(value)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO user_settings(user_id, key, value_json, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, key) DO UPDATE
        SET value_json = excluded.value_json, updated_at = excluded.updated_at
        """,
        (uid, str(key), payload, now),
    )
    conn.commit()


def set_setting(conn: sqlite3.Connection, key: str, value: Any) -> None:
    payload = json.dumps(value)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO app_settings(key, value_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
        """,
        (key, payload, now),
    )
    conn.commit()


def get_settings(conn: sqlite3.Connection) -> Dict[str, Any]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT key, value_json FROM app_settings").fetchall()
    out: Dict[str, Any] = {}
    for row in rows:
        try:
            out[row["key"]] = json.loads(row["value_json"])
        except (TypeError, json.JSONDecodeError):
            continue
    return out


def get_app_settings(conn: sqlite3.Connection) -> Dict[str, Any]:
    defaults = {
        "initial_balance_default": 1000.0,
        "fee_spot_pct": 0.10,
        "fee_perps_pct": 0.06,
        "fee_stocks_pct": 0.10,
        "min_net_return_pct": 20.0,
        "results_grid_limit": 5000,
        "jobs_refresh_seconds": 2.0,
        "prefer_bbo_maker": True,
        "bbo_queue_level": 1,
        "live_kill_switch_enabled": False,
        "live_trading_enabled": False,
    }
    current = get_settings(conn)
    merged = {**defaults, **current}

    # Backfill missing keys for new installs/upgrades.
    try:
        if "live_trading_enabled" not in current:
            set_setting(conn, "live_trading_enabled", bool(defaults["live_trading_enabled"]))
    except Exception:
        pass

    # Backfill Settings-page keys so they persist across restarts even if the
    # user hasn't opened the Settings page yet.
    for k in (
        "results_grid_limit",
        "jobs_refresh_seconds",
        "prefer_bbo_maker",
        "bbo_queue_level",
    ):
        try:
            if k not in current:
                set_setting(conn, k, defaults[k])
        except Exception:
            pass

    # Backward compatibility: if old fraction-based keys exist, map them to pct defaults when pct is missing.
    for old_key, pct_key in (
        ("fee_spot", "fee_spot_pct"),
        ("fee_perps", "fee_perps_pct"),
        ("fee_stocks", "fee_stocks_pct"),
    ):
        if pct_key not in merged and old_key in current:
            try:
                merged[pct_key] = float(current[old_key]) * 100.0
            except (TypeError, ValueError):
                continue
    return merged


def get_backtest_run_chart_artifacts(
    run_id: str,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[Dict[str, Any]]:
    """Load stored chart artifacts for a run (if present).

    These are stored on `backtest_run_details` to avoid re-running backtests for chart rendering.
    Returns None when not available.
    """

    rid = str(run_id)
    connection = conn or _get_conn()
    if connection.row_factory is None:
        connection.row_factory = sqlite3.Row

    # Guard for legacy DBs that don't have the columns yet.
    try:
        cols = {row[1] for row in connection.execute("PRAGMA table_info(backtest_run_details)").fetchall()}
    except Exception:
        cols = set()
    required = {"equity_curve_json", "equity_timestamps_json", "extra_series_json"}
    if not required.issubset(cols):
        if conn is None:
            connection.close()
        return None

    row = None
    try:
        select_cols = ["equity_curve_json", "equity_timestamps_json", "extra_series_json"]
        for opt in ("candles_json", "params_json", "run_context_json", "computed_metrics_json"):
            if opt in cols:
                select_cols.append(opt)
        row = connection.execute(
            f"SELECT {', '.join(select_cols)} FROM backtest_run_details WHERE run_id = ?",
            (rid,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None

    if conn is None:
        connection.close()

    if row is None:
        return None

    eq = None
    ts = None
    extra = None
    candles = None
    params = None
    run_context = None
    computed_metrics = None
    try:
        eq = json.loads(row["equity_curve_json"]) if row["equity_curve_json"] else None
    except (TypeError, json.JSONDecodeError):
        eq = None
    try:
        ts = json.loads(row["equity_timestamps_json"]) if row["equity_timestamps_json"] else None
    except (TypeError, json.JSONDecodeError):
        ts = None
    try:
        extra = json.loads(row["extra_series_json"]) if row["extra_series_json"] else None
    except (TypeError, json.JSONDecodeError):
        extra = None

    # Optional artifacts
    try:
        if "candles_json" in row.keys():
            candles = json.loads(row["candles_json"]) if row["candles_json"] else None
    except (TypeError, json.JSONDecodeError):
        candles = None
    try:
        if "params_json" in row.keys():
            params = json.loads(row["params_json"]) if row["params_json"] else None
    except (TypeError, json.JSONDecodeError):
        params = None
    try:
        if "run_context_json" in row.keys():
            run_context = json.loads(row["run_context_json"]) if row["run_context_json"] else None
    except (TypeError, json.JSONDecodeError):
        run_context = None
    try:
        if "computed_metrics_json" in row.keys():
            computed_metrics = json.loads(row["computed_metrics_json"]) if row["computed_metrics_json"] else None
    except (TypeError, json.JSONDecodeError):
        computed_metrics = None

    if not isinstance(eq, list) or not eq:
        return None
    if ts is not None and not isinstance(ts, list):
        ts = None
    if extra is not None and not isinstance(extra, dict):
        extra = None

    if candles is not None and not isinstance(candles, list):
        candles = None
    if params is not None and not isinstance(params, dict):
        params = None
    if run_context is not None and not isinstance(run_context, dict):
        run_context = None
    if computed_metrics is not None and not isinstance(computed_metrics, dict):
        computed_metrics = None

    return {
        "equity_curve": eq,
        "equity_timestamps": ts,
        "extra_series": extra or {},
        "candles": candles or [],
        "params": params or {},
        "run_context": run_context or {},
        "computed_metrics": computed_metrics or {},
    }


def set_backtest_run_chart_artifacts(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    equity_curve: Optional[list],
    equity_timestamps: Optional[list],
    extra_series: Optional[dict],
) -> None:
    """Upsert chart artifacts onto backtest_run_details for an existing run."""

    _ensure_schema(conn)
    rid = str(run_id)

    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(backtest_run_details)").fetchall()}
    except Exception:
        cols = set()
    required = {"equity_curve_json", "equity_timestamps_json", "extra_series_json", "updated_at"}
    if not required.issubset(cols):
        return

    now = datetime.now(timezone.utc).isoformat()
    eq_payload = json.dumps(_sanitize_for_json(equity_curve)) if equity_curve is not None else None
    ts_payload = json.dumps(_sanitize_for_json(equity_timestamps)) if equity_timestamps is not None else None
    extra_payload = json.dumps(_sanitize_for_json(extra_series)) if extra_series is not None else None

    conn.execute(
        """
        UPDATE backtest_run_details
        SET equity_curve_json = COALESCE(?, equity_curve_json),
            equity_timestamps_json = COALESCE(?, equity_timestamps_json),
            extra_series_json = COALESCE(?, extra_series_json),
            updated_at = ?
        WHERE run_id = ?
        """,
        (eq_payload, ts_payload, extra_payload, now, rid),
    )
    conn.commit()


def get_setting_bool(conn: sqlite3.Connection, key: str, default: bool = False) -> bool:
    val = get_setting(conn, key, default)
    if isinstance(val, bool):
        return val
    if val is None:
        return bool(default)
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        v = val.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off"}:
            return False
    return bool(default)


def set_setting_bool(conn: sqlite3.Connection, key: str, value: bool) -> None:
    set_setting(conn, key, bool(value))


def create_job(conn: sqlite3.Connection, job_type: str, payload: Dict[str, Any], bot_id: Optional[int] = None) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(payload)
    cur = conn.execute(
        """
        INSERT INTO jobs (job_type, status, payload_json, sweep_id, run_id, bot_id, progress, message, created_at, updated_at)
        VALUES (?, 'queued', ?, NULL, NULL, ?, 0.0, '', ?, ?)
        """,
        (job_type, payload_json, bot_id, now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def create_bot(
    conn: sqlite3.Connection,
    *,
    name: Optional[str],
    exchange_id: Optional[str],
    symbol: Optional[str],
    timeframe: Optional[str],
    config: Any,
    status: str = "created",
    desired_status: str = "running",
    heartbeat_msg: Optional[str] = None,
    last_error: Optional[str] = None,
    user_id: Optional[str] = None,
    credentials_id: Optional[int] = None,
    account_id: Optional[int] = None,
) -> int:
    now = now_utc_iso()
    resolved_user_id = (user_id or "").strip() or DEFAULT_USER_EMAIL
    try:
        get_or_create_user(conn, resolved_user_id)
    except Exception:
        pass
    payload = {
        "name": name,
        "exchange_id": exchange_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "status": status,
        "desired_status": desired_status,
        "config_json": json.dumps(_sanitize_for_json(config) if config is not None else {}),
        "heartbeat_at": now,
        "heartbeat_msg": heartbeat_msg,
        "last_error": last_error,
        "created_at": now,
        "updated_at": now,
        "user_id": resolved_user_id,
        "credentials_id": credentials_id,
        "account_id": account_id,
    }
    bot_cols = {row[1] for row in conn.execute("PRAGMA table_info(bots)").fetchall()}
    if "account_id" in bot_cols:
        cur = conn.execute(
            """
            INSERT INTO bots (
                name, exchange_id, symbol, timeframe,
                status, desired_status, config_json,
                heartbeat_at, heartbeat_msg, last_error,
                created_at, updated_at,
                user_id, credentials_id, account_id
            ) VALUES (
                :name, :exchange_id, :symbol, :timeframe,
                :status, :desired_status, :config_json,
                :heartbeat_at, :heartbeat_msg, :last_error,
                :created_at, :updated_at,
                :user_id, :credentials_id, :account_id
            )
            """,
            payload,
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO bots (
                name, exchange_id, symbol, timeframe,
                status, desired_status, config_json,
                heartbeat_at, heartbeat_msg, last_error,
                created_at, updated_at,
                user_id, credentials_id
            ) VALUES (
                :name, :exchange_id, :symbol, :timeframe,
                :status, :desired_status, :config_json,
                :heartbeat_at, :heartbeat_msg, :last_error,
                :created_at, :updated_at,
                :user_id, :credentials_id
            )
            """,
            payload,
        )
    conn.commit()
    return int(cur.lastrowid)


def list_trading_accounts(
    conn: sqlite3.Connection,
    user_id: str,
    exchange_id: Optional[str] = None,
    *,
    include_deleted: bool = False,
) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    where = ["user_id = ?"]
    params: list[Any] = [uid]
    if exchange_id is not None:
        where.append("exchange_id = ?")
        params.append(str(exchange_id).strip().lower())
    if not include_deleted:
        where.append("status != 'deleted'")
    rows = conn.execute(
        f"SELECT * FROM trading_accounts WHERE {' AND '.join(where)} ORDER BY updated_at DESC, id DESC",
        tuple(params),
    ).fetchall()
    return [dict(r) for r in rows]


def create_trading_account(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    exchange_id: str,
    label: str,
    credential_id: int,
) -> int:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    get_or_create_user(conn, uid)
    now = now_utc_iso()
    ex = str(exchange_id or "").strip().lower()
    lbl = (label or "").strip()
    if not ex:
        raise ValueError("exchange_id is required")
    if not lbl:
        raise ValueError("label is required")
    cred_id = int(credential_id)
    try:
        cur = conn.execute(
            """
            INSERT INTO trading_accounts(
                user_id, exchange_id, label, credential_id, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'active', ?, ?)
            """,
            (uid, ex, lbl, cred_id, now, now),
        )
        conn.commit()
        return int(cur.lastrowid)
    except sqlite3.IntegrityError:
        # Likely UNIQUE(user_id, exchange_id, label)
        row = conn.execute(
            "SELECT id FROM trading_accounts WHERE user_id = ? AND exchange_id = ? AND label = ?",
            (uid, ex, lbl),
        ).fetchone()
        if not row:
            raise
        return int(row[0])


def get_trading_account(conn: sqlite3.Connection, user_id: str, account_id: int) -> Optional[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = conn.execute(
        "SELECT * FROM trading_accounts WHERE user_id = ? AND id = ?",
        (uid, int(account_id)),
    ).fetchone()
    return dict(row) if row else None


def set_trading_account_status(conn: sqlite3.Connection, user_id: str, account_id: int, status: str) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    st = str(status or "").strip().lower()
    if st not in {"active", "disabled", "deleted"}:
        raise ValueError("Invalid status")
    now = now_utc_iso()
    conn.execute(
        "UPDATE trading_accounts SET status = ?, updated_at = ? WHERE user_id = ? AND id = ?",
        (st, now, uid, int(account_id)),
    )
    conn.commit()


def set_trading_account_credential(conn: sqlite3.Connection, user_id: str, account_id: int, credential_id: int) -> None:
    """Rotate/update which credential a trading account points to."""
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    now = now_utc_iso()
    conn.execute(
        "UPDATE trading_accounts SET credential_id = ?, updated_at = ? WHERE user_id = ? AND id = ?",
        (int(credential_id), now, uid, int(account_id)),
    )
    conn.commit()


def count_bots_for_account(conn: sqlite3.Connection, user_id: str, account_id: int) -> int:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = conn.execute(
        "SELECT COUNT(1) FROM bots WHERE user_id = ? AND account_id = ?",
        (uid, int(account_id)),
    ).fetchone()
    try:
        return int(row[0]) if row else 0
    except Exception:
        return 0


def get_bot_context(conn: sqlite3.Connection, user_id: str, bot_id: int) -> Dict[str, Any]:
    """Fetch bot + its trading account (new source of truth).

    Returns {"bot": {...}, "account": {...}|None}.
    """
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = conn.execute(
        """
        SELECT
            b.*, 
            a.id AS a_id,
            a.user_id AS a_user_id,
            a.exchange_id AS a_exchange_id,
            a.label AS a_label,
            a.credential_id AS a_credential_id,
            a.status AS a_status,
            a.risk_block_new_entries AS a_risk_block_new_entries,
            a.risk_max_daily_loss_usd AS a_risk_max_daily_loss_usd,
            a.risk_max_total_loss_usd AS a_risk_max_total_loss_usd,
            a.created_at AS a_created_at,
            a.updated_at AS a_updated_at
        FROM bots b
        LEFT JOIN trading_accounts a ON a.id = b.account_id
        WHERE b.id = ? AND b.user_id = ?
        """,
        (int(bot_id), uid),
    ).fetchone()
    if not row:
        return {"bot": None, "account": None}

    data = dict(row)
    account = None
    if data.get("a_id") is not None:
        account = {
            "id": data.get("a_id"),
            "user_id": data.get("a_user_id"),
            "exchange_id": data.get("a_exchange_id"),
            "label": data.get("a_label"),
            "credential_id": data.get("a_credential_id"),
            "status": data.get("a_status"),
            "risk_block_new_entries": data.get("a_risk_block_new_entries"),
            "risk_max_daily_loss_usd": data.get("a_risk_max_daily_loss_usd"),
            "risk_max_total_loss_usd": data.get("a_risk_max_total_loss_usd"),
            "created_at": data.get("a_created_at"),
            "updated_at": data.get("a_updated_at"),
        }

    for k in [
        "a_id",
        "a_user_id",
        "a_exchange_id",
        "a_label",
        "a_credential_id",
        "a_status",
        "a_created_at",
        "a_updated_at",
        "a_risk_block_new_entries",
        "a_risk_max_daily_loss_usd",
        "a_risk_max_total_loss_usd",
    ]:
        data.pop(k, None)

    return {"bot": data, "account": account}


def create_credential(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    exchange_id: str,
    label: str,
    api_key: str,
    api_secret_plain: str,
) -> int:
    from project_dragon.crypto import encrypt_str

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    get_or_create_user(conn, uid)
    now = now_utc_iso()
    secret_enc = encrypt_str(api_secret_plain)
    cur = conn.execute(
        """
        INSERT INTO exchange_credentials(
            user_id, exchange_id, label, api_key, api_secret_enc,
            created_at, updated_at, last_used_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (uid, (exchange_id or "").strip(), (label or "").strip(), (api_key or "").strip(), secret_enc, now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_trading_account_risk(
    conn: sqlite3.Connection,
    user_id: str,
    account_id: int,
    *,
    risk_block_new_entries: Any = _UNSET,
    risk_max_daily_loss_usd: Any = _UNSET,
    risk_max_total_loss_usd: Any = _UNSET,
) -> None:
    """Update account-level risk settings.

    All fields are optional; only provided fields will be updated.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    fields: Dict[str, Any] = {}

    if risk_block_new_entries is not _UNSET:
        if risk_block_new_entries is not None:
            fields["risk_block_new_entries"] = 1 if bool(risk_block_new_entries) else 0

    if risk_max_daily_loss_usd is not _UNSET:
        if risk_max_daily_loss_usd is None:
            fields["risk_max_daily_loss_usd"] = None
        else:
            fields["risk_max_daily_loss_usd"] = float(risk_max_daily_loss_usd)

    if risk_max_total_loss_usd is not _UNSET:
        if risk_max_total_loss_usd is None:
            fields["risk_max_total_loss_usd"] = None
        else:
            fields["risk_max_total_loss_usd"] = float(risk_max_total_loss_usd)
    if not fields:
        return
    fields["updated_at"] = now_utc_iso()
    sets = ", ".join([f"{k} = ?" for k in fields.keys()])
    params = list(fields.values()) + [uid, int(account_id)]
    conn.execute(
        f"UPDATE trading_accounts SET {sets} WHERE user_id = ? AND id = ?",
        params,
    )
    conn.commit()


def sum_account_net_pnl(
    conn: sqlite3.Connection,
    user_id: str,
    account_id: int,
    *,
    since_ts: Optional[str] = None,
) -> float:
    """Return net PnL for an account across all its bots.

    net = SUM(pnl) + SUM(funding) - SUM(fee)
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    where = ["b.user_id = ?", "b.account_id = ?"]
    params: List[Any] = [uid, int(account_id)]
    if since_ts:
        where.append("l.event_ts >= ?")
        params.append(str(since_ts))
    row = conn.execute(
        f"""
        SELECT
            COALESCE(SUM(l.pnl), 0) + COALESCE(SUM(l.funding), 0) - COALESCE(SUM(l.fee), 0) AS net
        FROM bots b
        JOIN bot_ledger l ON l.bot_id = b.id
        WHERE {' AND '.join(where)}
        """,
        tuple(params),
    ).fetchone()
    try:
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception:
        return 0.0


def request_flatten_all_bots_in_account(
    conn: sqlite3.Connection,
    user_id: str,
    account_id: int,
    *,
    include_stopped: bool = False,
) -> int:
    """Set desired_action='flatten_now' for all bots under an account.

    Returns number of bots updated.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    now = now_utc_iso()
    where = "user_id = ? AND account_id = ?"
    params: List[Any] = [uid, int(account_id)]
    if not include_stopped:
        where += " AND COALESCE(desired_status, 'running') != 'stopped'"
    cur = conn.execute(
        f"UPDATE bots SET desired_action = 'flatten_now', updated_at = ? WHERE {where}",
        tuple([now] + params),
    )
    conn.commit()
    try:
        return int(cur.rowcount or 0)
    except Exception:
        return 0


def list_credentials(conn: sqlite3.Connection, user_id: str) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    rows = conn.execute(
        """
        SELECT id, user_id, exchange_id, label, api_key, created_at, updated_at, last_used_at
        FROM exchange_credentials
        WHERE user_id = ?
        ORDER BY updated_at DESC
        """,
        (uid,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_credential(conn: sqlite3.Connection, user_id: str, cred_id: int) -> Optional[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = conn.execute(
        """
        SELECT * FROM exchange_credentials
        WHERE user_id = ? AND id = ?
        """,
        (uid, int(cred_id)),
    ).fetchone()
    return dict(row) if row else None


def delete_credential(conn: sqlite3.Connection, user_id: str, cred_id: int) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    conn.execute(
        "DELETE FROM exchange_credentials WHERE user_id = ? AND id = ?",
        (uid, int(cred_id)),
    )
    conn.commit()


def touch_credential_last_used(conn: sqlite3.Connection, user_id: str, cred_id: int) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    now = now_utc_iso()
    conn.execute(
        """
        UPDATE exchange_credentials
        SET last_used_at = ?, updated_at = ?
        WHERE user_id = ? AND id = ?
        """,
        (now, now, uid, int(cred_id)),
    )
    conn.commit()


def set_bot_credentials(conn: sqlite3.Connection, bot_id: int, *, user_id: str, credentials_id: Optional[int]) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    # Ensure bots.user_id is set for legacy rows.
    update_bot(conn, int(bot_id), user_id=uid, credentials_id=credentials_id)


def update_job(conn: sqlite3.Connection, job_id: int, **fields: Any) -> None:
    if not fields:
        fields = {}
    now = datetime.now(timezone.utc).isoformat()
    fields = {**fields, "updated_at": now}
    columns = []
    params: list[Any] = []
    for key, value in fields.items():
        columns.append(f"{key} = ?")
        params.append(value)
    params.append(job_id)
    conn.execute(f"UPDATE jobs SET {', '.join(columns)} WHERE id = ?", tuple(params))
    conn.commit()


def list_jobs(conn: sqlite3.Connection, limit: int = 200) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM jobs
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    jobs: List[Dict[str, Any]] = []
    for r in rows:
        data = dict(r)
        if data.get("run_id") is not None:
            data["run_id"] = str(data["run_id"])
        jobs.append(data)
    return jobs


def get_job(conn: sqlite3.Connection, job_id: int) -> Optional[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return None
    data = dict(row)
    if data.get("run_id") is not None:
        data["run_id"] = str(data["run_id"])
    return data


def claim_job(conn: sqlite3.Connection, job_id: int, worker_id: str) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    # Atomic claim: only one worker can transition queued -> running.
    cur = conn.execute(
        """
        UPDATE jobs
        SET status='running', worker_id=?, started_at=COALESCE(started_at, ?), updated_at=?
        WHERE id = ? AND status = 'queued' AND (worker_id IS NULL OR worker_id = '')
        """,
        (worker_id, now, now, job_id),
    )
    conn.commit()
    return cur.rowcount > 0


def claim_job_with_lease(conn: sqlite3.Connection, *, worker_id: str, lease_s: float) -> Optional[Dict[str, Any]]:
    """Claim the next queued live_bot job using a time-based lease.

    This is intentionally conservative and minimal:
    - Only claims `job_type='live_bot'`
    - Transitions `queued` -> `running`
    - Sets both legacy `worker_id` and lease fields (`claimed_by`, `lease_expires_at`, ...)
    """

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row

    lease_seconds = float(lease_s or 0.0)
    if lease_seconds <= 0:
        lease_seconds = 30.0

    # Best-effort retry loop to handle races between multiple workers.
    for _ in range(3):
        row = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE job_type = 'live_bot'
              AND status = 'queued'
            ORDER BY COALESCE(created_at, updated_at) ASC
            LIMIT 1
            """,
        ).fetchone()
        if not row:
            return None

        job_id = int(row[0])
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        lease_expires_iso = (now_dt + timedelta(seconds=lease_seconds)).isoformat()

        cur = conn.execute(
            """
            UPDATE jobs
            SET
              status = 'running',
              worker_id = ?,
              claimed_by = ?,
              claimed_at = ?,
              lease_expires_at = ?,
              last_lease_renew_at = ?,
              lease_version = COALESCE(lease_version, 0) + 1,
              started_at = COALESCE(started_at, ?),
              updated_at = ?
            WHERE id = ?
              AND status = 'queued'
            """,
            (
                str(worker_id),
                str(worker_id),
                now_iso,
                lease_expires_iso,
                now_iso,
                now_iso,
                now_iso,
                job_id,
            ),
        )
        conn.commit()
        if cur.rowcount > 0:
            return get_job(conn, job_id)

    return None


def renew_job_lease(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    lease_s: float,
    expected_lease_version: int,
) -> bool:
    """Renew a running job lease only if we still own it (version + owner check)."""

    lease_seconds = float(lease_s or 0.0)
    if lease_seconds <= 0:
        lease_seconds = 30.0

    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    lease_expires_iso = (now_dt + timedelta(seconds=lease_seconds)).isoformat()

    cur = conn.execute(
        """
        UPDATE jobs
        SET
          lease_expires_at = ?,
          last_lease_renew_at = ?,
          updated_at = ?,
          worker_id = COALESCE(worker_id, ?),
          claimed_by = COALESCE(claimed_by, ?)
        WHERE id = ?
          AND status = 'running'
          AND claimed_by = ?
          AND lease_version = ?
        """,
        (
            lease_expires_iso,
            now_iso,
            now_iso,
            str(worker_id),
            str(worker_id),
            int(job_id),
            str(worker_id),
            int(expected_lease_version),
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def reclaim_stale_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    new_worker_id: str,
    lease_s: float,
) -> bool:
    """Reclaim a stale (lease-expired) running job for this worker."""

    lease_seconds = float(lease_s or 0.0)
    if lease_seconds <= 0:
        lease_seconds = 30.0

    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    lease_expires_iso = (now_dt + timedelta(seconds=lease_seconds)).isoformat()

    # Only reclaim when lease_expires_at < now (ISO strings are comparable in SQLite).
    cur = conn.execute(
        """
        UPDATE jobs
        SET
          worker_id = ?,
          claimed_by = ?,
          claimed_at = ?,
          lease_expires_at = ?,
          last_lease_renew_at = ?,
          lease_version = COALESCE(lease_version, 0) + 1,
          stale_reclaims = COALESCE(stale_reclaims, 0) + 1,
          updated_at = ?
        WHERE id = ?
          AND status = 'running'
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at < ?
        """,
        (
            str(new_worker_id),
            str(new_worker_id),
            now_iso,
            lease_expires_iso,
            now_iso,
            now_iso,
            int(job_id),
            now_iso,
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def update_bot(conn: sqlite3.Connection, bot_id: int, **fields: Any) -> None:
    if not fields:
        return
    now = datetime.now(timezone.utc).isoformat()
    fields = {**fields, "updated_at": now}
    columns = []
    params: list[Any] = []
    for key, value in fields.items():
        columns.append(f"{key} = ?")
        params.append(value)
    params.append(bot_id)
    conn.execute(f"UPDATE bots SET {', '.join(columns)} WHERE id = ?", tuple(params))
    conn.commit()


def set_bot_desired_action(bot_id: int, desired_action: Optional[str]) -> None:
    """Convenience wrapper for Streamlit/UI code.

    Sets `bots.desired_action` and updates `updated_at`.
    """
    with open_db_connection() as conn:
        update_bot(conn, int(bot_id), desired_action=desired_action)


def increment_bot_blocked_actions(bot_id: int, delta: int = 1) -> None:
    """Increment bots.blocked_actions_count by delta (default 1).

    Must update updated_at.
    """
    try:
        delta_int = int(delta)
    except (TypeError, ValueError):
        delta_int = 1
    if delta_int == 0:
        return
    now = datetime.now(timezone.utc).isoformat()
    with open_db_connection() as conn:
        conn.execute(
            "UPDATE bots SET blocked_actions_count = COALESCE(blocked_actions_count, 0) + ?, updated_at = ? WHERE id = ?",
            (delta_int, now, int(bot_id)),
        )
        conn.commit()


def set_bot_status(
    conn: sqlite3.Connection,
    bot_id: int,
    status: str,
    desired_status: Optional[str] = None,
    last_error: Optional[str] = None,
    heartbeat_msg: Optional[str] = None,
    heartbeat_at: Optional[str] = None,
) -> None:
    fields: Dict[str, Any] = {"status": status}
    if desired_status is not None:
        fields["desired_status"] = desired_status
    if last_error is not None:
        fields["last_error"] = last_error
    if heartbeat_msg is not None:
        fields["heartbeat_msg"] = heartbeat_msg
    fields["heartbeat_at"] = heartbeat_at or datetime.now(timezone.utc).isoformat()
    update_bot(conn, bot_id, **fields)


def get_bot(conn: sqlite3.Connection, bot_id: int) -> Optional[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM bots WHERE id = ?", (bot_id,)).fetchone()
    return dict(row) if row else None


def list_bots(conn: sqlite3.Connection, limit: int = 200, status: Optional[str] = None) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    query = "SELECT * FROM bots"
    params: list[Any] = []
    if status is not None:
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]

def list_bots_with_accounts(
    conn: sqlite3.Connection,
    user_id: str,
    *,
    limit: int = 200,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List bots for a user with account label/status joined.

    This is a UI/helper query (bots → trading_accounts) so Live Bots can display account metadata.
    """

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL

    base = (
        "SELECT b.*, a.label AS account_label, a.status AS account_status "
        "FROM bots b "
        "LEFT JOIN trading_accounts a ON a.id = b.account_id "
        "WHERE b.user_id = ?"
    )
    params: List[Any] = [uid]
    if status:
        base += " AND b.status = ?"
        params.append(str(status))
    base += " ORDER BY b.updated_at DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(base, params).fetchall()
    return [dict(r) for r in rows]


def upsert_bot_snapshot(conn: sqlite3.Connection, snapshot: Dict[str, Any]) -> None:
    """Upsert a durable bot snapshot.

    Must be safe to call frequently. Callers should throttle and treat failures as best-effort.
    """

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row

    bot_id = snapshot.get("bot_id")
    if bot_id is None:
        raise ValueError("snapshot missing bot_id")

    fields = {
        "bot_id": int(bot_id),
        "user_id": str(snapshot.get("user_id") or "").strip() or DEFAULT_USER_EMAIL,
        "exchange_id": str(snapshot.get("exchange_id") or "").strip().lower() or "woox",
        "symbol": str(snapshot.get("symbol") or "").strip() or "",
        "timeframe": str(snapshot.get("timeframe") or "").strip() or "",
        "updated_at": str(snapshot.get("updated_at") or now_utc_iso()),
        "worker_id": snapshot.get("worker_id"),
        "status": snapshot.get("status"),
        "desired_status": snapshot.get("desired_status"),
        "desired_action": snapshot.get("desired_action"),
        "next_action": snapshot.get("next_action"),
        "health_status": snapshot.get("health_status"),
        "health_json": snapshot.get("health_json"),
        "pos_status": snapshot.get("pos_status"),
        "positions_summary_json": snapshot.get("positions_summary_json"),
        "open_orders_count": snapshot.get("open_orders_count"),
        "open_intents_count": snapshot.get("open_intents_count"),
        "account_id": snapshot.get("account_id"),
        "risk_blocked": snapshot.get("risk_blocked"),
        "risk_reason": snapshot.get("risk_reason"),
        "last_error": snapshot.get("last_error"),
        "last_event_ts": snapshot.get("last_event_ts"),
        "last_event_level": snapshot.get("last_event_level"),
        "last_event_type": snapshot.get("last_event_type"),
        "last_event_message": snapshot.get("last_event_message"),
        "exchange_state": snapshot.get("exchange_state"),
        "last_exchange_error_at": snapshot.get("last_exchange_error_at"),
    }

    cols = ",".join(fields.keys())
    placeholders = ",".join(["?"] * len(fields))
    update_clause = ",".join([f"{k}=excluded.{k}" for k in fields.keys() if k != "bot_id"])
    conn.execute(
        f"""
        INSERT INTO bot_state_snapshots ({cols})
        VALUES ({placeholders})
        ON CONFLICT(bot_id) DO UPDATE SET {update_clause}
        """,
        tuple(fields.values()),
    )
    conn.commit()


def get_bot_snapshot(conn: sqlite3.Connection, bot_id: int) -> Optional[Dict[str, Any]]:
    """Get a bot row joined with its latest durable snapshot (if present)."""
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row

    row = conn.execute(
        """
        SELECT
            b.*,
            a.label AS account_label,
            a.status AS account_status,
            s.updated_at AS snapshot_updated_at,
            s.worker_id AS snapshot_worker_id,
            s.status AS snapshot_status,
            s.desired_status AS snapshot_desired_status,
            s.desired_action AS snapshot_desired_action,
            s.next_action AS snapshot_next_action,
            s.health_status AS snapshot_health_status,
            s.health_json AS snapshot_health_json,
            s.pos_status AS snapshot_pos_status,
            s.positions_summary_json AS snapshot_positions_summary_json,
            s.open_orders_count AS snapshot_open_orders_count,
            s.open_intents_count AS snapshot_open_intents_count,
            s.risk_blocked AS snapshot_risk_blocked,
            s.risk_reason AS snapshot_risk_reason,
            s.last_error AS snapshot_last_error,
            s.last_event_ts AS snapshot_last_event_ts,
            s.last_event_level AS snapshot_last_event_level,
            s.last_event_type AS snapshot_last_event_type,
            s.last_event_message AS snapshot_last_event_message,
            s.exchange_state AS snapshot_exchange_state,
            s.last_exchange_error_at AS snapshot_last_exchange_error_at
        FROM bots b
        LEFT JOIN trading_accounts a ON a.id = b.account_id
        LEFT JOIN bot_state_snapshots s ON s.bot_id = b.id
        WHERE b.id = ?
        LIMIT 1
        """,
        (int(bot_id),),
    ).fetchone()
    return dict(row) if row else None


def get_bot_snapshots(
    conn: sqlite3.Connection,
    user_id: str,
    *,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 500,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Single-query bot overview for UI.

    Returns bots joined with accounts and latest snapshot fields.
    """

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    _ = filters  # reserved for future SQL WHERE clauses

    base = (
        "SELECT "
        "  b.*, "
        "  a.label AS account_label, a.status AS account_status, "
        "  s.updated_at AS snapshot_updated_at, "
        "  s.worker_id AS snapshot_worker_id, "
        "  s.next_action AS snapshot_next_action, "
        "  s.health_status AS snapshot_health_status, "
        "  s.pos_status AS snapshot_pos_status, "
        "  s.open_orders_count AS snapshot_open_orders_count, "
        "  s.open_intents_count AS snapshot_open_intents_count, "
        "  s.risk_blocked AS snapshot_risk_blocked, "
        "  s.risk_reason AS snapshot_risk_reason, "
        "  s.last_error AS snapshot_last_error, "
        "  s.last_event_ts AS snapshot_last_event_ts, "
        "  s.last_event_level AS snapshot_last_event_level, "
        "  s.last_event_type AS snapshot_last_event_type, "
        "  s.last_event_message AS snapshot_last_event_message, "
        "  s.exchange_state AS snapshot_exchange_state, "
        "  s.last_exchange_error_at AS snapshot_last_exchange_error_at "
        "FROM bots b "
        "LEFT JOIN trading_accounts a ON a.id = b.account_id "
        "LEFT JOIN bot_state_snapshots s ON s.bot_id = b.id "
        "WHERE b.user_id = ?"
    )
    params: List[Any] = [uid]
    if status:
        base += " AND b.status = ?"
        params.append(str(status))
    base += " ORDER BY b.updated_at DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(base, params).fetchall()
    return [dict(r) for r in rows]


def upsert_account_snapshot(conn: sqlite3.Connection, snapshot: Dict[str, Any]) -> None:
    """Upsert a durable trading account snapshot."""

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row

    account_id = snapshot.get("account_id")
    if account_id is None:
        raise ValueError("snapshot missing account_id")

    fields = {
        "account_id": int(account_id),
        "user_id": str(snapshot.get("user_id") or "").strip() or DEFAULT_USER_EMAIL,
        "exchange_id": str(snapshot.get("exchange_id") or "").strip().lower() or "woox",
        "updated_at": str(snapshot.get("updated_at") or now_utc_iso()),
        "worker_id": snapshot.get("worker_id"),
        "status": snapshot.get("status"),
        "risk_blocked": snapshot.get("risk_blocked"),
        "risk_reason": snapshot.get("risk_reason"),
        "positions_summary_json": snapshot.get("positions_summary_json"),
        "open_orders_count": snapshot.get("open_orders_count"),
        "margin_ratio": snapshot.get("margin_ratio"),
        "wallet_balance": snapshot.get("wallet_balance"),
        "last_error": snapshot.get("last_error"),
          "exchange_state": snapshot.get("exchange_state"),
          "last_exchange_error_at": snapshot.get("last_exchange_error_at"),
    }

    cols = ",".join(fields.keys())
    placeholders = ",".join(["?"] * len(fields))
    update_clause = ",".join([f"{k}=excluded.{k}" for k in fields.keys() if k != "account_id"])
    conn.execute(
        f"""
        INSERT INTO account_state_snapshots ({cols})
        VALUES ({placeholders})
        ON CONFLICT(account_id) DO UPDATE SET {update_clause}
        """,
        tuple(fields.values()),
    )
    conn.commit()


def get_account_snapshots(
    conn: sqlite3.Connection,
    user_id: str,
    *,
    include_deleted: bool = True,
    exchange_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Single-query account overview joined with latest durable snapshot."""

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL

    base = (
        "SELECT "
        "  a.*, "
        "  s.updated_at AS snapshot_updated_at, "
        "  s.worker_id AS snapshot_worker_id, "
        "  s.status AS snapshot_status, "
        "  s.risk_blocked AS snapshot_risk_blocked, "
        "  s.risk_reason AS snapshot_risk_reason, "
        "  s.positions_summary_json AS snapshot_positions_summary_json, "
        "  s.open_orders_count AS snapshot_open_orders_count, "
        "  s.margin_ratio AS snapshot_margin_ratio, "
        "  s.wallet_balance AS snapshot_wallet_balance, "
          "  s.last_error AS snapshot_last_error, "
          "  s.exchange_state AS snapshot_exchange_state, "
          "  s.last_exchange_error_at AS snapshot_last_exchange_error_at "
        "FROM trading_accounts a "
        "LEFT JOIN account_state_snapshots s ON s.account_id = a.id "
        "WHERE a.user_id = ?"
    )
    params: List[Any] = [uid]
    if not include_deleted:
        base += " AND COALESCE(NULLIF(TRIM(a.status), ''), 'active') != 'deleted'"
    if exchange_id:
        base += " AND a.exchange_id = ?"
        params.append(str(exchange_id).strip().lower())
    base += " ORDER BY a.updated_at DESC, a.id DESC"
    rows = conn.execute(base, params).fetchall()
    return [dict(r) for r in rows]

def set_job_link(
    conn: sqlite3.Connection,
    job_id: int,
    sweep_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> None:
    fields: Dict[str, Any] = {}
    if sweep_id is not None:
        fields["sweep_id"] = sweep_id
    if run_id is not None:
        fields["run_id"] = str(run_id)
    if not fields:
        return
    update_job(conn, job_id, **fields)


def request_cancel_job(conn: sqlite3.Connection, job_id: int) -> None:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return
    status = row["status"] if isinstance(row, sqlite3.Row) else row[0]
    if status in {"queued", "running"}:
        # Use update_job so updated_at is refreshed.
        update_job(conn, job_id, status="cancel_requested")


def reconcile_inprocess_jobs_after_restart(
    conn: sqlite3.Connection,
    *,
    current_worker_id: str,
    job_types: Sequence[str] = ("backtest", "sweep"),
    min_age_s_unowned_running: float = 10.0,
) -> Dict[str, int]:
    """Reconcile Streamlit in-process jobs after a restart.

    Project Dragon runs backtests/sweeps in-process (ThreadPoolExecutor). When
    Streamlit restarts, any previously-running in-process jobs are stranded in
    DB as status='running' with no worker thread.

    Policy:
    - If a job is `cancel_requested` (queued/running), mark it `cancelled`.
    - If a job is `running` but belongs to a different Streamlit worker_id,
      re-queue it for re-execution.
    - If a job is `running` with no worker_id and it's older than a small
      threshold, treat it as orphaned and re-queue it.

    Returns counts: {"requeued": int, "cancelled": int}
    """

    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row

    types = [str(t).strip() for t in (job_types or []) if str(t).strip()]
    if not types:
        return {"requeued": 0, "cancelled": 0}

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(seconds=float(min_age_s_unowned_running))).isoformat()
    now_iso = now.isoformat()
    placeholders = ",".join(["?"] * len(types))

    # 1) Finalize cancel requests (no worker should keep them alive after restart).
    cur_cancel = conn.execute(
        f"""
        UPDATE jobs
        SET status = 'cancelled', finished_at = COALESCE(finished_at, ?), message = COALESCE(NULLIF(message, ''), 'Cancelled'), updated_at = ?
        WHERE job_type IN ({placeholders})
          AND status = 'cancel_requested'
        """,
        tuple([now_iso, now_iso] + types),
    )

    # 2) Requeue orphaned running jobs.
    cur_requeue = conn.execute(
        f"""
        UPDATE jobs
        SET status = 'queued', worker_id = NULL, progress = 0.0,
            message = 'Resumed after restart', updated_at = ?
        WHERE job_type IN ({placeholders})
          AND status = 'running'
          AND (
            (worker_id LIKE 'streamlit:%' AND worker_id != ?)
            OR ((worker_id IS NULL OR worker_id = '') AND COALESCE(updated_at, created_at) < ?)
          )
        """,
        tuple([now_iso] + types + [str(current_worker_id), cutoff]),
    )

    conn.commit()
    try:
        cancelled = int(cur_cancel.rowcount or 0)
    except Exception:
        cancelled = 0
    try:
        requeued = int(cur_requeue.rowcount or 0)
    except Exception:
        requeued = 0
    return {"requeued": requeued, "cancelled": cancelled}


def link_bot_to_run(conn: sqlite3.Connection, bot_id: int, run_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO bot_run_map (bot_id, run_id, created_at)
        VALUES (?, ?, ?)
        """,
        (bot_id, str(run_id), now),
    )
    conn.commit()


def add_bot_event(
    conn: sqlite3.Connection,
    bot_id: int,
    level: str,
    event_type: str,
    message: str,
    payload: Optional[Dict[str, Any]] = None,
) -> int:
    ts = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(payload or {})
    cur = conn.execute(
        """
        INSERT INTO bot_events (bot_id, ts, level, event_type, message, json_payload)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (bot_id, ts, level, event_type, message, payload_json),
    )
    conn.commit()
    return int(cur.lastrowid)


def add_ledger_row(
    conn: sqlite3.Connection,
    *,
    bot_id: int,
    event_ts: str,
    kind: str,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    position_side: Optional[str] = None,
    qty: Optional[float] = None,
    price: Optional[float] = None,
    fee: Optional[float] = None,
    funding: Optional[float] = None,
    pnl: Optional[float] = None,
    ref_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """Insert a ledger row, idempotent when ref_id is provided.

    Uses INSERT OR IGNORE against the unique index (bot_id, kind, ref_id) where ref_id is not null.
    """
    meta_json = json.dumps(meta or {})
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO bot_ledger (
            bot_id, event_ts, kind, symbol, side, position_side,
            qty, price, fee, funding, pnl, ref_id, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(bot_id),
            str(event_ts),
            str(kind),
            symbol,
            side,
            position_side,
            qty,
            price,
            fee,
            funding,
            pnl,
            ref_id,
            meta_json,
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def list_ledger(conn: sqlite3.Connection, bot_id: int, limit: int = 500) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, bot_id, event_ts, kind, symbol, side, position_side, qty, price, fee, funding, pnl, ref_id, meta_json
        FROM bot_ledger
        WHERE bot_id = ?
        ORDER BY event_ts DESC, id DESC
        LIMIT ?
        """,
        (int(bot_id), int(limit)),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        if data.get("meta_json"):
            try:
                data["meta"] = json.loads(data["meta_json"])
            except (TypeError, json.JSONDecodeError):
                data["meta"] = {}
        out.append(data)
    return out


def sum_ledger(conn: sqlite3.Connection, bot_id: int) -> Dict[str, float]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(COALESCE(fee, 0.0)), 0.0) AS fees_total,
            COALESCE(SUM(COALESCE(funding, 0.0)), 0.0) AS funding_total,
            COALESCE(SUM(COALESCE(pnl, 0.0)), 0.0) AS realized_total
        FROM bot_ledger
        WHERE bot_id = ?
        """,
        (int(bot_id),),
    ).fetchone()
    if not row:
        return {"fees_total": 0.0, "funding_total": 0.0, "realized_total": 0.0}
    return {
        "fees_total": float(row["fees_total"] or 0.0),
        "funding_total": float(row["funding_total"] or 0.0),
        "realized_total": float(row["realized_total"] or 0.0),
    }


def list_bot_events(conn: sqlite3.Connection, bot_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, bot_id, ts, level, event_type, message, json_payload
        FROM bot_events
        WHERE bot_id = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (bot_id, limit),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        if data.get("json_payload"):
            try:
                data["payload"] = json.loads(data.get("json_payload"))
            except json.JSONDecodeError:
                data["payload"] = {}
        out.append(data)
    return out


def add_bot_fill(conn: sqlite3.Connection, fill: Dict[str, Any]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "bot_id": fill.get("bot_id"),
        "run_id": fill.get("run_id"),
        "symbol": fill.get("symbol"),
        "exchange_id": fill.get("exchange_id"),
        "position_side": fill.get("position_side"),
        "order_action": fill.get("order_action"),
        "client_order_id": fill.get("client_order_id"),
        "external_order_id": fill.get("external_order_id"),
        "filled_qty": float(fill.get("filled_qty") or 0.0),
        "avg_fill_price": float(fill.get("avg_fill_price") or 0.0),
        "fee_paid": fill.get("fee_paid"),
        "fee_asset": fill.get("fee_asset"),
        "is_reduce_only": 1 if fill.get("is_reduce_only") else 0,
        "is_dca": 1 if fill.get("is_dca") else 0,
        "note": fill.get("note"),
        "event_ts": fill.get("event_ts") or now,
        "created_at": now,
    }
    cur = conn.execute(
        """
        INSERT INTO bot_fills (
            bot_id, run_id, symbol, exchange_id, position_side, order_action,
            client_order_id, external_order_id, filled_qty, avg_fill_price,
            fee_paid, fee_asset, is_reduce_only, is_dca, note, event_ts, created_at
        ) VALUES (
            :bot_id, :run_id, :symbol, :exchange_id, :position_side, :order_action,
            :client_order_id, :external_order_id, :filled_qty, :avg_fill_price,
            :fee_paid, :fee_asset, :is_reduce_only, :is_dca, :note, :event_ts, :created_at
        )
        """,
        payload,
    )
    conn.commit()
    return int(cur.lastrowid)


def upsert_bot_order_intent(
    conn: sqlite3.Connection,
    *,
    bot_id: int,
    intent_key: str,
    kind: str,
    status: str,
    local_id: int,
    note: Optional[str] = None,
    position_side: Optional[str] = None,
    activation_pct: Optional[float] = None,
    target_price: Optional[float] = None,
    qty: Optional[float] = None,
    reduce_only: bool = False,
    post_only: bool = False,
    client_order_id: Optional[str] = None,
    external_order_id: Optional[str] = None,
    last_error: Optional[str] = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO bot_order_intents(
            bot_id, intent_key, kind, status, local_id, note, position_side,
            activation_pct, target_price, qty, reduce_only, post_only,
            client_order_id, external_order_id, last_error, created_at, updated_at
        ) VALUES (
            :bot_id, :intent_key, :kind, :status, :local_id, :note, :position_side,
            :activation_pct, :target_price, :qty, :reduce_only, :post_only,
            :client_order_id, :external_order_id, :last_error, :created_at, :updated_at
        )
        ON CONFLICT(bot_id, intent_key) DO UPDATE SET
            status=excluded.status,
            local_id=excluded.local_id,
            note=excluded.note,
            position_side=excluded.position_side,
            activation_pct=excluded.activation_pct,
            target_price=excluded.target_price,
            qty=excluded.qty,
            reduce_only=excluded.reduce_only,
            post_only=excluded.post_only,
            client_order_id=COALESCE(excluded.client_order_id, bot_order_intents.client_order_id),
            external_order_id=COALESCE(excluded.external_order_id, bot_order_intents.external_order_id),
            last_error=excluded.last_error,
            updated_at=excluded.updated_at
        """,
        {
            "bot_id": int(bot_id),
            "intent_key": str(intent_key),
            "kind": str(kind),
            "status": str(status),
            "local_id": int(local_id),
            "note": note,
            "position_side": position_side,
            "activation_pct": activation_pct,
            "target_price": target_price,
            "qty": qty,
            "reduce_only": 1 if reduce_only else 0,
            "post_only": 1 if post_only else 0,
            "client_order_id": client_order_id,
            "external_order_id": external_order_id,
            "last_error": last_error,
            "created_at": now,
            "updated_at": now,
        },
    )
    conn.commit()


def list_bot_order_intents(
    conn: sqlite3.Connection,
    bot_id: int,
    *,
    statuses: Optional[List[str]] = None,
    kind: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    clauses = ["bot_id = ?"]
    params: List[Any] = [int(bot_id)]
    if kind:
        clauses.append("kind = ?")
        params.append(str(kind))
    if statuses:
        placeholders = ",".join(["?"] * len(statuses))
        clauses.append(f"status IN ({placeholders})")
        params.extend([str(s) for s in statuses])
    sql = (
        "SELECT * FROM bot_order_intents WHERE "
        + " AND ".join(clauses)
        + " ORDER BY updated_at DESC, id DESC LIMIT ?"
    )
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_bot_order_intent(conn: sqlite3.Connection, bot_id: int, intent_key: str) -> Optional[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM bot_order_intents WHERE bot_id = ? AND intent_key = ?",
        (int(bot_id), str(intent_key)),
    ).fetchone()
    return dict(row) if row else None


def get_bot_order_intent_by_local_id(
    conn: sqlite3.Connection,
    bot_id: int,
    local_id: int,
    *,
    statuses: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    params: List[Any] = [int(bot_id), int(local_id)]
    sql = "SELECT * FROM bot_order_intents WHERE bot_id = ? AND local_id = ?"
    if statuses:
        placeholders = ",".join(["?"] * len(statuses))
        sql += f" AND status IN ({placeholders})"
        params.extend([str(s) for s in statuses])
    sql += " ORDER BY updated_at DESC, id DESC LIMIT 1"
    row = conn.execute(sql, tuple(params)).fetchone()
    return dict(row) if row else None


def update_bot_order_intent(
    conn: sqlite3.Connection,
    *,
    bot_id: int,
    intent_key: str,
    **fields: Any,
) -> None:
    if not fields:
        return
    now = datetime.now(timezone.utc).isoformat()
    fields = dict(fields)
    fields["updated_at"] = now
    sets = ", ".join([f"{k} = :{k}" for k in fields.keys()])
    payload = {"bot_id": int(bot_id), "intent_key": str(intent_key), **fields}
    conn.execute(
        f"UPDATE bot_order_intents SET {sets} WHERE bot_id = :bot_id AND intent_key = :intent_key",
        payload,
    )
    conn.commit()


def list_bot_fills(conn: sqlite3.Connection, bot_id: int, limit: int = 200, since_ts: Optional[str] = None) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    params: List[Any] = [bot_id]
    query = [
        "SELECT * FROM bot_fills WHERE bot_id = ?",
    ]
    if since_ts:
        query.append("AND event_ts >= ?")
        params.append(since_ts)
    query.append("ORDER BY event_ts DESC LIMIT ?")
    params.append(limit)
    rows = conn.execute("\n".join(query), tuple(params)).fetchall()
    return [dict(r) for r in rows]


def create_position_history(conn: sqlite3.Connection, row: Dict[str, Any]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "bot_id": row.get("bot_id"),
        "run_id": row.get("run_id"),
        "symbol": row.get("symbol"),
        "exchange_id": row.get("exchange_id"),
        "position_side": row.get("position_side"),
        "opened_at": row.get("opened_at"),
        "closed_at": row.get("closed_at"),
        "max_size": row.get("max_size"),
        "entry_avg_price": row.get("entry_avg_price"),
        "exit_avg_price": row.get("exit_avg_price"),
        "realized_pnl": row.get("realized_pnl"),
        "fees_paid": row.get("fees_paid"),
        "num_fills": row.get("num_fills"),
        "num_dca_fills": row.get("num_dca_fills"),
        "metadata_json": json.dumps(row.get("metadata", {})),
        "created_at": now,
    }
    cur = conn.execute(
        """
        INSERT INTO bot_positions_history (
            bot_id, run_id, symbol, exchange_id, position_side, opened_at, closed_at,
            max_size, entry_avg_price, exit_avg_price, realized_pnl, fees_paid,
            num_fills, num_dca_fills, metadata_json, created_at
        ) VALUES (
            :bot_id, :run_id, :symbol, :exchange_id, :position_side, :opened_at, :closed_at,
            :max_size, :entry_avg_price, :exit_avg_price, :realized_pnl, :fees_paid,
            :num_fills, :num_dca_fills, :metadata_json, :created_at
        )
        """,
        payload,
    )
    conn.commit()
    return int(cur.lastrowid)


def list_positions_history(conn: sqlite3.Connection, bot_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM bot_positions_history
        WHERE bot_id = ?
        ORDER BY closed_at DESC
        LIMIT ?
        """,
        (bot_id, limit),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        data = dict(r)
        meta_raw = data.get("metadata_json")
        if isinstance(meta_raw, str):
            try:
                data["metadata"] = json.loads(meta_raw)
            except json.JSONDecodeError:
                data["metadata"] = {}
        out.append(data)
    return out


def list_bots_for_run(conn: sqlite3.Connection, run_id: str) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT bot_id, run_id, created_at
        FROM bot_run_map
        WHERE run_id = ?
        ORDER BY created_at DESC
        """,
        (run_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def list_runs_for_bot(conn: sqlite3.Connection, bot_id: int) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT bot_id, run_id, created_at
        FROM bot_run_map
        WHERE bot_id = ?
        ORDER BY created_at DESC
        """,
        (bot_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _ts_to_ms(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, datetime):
        return int(ts.timestamp() * 1000)
    return None


def upsert_candles(
    conn: sqlite3.Connection,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
    candles: List["Candle" | Any],
) -> None:
    if not candles:
        return
    market_type = (market_type or "unknown").lower()
    rows = []
    for c in candles:
        ts_ms = _ts_to_ms(getattr(c, "timestamp", None))
        if ts_ms is None:
            continue
        rows.append(
            (
                exchange_id,
                market_type,
                symbol,
                timeframe,
                ts_ms,
                float(getattr(c, "open", 0.0)),
                float(getattr(c, "high", 0.0)),
                float(getattr(c, "low", 0.0)),
                float(getattr(c, "close", 0.0)),
                float(getattr(c, "volume", 0.0)),
            )
        )
    if not rows:
        return
    conn.executemany(
        """
        INSERT OR REPLACE INTO candles_cache (
            exchange_id, market_type, symbol, timeframe, timestamp_ms,
            open, high, low, close, volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def load_cached_range(
    conn: sqlite3.Connection,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    market_type = (market_type or "unknown").lower()
    start_ms = _ts_to_ms(start_ms)
    end_ms = _ts_to_ms(end_ms)
    params: list[Any] = [exchange_id, market_type, symbol, timeframe]
    conditions = ["exchange_id = ?", "market_type = ?", "symbol = ?", "timeframe = ?"]
    if start_ms is not None:
        conditions.append("timestamp_ms >= ?")
        params.append(int(start_ms))
    if end_ms is not None:
        conditions.append("timestamp_ms <= ?")
        params.append(int(end_ms))
    query = (
        "SELECT timestamp_ms, open, high, low, close, volume FROM candles_cache "
        + " WHERE "
        + " AND ".join(conditions)
        + " ORDER BY timestamp_ms ASC"
    )
    rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_cached_coverage(
    conn: sqlite3.Connection,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
) -> tuple[Optional[int], Optional[int]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    market_type = (market_type or "unknown").lower()
    row = conn.execute(
        """
        SELECT MIN(timestamp_ms) AS min_ts, MAX(timestamp_ms) AS max_ts
        FROM candles_cache
        WHERE exchange_id = ? AND market_type = ? AND symbol = ? AND timeframe = ?
        """,
        (exchange_id, market_type, symbol, timeframe),
    ).fetchone()
    if not row:
        return None, None
    return row["min_ts"], row["max_ts"]


def purge_old_candles(conn: sqlite3.Connection, keep_months: int) -> int:
    if keep_months <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=30 * keep_months)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    cur = conn.execute("DELETE FROM candles_cache WHERE timestamp_ms < ?", (cutoff_ms,))
    conn.commit()
    return cur.rowcount


def list_cached_coverages(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT exchange_id, market_type, symbol, timeframe,
               MIN(timestamp_ms) AS min_ts, MAX(timestamp_ms) AS max_ts, COUNT(*) AS rows_count
        FROM candles_cache
        GROUP BY exchange_id, market_type, symbol, timeframe
        ORDER BY symbol, timeframe
        """
    ).fetchall()
    return [dict(r) for r in rows]


def _extract_initial_entry_notional_usd(config_obj: Any, metadata: Dict[str, Any]) -> Optional[float]:
    """Best-effort: infer initial entry notional from Dragon config + metadata."""

    # Try dict (most common)
    cfg = config_obj
    if is_dataclass(config_obj):
        try:
            cfg = asdict(config_obj)
        except Exception:
            cfg = config_obj
    if not isinstance(cfg, dict):
        return None

    general = cfg.get("general") if isinstance(cfg.get("general"), dict) else {}
    mode_raw = general.get("initial_entry_sizing_mode")
    mode = str(mode_raw).strip().lower() if mode_raw is not None else ""
    if mode in {"fixed_usd", "fixed", "usd"}:
        v = _safe_float(general.get("initial_entry_fixed_usd"))
        return v if (v is not None and v > 0) else None

    # Default: pct balance
    pct = _safe_float(general.get("initial_entry_balance_pct"))
    if pct is None or pct <= 0:
        return None
    initial_balance = _safe_float(metadata.get("initial_balance") or metadata.get("initial_equity"))
    if initial_balance is None or initial_balance <= 0:
        return None
    return initial_balance * (pct / 100.0)

def save_backtest_run(
    config: Any,
    metrics: Dict[str, Any],
    metadata: Dict[str, Any],
    sweep_id: Optional[str] = None,
    result: Optional["BacktestResult"] = None,
) -> str:
    run_id = metadata.get("id") or str(uuid4())
    created_at = metadata.get("created_at") or datetime.now(timezone.utc).isoformat()
    symbol = metadata.get("symbol", "SYNTH")
    timeframe = metadata.get("timeframe", "1m")
    strategy_name = metadata.get("strategy_name", "DragonDcaAtr")
    strategy_version = metadata.get("strategy_version", "0.0.1")

    market_type_raw = metadata.get("market_type") or metadata.get("market") or metadata.get("market_type_hint")
    market_type = (str(market_type_raw).strip().lower() if market_type_raw is not None else "")
    if market_type in {"perp", "perps", "futures"}:
        market_type = "perps"
    elif market_type == "spot":
        market_type = "spot"
    elif not market_type:
        # Best-effort fallback: some callers store it in config payload.
        try:
            if isinstance(config, dict):
                ds = config.get("data_settings") if isinstance(config.get("data_settings"), dict) else None
                market_type = (str((ds or {}).get("market_type") or config.get("market_type") or "").strip().lower())
        except Exception:
            market_type = ""

    metrics_source = (result.metrics if result is not None else metrics) or {}
    start_dt = getattr(result, "start_time", None) if result is not None else metadata.get("start_time")
    end_dt = getattr(result, "end_time", None) if result is not None else metadata.get("end_time")

    # Best-effort: ROI% on margin for futures/perps runs.
    # Stored as a percent value (e.g., 12.34 means +12.34%).
    roi_pct_on_margin: Optional[float] = None
    try:
        from project_dragon.metrics import compute_max_drawdown, compute_roi_pct_on_margin, get_effective_leverage

        lev = get_effective_leverage(config) or get_effective_leverage(metadata)
        notional = _extract_initial_entry_notional_usd(config, metadata)
        net_pnl = _safe_float(metrics_source.get("net_profit"))
        roi_pct_on_margin = compute_roi_pct_on_margin(net_pnl=net_pnl, notional=notional, leverage=lev)

        # Max drawdown: compute directly from the mark-to-market equity curve.
        # This corresponds to unrealized PnL (uPnL) based equity:
        # LONG:  uPnL = (Pmark - Pentry) * qty
        # SHORT: uPnL = (Pentry - Pmark) * qty
        # with equity[t] = initial_balance + realized_pnl[t] + sum(uPnL[t]).
        equity_curve = getattr(result, "equity_curve", None) if result is not None else None
        if isinstance(equity_curve, list) and equity_curve:
            try:
                dd_abs_e, dd_pct_e = compute_max_drawdown([float(e) for e in equity_curve])
                if isinstance(metrics_source, dict):
                    metrics_source = dict(metrics_source)
                    metrics_source["max_drawdown_abs"] = float(dd_abs_e)
                    metrics_source["max_drawdown_pct"] = float(dd_pct_e)
                    metrics_source["max_drawdown"] = float(dd_pct_e)
            except Exception:
                pass

        if isinstance(metrics_source, dict) and roi_pct_on_margin is not None:
            metrics_source = dict(metrics_source)
            metrics_source["roi_pct_on_margin"] = roi_pct_on_margin
    except Exception:
        roi_pct_on_margin = None

    start_time = _to_iso8601(start_dt)
    end_time = _to_iso8601(end_dt)

    config_payload = json.dumps(_sanitize_for_json(config))
    metrics_payload = json.dumps(_sanitize_for_json(metrics_source))
    metadata_payload = json.dumps(_sanitize_for_json(metadata))

    trades_json: Optional[str] = None
    if result is not None and getattr(result, "trades", None) is not None:
        trades_json = json.dumps([t.to_dict() if hasattr(t, "to_dict") else _sanitize_for_json(t) for t in result.trades])

    # Artifacts (enables Run-page rendering + analysis without re-running).
    equity_curve_json: Optional[str] = None
    equity_timestamps_json: Optional[str] = None
    extra_series_json: Optional[str] = None
    candles_json: Optional[str] = None
    params_json: Optional[str] = None
    run_context_json: Optional[str] = None
    computed_metrics_json: Optional[str] = None
    if result is not None:
        try:
            eq = getattr(result, "equity_curve", None)
            if isinstance(eq, list) and eq:
                equity_curve_json = json.dumps(_sanitize_for_json(eq))
        except Exception:
            equity_curve_json = None
        try:
            # Prefer explicit equity_timestamps if present; otherwise fall back to candle timestamps.
            eq_ts = getattr(result, "equity_timestamps", None)
            if isinstance(eq_ts, list) and eq_ts:
                ts_list = []
                for ts in eq_ts:
                    if ts is None:
                        ts_list.append(None)
                    elif hasattr(ts, "isoformat"):
                        ts_list.append(ts.isoformat())
                    else:
                        ts_list.append(str(ts))
                equity_timestamps_json = json.dumps(_sanitize_for_json(ts_list))
            else:
                candles = getattr(result, "candles", None)
                if isinstance(candles, list) and candles:
                    ts_list = []
                    for c in candles:
                        ts = getattr(c, "timestamp", None)
                        if ts is None:
                            ts_list.append(None)
                        elif hasattr(ts, "isoformat"):
                            ts_list.append(ts.isoformat())
                        else:
                            ts_list.append(str(ts))
                    equity_timestamps_json = json.dumps(_sanitize_for_json(ts_list))
        except Exception:
            equity_timestamps_json = None
        try:
            extra = getattr(result, "extra_series", None)
            if isinstance(extra, dict) and extra:
                extra_series_json = json.dumps(_sanitize_for_json(extra))
        except Exception:
            extra_series_json = None
        try:
            candles = getattr(result, "candles", None)
            if isinstance(candles, list) and candles:
                packed = []
                for c in candles:
                    ts = getattr(c, "timestamp", None)
                    if ts is None:
                        ts_s = None
                    elif hasattr(ts, "isoformat"):
                        ts_s = ts.isoformat()
                    else:
                        ts_s = str(ts)
                    packed.append(
                        [
                            ts_s,
                            _safe_float(getattr(c, "open", None)),
                            _safe_float(getattr(c, "high", None)),
                            _safe_float(getattr(c, "low", None)),
                            _safe_float(getattr(c, "close", None)),
                            _safe_float(getattr(c, "volume", None)),
                        ]
                    )
                candles_json = json.dumps(_sanitize_for_json(packed))
        except Exception:
            candles_json = None

        try:
            params = getattr(result, "params", None)
            if isinstance(params, dict) and params:
                params_json = json.dumps(_sanitize_for_json(params))
        except Exception:
            params_json = None
        try:
            rc = getattr(result, "run_context", None)
            if isinstance(rc, dict) and rc:
                run_context_json = json.dumps(_sanitize_for_json(rc))
        except Exception:
            run_context_json = None
        try:
            cm = getattr(result, "computed_metrics", None)
            if isinstance(cm, dict) and cm:
                computed_metrics_json = json.dumps(_sanitize_for_json(cm))
        except Exception:
            computed_metrics_json = None

    payload = {
        "id": run_id,
        "created_at": created_at,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_name": strategy_name,
        "strategy_version": strategy_version,
        "config_json": config_payload,
        "metrics_json": metrics_payload,
        "metadata_json": metadata_payload,
        "sweep_id": sweep_id,
        "market_type": market_type or None,
        "start_time": start_time,
        "end_time": end_time,
        "net_profit": _safe_float(metrics_source.get("net_profit")),
        "net_return_pct": _safe_float(
            metrics_source.get("net_return_pct", metrics_source.get("return_pct"))
        ),
        "roi_pct_on_margin": _safe_float(metrics_source.get("roi_pct_on_margin", roi_pct_on_margin)),
        "max_drawdown_pct": _safe_float(
            metrics_source.get("max_drawdown_pct", metrics_source.get("max_drawdown"))
        ),
        "sharpe": _safe_float(metrics_source.get("sharpe", metrics_source.get("sharpe_ratio"))),
        "sortino": _safe_float(metrics_source.get("sortino", metrics_source.get("sortino_ratio"))),
        "win_rate": _safe_float(metrics_source.get("win_rate")),
        "profit_factor": _safe_float(metrics_source.get("profit_factor")),
        "cpc_index": _safe_float(metrics_source.get("cpc_index")),
        "common_sense_ratio": _safe_float(metrics_source.get("common_sense_ratio")),
        "avg_position_time_s": _safe_float(metrics_source.get("avg_position_time_s")),
        "trades_json": trades_json,
    }

    with _get_conn() as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO backtest_runs (
                id, created_at, symbol, timeframe,
                strategy_name, strategy_version,
                config_json, metrics_json, metadata_json,
                market_type,
                start_time, end_time,
                net_profit, net_return_pct, roi_pct_on_margin, max_drawdown_pct,
                sharpe, sortino, win_rate, profit_factor,
                cpc_index, common_sense_ratio, avg_position_time_s, trades_json,
                sweep_id
            )
            VALUES (
                :id, :created_at, :symbol, :timeframe,
                :strategy_name, :strategy_version,
                :config_json, :metrics_json, :metadata_json,
                :market_type,
                :start_time, :end_time,
                :net_profit, :net_return_pct, :roi_pct_on_margin, :max_drawdown_pct,
                :sharpe, :sortino, :win_rate, :profit_factor,
                :cpc_index, :common_sense_ratio, :avg_position_time_s, :trades_json,
                :sweep_id
            )
            """,
            payload,
        )

        # Keep details table in sync for newer DBs. This enables summary-only list queries.
        try:
            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO backtest_run_details(
                        run_id, config_json, metrics_json, metadata_json, trades_json,
                        equity_curve_json, equity_timestamps_json, extra_series_json,
                        candles_json, params_json, run_context_json, computed_metrics_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        config_payload,
                        metrics_payload,
                        metadata_payload,
                        trades_json,
                        equity_curve_json,
                        equity_timestamps_json,
                        extra_series_json,
                        candles_json,
                        params_json,
                        run_context_json,
                        computed_metrics_json,
                        created_at,
                        created_at,
                    ),
                )
            except sqlite3.OperationalError:
                # Legacy DB without the new artifact columns.
                conn.execute(
                    """
                    INSERT OR REPLACE INTO backtest_run_details(
                        run_id, config_json, metrics_json, metadata_json, trades_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        config_payload,
                        metrics_payload,
                        metadata_payload,
                        trades_json,
                        created_at,
                        created_at,
                    ),
                )
        except sqlite3.OperationalError as e:
            # Allow legacy DBs that haven't been migrated yet.
            # Otherwise, surface the error so the caller gets an atomic rollback.
            msg = str(e).lower()
            if "no such table: backtest_run_details" in msg:
                pass
            else:
                raise
    return run_id


def count_backtest_runs_missing_details(conn: sqlite3.Connection) -> int:
    """Count backtest_runs that don't have a matching backtest_run_details row."""

    row = conn.execute(
        """
        SELECT COUNT(1)
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        """
    ).fetchone()
    try:
        return int(row[0]) if row else 0
    except Exception:
        return 0


def sample_backtest_runs_missing_details(conn: sqlite3.Connection, limit: int = 5) -> List[str]:
    """Return up to `limit` run IDs missing details rows."""

    lim = max(1, int(limit))
    rows = conn.execute(
        """
        SELECT r.id
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        ORDER BY r.created_at DESC
        LIMIT ?
        """,
        (lim,),
    ).fetchall()
    out: List[str] = []
    for r in rows or []:
        try:
            out.append(str(r[0]))
        except Exception:
            continue
    return out


def load_backtest_run_summaries(
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> List[Dict[str, Any]]:
    """Load summary rows for list views (no blob JSON parsing).

    Returns only index-friendly columns from backtest_runs.
    """

    query = """
         SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version,
             start_time, end_time, net_profit, net_return_pct, roi_pct_on_margin, max_drawdown_pct,
             sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
             sweep_id
        FROM backtest_runs
    """
    where_clauses: list[str] = []
    params_list: list[Any] = []
    filters = filters or {}
    min_ret = filters.get("min_net_return_pct")
    if min_ret is not None:
        where_clauses.append("net_return_pct IS NOT NULL AND net_return_pct >= ?")
        params_list.append(min_ret)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY created_at DESC"

    if limit is not None:
        query += " LIMIT ?"
        params_list.append(limit)

    connection = conn or _get_conn()
    if connection.row_factory is None:
        connection.row_factory = sqlite3.Row
    with profile_span(
        "db.load_backtest_run_summaries",
        meta={"limit": limit if limit is not None else None, "has_filters": bool(filters)},
    ):
        rows = connection.execute(query, tuple(params_list)).fetchall()
    if conn is None:
        connection.close()
    return [dict(r) for r in rows]


def get_backtest_run_details(
    run_id: str,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[Dict[str, Any]]:
    """Load the large JSON payloads for a run.

    Prefers backtest_run_details; falls back to legacy columns in backtest_runs.
    """

    rid = str(run_id)
    connection = conn or _get_conn()
    if connection.row_factory is None:
        connection.row_factory = sqlite3.Row

    row = None
    try:
        with profile_span("db.get_backtest_run_details", meta={"run_id": rid}):
            row = connection.execute(
                """
                SELECT run_id, config_json, metrics_json, metadata_json, trades_json
                FROM backtest_run_details
                WHERE run_id = ?
                """,
                (rid,),
            ).fetchone()
    except sqlite3.OperationalError:
        row = None

    if row is None:
        # Legacy fallback.
        with profile_span("db.get_backtest_run_details_legacy", meta={"run_id": rid}):
            row = connection.execute(
                """
                SELECT id AS run_id, config_json, metrics_json, metadata_json, trades_json
                FROM backtest_runs
                WHERE id = ?
                """,
                (rid,),
            ).fetchone()

    if conn is None:
        connection.close()

    if row is None:
        return None

    config_obj: Any = {}
    metrics_obj: Any = {}
    metadata_obj: Any = {}
    try:
        config_obj = json.loads(row["config_json"]) if row["config_json"] else {}
    except (TypeError, json.JSONDecodeError):
        config_obj = {}
    try:
        metrics_obj = json.loads(row["metrics_json"]) if row["metrics_json"] else {}
    except (TypeError, json.JSONDecodeError):
        metrics_obj = {}
    try:
        metadata_obj = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
    except (TypeError, json.JSONDecodeError):
        metadata_obj = {}

    return {
        "run_id": row["run_id"],
        "config": config_obj,
        "metrics": metrics_obj,
        "metadata": metadata_obj,
        "trades_json": row["trades_json"],
    }


def load_backtest_runs(
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> List[Dict[str, Any]]:
    query = """
         SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version,
             config_json, metrics_json, metadata_json,
             start_time, end_time, net_profit, net_return_pct, roi_pct_on_margin, max_drawdown_pct,
             sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
             trades_json, sweep_id
        FROM backtest_runs
    """
    where_clauses: list[str] = []
    params_list: list[Any] = []
    filters = filters or {}
    min_ret = filters.get("min_net_return_pct")
    if min_ret is not None:
        where_clauses.append("net_return_pct IS NOT NULL AND net_return_pct >= ?")
        params_list.append(min_ret)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY created_at DESC"

    if limit is not None:
        query += " LIMIT ?"
        params_list.append(limit)

    connection = conn or _get_conn()
    with profile_span(
        "db.load_backtest_runs",
        meta={"limit": limit if limit is not None else None, "has_filters": bool(filters)},
    ):
        rows = connection.execute(query, tuple(params_list)).fetchall()
    if conn is None:
        connection.close()

    return [
        {
            "id": row["id"],
            "created_at": row["created_at"],
            "symbol": row["symbol"],
            "timeframe": row["timeframe"],
            "strategy_name": row["strategy_name"],
            "strategy_version": row["strategy_version"],
            "config": json.loads(row["config_json"]),
            "metrics": json.loads(row["metrics_json"]),
            "metadata": json.loads(row["metadata_json"]) if row["metadata_json"] else {},
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "net_profit": row["net_profit"],
            "net_return_pct": row["net_return_pct"],
            "roi_pct_on_margin": row["roi_pct_on_margin"],
            "max_drawdown_pct": row["max_drawdown_pct"],
            "sharpe": row["sharpe"],
            "sortino": row["sortino"],
            "win_rate": row["win_rate"],
            "profit_factor": row["profit_factor"],
            "cpc_index": row["cpc_index"],
            "common_sense_ratio": row["common_sense_ratio"],
            "trades_json": row["trades_json"],
            "sweep_id": row["sweep_id"],
        }
        for row in rows
    ]


def get_user_run_shortlist_map(user_id: str) -> Dict[str, Dict[str, Any]]:
    """Return mapping run_id -> {shortlisted, note, updated_at} for a user."""

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT run_id, shortlisted, note, updated_at
                FROM run_shortlists
                WHERE user_id = ? AND shortlisted = 1
                ORDER BY updated_at DESC
                """,
                (uid,),
            ).fetchall()
        except sqlite3.OperationalError:
            return {}
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        rid = str(r["run_id"]) if isinstance(r, sqlite3.Row) else str(r[0])
        out[rid] = {
            "shortlisted": bool(r["shortlisted"] if isinstance(r, sqlite3.Row) else r[1]),
            "note": (r["note"] if isinstance(r, sqlite3.Row) else r[2]) or "",
            "updated_at": (r["updated_at"] if isinstance(r, sqlite3.Row) else r[3]),
        }
    return out


def set_user_run_shortlists(
    user_id: str,
    run_id_to_shortlisted: Mapping[str, bool],
    *,
    run_id_to_note: Optional[Mapping[str, str]] = None,
) -> None:
    """Bulk upsert run shortlist rows for a user.

    Does not delete rows; sets shortlisted to 0 when false.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    run_id_to_note = run_id_to_note or {}
    now = now_utc_iso()
    rows: List[Tuple[Any, ...]] = []
    for rid_raw, flag in run_id_to_shortlisted.items():
        rid = (str(rid_raw or "").strip())
        if not rid:
            continue
        note = run_id_to_note.get(rid, "")
        note_s = "" if note is None else str(note)
        rows.append((uid, rid, 1 if bool(flag) else 0, note_s, now))
    if not rows:
        return

    with _get_conn() as conn:
        _ensure_schema(conn)
        try:
            conn.executemany(
                """
                INSERT INTO run_shortlists(user_id, run_id, shortlisted, note, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, run_id) DO UPDATE SET
                    shortlisted = excluded.shortlisted,
                    note = excluded.note,
                    updated_at = excluded.updated_at
                """,
                rows,
            )
        except sqlite3.OperationalError:
            # Table may not exist on legacy DBs.
            return


def load_backtest_runs_explorer_rows(
    *,
    user_id: Optional[str] = None,
    limit: int = 2000,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Load rows for the Runs Explorer grid.

    - Uses stored `backtest_runs.market_type` (never derived from symbol).
    - Joins `symbols.icon_uri` by exact symbol match.
    - Joins per-user shortlist (optional).
    """

    lim = max(1, min(int(limit or 2000), 10000))
    uid = (str(user_id).strip() if user_id is not None else "")
    filters = filters or {}

    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row

        shortlist_join = ""
        shortlist_cols = "0 AS shortlisted, '' AS shortlist_note"
        filter_params: List[Any] = []
        params: List[Any] = []
        if uid:
            shortlist_join = "LEFT JOIN run_shortlists rs ON rs.run_id = r.id AND rs.user_id = ?"
            shortlist_cols = "COALESCE(rs.shortlisted, 0) AS shortlisted, COALESCE(rs.note, '') AS shortlist_note"
            params.append(uid)

        where_clauses: list[str] = []
        if filters.get("sweep_id") is not None:
            where_clauses.append("r.sweep_id = ?")
            filter_params.append(filters.get("sweep_id"))
        if filters.get("min_net_return_pct") is not None:
            where_clauses.append("r.net_return_pct IS NOT NULL AND r.net_return_pct >= ?")
            filter_params.append(filters.get("min_net_return_pct"))

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Ensure symbol rows exist for the symbols we are about to display.
        # Doing a full-table scan each time makes the Results page feel very slow.
        try:
            sym_rows = conn.execute(
                f"SELECT r.symbol FROM backtest_runs r{where_sql} ORDER BY r.created_at DESC LIMIT ?",
                tuple(list(filter_params) + [lim]),
            ).fetchall()
            symbols = []
            for r in sym_rows or []:
                try:
                    v = r[0]
                except Exception:
                    v = None
                s = ("" if v is None else str(v)).strip()
                if s:
                    symbols.append(s)
            if symbols:
                ensure_symbols_for_exchange_symbols(conn, list(dict.fromkeys(symbols)))
        except Exception:
            pass

        params.extend(filter_params)

        sql = f"""
            SELECT
                r.id AS run_id,
                r.created_at,
                r.symbol,
                r.timeframe,
                r.strategy_name,
                r.strategy_version,
                r.market_type,
                r.config_json,
                r.start_time,
                r.end_time,
                r.net_profit,
                r.net_return_pct,
                r.roi_pct_on_margin,
                r.max_drawdown_pct,
                r.sharpe,
                r.sortino,
                r.win_rate,
                r.profit_factor,
                r.cpc_index,
                r.common_sense_ratio,
                r.avg_position_time_s,
                r.sweep_id,
                sw.name AS sweep_name,
                sw.exchange_id AS sweep_exchange_id,
                sw.sweep_scope AS sweep_scope,
                sw.sweep_assets_json AS sweep_assets_json,
                sw.sweep_category_id AS sweep_category_id,
                s.base_asset,
                s.quote_asset,
                s.icon_uri,
                {shortlist_cols}
            FROM backtest_runs r
            LEFT JOIN sweeps sw ON sw.id = r.sweep_id
            LEFT JOIN symbols s ON s.exchange_symbol = r.symbol
            {shortlist_join}
            {where_sql}
            ORDER BY r.created_at DESC
            LIMIT ?
        """
        params.append(lim)

        rows = conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]


def list_backtest_run_strategies(*, limit: int = 5000) -> List[str]:
    """List distinct strategy_name values seen in backtest_runs.

    This is intended for UI filter dropdowns.
    """

    lim = max(1, min(int(limit or 5000), 50000))
    with _get_conn() as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT DISTINCT strategy_name FROM backtest_runs WHERE strategy_name IS NOT NULL ORDER BY strategy_name LIMIT ?",
            (lim,),
        ).fetchall()
    out: List[str] = []
    for r in rows or []:
        try:
            s = str(r[0] or "").strip()
        except Exception:
            s = ""
        if s:
            out.append(s)
    return out


def get_runs_explorer_rows(
    user_id: str,
    filters: Dict[str, Any] | None,
    *,
    limit: int = 500,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """Runs Explorer read API.

    Returns (rows, total_count).

    - Uses a single SQL query for rows+total_count via a window function.
    - No per-row DB calls.
    - Returns config_json and metrics_json (preferring backtest_run_details when available).

    Filters (all optional):
    - strategy_name: str
    - symbols: list[str]
    - sweep_id: str|int
    - created_from: ISO date/time string (inclusive)
    - created_to: ISO date/time string (exclusive)
    """

    _ = (user_id or "").strip()  # reserved for future per-user scoping
    lim = max(1, min(int(limit or 500), 5000))
    off = max(0, int(offset or 0))
    filters = filters or {}

    where: List[str] = []
    params: List[Any] = []

    strategy_name = filters.get("strategy_name")
    if strategy_name:
        where.append("r.strategy_name = ?")
        params.append(str(strategy_name))

    sweep_id = filters.get("sweep_id")
    if sweep_id is not None and str(sweep_id).strip() != "":
        where.append("r.sweep_id = ?")
        params.append(sweep_id)

    created_from = filters.get("created_from")
    if created_from:
        where.append("r.created_at >= ?")
        params.append(str(created_from))

    created_to = filters.get("created_to")
    if created_to:
        where.append("r.created_at < ?")
        params.append(str(created_to))

    symbols = filters.get("symbols")
    if isinstance(symbols, list) and symbols:
        syms = [str(s).strip() for s in symbols if str(s).strip()]
        if syms:
            where.append("r.symbol IN (" + ",".join(["?"] * len(syms)) + ")")
            params.extend(syms)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT
            r.id AS run_id,
            r.created_at,
            r.start_time,
            r.end_time,
            r.symbol,
            r.timeframe,
            r.sweep_id,
            r.strategy_name,
            r.strategy_version,
            r.market_type,
            r.net_profit,
            r.net_return_pct,
            r.roi_pct_on_margin,
            r.max_drawdown_pct,
            r.sharpe,
            r.sortino,
            r.win_rate,
            r.profit_factor,
            r.cpc_index,
            r.common_sense_ratio,
            COALESCE(d.config_json, r.config_json) AS config_json,
            COALESCE(d.metrics_json, r.metrics_json) AS metrics_json,
            COALESCE(d.metadata_json, r.metadata_json) AS metadata_json,
            sw.name AS sweep_name,
            sw.exchange_id AS sweep_exchange_id,
            s.base_asset,
            s.quote_asset,
            s.icon_uri,
            COUNT(1) OVER() AS total_count
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        LEFT JOIN sweeps sw ON sw.id = r.sweep_id
        LEFT JOIN symbols s ON s.exchange_symbol = r.symbol
        {where_sql}
        ORDER BY r.created_at DESC
        LIMIT ? OFFSET ?
    """
    params2 = list(params)
    params2.extend([lim, off])

    with _get_conn() as conn:
        _ensure_schema(conn)
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row
        with profile_span(
            "db.get_runs_explorer_rows",
            meta={
                "limit": lim,
                "offset": off,
                "has_filters": bool(filters),
            },
        ):
            rows = conn.execute(sql, tuple(params2)).fetchall()

    if not rows:
        return [], 0

    total = 0
    try:
        total = int(rows[0]["total_count"]) if isinstance(rows[0], sqlite3.Row) else int(rows[0]["total_count"])
    except Exception:
        try:
            total = int(dict(rows[0]).get("total_count") or 0)
        except Exception:
            total = 0

    out: List[Dict[str, Any]] = []
    for r in rows:
        drow = dict(r)
        drow.pop("total_count", None)
        out.append(drow)
    return out, total


def get_backtest_run(run_id: str) -> Optional[Dict[str, Any]]:
    with _get_conn() as conn:
        row = conn.execute(
            """
                 SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version,
                     start_time, end_time, net_profit, net_return_pct, max_drawdown_pct,
                     sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
                     sweep_id
            FROM backtest_runs
            WHERE id = ?
            """,
            (run_id,),
        ).fetchone()

        if row is None:
            return None

        details = get_backtest_run_details(str(run_id), conn=conn) or {
            "config": {},
            "metrics": {},
            "metadata": {},
            "trades_json": None,
        }

        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "symbol": row["symbol"],
            "timeframe": row["timeframe"],
            "strategy_name": row["strategy_name"],
            "strategy_version": row["strategy_version"],
            "config": details.get("config") or {},
            "metrics": details.get("metrics") or {},
            "metadata": details.get("metadata") or {},
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "net_profit": row["net_profit"],
            "net_return_pct": row["net_return_pct"],
            "max_drawdown_pct": row["max_drawdown_pct"],
            "sharpe": row["sharpe"],
            "sortino": row["sortino"],
            "win_rate": row["win_rate"],
            "profit_factor": row["profit_factor"],
            "cpc_index": row["cpc_index"],
            "common_sense_ratio": row["common_sense_ratio"],
            "trades_json": details.get("trades_json"),
            "sweep_id": row["sweep_id"],
        }


def _sanitize_for_json(value: Any) -> Any:
    if is_dataclass(value):
        return _sanitize_for_json(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _to_iso8601(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    return None
