from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import logging
import os
from typing import Any, Callable, Dict, Mapping, Optional

from project_dragon.time_utils import fmt_date, fmt_dt, fmt_dt_short
from project_dragon.ui_formatters import format_duration_dhm
from project_dragon.storage import execute_fetchone, get_app_settings, get_setting, set_setting


@dataclass(frozen=False)
class UIFormatters:
    format_duration_dhm: Callable[[Optional[float | int]], str]
    fmt_date: Callable[..., Any]
    fmt_dt: Callable[..., Any]
    fmt_dt_short: Callable[..., Any]


class DbConnFactory:
    """Lightweight connection factory + context manager.

    Pages can call ctx.conn() to open a new connection, or use:
        with ctx.conn as conn:
            ...
    """

    def __init__(self, factory: Callable[..., Any]) -> None:
        self._factory = factory
        self._active_conn: Optional[Any] = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._factory(*args, **kwargs)

    def __enter__(self) -> Any:
        self._active_conn = self._factory()
        return self._active_conn

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        conn = self._active_conn
        self._active_conn = None
        if conn is None:
            return
        try:
            if hasattr(conn, "close"):
                conn.close()
        except Exception:
            pass


class AssetRegistry:
    """Best-effort asset metadata lookup (cached)."""

    def __init__(self, conn_factory: Callable[..., Any], logger: logging.Logger) -> None:
        self._conn_factory = conn_factory
        self._logger = logger

    @lru_cache(maxsize=4096)
    def get(self, exchange_symbol: str) -> Dict[str, Any]:
        symbol = (exchange_symbol or "").strip()
        if not symbol:
            return {}
        try:
            with self._conn_factory() as conn:
                row = execute_fetchone(
                    conn,
                    """
                    SELECT exchange_symbol, base_asset, quote_asset, market_type, icon_uri
                    FROM symbols
                    WHERE exchange_symbol = %s
                    LIMIT 1
                    """,
                    (symbol,),
                )
                return dict(row) if row else {}
        except Exception:
            self._logger.debug("asset_registry_lookup_failed", exc_info=True)
            return {}


class IconRegistry:
    """Resolve asset icon URIs from DB or local icon pack (cached)."""

    def __init__(self, conn_factory: Callable[..., Any], logger: logging.Logger) -> None:
        self._conn_factory = conn_factory
        self._logger = logger

    @lru_cache(maxsize=4096)
    def resolve(self, symbol: str) -> str:
        asset_key = _normalize_symbol_to_asset_key(symbol)
        if not asset_key:
            return ""
        try:
            with self._conn_factory() as conn:
                row = execute_fetchone(
                    conn,
                    """
                    SELECT icon_uri
                    FROM symbols
                    WHERE base_asset = %s
                      AND icon_uri IS NOT NULL
                      AND TRIM(icon_uri) != ''
                    LIMIT 1
                    """,
                    (asset_key,),
                )
                if row and row.get("icon_uri"):
                    return str(row.get("icon_uri") or "").strip()

                row = execute_fetchone(
                    conn,
                    """
                    SELECT icon_uri
                    FROM symbols
                    WHERE exchange_symbol = %s
                      AND icon_uri IS NOT NULL
                      AND TRIM(icon_uri) != ''
                    LIMIT 1
                    """,
                    (str(symbol or "").strip(),),
                )
                if row and row.get("icon_uri"):
                    return str(row.get("icon_uri") or "").strip()
        except Exception:
            self._logger.debug("icon_registry_db_lookup_failed", exc_info=True)

        try:
            from project_dragon.ui.crypto_icons import icon_bytes_to_data_uri, resolve_crypto_icon

            resolved = resolve_crypto_icon(asset_key)
            if resolved:
                mime, b = resolved
                return icon_bytes_to_data_uri(mime, b)
        except Exception:
            self._logger.debug("icon_registry_pack_lookup_failed", exc_info=True)
        return ""


@dataclass(frozen=False)
class UIContext:
    conn: DbConnFactory
    db: Any
    logger: logging.Logger
    config: Mapping[str, Any]
    env: Mapping[str, Any]
    settings: Dict[str, Any]
    set_setting: Callable[[Any, str, Any], None]
    get_setting: Callable[[Any, str, Any], Any]
    asset_registry: AssetRegistry
    icon_registry: IconRegistry
    formatters: UIFormatters
    grid_columns: Any
    components: Any
    render_asset_block: Callable[..., str]
    flags: Dict[str, Any] = field(default_factory=dict)


def _cache_resource(fn: Callable[..., Any]) -> Callable[..., Any]:
    try:
        import streamlit as st  # type: ignore

        return st.cache_resource(
            show_spinner=False,
            hash_funcs={
                DbConnFactory: lambda _: 0,
                logging.Logger: lambda _: 0,
            },
        )(fn)
    except Exception:
        return lru_cache(maxsize=1)(fn)


@_cache_resource
def _build_asset_registry(conn_factory: Callable[..., Any], logger: logging.Logger) -> AssetRegistry:
    return AssetRegistry(conn_factory, logger)


@_cache_resource
def _build_icon_registry(conn_factory: Callable[..., Any], logger: logging.Logger) -> IconRegistry:
    return IconRegistry(conn_factory, logger)


def _normalize_symbol_to_asset_key(symbol: str) -> str:
    s = str(symbol or "").strip()
    if not s:
        return ""
    if ":" in s:
        s = s.split(":", 1)[0]

    up = s.upper().strip()
    if up.startswith("PERP_") or up.startswith("SPOT_"):
        parts = [p for p in up.split("_") if p]
        if len(parts) >= 2:
            up = parts[1]
        else:
            up = up.replace("PERP_", "").replace("SPOT_", "")

    if "/" in up:
        base = up.split("/", 1)[0].strip()
    elif "-" in up:
        base = up.split("-", 1)[0].strip()
    elif "_" in up:
        base = up.split("_", 1)[0].strip()
    else:
        base = up

    if base == up and up.isalnum():
        quote_suffixes = [
            "USDT",
            "USDC",
            "BUSD",
            "USD",
            "BTC",
            "ETH",
            "BNB",
            "DAI",
            "EUR",
            "GBP",
            "JPY",
            "KRW",
            "TRY",
            "AUD",
            "CAD",
            "CHF",
        ]
        for suf in sorted(quote_suffixes, key=len, reverse=True):
            if up.endswith(suf) and len(up) > len(suf):
                base = up[: -len(suf)]
                break

    base = "".join([c for c in base if not c.isdigit()])
    return base.strip().upper()


def _format_asset_label(
    *,
    symbol: str,
    market: str,
    base_asset: Optional[str] = None,
    asset: Optional[str] = None,
) -> str:
    base = (base_asset or "").strip() or (asset or "").strip()
    if not base:
        base = (symbol or "").strip()
    if base.endswith(":USDT"):
        base = base[: -len(":USDT")]
    mkt = (market or "").strip().upper()
    return f"{base} - {mkt}" if base and mkt else base


def render_asset_block(
    *,
    symbol: str,
    market: str,
    icon_uri: str = "",
    base_asset: Optional[str] = None,
    asset: Optional[str] = None,
    size_px: int = 18,
    text_color: str = "#FFFFFF",
) -> str:
    label = _format_asset_label(symbol=symbol, market=market, base_asset=base_asset, asset=asset)
    icon = str(icon_uri or "").strip()
    if icon:
        return (
            f"<div style='display:flex; align-items:center; gap:8px;'>"
            f"<img src='{icon}' style='width:{int(size_px)}px; height:{int(size_px)}px; border-radius:3px;' />"
            f"<span style='color:{text_color}; font-weight:500;'>{label}</span>"
            f"</div>"
        )
    return f"<span style='color:{text_color}; font-weight:500;'>{label}</span>"


def build_ui_context(
    *,
    conn_factory: Optional[Callable[..., Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    config: Optional[Mapping[str, Any]] = None,
    env: Optional[Mapping[str, Any]] = None,
    flags: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> UIContext:
    if conn_factory is None:
        from project_dragon.storage import open_db_connection

        conn_factory = open_db_connection

    logger = logger or logging.getLogger(__name__)

    if settings is None:
        try:
            with conn_factory() as conn:
                settings = get_app_settings(conn)
        except Exception:
            settings = {}

    env = dict(env or {})
    env.setdefault("name", (os.getenv("DRAGON_ENV") or "prod").strip().lower())
    env.setdefault("is_prod", env.get("name") == "prod")
    env.setdefault("is_docker", os.path.exists("/.dockerenv"))
    env.setdefault("version", os.getenv("DRAGON_VERSION") or os.getenv("APP_VERSION"))
    env.setdefault("build_sha", os.getenv("DRAGON_BUILD_SHA") or os.getenv("GITHUB_SHA"))

    config = dict(config or {})
    config.setdefault("dragon_env", env.get("name"))
    config.setdefault("debug", (os.getenv("DRAGON_DEBUG") or "").strip().lower() in {"1", "true", "yes", "on"})
    config.setdefault("profile", (os.getenv("DRAGON_PROFILE") or "").strip().lower() in {"1", "true", "yes", "on"})

    try:
        from project_dragon import storage as storage_module
    except Exception:
        storage_module = None

    try:
        from project_dragon.ui import components as components_module
        from project_dragon.ui import grid_columns as grid_columns_module
    except Exception:
        components_module = None
        grid_columns_module = None

    formatters = UIFormatters(
        format_duration_dhm=format_duration_dhm,
        fmt_date=fmt_date,
        fmt_dt=fmt_dt,
        fmt_dt_short=fmt_dt_short,
    )

    factory = DbConnFactory(conn_factory)

    asset_registry = _build_asset_registry(factory, logger)
    icon_registry = _build_icon_registry(factory, logger)

    return UIContext(
        conn=factory,
        db=storage_module,
        logger=logger,
        config=config,
        env=env,
        settings=dict(settings or {}),
        set_setting=set_setting,
        get_setting=get_setting,
        asset_registry=asset_registry,
        icon_registry=icon_registry,
        formatters=formatters,
        grid_columns=grid_columns_module,
        components=components_module,
        render_asset_block=render_asset_block,
        flags=dict(flags or {}),
    )
