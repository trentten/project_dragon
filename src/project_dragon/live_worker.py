from __future__ import annotations

import argparse
import contextvars
import json
import logging
import os
import socket
import time
import traceback
from dataclasses import dataclass
from math import isfinite
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import ccxt  # noqa: F401 - imported for side effects / exchange discovery

from project_dragon.brokers.woox_broker import WooXBroker, WooXBrokerError
from project_dragon.brokers.woox_client import WooXClient
from project_dragon.config_dragon import (
    BBandsConfig,
    DcaSettings,
    DragonAlgoConfig,
    DynamicActivationConfig,
    ExitSettings,
    GeneralSettings,
    InitialEntrySizingMode,
    MACDConfig,
    OrderStyle,
    RSIConfig,
    StopLossMode,
    TakeProfitMode,
    TrendFilterConfig,
)
from project_dragon.data_online import load_ccxt_candles, load_ccxt_ticker
from project_dragon.domain import Candle, OrderActivationState, OrderStatus, Side
from project_dragon.observability import profile_span
from project_dragon.storage import (
    add_bot_event,
    add_bot_fill,
    add_ledger_row,
    claim_job,
    claim_job_with_lease,
    reclaim_stale_job,
    renew_job_lease,
    get_credential,
    create_position_history,
    get_app_settings,
    get_bot,
    get_bot_context,
    get_job,
    get_bot_order_intent,
    get_bot_order_intent_by_local_id,
    increment_bot_blocked_actions,
    list_bot_order_intents,
    sum_account_net_pnl,
    sum_ledger,
    update_bot_order_intent,
    upsert_bot_order_intent,
    list_jobs,
    list_bot_fills,
    list_positions_history,
    set_bot_status,
    touch_credential_last_used,
    update_bot,
    update_job,
    open_db_connection,
    upsert_bot_snapshot,
    upsert_account_snapshot,
)
from project_dragon.strategy_dragon import DragonDcaAtrStrategy
from project_dragon.storage import _db_path, init_db  # type: ignore
import sqlite3

from project_dragon.crypto import CryptoConfigError, decrypt_str
from project_dragon.api_resilience import CircuitOpenError, RateLimitTimeout, CircuitState, cooldown_remaining_s, get_state


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dragon.live_worker")

# In-process hedge-mode check cache to avoid duplicate probes when multiple bots
# share the same account and symbol within a single worker runtime.
#
# Key: (account_id, symbol)
# Value: {ok: bool, reason: str, evidence: dict, checked_at_s: float, ttl_s: float}
_HEDGE_MODE_CHECK_CACHE: Dict[tuple[int, str], Dict[str, Any]] = {}
_HEDGE_MODE_CHECK_TTL_OK_S = 20 * 60
_HEDGE_MODE_CHECK_TTL_UNKNOWN_S = 2 * 60
_HEDGE_MODE_PROBE_COOLDOWN_S = 0.35

# Snapshot throttling caches (in-process only).
#
# Keyed by bot_id / account_id. Values hold last write time and payload hash.
_BOT_SNAPSHOT_CACHE: Dict[int, Dict[str, Any]] = {}
_ACCOUNT_SNAPSHOT_CACHE: Dict[int, Dict[str, Any]] = {}


def _env_float(name: str, default: float) -> float:
    try:
        v = os.environ.get(name)
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


POLL_SECONDS = _env_float("DRAGON_WORKER_POLL_S", 1.0)
STALE_JOB_SECONDS = max(POLL_SECONDS * 3, 10.0)
ACTIVE_JOB_STATUSES = {"queued", "running", "cancel_requested"}
_STALE_WARN_AT: Dict[int, float] = {}

JOB_LEASE_S = _env_float("DRAGON_JOB_LEASE_S", 30.0)
JOB_LEASE_RENEW_EVERY_S = _env_float("DRAGON_JOB_LEASE_RENEW_EVERY_S", 10.0)

# Per-worker, in-process lease tracking. This prevents a worker from accidentally
# picking up a newer lease_version (after being reclaimed) and continuing work.
_JOB_EXPECTED_LEASE_VERSION: Dict[int, int] = {}
_JOB_LAST_LEASE_RENEW_S: Dict[int, float] = {}


def _parse_iso_utc(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    try:
        dt = datetime.fromisoformat(str(s))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _maybe_renew_job_lease(
    *,
    conn: sqlite3.Connection,
    job: Dict[str, Any],
    worker_id: str,
    lease_s: float,
    renew_every_s: float,
    emit: Callable[[str, str, str, Optional[Dict[str, Any]]], None],
) -> bool:
    """Renew the job lease if due. Return False if lease is lost."""

    job_id = int(job.get("id"))
    bot_id = job.get("bot_id")

    expected = _JOB_EXPECTED_LEASE_VERSION.get(job_id)
    if expected is None:
        try:
            expected = int(job.get("lease_version") or 0)
        except Exception:
            expected = 0
        _JOB_EXPECTED_LEASE_VERSION[job_id] = int(expected)

    now_s = time.time()
    last_s = float(_JOB_LAST_LEASE_RENEW_S.get(job_id) or 0.0)
    if now_s - last_s < max(1.0, float(renew_every_s or 0.0)):
        return True

    ok = renew_job_lease(
        conn,
        job_id=job_id,
        worker_id=str(worker_id),
        lease_s=float(lease_s or 30.0),
        expected_lease_version=int(expected),
    )
    _JOB_LAST_LEASE_RENEW_S[job_id] = float(now_s)

    if ok:
        return True

    emit(
        "warn",
        "job_lease_lost",
        "Job lease lost; stopping processing",
        {
            "job_id": job_id,
            "bot_id": bot_id,
            "worker_id": str(worker_id),
            "expected_lease_version": int(expected),
        },
    )
    return False


def _fast_interval_s(global_settings: Dict[str, Any]) -> float:
    env_v = os.environ.get("DRAGON_WORKER_FAST_INTERVAL_S")
    if env_v is not None:
        try:
            return max(1.0, float(env_v))
        except Exception:
            pass
    try:
        gs_v = (global_settings or {}).get("worker_fast_interval_s")
        if gs_v is not None:
            return max(1.0, float(gs_v))
    except Exception:
        pass
    return 3.0


# In-process scheduling state: bot_id -> {next_fast_s, next_bar_check_s}
_BOT_SCHED: Dict[int, Dict[str, float]] = {}


class KillSwitchBlocked(Exception):
    def __init__(self, action: str) -> None:
        super().__init__(f"Blocked by kill switch: {action}")
        self.action = action


class RiskLimitTrip(Exception):
    def __init__(self, action: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.action = action
        self.payload = payload or {}


class AccountRiskBlocked(Exception):
    def __init__(self, action: str, reason: str, payload: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(reason)
        self.action = action
        self.reason = reason
        self.payload = payload or {}


class ExchangeDegradedBlocked(Exception):
    def __init__(self, action: str, state: str, reason: str, payload: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(reason)
        self.action = action
        self.state = state
        self.reason = reason
        self.payload = payload or {}


@dataclass
class TradingGuardContext:
    conn: sqlite3.Connection
    emit_fn: Callable[[str, str, str, Dict[str, Any]], None]
    bot_row: Dict[str, Any]
    job_row: Dict[str, Any]
    global_settings: Dict[str, Any]
    risk_cfg: Dict[str, Any]
    broker: WooXBroker
    price_estimate: float
    runtime: Optional[Dict[str, Any]] = None
    kill_blocked_this_bar: bool = False
    account_row: Optional[Dict[str, Any]] = None
    account_blocked_this_bar: bool = False
    exchange_blocked_this_bar: bool = False


def _utc_day_start_iso() -> str:
    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


_GUARD_CTX: contextvars.ContextVar[Optional[TradingGuardContext]] = contextvars.ContextVar("dragon_guard_ctx", default=None)


@dataclass
class BrokerCapabilities:
    has_balance: bool
    has_fee_rate: bool


def _broker_caps(broker: Any) -> BrokerCapabilities:
    return BrokerCapabilities(
        has_balance=hasattr(broker, "balance"),
        has_fee_rate=hasattr(broker, "fee_rate"),
    )


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _safe_set_float(obj: Any, name: str, value: Any) -> bool:
    try:
        setattr(obj, name, float(value))
        return True
    except Exception:
        return False


def _exc_text() -> str:
    try:
        return traceback.format_exc()
    except Exception:
        return ""


def ensure_trading_allowed(bot_row: Dict[str, Any], job_row: Dict[str, Any], global_settings: Dict[str, Any], action: str) -> bool:
    """Central guard called before any order placement/cancel/flatten step."""

    ctx = _GUARD_CTX.get()
    if ctx is None:
        return True

    global_kill = bool((global_settings or {}).get("live_kill_switch_enabled", False))
    bot_kill = bool((ctx.risk_cfg or {}).get("kill_switch", False))
    if not (global_kill or bot_kill):
        return True

    ctx.kill_blocked_this_bar = True
    bot_id = int(bot_row.get("id")) if bot_row.get("id") is not None else None
    job_id = int(job_row.get("id")) if job_row.get("id") is not None else None

    if bot_id is not None:
        try:
            increment_bot_blocked_actions(int(bot_id), 1)
        except Exception:
            pass
    ctx.emit_fn(
        "warn",
        "kill_switch_block",
        "Blocked by kill switch",
        {
            "action": action,
            "bot_id": bot_id,
            "job_id": job_id,
            "global_kill": global_kill,
            "bot_kill": bot_kill,
            "timestamp": _now_iso(),
        },
    )

    # Keep heartbeat message loud + consistent.
    try:
        status_now = bot_row.get("status") or "running"
        set_bot_status(ctx.conn, int(bot_id), status=str(status_now), heartbeat_msg="Blocked by kill switch")
    except Exception:
        pass
    return False


def _risk_get(cfg: Dict[str, Any], key: str, default: Any) -> Any:
    if not isinstance(cfg, dict):
        return default
    if key in cfg and cfg.get(key) is not None:
        return cfg.get(key)
    return default


def _open_orders_count(broker: WooXBroker) -> int:
    try:
        snap = _order_state_snapshot(broker)
        return len(_open_orders_snapshot(snap))
    except Exception:
        return int(len(getattr(broker, "open_orders", {}) or {}))


def _trip_risk(action: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ctx = _GUARD_CTX.get()
    if ctx is None:
        raise RiskLimitTrip(action, message, payload)
    bot_row = ctx.bot_row
    job_row = ctx.job_row
    bot_id = int(bot_row.get("id"))
    job_id = int(job_row.get("id"))
    merged = dict(payload or {})
    merged.update({"action": action, "bot_id": bot_id, "job_id": job_id, "timestamp": _now_iso()})
    ctx.emit_fn("error", "risk_limit_trip", message, merged)
    set_bot_status(ctx.conn, bot_id, status="error", last_error=message, heartbeat_msg=message)
    update_job(ctx.conn, job_id, status="error", error_text=message, finished_at=_now_iso())
    raise RiskLimitTrip(action, message, merged)


def safe_cancel_order(*, bot_row: Dict[str, Any], job_row: Dict[str, Any], global_settings: Dict[str, Any], action: str, delegate: Callable[[], Any]) -> Any:
    if not ensure_trading_allowed(bot_row, job_row, global_settings, action):
        raise KillSwitchBlocked(action)
    return delegate()


    class AccountRiskBlocked(Exception):
        def __init__(self, action: str, reason: str, payload: Optional[Dict[str, Any]] = None) -> None:
            super().__init__(f"Blocked by account risk: {reason} ({action})")
            self.action = action
            self.reason = reason
            self.payload = payload or {}


def safe_place_order(
    *,
    bot_row: Dict[str, Any],
    job_row: Dict[str, Any],
    global_settings: Dict[str, Any],
    action: str,
    qty: float,
    price_estimate: float,
    position_side: Optional[str],
    reduce_only: bool,
    delegate: Callable[[], Any],
) -> Any:
    ctx = _GUARD_CTX.get()
    risk_cfg = (ctx.risk_cfg if ctx else {}) or {}
    broker = ctx.broker if ctx else None

    try:
        qty_f = float(qty or 0.0)
    except (TypeError, ValueError):
        qty_f = 0.0
    if qty_f <= 0:
        return None

    try:
        px = float(price_estimate or 0.0)
    except (TypeError, ValueError):
        px = 0.0
    if px <= 0 and ctx is not None:
        px = float(ctx.price_estimate or 0.0)

    max_order_qty = _risk_get(risk_cfg, "max_order_qty", 1e12)
    max_order_notional = _risk_get(risk_cfg, "max_order_notional", 1e18)
    max_total_open_orders = int(_risk_get(risk_cfg, "max_total_open_orders", _risk_get(risk_cfg, "max_open_orders", 50)))
    max_position_notional_per_side = _risk_get(
        risk_cfg,
        "max_position_notional_per_side",
        _risk_get(risk_cfg, "max_notional_per_side", 1e18),
    )

    try:
        if qty_f > float(max_order_qty):
            _trip_risk(action, "Risk trip: qty exceeds max_order_qty", {"qty": qty_f, "max_order_qty": max_order_qty})
    except Exception:
        pass

    if px > 0:
        notional = qty_f * px
        try:
            if notional > float(max_order_notional):
                _trip_risk(
                    action,
                    "Risk trip: notional exceeds max_order_notional",
                    {"qty": qty_f, "price": px, "notional": notional, "max_order_notional": max_order_notional},
                )
        except Exception:
            pass

    if broker is not None:
        open_count = _open_orders_count(broker)
        if open_count >= max_total_open_orders:
            _trip_risk(
                action,
                "Risk trip: open orders exceeds max_total_open_orders",
                {"open_orders": open_count, "max_total_open_orders": max_total_open_orders},
            )

        if not reduce_only and px > 0 and position_side:
            side_norm = str(position_side).upper()
            if side_norm in {"LONG", "SHORT"}:
                pos = broker.get_position(Side.LONG if side_norm == "LONG" else Side.SHORT)
                holding = float(getattr(pos, "holding", 0.0) or 0.0) if pos is not None else 0.0
                projected_notional = (abs(holding) + qty_f) * px
                try:
                    if projected_notional > float(max_position_notional_per_side):
                        _trip_risk(
                            action,
                            "Risk trip: position notional exceeds max_position_notional_per_side",
                            {
                                "position_side": side_norm,
                                "holding": holding,
                                "qty": qty_f,
                                "price": px,
                                "projected_notional": projected_notional,
                                "max_position_notional_per_side": max_position_notional_per_side,
                            },
                        )
                except Exception:
                    pass

    if not ensure_trading_allowed(bot_row, job_row, global_settings, action):
        raise KillSwitchBlocked(action)

    # Exchange reliability gate: block new entries when the account circuit breaker is OPEN/HALF_OPEN.
    # Reduce-only actions are allowed to proceed best-effort.
    if ctx is not None and not reduce_only:
        acct = getattr(ctx, "account_row", None)
        if isinstance(acct, dict) and acct.get("id") is not None:
            try:
                acct_id = int(acct.get("id"))
            except Exception:
                acct_id = None
            if acct_id is not None and acct_id > 0:
                st = get_state(acct_id)
                if st in (CircuitState.OPEN, CircuitState.HALF_OPEN):
                    details: Dict[str, Any] = {
                        "account_id": int(acct_id),
                        "exchange_state": str(st.value),
                        "cooldown_remaining_s": cooldown_remaining_s(acct_id),
                        "timestamp": _now_iso(),
                    }
                    if not getattr(ctx, "exchange_blocked_this_bar", False):
                        ctx.exchange_blocked_this_bar = True
                        ctx.emit_fn(
                            "warn",
                            "exchange_degraded_block",
                            "Blocked: exchange degraded (new entries disabled)",
                            {"action": action, **details},
                        )
                        try:
                            bot_id_int = int(bot_row.get("id"))
                            status_now = bot_row.get("status") or "running"
                            set_bot_status(
                                ctx.conn,
                                bot_id_int,
                                status=str(status_now),
                                heartbeat_msg=f"Broker degraded: {st.value}",
                            )
                        except Exception:
                            pass
                    raise ExchangeDegradedBlocked(action, str(st.value), "exchange_degraded", details)

    # Account-level risk gate: block new entries but keep bot/job running.
    if ctx is not None and not reduce_only:
        acct = getattr(ctx, "account_row", None)
        if isinstance(acct, dict) and acct.get("id") is not None:
            acct_status = str(acct.get("status") or "").strip().lower()
            acct_id = int(acct.get("id"))
            acct_user_id = str(acct.get("user_id") or bot_row.get("user_id") or "admin@local")

            block_entries = True
            try:
                block_entries = bool(int(acct.get("risk_block_new_entries") if acct.get("risk_block_new_entries") is not None else 1))
            except Exception:
                block_entries = True

            if acct_status and acct_status != "active" and block_entries:
                payload = {
                    "account_id": acct_id,
                    "account_status": acct_status,
                    "timestamp": _now_iso(),
                }
                if not getattr(ctx, "account_blocked_this_bar", False):
                    ctx.account_blocked_this_bar = True
                    ctx.emit_fn("warn", "account_risk_block", "Blocked: trading account not active", {"action": action, **payload})
                    try:
                        bot_id = int(bot_row.get("id"))
                        status_now = bot_row.get("status") or "running"
                        set_bot_status(ctx.conn, bot_id, status=str(status_now), heartbeat_msg="Blocked by account")
                    except Exception:
                        pass
                raise AccountRiskBlocked(action, "account_not_active", payload)

            if block_entries:
                daily_limit = acct.get("risk_max_daily_loss_usd")
                total_limit = acct.get("risk_max_total_loss_usd")
                try:
                    daily_limit_f = float(daily_limit) if daily_limit is not None else 0.0
                except (TypeError, ValueError):
                    daily_limit_f = 0.0
                try:
                    total_limit_f = float(total_limit) if total_limit is not None else 0.0
                except (TypeError, ValueError):
                    total_limit_f = 0.0

                day_start = _utc_day_start_iso()
                tripped = False
                reason = ""
                details: Dict[str, Any] = {
                    "account_id": acct_id,
                    "timestamp": _now_iso(),
                }

                try:
                    if daily_limit_f > 0:
                        daily_net = float(sum_account_net_pnl(ctx.conn, acct_user_id, acct_id, since_ts=day_start))
                        details.update({"daily_net": daily_net, "daily_limit": daily_limit_f, "day_start": day_start})
                        if daily_net <= -abs(daily_limit_f):
                            tripped = True
                            reason = "daily_loss_limit"
                    if (not tripped) and total_limit_f > 0:
                        total_net = float(sum_account_net_pnl(ctx.conn, acct_user_id, acct_id))
                        details.update({"total_net": total_net, "total_limit": total_limit_f})
                        if total_net <= -abs(total_limit_f):
                            tripped = True
                            reason = "total_loss_limit"
                except Exception:
                    tripped = False

                if tripped:
                    if not getattr(ctx, "account_blocked_this_bar", False):
                        ctx.account_blocked_this_bar = True
                        ctx.emit_fn(
                            "warn",
                            "account_risk_block",
                            "Blocked by account loss limit",
                            {"action": action, "reason": reason, **details},
                        )
                        try:
                            bot_id = int(bot_row.get("id"))
                            status_now = bot_row.get("status") or "running"
                            set_bot_status(ctx.conn, bot_id, status=str(status_now), heartbeat_msg="Blocked by account risk")
                        except Exception:
                            pass
                    raise AccountRiskBlocked(action, reason, details)

    return delegate()


class GuardedStrategyBroker:
    """Adapter to make WooXBroker compatible with DragonDcaAtrStrategy (BrokerSim-like API).

    All order/cancel operations go through safe_* wrappers.
    """

    def __init__(self, broker: WooXBroker, bot_row: Dict[str, Any], job_row: Dict[str, Any], global_settings: Dict[str, Any]):
        self._b = broker
        self._bot = bot_row
        self._job = job_row
        self._global_settings = global_settings

    def __getattr__(self, name: str) -> Any:
        return getattr(self._b, name)

    @property
    def orders(self) -> Dict[int, Any]:
        return getattr(self._b, "open_orders", {})

    def get_order(self, order_id: int) -> Any:
        return self._b.get_order(order_id)

    def cancel_order(self, order_id: int) -> None:
        ctx = _GUARD_CTX.get()
        intent = None
        if ctx is not None:
            try:
                intent = get_bot_order_intent_by_local_id(
                    ctx.conn,
                    int(self._bot.get("id")),
                    int(order_id),
                    statuses=["parked", "active"],
                )
            except Exception:
                intent = None

        safe_cancel_order(
            bot_row=self._bot,
            job_row=self._job,
            global_settings=self._global_settings,
            action="cancel_order",
            delegate=lambda: self._b.cancel_order(order_id),
        )

        if ctx is not None and isinstance(intent, dict) and intent.get("intent_key"):
            try:
                update_bot_order_intent(
                    ctx.conn,
                    bot_id=int(self._bot.get("id")),
                    intent_key=str(intent.get("intent_key")),
                    status="cancelled",
                    client_order_id=None,
                    external_order_id=None,
                )
            except Exception:
                pass

    def place_dynamic_limit(
        self,
        *,
        side: Side,
        price: float,
        size: float,
        activation_pct: float,
        note: str = "",
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> int:
        # Strategy uses Side.LONG as BUY and Side.SHORT as SELL.
        # This bot controls a single hedge leg: LONG or SHORT.
        primary = str(getattr(self._b, "primary_position_side", "LONG") or "LONG").upper()
        position_side = "SHORT" if primary == "SHORT" else "LONG"
        order_action = "BUY" if side == Side.LONG else "SELL"
        reduce_only = bool(reduce_only) or _is_reduce_only(order_action, position_side)
        intent_key = _dynamic_intent_key(
            note=note,
            position_side=position_side,
            price=price,
            qty=size,
            activation_pct=activation_pct,
            reduce_only=bool(reduce_only),
            post_only=bool(post_only),
        )

        ctx = _GUARD_CTX.get()
        if ctx is not None:
            try:
                existing = get_bot_order_intent(ctx.conn, int(self._bot.get("id")), intent_key)
            except Exception:
                existing = None
            if isinstance(existing, dict) and existing.get("status") in {"parked", "active"}:
                try:
                    return int(existing.get("local_id"))
                except (TypeError, ValueError):
                    pass

        local_id = None
        if ctx is not None and isinstance(ctx.runtime, dict):
            # Ensure counter stays ahead of existing intents.
            try:
                existing_max = 0
                for row in list_bot_order_intents(ctx.conn, int(self._bot.get("id")), statuses=["parked", "active"], kind="dynamic_limit", limit=2000):
                    try:
                        existing_max = max(existing_max, int(row.get("local_id") or 0))
                    except (TypeError, ValueError):
                        pass
                local_id = _reserve_local_order_id(ctx.runtime, floor=existing_max + 1)
            except Exception:
                local_id = _reserve_local_order_id(ctx.runtime)

        if local_id is None:
            # Fallback (should be rare): let broker allocate.
            return int(
                safe_place_order(
                    bot_row=self._bot,
                    job_row=self._job,
                    global_settings=self._global_settings,
                    action="place_dynamic_limit",
                    qty=size,
                    price_estimate=price,
                    position_side=position_side,
                    reduce_only=bool(reduce_only),
                    delegate=lambda: self._b.place_dynamic_limit(
                        order_side=order_action,
                        position_side=position_side,
                        price=price,
                        size=size,
                        activation_pct=activation_pct,
                        note=note,
                        reduce_only=reduce_only,
                        post_only=post_only,
                    ),
                )
            )

        oid = int(
            safe_place_order(
                bot_row=self._bot,
                job_row=self._job,
                global_settings=self._global_settings,
                action="place_dynamic_limit",
                qty=size,
                price_estimate=price,
                position_side=position_side,
                reduce_only=bool(reduce_only),
                delegate=lambda: self._b.place_dynamic_limit(
                    order_side=order_action,
                    position_side=position_side,
                    price=price,
                    size=size,
                    activation_pct=activation_pct,
                    note=note,
                    reduce_only=reduce_only,
                    post_only=post_only,
                    local_id=int(local_id),
                ),
            )
        )

        if ctx is not None:
            try:
                upsert_bot_order_intent(
                    ctx.conn,
                    bot_id=int(self._bot.get("id")),
                    intent_key=intent_key,
                    kind="dynamic_limit",
                    status="parked",
                    local_id=int(oid),
                    note=note,
                    position_side=position_side,
                    activation_pct=float(activation_pct or 0.0),
                    target_price=float(price or 0.0),
                    qty=float(size or 0.0),
                    reduce_only=bool(reduce_only),
                    post_only=bool(post_only),
                )
            except Exception:
                pass
        return int(oid)

    def place_market(self, *, side: Side, price: float, size: float, note: str = "") -> Any:
        primary = str(getattr(self._b, "primary_position_side", "LONG") or "LONG").upper()
        position_side = "SHORT" if primary == "SHORT" else "LONG"
        order_side = "BUY" if side == Side.LONG else "SELL"
        reduce_only = _is_reduce_only(order_side, position_side)

        return safe_place_order(
            bot_row=self._bot,
            job_row=self._job,
            global_settings=self._global_settings,
            action="place_market",
            qty=size,
            price_estimate=price,
            position_side=position_side,
            reduce_only=reduce_only,
            delegate=lambda: self._b.place_market(
                order_side=order_side,
                position_side=position_side,
                price=price,
                size=size,
                note=note,
                reduce_only=reduce_only,
            ),
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_dca(note: Optional[str]) -> bool:
    if not note:
        return False
    lower = str(note).lower()
    return "dca" in lower or "rung" in lower or "ladder" in lower


def _is_reduce_only(order_action: Optional[str], position_side: Optional[str]) -> bool:
    action = (order_action or "").upper()
    side = (position_side or "").upper()
    if not action or not side:
        return False
    if side == "LONG" and action == "SELL":
        return True
    if side == "SHORT" and action == "BUY":
        return True
    return False


def _empty_leg() -> Dict[str, Any]:
    return {
        "size": 0.0,
        "avg_entry": 0.0,
        "dca_fills": 0,
        "fees": 0.0,
        "realized": 0.0,
        "opened_at": None,
        "max_size": 0.0,
        "num_fills": 0,
    }


def _reserve_local_order_id(runtime: Dict[str, Any], *, floor: int = 1) -> int:
    runtime = _ensure_runtime(runtime)
    try:
        nxt = int(runtime.get("next_local_order_id") or 0)
    except (TypeError, ValueError):
        nxt = 0
    if nxt < int(floor):
        nxt = int(floor)
    runtime["next_local_order_id"] = int(nxt) + 1
    return int(nxt)


def _serialize_strategy_state(strategy: DragonDcaAtrStrategy) -> Dict[str, Any]:
    st = getattr(strategy, "state", None)
    if st is None:
        return {}
    # Only persist simple scalars needed for idempotent order lifecycle.
    return {
        "dca_level": getattr(st, "dca_level", 0),
        "entry_count": getattr(st, "entry_count", 0),
        "base_price": getattr(st, "base_price", 0.0),
        "base_qty": getattr(st, "base_qty", 0.0),
        "pending_entry_order_id": getattr(st, "pending_entry_order_id", None),
        "pending_dca_order_id": getattr(st, "pending_dca_order_id", None),
        "tp_order_id": getattr(st, "tp_order_id", None),
        "tp_init_dist": getattr(st, "tp_init_dist", 0.0),
        "dca_recalc_tp": getattr(st, "dca_recalc_tp", False),
        "saved_dev_base": getattr(st, "saved_dev_base", 0.0),
        "active_tp_price": getattr(st, "active_tp_price", None),
        "active_sl_price": getattr(st, "active_sl_price", None),
        "current_tp_price": getattr(st, "current_tp_price", None),
        "current_sl_price": getattr(st, "current_sl_price", None),
        "trailing_active": getattr(st, "trailing_active", False),
        "trailing_peak": getattr(st, "trailing_peak", 0.0),
        "last_entry_index": getattr(st, "last_entry_index", None),
        "next_dca_price": getattr(st, "next_dca_price", None),
    }


def _restore_strategy_state(strategy: DragonDcaAtrStrategy, state_dict: Any) -> None:
    if not isinstance(state_dict, dict):
        return
    st = getattr(strategy, "state", None)
    if st is None:
        return
    for k, v in state_dict.items():
        if k in {"price_history", "last_indicator_ctx", "base_interval_min"}:
            continue
        try:
            setattr(st, k, v)
        except Exception:
            pass


def _dynamic_intent_key(
    *,
    note: str,
    position_side: str,
    price: float,
    qty: float,
    activation_pct: float,
    reduce_only: bool,
    post_only: bool,
) -> str:
    # Stable, restart-safe key. Includes target_price so TP replacements don't collide.
    try:
        ap = float(activation_pct or 0.0)
    except (TypeError, ValueError):
        ap = 0.0
    try:
        px = float(price or 0.0)
    except (TypeError, ValueError):
        px = 0.0
    try:
        q = float(qty or 0.0)
    except (TypeError, ValueError):
        q = 0.0
    ps = (position_side or "").strip().upper() or "UNKNOWN"
    tag = (note or "").strip()
    return f"dyn:{ps}:{'R' if reduce_only else 'E'}:{'P' if post_only else 'L'}:{ap:.6f}:{px:.8f}:{q:.8f}:{tag}"


def _rehydrate_intent_orders_into_broker(broker: WooXBroker, intents: List[Dict[str, Any]]) -> None:
    from project_dragon.domain import Order, OrderType  # local import to avoid cycles

    for row in intents:
        try:
            local_id = int(row.get("local_id"))
        except (TypeError, ValueError):
            continue
        if local_id <= 0:
            continue
        if local_id in getattr(broker, "open_orders", {}):
            continue

        status = str(row.get("status") or "parked").strip().lower()
        note = row.get("note")
        position_side = str(row.get("position_side") or "").upper()
        try:
            px = float(row.get("target_price") or 0.0)
        except (TypeError, ValueError):
            px = 0.0
        try:
            qty = float(row.get("qty") or 0.0)
        except (TypeError, ValueError):
            qty = 0.0

        side_enum = Side.LONG if position_side == "LONG" else Side.SHORT
        ord_obj = Order(
            id=local_id,
            side=side_enum,
            type=OrderType.LIMIT,
            price=px if px > 0 else float("nan"),
            size=qty,
            status=OrderStatus.OPEN,
            note=str(note or "dynamic"),
            activation_state=(OrderActivationState.ACTIVE if status == "active" else OrderActivationState.PARKED),
            activation_band_pct=row.get("activation_pct"),
            target_price=px if px > 0 else None,
        )
        setattr(ord_obj, "position_side", position_side)
        setattr(ord_obj, "exchange_type", "LIMIT")
        # Only set client_order_id / external_id for active orders; parked intents are local-only.
        if status == "active":
            expected_client_oid = f"{broker.client_id_prefix}-{local_id}"
            setattr(ord_obj, "client_order_id", row.get("client_order_id") or expected_client_oid)
            if row.get("external_order_id"):
                setattr(ord_obj, "external_id", str(row.get("external_order_id")))
        getattr(broker, "open_orders", {})[local_id] = ord_obj


def _should_activate_dynamic(*, ref_price: float, target_price: float, activation_pct: float) -> bool:
    if target_price <= 0:
        return False
    try:
        ap = float(activation_pct or 0.0)
    except (TypeError, ValueError):
        ap = 0.0
    if ap <= 0:
        return True
    dist = abs(ref_price - target_price) / target_price
    return dist <= (ap / 100.0)


def _park_active_dynamic_order(
    *,
    broker: WooXBroker,
    order_id: int,
    bot_row: Dict[str, Any],
    job_row: Dict[str, Any],
    global_settings: Dict[str, Any],
) -> None:
    ord_obj = getattr(broker, "open_orders", {}).get(int(order_id))
    if ord_obj is None:
        return
    ext_id = getattr(ord_obj, "external_id", None)
    client_oid = getattr(ord_obj, "client_order_id", None)
    if not (ext_id or client_oid):
        # Already local-only
        ord_obj.activation_state = OrderActivationState.PARKED
        ord_obj.status = OrderStatus.OPEN
        return

    def _delegate() -> None:
        broker.client.cancel_order(ext_id, broker.symbol, client_order_id=client_oid)
        ord_obj.activation_state = OrderActivationState.PARKED
        ord_obj.status = OrderStatus.OPEN
        setattr(ord_obj, "external_id", None)
        setattr(ord_obj, "client_order_id", None)

    safe_cancel_order(
        bot_row=bot_row,
        job_row=job_row,
        global_settings=global_settings,
        action="dynamic_park",
        delegate=_delegate,
    )


def _apply_dynamic_order_intents(
    *,
    conn: sqlite3.Connection,
    broker: WooXBroker,
    intents: List[Dict[str, Any]],
    ref_price: float,
    bot_row: Dict[str, Any],
    job_row: Dict[str, Any],
    global_settings: Dict[str, Any],
    dry_run: bool,
    emit: Callable[[str, str, str, Dict[str, Any]], None],
) -> None:
    if ref_price <= 0:
        return
    for row in intents:
        if str(row.get("kind") or "") != "dynamic_limit":
            continue
        status = str(row.get("status") or "parked").strip().lower()
        if status not in {"parked", "active"}:
            continue
        intent_key = str(row.get("intent_key") or "")
        try:
            local_id = int(row.get("local_id"))
        except (TypeError, ValueError):
            continue
        try:
            target_price = float(row.get("target_price") or 0.0)
        except (TypeError, ValueError):
            target_price = 0.0
        try:
            qty = float(row.get("qty") or 0.0)
        except (TypeError, ValueError):
            qty = 0.0
        try:
            activation_pct = float(row.get("activation_pct") or 0.0)
        except (TypeError, ValueError):
            activation_pct = 0.0
        position_side = str(row.get("position_side") or "").strip().upper()
        note = str(row.get("note") or "")
        reduce_only = bool(int(row.get("reduce_only") or 0))
        post_only = bool(int(row.get("post_only") or 0))

        should_be_active = _should_activate_dynamic(ref_price=ref_price, target_price=target_price, activation_pct=activation_pct)

        if status == "parked" and should_be_active:
            if dry_run:
                update_bot_order_intent(conn, bot_id=int(bot_row.get("id")), intent_key=intent_key, status="active")
                emit(
                    "info",
                    "dynamic_intent_activate_dry_run",
                    "Dry-run: would activate dynamic order",
                    {"intent_key": intent_key, "local_id": local_id, "price": target_price, "qty": qty},
                )
                continue

            order_side = "BUY" if position_side == "LONG" else "SELL"
            px_est = ref_price if ref_price > 0 else target_price

            try:
                safe_place_order(
                    bot_row=bot_row,
                    job_row=job_row,
                    global_settings=global_settings,
                    action="dynamic_activate",
                    qty=qty,
                    price_estimate=px_est,
                    position_side=position_side,
                    reduce_only=reduce_only,
                    delegate=(
                        (lambda: broker.place_bbo_queue_limit(order_side, position_side, qty, bid_ask_level=broker.bbo_level, note=note, reduce_only=reduce_only, local_id=local_id))
                        if getattr(broker, "prefer_bbo_maker", False)
                        else (lambda: broker.place_limit(order_side, position_side, target_price, qty, note=note, reduce_only=reduce_only, post_only=post_only, local_id=local_id))
                    ),
                )
                ord_obj = getattr(broker, "open_orders", {}).get(int(local_id))
                client_oid = getattr(ord_obj, "client_order_id", None) if ord_obj is not None else None
                ext_id = getattr(ord_obj, "external_id", None) if ord_obj is not None else None
                update_bot_order_intent(
                    conn,
                    bot_id=int(bot_row.get("id")),
                    intent_key=intent_key,
                    status="active",
                    client_order_id=client_oid,
                    external_order_id=ext_id,
                    last_error=None,
                )
            except Exception as exc:
                update_bot_order_intent(
                    conn,
                    bot_id=int(bot_row.get("id")),
                    intent_key=intent_key,
                    last_error=str(exc),
                )
                emit(
                    "warn",
                    "dynamic_intent_activate_failed",
                    "Dynamic activation failed",
                    {"intent_key": intent_key, "local_id": local_id, "error": str(exc)},
                )
            continue

        if status == "active" and not should_be_active:
            if dry_run:
                update_bot_order_intent(conn, bot_id=int(bot_row.get("id")), intent_key=intent_key, status="parked", client_order_id=None, external_order_id=None)
                emit(
                    "info",
                    "dynamic_intent_park_dry_run",
                    "Dry-run: would park dynamic order",
                    {"intent_key": intent_key, "local_id": local_id},
                )
                continue
            try:
                _park_active_dynamic_order(
                    broker=broker,
                    order_id=local_id,
                    bot_row=bot_row,
                    job_row=job_row,
                    global_settings=global_settings,
                )
                update_bot_order_intent(
                    conn,
                    bot_id=int(bot_row.get("id")),
                    intent_key=intent_key,
                    status="parked",
                    client_order_id=None,
                    external_order_id=None,
                    last_error=None,
                )
            except Exception as exc:
                update_bot_order_intent(
                    conn,
                    bot_id=int(bot_row.get("id")),
                    intent_key=intent_key,
                    last_error=str(exc),
                )
                emit(
                    "warn",
                    "dynamic_intent_park_failed",
                    "Dynamic park failed",
                    {"intent_key": intent_key, "local_id": local_id, "error": str(exc)},
                )


def _ensure_runtime(runtime: Dict[str, Any]) -> Dict[str, Any]:
    runtime.setdefault("legs", {"LONG": _empty_leg(), "SHORT": _empty_leg()})
    runtime.setdefault("order_fill_progress", {})
    runtime.setdefault("last_price", None)
    runtime.setdefault("last_mark_ts", None)
    runtime.setdefault("last_funding_ts_ms", None)
    runtime.setdefault("applied_leverage", None)
    runtime.setdefault("leverage_applied_at", None)
    return runtime


def _maybe_apply_futures_leverage(
    *,
    client: Any,
    symbol: str,
    config_dict: Dict[str, Any],
    runtime: Dict[str, Any],
    dry_run: bool,
    emit: Callable[[str, str, str, Optional[Dict[str, Any]]], None],
) -> None:
    futures_cfg = (config_dict.get("_futures") or {}) if isinstance(config_dict, dict) else {}
    if not isinstance(futures_cfg, dict):
        return
    apply_flag = bool(futures_cfg.get("apply_leverage_on_start", False))
    if not apply_flag:
        return
    lev_raw = futures_cfg.get("leverage")
    if lev_raw is None:
        return
    try:
        leverage = int(lev_raw)
    except (TypeError, ValueError):
        emit("warn", "leverage_config_invalid", "Invalid leverage in config; skipping", {"value": lev_raw})
        return

    risk_cfg = (config_dict.get("_risk") or {}) if isinstance(config_dict, dict) else {}
    max_lev = 50
    if isinstance(risk_cfg, dict):
        try:
            max_lev = int(risk_cfg.get("max_leverage") or 50)
        except (TypeError, ValueError):
            max_lev = 50
    if leverage > max_lev:
        emit(
            "warn",
            "leverage_blocked_max",
            "Leverage exceeds max_leverage; skipping apply",
            {"leverage": leverage, "max_leverage": max_lev},
        )
        return

    applied = runtime.get("applied_leverage")
    try:
        applied_int = int(applied) if applied is not None else None
    except (TypeError, ValueError):
        applied_int = None

    if applied_int == leverage:
        return

    if dry_run:
        runtime["applied_leverage"] = leverage
        runtime["leverage_applied_at"] = _now_iso()
        emit("info", "leverage_set_dry_run", "Dry-run: leverage would be set", {"symbol": symbol, "leverage": leverage})
        return

    if not hasattr(client, "set_leverage"):
        emit("warn", "leverage_endpoint_missing", "Client has no set_leverage(); skipping", {"symbol": symbol, "leverage": leverage})
        return

    try:
        client.set_leverage(symbol, leverage)
        runtime["applied_leverage"] = leverage
        runtime["leverage_applied_at"] = _now_iso()
        emit("info", "leverage_set", "Leverage set", {"symbol": symbol, "leverage": leverage})
    except Exception as exc:
        emit("warn", "leverage_set_failed", "Failed to set leverage", {"symbol": symbol, "leverage": leverage, "error": str(exc)})


def _parse_funding_rows(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data") or payload.get("result") or []
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def _funding_row_ts_ms(row: Dict[str, Any]) -> Optional[int]:
    for key in ("timestamp", "time", "createdTime", "createdAt", "fundingTime", "ts"):
        val = row.get(key)
        if val is None:
            continue
        try:
            ts = float(val)
        except (TypeError, ValueError):
            continue
        if ts > 1e12:
            return int(ts)
        return int(ts * 1000.0)
    return None


def _funding_row_amount(row: Dict[str, Any]) -> Optional[float]:
    for key in ("funding", "fundingFee", "income", "amount", "realizedPnl"):
        if row.get(key) is None:
            continue
        try:
            return float(row.get(key))
        except (TypeError, ValueError):
            continue
    return None


def _funding_row_ref_id(row: Dict[str, Any]) -> Optional[str]:
    for key in ("id", "fundingId", "incomeId", "transactionId"):
        if row.get(key) is not None:
            return str(row.get(key))
    # fallback: timestamp + amount
    ts = _funding_row_ts_ms(row)
    amt = _funding_row_amount(row)
    if ts is None or amt is None:
        return None
    return f"funding-{ts}-{amt}"


def _safe_float(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(f):
        return None
    return f


def normalize_positions_snapshot(raw: Any, mark_price: Optional[float]) -> Dict[str, Any]:
    """Normalize WooX positions snapshot into a stable schema.

    Output schema:
      {
        "mark_price": float|None,
        "per_side": {
          "LONG": {...},
          "SHORT": {...}
        },
        "totals": {...},
        "source_fields_present": ["..."]
      }

    Values are best-effort: exchange-provided fields are preserved when present,
    and computed fallbacks are included to aid debugging.
    """
    payload = raw if isinstance(raw, dict) else {}
    rows = []
    if isinstance(payload, dict):
        maybe_rows = payload.get("rows") or payload.get("data") or payload.get("result") or []
        if isinstance(maybe_rows, list):
            rows = [r for r in maybe_rows if isinstance(r, dict)]

    per_side: Dict[str, Dict[str, Any]] = {
        "LONG": {"qty": 0.0},
        "SHORT": {"qty": 0.0},
    }
    fields_present: set[str] = set()

    def _side(val: Any) -> Optional[str]:
        if val is None:
            return None
        s = str(val).strip().upper()
        return s if s in {"LONG", "SHORT"} else None

    for row in rows:
        side = _side(row.get("positionSide") or row.get("position_side") or row.get("side"))
        if side is None:
            continue

        for k in row.keys():
            fields_present.add(str(k))

        qty = (
            _safe_float(row.get("holding"))
            or _safe_float(row.get("qty"))
            or _safe_float(row.get("quantity"))
            or _safe_float(row.get("positionQty"))
            or 0.0
        )
        avg_entry = (
            _safe_float(row.get("averageOpenPrice"))
            or _safe_float(row.get("avgOpenPrice"))
            or _safe_float(row.get("entryPrice"))
            or _safe_float(row.get("openPrice"))
            or _safe_float(row.get("avgPrice"))
        )
        exch_mark = _safe_float(row.get("markPrice")) or _safe_float(row.get("mark"))
        liq = _safe_float(row.get("liquidationPrice")) or _safe_float(row.get("liqPrice"))
        margin = _safe_float(row.get("positionMargin")) or _safe_float(row.get("margin"))
        unreal_ex = (
            _safe_float(row.get("unrealizedPnl"))
            or _safe_float(row.get("unrealizedProfit"))
            or _safe_float(row.get("unrealized"))
        )
        realized_ex = _safe_float(row.get("realizedPnl")) or _safe_float(row.get("realized"))

        mark_used = _safe_float(mark_price) if _safe_float(mark_price) is not None else exch_mark
        unreal_calc = None
        if mark_used is not None and avg_entry is not None and qty and qty != 0:
            if side == "LONG":
                unreal_calc = (mark_used - avg_entry) * qty
            else:
                unreal_calc = (avg_entry - mark_used) * abs(qty)

        unreal_final = unreal_ex if unreal_ex is not None else unreal_calc
        notional = (abs(qty) * mark_used) if (mark_used is not None) else None

        per_side[side] = {
            "qty": float(qty or 0.0),
            "avg_entry": avg_entry,
            "mark": mark_used,
            "notional": notional,
            "unrealized_pnl": unreal_final,
            "unrealized_pnl_exchange": unreal_ex,
            "unrealized_pnl_computed": unreal_calc,
            "realized_pnl_exchange": realized_ex,
            "liquidation": liq,
            "margin": margin,
        }

    totals_unreal_ex = 0.0
    totals_unreal_calc = 0.0
    totals_unreal_any = 0.0
    totals_real_ex = 0.0
    totals_notional = 0.0
    has_unreal_ex = False
    has_unreal_calc = False
    has_unreal_any = False
    has_real_ex = False
    has_notional = False
    for side in ("LONG", "SHORT"):
        s = per_side.get(side) or {}
        ue = _safe_float(s.get("unrealized_pnl_exchange"))
        uc = _safe_float(s.get("unrealized_pnl_computed"))
        ua = _safe_float(s.get("unrealized_pnl"))
        re = _safe_float(s.get("realized_pnl_exchange"))
        no = _safe_float(s.get("notional"))
        if ue is not None:
            totals_unreal_ex += ue
            has_unreal_ex = True
        if uc is not None:
            totals_unreal_calc += uc
            has_unreal_calc = True
        if ua is not None:
            totals_unreal_any += ua
            has_unreal_any = True
        if re is not None:
            totals_real_ex += re
            has_real_ex = True
        if no is not None:
            totals_notional += no
            has_notional = True

    return {
        "mark_price": _safe_float(mark_price),
        "per_side": per_side,
        "totals": {
            "unrealized_pnl_exchange": totals_unreal_ex if has_unreal_ex else None,
            "unrealized_pnl_computed": totals_unreal_calc if has_unreal_calc else None,
            "unrealized_pnl": totals_unreal_any if has_unreal_any else None,
            "realized_pnl_exchange": totals_real_ex if has_real_ex else None,
            "notional": totals_notional if has_notional else None,
        },
        "source_fields_present": sorted(fields_present),
    }


def reconcile_pnl(ledger_totals: Dict[str, float], positions_norm: Dict[str, Any]) -> Dict[str, Any]:
    """Compare ledger-based totals vs exchange snapshot totals (best-effort)."""
    led_real = _safe_float(ledger_totals.get("realized_total")) or 0.0
    led_fees = _safe_float(ledger_totals.get("fees_total")) or 0.0
    led_funding = _safe_float(ledger_totals.get("funding_total")) or 0.0

    totals = (positions_norm or {}).get("totals") or {}
    exch_unreal = _safe_float(totals.get("unrealized_pnl_exchange"))
    exch_real = _safe_float(totals.get("realized_pnl_exchange"))
    comp_unreal = _safe_float(totals.get("unrealized_pnl_computed"))

    return {
        "exchange_unrealized": exch_unreal,
        "computed_unrealized": comp_unreal,
        "exchange_realized": exch_real,
        "ledger_realized": led_real,
        "ledger_fees": led_fees,
        "ledger_funding": led_funding,
        "diff_unreal": (exch_unreal - comp_unreal) if (exch_unreal is not None and comp_unreal is not None) else None,
        "diff_realized": (exch_real - led_real) if (exch_real is not None) else None,
    }


def _get_conn() -> sqlite3.Connection:
    return open_db_connection(_db_path)


def _safe_order_style(value: Any, default: OrderStyle = OrderStyle.MARKET) -> OrderStyle:
    if isinstance(value, OrderStyle):
        return value
    if isinstance(value, str):
        try:
            return OrderStyle(value)
        except ValueError:
            try:
                return OrderStyle(value.lower())
            except ValueError:
                return default
    return default


def _safe_initial_entry_sizing_mode(value: Any) -> InitialEntrySizingMode:
    if isinstance(value, InitialEntrySizingMode):
        return value
    raw = str(value or "").strip().lower()
    if raw in {"fixed_usd", "fixed", "usd", "$"} or raw.endswith("fixed_usd"):
        return InitialEntrySizingMode.FIXED_USD
    return InitialEntrySizingMode.PCT_BALANCE


def _order_state_snapshot(broker: WooXBroker) -> Dict[int, Dict[str, Any]]:
    snapshot: Dict[int, Dict[str, Any]] = {}
    for oid, order in getattr(broker, "open_orders", {}).items():
        snapshot[oid] = {
            "status": getattr(order, "status", None),
            "filled_size": float(getattr(order, "filled_size", 0.0) or 0.0),
            "client_order_id": getattr(order, "client_order_id", None),
            "external_id": getattr(order, "external_id", None),
            "activation_state": getattr(order, "activation_state", None),
            "price": getattr(order, "price", None),
            "size": float(getattr(order, "size", 0.0) or 0.0),
            "avg_fill_price": getattr(order, "avg_fill_price", None),
            "note": getattr(order, "note", None),
            "side": getattr(order, "side", None),
            "position_side": getattr(order, "position_side", None) or getattr(order, "order_action", None),
            "exchange_type": getattr(order, "exchange_type", None),
            "updated_at": getattr(order, "updated_at", None),
        }
    return snapshot


def _status_name(value: Any) -> str:
    if isinstance(value, OrderStatus):
        return value.name
    return str(value) if value is not None else ""


def _order_payload_from_state(order_id: int, state: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "local_id": order_id,
        "client_order_id": state.get("client_order_id"),
        "order_id": state.get("external_id"),
        "side": getattr(state.get("side"), "name", state.get("side")),
        "position_side": state.get("position_side"),
        "type": state.get("exchange_type") or getattr(state.get("side"), "name", None),
        "price": state.get("price"),
        "size": state.get("size"),
        "filled_size": state.get("filled_size"),
        "avg_fill_price": state.get("avg_fill_price"),
        "note": state.get("note"),
        "status": _status_name(state.get("status")),
        "activation_state": getattr(state.get("activation_state"), "name", state.get("activation_state")),
        "updated_at": state.get("updated_at"),
    }


def _apply_fill_to_runtime(
    runtime: Dict[str, Any],
    side: str,
    order_action: str,
    qty: float,
    price: float,
    *,
    is_dca: bool,
    reduce_only: bool,
    note: Optional[str],
    symbol: str,
    exchange_id: str,
    run_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    runtime = _ensure_runtime(runtime)
    runtime.setdefault("realized_total", 0.0)
    side_key = side.upper()
    if side_key not in runtime["legs"]:
        runtime["legs"][side_key] = _empty_leg()
    leg = runtime["legs"][side_key]
    now_iso = _now_iso()
    leg["num_fills"] = int(leg.get("num_fills", 0) or 0) + 1

    expanding = not reduce_only
    if expanding:
        old_size = float(leg.get("size", 0.0) or 0.0)
        new_size = old_size + qty
        old_avg = float(leg.get("avg_entry", 0.0) or 0.0)
        new_avg = ((old_avg * old_size) + (price * qty)) / new_size if new_size > 0 else 0.0
        leg.update({
            "size": new_size,
            "avg_entry": new_avg,
            "opened_at": leg.get("opened_at") or now_iso,
            "max_size": max(float(leg.get("max_size", 0.0) or 0.0), new_size),
        })
        if is_dca:
            leg["dca_fills"] = int(leg.get("dca_fills", 0) or 0) + 1
        return None

    # Reducing / closing
    leg_size = float(leg.get("size", 0.0) or 0.0)
    avg_entry = float(leg.get("avg_entry", 0.0) or 0.0)
    qty_used = min(qty, leg_size) if leg_size > 0 else qty
    realized_delta = 0.0
    if side_key == "LONG":
        realized_delta = (price - avg_entry) * qty_used
    else:
        realized_delta = (avg_entry - price) * qty_used
    leg["realized"] = float(leg.get("realized", 0.0) or 0.0) + realized_delta
    runtime["realized_total"] = float(runtime.get("realized_total", 0.0) or 0.0) + realized_delta
    leg["size"] = max(0.0, leg_size - qty_used)
    if leg.get("size", 0.0) <= 0:
        closed_row = {
            "bot_id": None,
            "run_id": run_id,
            "symbol": symbol,
            "exchange_id": exchange_id,
            "position_side": side_key,
            "opened_at": leg.get("opened_at") or now_iso,
            "closed_at": now_iso,
            "max_size": leg.get("max_size"),
            "entry_avg_price": avg_entry,
            "exit_avg_price": price,
            "realized_pnl": leg.get("realized", 0.0),
            "fees_paid": leg.get("fees", 0.0),
            "num_fills": leg.get("num_fills", 0),
            "num_dca_fills": leg.get("dca_fills", 0),
            "metadata": {"note": note} if note else {},
        }
        runtime["legs"][side_key] = _empty_leg()
        return closed_row
    return None


def _compute_unrealized(runtime: Dict[str, Any], mid_price: Optional[float]) -> float:
    if mid_price is None:
        return 0.0
    runtime = _ensure_runtime(runtime)
    total = 0.0
    for side_key, leg in runtime["legs"].items():
        size = float(leg.get("size", 0.0) or 0.0)
        avg_entry = float(leg.get("avg_entry", 0.0) or 0.0)
        if size <= 0 or avg_entry <= 0:
            continue
        if side_key == "LONG":
            total += (mid_price - avg_entry) * size
        else:
            total += (avg_entry - mid_price) * size
    return total


def _open_orders_snapshot(curr: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for oid, state in curr.items():
        status = state.get("status")
        status_name = status.name if isinstance(status, OrderStatus) else str(status).upper()
        if status not in (OrderStatus.OPEN, OrderStatus.PARTIAL) and status_name not in {"OPEN", "PARTIAL"}:
            continue
        activation = state.get("activation_state")
        if activation == OrderActivationState.PARKED or str(activation).upper() == "PARKED":
            continue
        rows.append(_order_payload_from_state(oid, state))
    return rows


def flatten_now_guarded(broker: WooXBroker) -> None:
    """Flatten positions safely: cancel open orders then market-close legs reduce-only.

    Backwards-compatible wrapper that logs events to the worker logger.
    The live worker uses an inline guarded flatten path so it can emit bot_events and enforce kill switch + limits.
    """

    def _emit(level: str, event_type: str, message: str, payload: Dict[str, Any]) -> None:
        log = logger.info
        if level == "warn":
            log = logger.warning
        elif level == "error":
            log = logger.error
        log("%s: %s (%s)", event_type, message, payload)

    flatten_now_guarded_v2(broker, _emit, price_hint=None, dry_run=False)


def flatten_now_guarded_v2(
    broker: WooXBroker,
    emit_fn: Callable[[str, str, str, Dict[str, Any]], None],
    *,
    price_hint: Optional[float] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    emit_fn(
        "warn",
        "flatten_started",
        "Flatten started (guarded)",
        {"dry_run": dry_run, "symbol": getattr(broker, 'symbol', None)},
    )

    # 1) Cancel open orders (exchange-visible only)
    broker.sync_orders()
    to_cancel: List[int] = []
    for oid, ord_obj in getattr(broker, "open_orders", {}).items():
        status = getattr(ord_obj, "status", None)
        status_name = _status_name(status)
        if status_name not in {"OPEN", "PARTIAL"}:
            continue
        activation = getattr(ord_obj, "activation_state", None)
        if activation == OrderActivationState.PARKED or str(activation).upper() == "PARKED":
            continue
        ext_id = getattr(ord_obj, "external_id", None)
        client_oid = getattr(ord_obj, "client_order_id", None)
        if not (ext_id or client_oid):
            continue
        to_cancel.append(int(oid))

    cancelled: List[int] = []
    for oid in to_cancel:
        try:
            broker.cancel_order(int(oid))
            cancelled.append(int(oid))
        except Exception as exc:
            emit_fn(
                "warn",
                "flatten_cancel_failed",
                "Cancel failed during flatten",
                {"order_id": int(oid), "error": str(exc), "dry_run": dry_run},
            )

    emit_fn(
        "info",
        "flatten_cancel_summary",
        "Cancelled open orders",
        {"cancel_attempted": len(to_cancel), "cancelled": len(cancelled), "order_ids": cancelled[:50], "dry_run": dry_run},
    )

    # 2) Close positions (hedge-mode legs) with reduce-only market orders
    broker.sync_positions()
    best = None
    try:
        best = broker.get_best_bid_ask()
    except Exception:
        best = None

    try:
        price_fallback = float(price_hint) if price_hint is not None else None
    except (TypeError, ValueError):
        price_fallback = None

    def _price_for(side: Side) -> float:
        if best:
            bid, ask = float(best[0]), float(best[1])
            if side == Side.LONG:
                return bid
            return ask
        return float(price_fallback or 0.0)

    closed = []
    for side in (Side.LONG, Side.SHORT):
        pos = broker.get_position(side)
        holding = float(getattr(pos, "holding", 0.0) or 0.0) if pos is not None else 0.0
        if holding <= 0:
            continue
        px = _price_for(side)
        try:
            if side == Side.LONG:
                broker.close_long_market(size=holding, price=px, note="flatten_now")
                closed.append({"side": "LONG", "size": holding, "price": px})
            else:
                broker.close_short_market(size=holding, price=px, note="flatten_now")
                closed.append({"side": "SHORT", "size": holding, "price": px})
            emit_fn(
                "warn",
                "flatten_position_close",
                "Closed position leg",
                {"side": "LONG" if side == Side.LONG else "SHORT", "size": holding, "price": px, "dry_run": dry_run},
            )
        except Exception as exc:
            emit_fn(
                "error",
                "flatten_close_failed",
                "Position close failed during flatten",
                {"side": "LONG" if side == Side.LONG else "SHORT", "size": holding, "error": str(exc), "dry_run": dry_run},
            )
            raise

    broker.sync_positions()
    remaining = {
        "LONG": float(getattr(broker.get_position(Side.LONG), "holding", 0.0) or 0.0) if broker.get_position(Side.LONG) else 0.0,
        "SHORT": float(getattr(broker.get_position(Side.SHORT), "holding", 0.0) or 0.0) if broker.get_position(Side.SHORT) else 0.0,
    }
    if remaining.get("LONG", 0.0) > 0 or remaining.get("SHORT", 0.0) > 0:
        emit_fn(
            "warn",
            "flatten_incomplete",
            "Flatten completed but some position remains",
            {"remaining": remaining, "dry_run": dry_run},
        )

    emit_fn(
        "warn",
        "flatten_completed",
        "Flatten completed (guarded)",
        {"closed": closed, "remaining": remaining, "dry_run": dry_run},
    )
    return {"cancelled_orders": cancelled, "closed": closed, "remaining": remaining}


def _emit_order_diffs(
    prev: Dict[int, Dict[str, Any]],
    curr: Dict[int, Dict[str, Any]],
    emit_fn: Callable[[str, str, str, Dict[str, Any]], None],
    on_fill: Optional[Callable[[Dict[str, Any], Dict[str, Any], Dict[str, Any]], None]] = None,
) -> None:
    now_iso = _now_iso()
    for oid, state in curr.items():
        payload = _order_payload_from_state(oid, state)
        payload["timestamp"] = now_iso
        prev_state = prev.get(oid)
        if prev_state is None:
            emit_fn("info", "order_placed", "Order placed", payload)
            continue
        if state.get("activation_state") == OrderActivationState.ACTIVE and prev_state.get("activation_state") != OrderActivationState.ACTIVE:
            emit_fn("info", "order_activated", "Order activated", payload)

        filled_prev = float(prev_state.get("filled_size") or 0.0)
        filled_curr = float(state.get("filled_size") or 0.0)
        if filled_curr > filled_prev and state.get("status") != OrderStatus.FILLED:
            pf_payload = dict(payload)
            pf_payload["prev_filled_size"] = filled_prev
            emit_fn("info", "order_partial_fill", "Order partially filled", pf_payload)
            if on_fill:
                on_fill(pf_payload, state, prev_state)

        status_prev = prev_state.get("status")
        status_curr = state.get("status")
        if status_prev != status_curr:
            status_payload = dict(payload)
            status_payload["prev_status"] = _status_name(status_prev)
            status_payload["status"] = _status_name(status_curr)
            event_type = "order_status"
            level = "info"
            message = f"Order status -> {status_payload['status']}"
            if status_curr == OrderStatus.FILLED:
                event_type = "order_filled"
                message = "Order filled"
            elif status_curr == OrderStatus.CANCELLED:
                event_type = "order_cancelled"
                message = "Order cancelled"
            elif status_curr == OrderStatus.REJECTED:
                event_type = "order_rejected"
                level = "error"
                message = "Order rejected"
            elif status_curr == OrderStatus.PARTIAL:
                event_type = "order_partial_fill"
                message = "Order partial"
            emit_fn(level, event_type, message, status_payload)
        if status_curr == OrderStatus.FILLED and on_fill and filled_curr >= filled_prev:
            on_fill(payload, state, prev_state)


def _dragon_config_from_snapshot(config_dict: Dict[str, Any]) -> DragonAlgoConfig:
    if not config_dict:
        raise ValueError("Missing config data")

    general_dict = config_dict.get("general", {})
    dynamic_dict = general_dict.get("dynamic_activation", {})
    general = GeneralSettings(
        max_entries=int(general_dict.get("max_entries", 1) or 1),
        initial_entry_balance_pct=float(general_dict.get("initial_entry_balance_pct") or 10.0),
        initial_entry_sizing_mode=_safe_initial_entry_sizing_mode(general_dict.get("initial_entry_sizing_mode")),
        initial_entry_fixed_usd=float(general_dict.get("initial_entry_fixed_usd") or 0.0),
        use_indicator_consensus=bool(general_dict.get("use_indicator_consensus", True)),
        entry_order_style=_safe_order_style(general_dict.get("entry_order_style", OrderStyle.MARKET.value)),
        entry_timeout_min=int(general_dict.get("entry_timeout_min", 0) or 0),
        entry_cooldown_min=int(general_dict.get("entry_cooldown_min", 0) or 0),
        exit_order_style=_safe_order_style(general_dict.get("exit_order_style", OrderStyle.MARKET.value)),
        exit_timeout_min=int(general_dict.get("exit_timeout_min", 0) or 0),
        allow_long=bool(general_dict.get("allow_long", True)),
        allow_short=bool(general_dict.get("allow_short", False)),
        lock_atr_on_entry_for_dca=bool(general_dict.get("lock_atr_on_entry_for_dca", False)),
        use_avg_entry_for_dca_base=bool(general_dict.get("use_avg_entry_for_dca_base", True)),
        plot_dca_levels=bool(general_dict.get("plot_dca_levels", False)),
        dynamic_activation=DynamicActivationConfig(
            entry_pct=float(dynamic_dict.get("entry_pct") or 0.0),
            dca_pct=float(dynamic_dict.get("dca_pct") or 0.0),
            tp_pct=float(dynamic_dict.get("tp_pct") or 0.0),
        ),
        prefer_bbo_maker=bool(general_dict.get("prefer_bbo_maker", True)),
        bbo_queue_level=int(general_dict.get("bbo_queue_level", 1) or 1),
        use_ma_direction=bool(general_dict.get("use_ma_direction", False)),
        ma_type=str(general_dict.get("ma_type") or "Ema"),
        ma_length=int(general_dict.get("ma_length") or 200),
        ma_source=str(general_dict.get("ma_source") or "close"),
    )

    exits_dict = config_dict.get("exits", {})
    sl_mode_raw = str(exits_dict.get("sl_mode") or "").strip().upper()
    try:
        sl_mode = StopLossMode(sl_mode_raw) if sl_mode_raw else StopLossMode.PCT
    except Exception:
        sl_mode = StopLossMode.PCT

    tp_mode_raw = str(exits_dict.get("tp_mode") or "").strip().upper()
    if not tp_mode_raw:
        tp_mode_raw = TakeProfitMode.ATR.value if bool(exits_dict.get("use_atr_tp", False)) else TakeProfitMode.PCT.value
    try:
        tp_mode = TakeProfitMode(tp_mode_raw)
    except Exception:
        tp_mode = TakeProfitMode.ATR

    atr_period = int(exits_dict.get("atr_period") or exits_dict.get("tp_atr_period") or 14)

    exits = ExitSettings(
        sl_mode=sl_mode,
        sl_pct=float(exits_dict.get("sl_pct") or exits_dict.get("fixed_sl_pct") or 0.0),
        trail_activation_pct=float(exits_dict.get("trail_activation_pct") or 0.0),
        trail_distance_pct=float(exits_dict.get("trail_distance_pct") or exits_dict.get("trail_stop_pct") or 0.0),
        atr_period=atr_period,
        sl_atr_mult=float(exits_dict.get("sl_atr_mult") or 0.0),
        trail_activation_atr_mult=float(exits_dict.get("trail_activation_atr_mult") or 0.0),
        trail_distance_atr_mult=float(exits_dict.get("trail_distance_atr_mult") or 0.0),
        tp_mode=tp_mode,
        tp_pct=float(exits_dict.get("tp_pct") or exits_dict.get("fixed_tp_pct") or 0.0),
        tp_atr_mult=float(exits_dict.get("tp_atr_mult") or exits_dict.get("tp_atr_multiple") or 0.0),
        tp_replace_threshold_pct=float(exits_dict.get("tp_replace_threshold_pct") or 0.0),
    )

    dca_dict = config_dict.get("dca", {})
    dca = DcaSettings(
        base_deviation_pct=float(dca_dict.get("base_deviation_pct") or 0.0),
        deviation_multiplier=float(dca_dict.get("deviation_multiplier") or 0.0),
        volume_multiplier=float(dca_dict.get("volume_multiplier") or 0.0),
    )

    trend_dict = config_dict.get("trend", {})
    trend = TrendFilterConfig(
        ma_interval_min=int(trend_dict.get("ma_interval_min", 1) or 1),
        ma_len=int(trend_dict.get("ma_len", 1) or 1),
        ma_type=str(trend_dict.get("ma_type", "Sma")),
    )

    bb_dict = config_dict.get("bbands", {})
    bbands = BBandsConfig(
        interval_min=int(bb_dict.get("interval_min", 1) or 1),
        length=int(bb_dict.get("length", 1) or 1),
        dev_up=float(bb_dict.get("dev_up") or 0.0),
        dev_down=float(bb_dict.get("dev_down") or 0.0),
        ma_type=str(bb_dict.get("ma_type", "Sma")),
        deviation=float(bb_dict.get("deviation") or 0.0),
        require_fcc=bool(bb_dict.get("require_fcc", False)),
        reset_middle=bool(bb_dict.get("reset_middle", False)),
        allow_mid_sells=bool(bb_dict.get("allow_mid_sells", False)),
    )

    macd_dict = config_dict.get("macd", {})
    macd_cfg = MACDConfig(
        interval_min=int(macd_dict.get("interval_min", 1) or 1),
        fast=int(macd_dict.get("fast", 12) or 12),
        slow=int(macd_dict.get("slow", 26) or 26),
        signal=int(macd_dict.get("signal", 9) or 9),
    )

    rsi_dict = config_dict.get("rsi", {})
    rsi_cfg = RSIConfig(
        interval_min=int(rsi_dict.get("interval_min", 1) or 1),
        length=int(rsi_dict.get("length", 14) or 14),
        buy_level=float(rsi_dict.get("buy_level", 30) or 30),
        sell_level=float(rsi_dict.get("sell_level", 70) or 70),
    )

    return DragonAlgoConfig(
        general=general,
        exits=exits,
        dca=dca,
        trend=trend,
        bbands=bbands,
        macd=macd_cfg,
        rsi=rsi_cfg,
    )


def _load_config(bot_row: Dict[str, Any]) -> Dict[str, Any]:
    raw = bot_row.get("config_json")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    if isinstance(raw, dict):
        return raw
    return {}


def _save_bot_config(conn: sqlite3.Connection, bot_id: int, config_dict: Dict[str, Any]) -> None:
    update_bot(conn, bot_id, config_json=json.dumps(config_dict))


def _fetch_latest_closed_candle(exchange_id: str, symbol: str, timeframe: str) -> Optional[Candle]:
    candles = load_ccxt_candles(exchange_id, symbol, timeframe, limit=2)
    if len(candles) < 2:
        return None
    return candles[-2]


def _fetch_recent_closed_candles(exchange_id: str, symbol: str, timeframe: str, *, limit: int) -> List[Candle]:
    """Fetch up to `limit` most-recent *closed* candles.

    ccxt may include the currently-forming candle at the end of fetch_ohlcv(). We drop the last candle
    to keep history consistent with _fetch_latest_closed_candle.
    """

    try:
        lim = int(limit)
    except (TypeError, ValueError):
        lim = 0
    if lim <= 0:
        return []
    # Fetch one extra and drop the last (possibly in-progress) candle.
    candles = load_ccxt_candles(exchange_id, symbol, timeframe, limit=lim + 1)
    if len(candles) <= 1:
        return []
    closed = candles[:-1]
    return closed[-lim:]


def _runtime_candle_history(runtime: Dict[str, Any]) -> List[Candle]:
    raw = runtime.get("price_history") or []
    if not isinstance(raw, list):
        return []
    out: List[Candle] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            ts_ms = int(item.get("timestamp_ms"))
        except Exception:
            continue
        ts = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=None)
        try:
            out.append(
                Candle(
                    timestamp=ts,
                    open=float(item.get("open") or 0.0),
                    high=float(item.get("high") or 0.0),
                    low=float(item.get("low") or 0.0),
                    close=float(item.get("close") or 0.0),
                    volume=float(item.get("volume") or 0.0),
                )
            )
        except Exception:
            continue
    out.sort(key=lambda c: c.timestamp)
    return out


def _set_runtime_candle_history(runtime: Dict[str, Any], candles: List[Candle], *, max_len: int) -> None:
    try:
        m = int(max_len)
    except (TypeError, ValueError):
        m = 0
    if m <= 0:
        m = 500
    if not candles:
        runtime["price_history"] = []
        return
    trimmed = candles[-m:]
    runtime["price_history"] = [
        {
            "timestamp_ms": _to_ms(c.timestamp),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume),
        }
        for c in trimmed
    ]


def _to_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


@dataclass
class BotTickContext:
    conn: sqlite3.Connection
    bot: Dict[str, Any]
    job: Dict[str, Any]
    bot_id: int
    job_id: int
    worker_id: str
    dry_run: bool
    global_settings: Dict[str, Any]
    account_row: Optional[Dict[str, Any]]
    config_dict: Dict[str, Any]
    runtime: Dict[str, Any]
    config_obj: DragonAlgoConfig
    exchange_id: str
    symbol: str
    timeframe: str
    run_id: Optional[str]
    client: Any
    emit: Callable[[str, str, str, Optional[Dict[str, Any]]], None]


def get_latest_closed_bar_ts_ms(exchange_id: str, symbol: str, timeframe: str) -> Optional[int]:
    c = _fetch_latest_closed_candle(exchange_id, symbol, timeframe)
    if c is None:
        return None
    try:
        return _to_ms(c.timestamp)
    except Exception:
        return None


def _account_entry_block_status(
    conn: sqlite3.Connection,
    *,
    bot_row: Dict[str, Any],
    account_row: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute whether *new entries* should be blocked for this account (non-fatal)."""

    acct = account_row
    if not isinstance(acct, dict) or acct.get("id") is None:
        return {"blocked": False, "reason": None, "details": {}}

    acct_id = int(acct.get("id"))
    acct_status = str(acct.get("status") or "").strip().lower()
    acct_user_id = str(acct.get("user_id") or bot_row.get("user_id") or "admin@local")

    block_entries = True
    try:
        block_entries = bool(int(acct.get("risk_block_new_entries") if acct.get("risk_block_new_entries") is not None else 1))
    except Exception:
        block_entries = True

    if acct_status and acct_status != "active" and block_entries:
        return {
            "blocked": True,
            "reason": "account_not_active",
            "details": {"account_id": acct_id, "account_status": acct_status},
        }

    if not block_entries:
        return {"blocked": False, "reason": None, "details": {"account_id": acct_id}}

    daily_limit = acct.get("risk_max_daily_loss_usd")
    total_limit = acct.get("risk_max_total_loss_usd")
    try:
        daily_limit_f = float(daily_limit) if daily_limit is not None else 0.0
    except (TypeError, ValueError):
        daily_limit_f = 0.0
    try:
        total_limit_f = float(total_limit) if total_limit is not None else 0.0
    except (TypeError, ValueError):
        total_limit_f = 0.0

    day_start = _utc_day_start_iso()
    details: Dict[str, Any] = {"account_id": acct_id, "day_start": day_start}
    try:
        if daily_limit_f > 0:
            daily_net = float(sum_account_net_pnl(conn, acct_user_id, acct_id, since_ts=day_start))
            details.update({"daily_net": daily_net, "daily_limit": daily_limit_f})
            if daily_net <= -abs(daily_limit_f):
                return {"blocked": True, "reason": "daily_loss_limit", "details": details}
        if total_limit_f > 0:
            total_net = float(sum_account_net_pnl(conn, acct_user_id, acct_id))
            details.update({"total_net": total_net, "total_limit": total_limit_f})
            if total_net <= -abs(total_limit_f):
                return {"blocked": True, "reason": "total_loss_limit", "details": details}
    except Exception:
        # Fail-soft: if we can't compute, do not block.
        return {"blocked": False, "reason": None, "details": {"account_id": acct_id, "compute": "failed"}}

    return {"blocked": False, "reason": None, "details": details}


def fast_tick(ctx: BotTickContext, broker: WooXBroker, now: datetime) -> None:
    """Fast cadence work: sync + heartbeat + reconciliation. No strategy decisions."""

    runtime = ctx.runtime
    health = runtime.setdefault("health", {}) if isinstance(runtime, dict) else {}
    health["last_fast_tick_at"] = now.isoformat()
    health["fast_interval_s"] = float(_fast_interval_s(ctx.global_settings))
    health["worker_id"] = ctx.worker_id

    # Update health signals used by UI.
    acct_block = _account_entry_block_status(ctx.conn, bot_row=ctx.bot, account_row=ctx.account_row)
    health["account_risk"] = {
        **(acct_block or {}),
        "checked_at": now.isoformat(),
    }

    # Global gates: live_trading_enabled blocks broker I/O (but heartbeat still updates).
    live_trading_enabled = bool((ctx.global_settings or {}).get("live_trading_enabled", False))
    if not live_trading_enabled:
        health["next_action"] = "Blocked: live trading disabled"
        set_bot_status(
            ctx.conn,
            int(ctx.bot_id),
            status=str(ctx.bot.get("status") or "running"),
            heartbeat_msg="Blocked: live trading disabled",
            heartbeat_at=_now_iso(),
        )
        update_job(ctx.conn, ctx.job_id, status="running", message="Blocked: live trading disabled", progress=0.0)
        return

    # Sync exchange state (throttle kept simple: cadence is already limited by fast_interval).
    try:
        broker.sync_positions()
    except (CircuitOpenError, RateLimitTimeout) as exc:
        ctx.emit(
            "warn",
            "broker_degraded",
            "Broker degraded during positions sync",
            {"op_name": "sync_positions", "error_type": type(exc).__name__, "error": str(exc)},
        )
    except Exception as exc:
        ctx.emit("warn", "positions_sync_failed", "Positions sync failed", {"error": str(exc)})
    order_snapshot_before = _order_state_snapshot(broker)
    try:
        broker.sync_orders()
    except (CircuitOpenError, RateLimitTimeout) as exc:
        ctx.emit(
            "warn",
            "broker_degraded",
            "Broker degraded during orders sync",
            {"op_name": "sync_orders", "error_type": type(exc).__name__, "error": str(exc)},
        )
    except Exception as exc:
        ctx.emit("warn", "orders_sync_failed", "Orders sync failed", {"error": str(exc)})
    order_snapshot_after = _order_state_snapshot(broker)

    # Record exchange connectivity state into runtime health for snapshots/UI.
    try:
        acct_id = None
        if isinstance(ctx.account_row, dict) and ctx.account_row.get("id") is not None:
            acct_id = int(ctx.account_row.get("id"))
        if acct_id is not None and acct_id > 0:
            st = get_state(acct_id)
            health["exchange_state"] = str(st.value)
            if st != CircuitState.CLOSED:
                health["exchange_degraded"] = True
                health["last_exchange_error_at"] = now.isoformat()
                health["exchange_reason"] = "circuit_breaker"
            else:
                health["exchange_degraded"] = False
    except Exception:
        pass

    intent_open_count: Optional[int] = None

    # Best-effort: terminalize intents when their mapped order reaches a terminal status.
    try:
        intent_rows = list_bot_order_intents(ctx.conn, int(ctx.bot_id), statuses=["parked", "active"], kind="dynamic_limit", limit=2000)
        try:
            intent_open_count = int(len(intent_rows or []))
        except Exception:
            intent_open_count = None
        for row in list(intent_rows or []):
            intent_key = str(row.get("intent_key") or "")
            if not intent_key:
                continue
            try:
                local_id = int(row.get("local_id"))
            except (TypeError, ValueError):
                continue
            ord_obj = getattr(broker, "open_orders", {}).get(local_id)
            if ord_obj is None:
                continue
            status = getattr(ord_obj, "status", None)
            status_name = _status_name(status)
            if status_name in {"FILLED", "CANCELLED", "REJECTED"}:
                update_bot_order_intent(ctx.conn, bot_id=int(ctx.bot_id), intent_key=intent_key, status=status_name.lower())
            else:
                ext_id = getattr(ord_obj, "external_id", None)
                client_oid = getattr(ord_obj, "client_order_id", None)
                if ext_id or client_oid:
                    update_bot_order_intent(
                        ctx.conn,
                        bot_id=int(ctx.bot_id),
                        intent_key=intent_key,
                        external_order_id=ext_id,
                        client_order_id=client_oid,
                    )
    except Exception:
        pass

    # Mark price (best-effort)
    mark = None
    try:
        t = load_ccxt_ticker(ctx.exchange_id, ctx.symbol) or {}
        info = t.get("info") if isinstance(t, dict) else None
        if isinstance(info, dict):
            mark = _safe_float(info.get("markPrice")) or _safe_float(info.get("mark_price")) or _safe_float(info.get("mark"))
        if mark is None and isinstance(t, dict):
            mark = _safe_float(t.get("mark")) or _safe_float(t.get("last")) or _safe_float(t.get("close"))
    except Exception:
        mark = None
    if mark is None:
        try:
            best = broker.get_best_bid_ask()
            if best:
                mark = (float(best[0]) + float(best[1])) / 2.0
        except Exception:
            mark = None
    if mark is not None:
        runtime["last_price"] = float(mark)
        runtime["last_mark_ts"] = _now_iso()

    # Fill + fee/ledger reconciliation (best-effort) on fast cadence.
    try:
        risk_cfg = ctx.config_dict.get("_risk", {}) if isinstance(ctx.config_dict, dict) else {}
        price_estimate = float(runtime.get("last_price") or 0.0)
        guard_ctx = TradingGuardContext(
            conn=ctx.conn,
            emit_fn=lambda lvl, et, msg, payload=None: ctx.emit(lvl, et, msg, payload or {}),
            bot_row=ctx.bot,
            job_row=ctx.job,
            global_settings=ctx.global_settings,
            risk_cfg=risk_cfg if isinstance(risk_cfg, dict) else {},
            broker=broker,
            price_estimate=price_estimate,
            runtime=runtime if isinstance(runtime, dict) else None,
            account_row=ctx.account_row if isinstance(ctx.account_row, dict) else None,
        )
        token = _GUARD_CTX.set(guard_ctx)

        def _on_fill(payload: Dict[str, Any], state: Dict[str, Any], prev_state: Dict[str, Any]) -> None:
            client_oid = payload.get("client_order_id") or state.get("client_order_id") or state.get("external_id") or state.get("order_id")
            if not client_oid:
                return
            prev_filled = float(prev_state.get("filled_size") or 0.0) if prev_state else 0.0
            curr_filled = float(state.get("filled_size") or 0.0)
            if curr_filled <= prev_filled:
                return
            progress = runtime.setdefault("order_fill_progress", {})
            last_recorded = float(progress.get(client_oid) or 0.0)
            delta = max(0.0, curr_filled - last_recorded)
            if delta <= 0:
                return
            progress[client_oid] = curr_filled
            try:
                price_val = float(state.get("avg_fill_price") or state.get("price") or 0.0)
            except (TypeError, ValueError):
                return
            if price_val <= 0:
                return
            position_side = (state.get("position_side") or payload.get("position_side") or "").upper()
            if not position_side:
                return
            order_action = (state.get("order_action") or state.get("side") or payload.get("side") or "").upper()
            reduce_only = _is_reduce_only(order_action, position_side)
            note_val = state.get("note") or payload.get("note")
            dca_flag = _is_dca(note_val)
            closed_row = _apply_fill_to_runtime(
                runtime,
                position_side,
                order_action,
                float(delta),
                float(price_val),
                is_dca=dca_flag,
                reduce_only=reduce_only,
                note=note_val,
                symbol=ctx.symbol,
                exchange_id=ctx.exchange_id,
                run_id=ctx.run_id,
            )
            fill_row = {
                "bot_id": int(ctx.bot_id),
                "run_id": ctx.run_id,
                "symbol": ctx.symbol,
                "exchange_id": ctx.exchange_id,
                "position_side": position_side,
                "order_action": order_action,
                "client_order_id": client_oid,
                "external_order_id": state.get("external_id"),
                "filled_qty": float(delta),
                "avg_fill_price": float(price_val),
                "fee_paid": None,
                "fee_asset": None,
                "is_reduce_only": reduce_only,
                "is_dca": dca_flag,
                "note": note_val,
                "event_ts": state.get("updated_at") or _now_iso(),
            }
            add_bot_fill(ctx.conn, fill_row)

            ext_id = state.get("external_id")
            if ext_id:
                fill_ref = f"fill-{ext_id}-{client_oid}-{state.get('updated_at') or ''}-{price_val}-{delta}"
            else:
                fill_ref = f"fill-{client_oid}-{state.get('updated_at') or ''}-{price_val}-{delta}"
            add_ledger_row(
                ctx.conn,
                bot_id=int(ctx.bot_id),
                event_ts=fill_row["event_ts"],
                kind="fill",
                symbol=ctx.symbol,
                side=order_action,
                position_side=position_side,
                qty=float(delta),
                price=float(price_val),
                ref_id=fill_ref,
                meta={"client_order_id": client_oid, "external_order_id": ext_id, "note": note_val},
            )

            fee_amt = None
            fee_meta = {"client_order_id": client_oid, "external_order_id": ext_id}
            if state.get("fee_paid") is not None:
                try:
                    fee_amt = float(state.get("fee_paid"))
                except (TypeError, ValueError):
                    fee_amt = None
            if fee_amt is None:
                try:
                    fee_rate = float(_safe_getattr(broker, "fee_rate", 0.0) or 0.0)
                except (TypeError, ValueError):
                    fee_rate = 0.0
                fee_amt = abs(float(delta) * float(price_val)) * fee_rate
                fee_meta["fee_estimated"] = True
                fee_meta["fee_rate"] = fee_rate

            add_ledger_row(
                ctx.conn,
                bot_id=int(ctx.bot_id),
                event_ts=fill_row["event_ts"],
                kind="fee",
                symbol=ctx.symbol,
                side=order_action,
                position_side=position_side,
                qty=float(delta),
                price=float(price_val),
                fee=float(fee_amt or 0.0),
                ref_id=f"fee-{fill_ref}",
                meta=fee_meta,
            )

            if closed_row:
                closed_row["bot_id"] = int(ctx.bot_id)
                create_position_history(ctx.conn, closed_row)
                try:
                    realized_val = float(closed_row.get("realized_pnl") or 0.0)
                except (TypeError, ValueError):
                    realized_val = 0.0
                add_ledger_row(
                    ctx.conn,
                    bot_id=int(ctx.bot_id),
                    event_ts=_now_iso(),
                    kind="adjustment",
                    symbol=ctx.symbol,
                    pnl=realized_val,
                    ref_id=f"pnl-{fill_ref}",
                    meta={"source": "position_close", "closed_row": closed_row},
                )

        _emit_order_diffs(order_snapshot_before, order_snapshot_after, lambda lvl, et, msg, payload: ctx.emit(lvl, et, msg, payload), _on_fill)
        _GUARD_CTX.reset(token)
    except Exception:
        try:
            _GUARD_CTX.reset(token)  # type: ignore[name-defined]
        except Exception:
            pass

    # Snapshot inputs used by both bots table persistence and durable snapshot tables.
    legs = runtime.get("legs", {}) if isinstance(runtime, dict) else {}
    open_orders_payload: List[Dict[str, Any]] = _open_orders_snapshot(order_snapshot_after)
    snap_realized_pnl = _safe_float(ctx.bot.get("realized_pnl"))
    snap_unrealized_pnl = _safe_float(ctx.bot.get("unrealized_pnl"))
    snap_mark_price = _safe_float(runtime.get("last_price"))

    # Persist UI-facing snapshots (positions/orders/mark) best-effort.
    try:
        dca_fills_current = sum(int(v.get("dca_fills", 0) or 0) for v in legs.values() if float(v.get("size", 0.0) or 0.0) > 0)
        fees_total = 0.0
        funding_total = 0.0
        realized_total = 0.0
        try:
            ledger_totals = sum_ledger(ctx.conn, int(ctx.bot_id))
            fees_total = float(ledger_totals.get("fees_total", 0.0) or 0.0)
            funding_total = float(ledger_totals.get("funding_total", 0.0) or 0.0)
            realized_total = float(ledger_totals.get("realized_total", 0.0) or 0.0)
        except Exception:
            pass

        realized_pnl = realized_total
        mark_price = runtime.get("last_price")
        unrealized_pnl = _compute_unrealized(runtime, mark_price)
        snap_realized_pnl = _safe_float(realized_pnl)
        snap_unrealized_pnl = _safe_float(unrealized_pnl)
        snap_mark_price = _safe_float(mark_price)
        position_json = json.dumps({k: v for k, v in (legs or {}).items()})
        open_orders_json = json.dumps(open_orders_payload)

        # Throttle raw positions snapshot (UI/debug) to avoid spamming.
        snap_ok = False
        last_snap_s = float(health.get("last_positions_snapshot_s") or 0.0)
        now_s = time.time()
        positions_snapshot: Dict[str, Any] = {}
        positions_normalized: Dict[str, Any] = {}
        if (now_s - last_snap_s) >= 15.0:
            try:
                payload = ctx.client.get_positions(ctx.symbol)
                positions_snapshot = payload if isinstance(payload, dict) else {"raw": payload}
                snap_ok = True
            except Exception as exc:
                ctx.emit("warn", "positions_snapshot_failed", "Failed to fetch positions snapshot", {"error": str(exc)})
                positions_snapshot = {}
            try:
                positions_normalized = normalize_positions_snapshot(positions_snapshot, _safe_float(runtime.get("last_price")))
            except Exception:
                positions_normalized = {}
            health["last_positions_snapshot_s"] = float(now_s)

        update_fields: Dict[str, Any] = {
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "fees_total": fees_total,
            "funding_total": funding_total,
            "dca_fills_current": dca_fills_current,
            "position_json": position_json,
            "open_orders_json": open_orders_json,
            "mark_price": mark_price,
        }
        if snap_ok:
            update_fields["positions_snapshot_json"] = json.dumps(positions_snapshot)
            update_fields["positions_normalized_json"] = json.dumps(positions_normalized)
        update_bot(ctx.conn, int(ctx.bot_id), **update_fields)
    except Exception:
        pass

    # Heartbeat message
    desired_action = (str(ctx.bot.get("desired_action") or "").strip().lower())
    next_action = "Waiting bar"
    if desired_action == "flatten_now":
        next_action = "Flattening"
    elif bool((ctx.global_settings or {}).get("live_kill_switch_enabled", False)) or bool(((ctx.config_dict or {}).get("_risk") or {}).get("kill_switch")):
        next_action = "Kill switch"
    elif bool((health or {}).get("exchange_degraded")):
        st = str((health or {}).get("exchange_state") or "DEGRADED")
        next_action = f"Exchange degraded ({st})"
    elif bool((acct_block or {}).get("blocked")):
        reason = (acct_block or {}).get("reason")
        next_action = "Risk blocked" + (f" ({reason})" if reason else "")
    health["next_action"] = next_action

    # Durable snapshots (bot/account) for UI and multi-worker readiness.
    # Best-effort: never block trading.
    try:
        now_iso = now.isoformat()
        now_s = time.time()

        def _payload_hash(obj: Dict[str, Any]) -> str:
            try:
                return json.dumps(obj, sort_keys=True, default=str, separators=(",", ":"))
            except Exception:
                return str(obj)

        def _should_write(cache: Dict[int, Dict[str, Any]], key: int, payload: Dict[str, Any]) -> bool:
            entry = cache.get(key) or {}
            last_s = float(entry.get("last_s") or 0.0)
            last_h = str(entry.get("last_h") or "")
            h = _payload_hash(payload)
            changed = (h != last_h)
            if not changed and (now_s - last_s) < 10.0:
                return False
            if changed and (now_s - last_s) < 1.0:
                return False
            entry["last_s"] = float(now_s)
            entry["last_h"] = h
            cache[key] = entry
            return True

        # Compute position summary from runtime legs (no extra broker calls).
        long_leg = (legs or {}).get("LONG") if isinstance(legs, dict) else None
        short_leg = (legs or {}).get("SHORT") if isinstance(legs, dict) else None
        try:
            long_qty = float((long_leg or {}).get("size") or 0.0) if isinstance(long_leg, dict) else 0.0
        except Exception:
            long_qty = 0.0
        try:
            short_qty = float((short_leg or {}).get("size") or 0.0) if isinstance(short_leg, dict) else 0.0
        except Exception:
            short_qty = 0.0

        pos_status = "flat"
        if abs(long_qty) > 1e-12 and abs(short_qty) > 1e-12:
            pos_status = "hedged"
        elif abs(long_qty) > 1e-12:
            pos_status = "long"
        elif abs(short_qty) > 1e-12:
            pos_status = "short"

        global_kill = bool((ctx.global_settings or {}).get("live_kill_switch_enabled", False))
        bot_kill = bool(((ctx.config_dict or {}).get("_risk") or {}).get("kill_switch"))
        bot_status_l = str(ctx.bot.get("status") or "").strip().lower()
        last_error = (str(ctx.bot.get("last_error") or "")).strip() or None
        health_status = "ok"
        if bot_status_l == "error" or last_error:
            health_status = "error"
        elif bool((acct_block or {}).get("blocked")) or bool((health or {}).get("exchange_degraded")) or global_kill or bot_kill:
            health_status = "warn"

        last_ev_ts = last_ev_level = last_ev_type = last_ev_msg = None
        try:
            row = ctx.conn.execute(
                "SELECT ts, level, event_type, message FROM bot_events WHERE bot_id = ? ORDER BY ts DESC, id DESC LIMIT 1",
                (int(ctx.bot_id),),
            ).fetchone()
            if row:
                last_ev_ts = row[0]
                last_ev_level = row[1]
                last_ev_type = row[2]
                last_ev_msg = row[3]
        except Exception:
            pass

        open_orders_count = None
        try:
            open_orders_count = int(len(open_orders_payload))
        except Exception:
            open_orders_count = None

        bot_snap = {
            "bot_id": int(ctx.bot_id),
            "user_id": str(ctx.bot.get("user_id") or "").strip() or "admin@local",
            "exchange_id": str(ctx.exchange_id or "").strip().lower() or "woox",
            "symbol": str(ctx.symbol or "").strip(),
            "timeframe": str(ctx.timeframe or "").strip(),
            "updated_at": now_iso,
            "worker_id": ctx.worker_id,
            "status": str(ctx.bot.get("status") or ""),
            "desired_status": str(ctx.bot.get("desired_status") or ""),
            "desired_action": str(ctx.bot.get("desired_action") or "") or None,
            "next_action": str((health or {}).get("next_action") or next_action or "") or None,
            "health_status": health_status,
            "health_json": json.dumps(health or {}),
            "pos_status": pos_status,
            "positions_summary_json": json.dumps(
                {
                    "long_qty": long_qty,
                    "short_qty": short_qty,
                    "mark_price": snap_mark_price,
                    "realized_pnl": snap_realized_pnl,
                    "unrealized_pnl": snap_unrealized_pnl,
                }
            ),
            "open_orders_count": open_orders_count,
            "open_intents_count": intent_open_count,
            "account_id": ctx.bot.get("account_id"),
            "risk_blocked": 1 if bool((acct_block or {}).get("blocked")) else 0,
            "risk_reason": (acct_block or {}).get("reason"),
            "last_error": last_error,
            "last_event_ts": last_ev_ts,
            "last_event_level": last_ev_level,
            "last_event_type": last_ev_type,
            "last_event_message": last_ev_msg,
            "exchange_state": (health or {}).get("exchange_state"),
            "last_exchange_error_at": (health or {}).get("last_exchange_error_at"),
        }

        if _should_write(_BOT_SNAPSHOT_CACHE, int(ctx.bot_id), bot_snap):
            try:
                upsert_bot_snapshot(ctx.conn, bot_snap)
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "locked" in msg or "busy" in msg:
                    try:
                        ctx.emit("warn", "snapshot_write_failed", "Snapshot write failed (db locked)", {"error": str(exc)})
                    except Exception:
                        pass
                else:
                    raise

        try:
            acct_id_raw = ctx.bot.get("account_id")
            acct_id = int(acct_id_raw) if acct_id_raw is not None else None
        except Exception:
            acct_id = None
        if acct_id is not None:
            acct_snap = {
                "account_id": int(acct_id),
                "user_id": str(ctx.bot.get("user_id") or "").strip() or "admin@local",
                "exchange_id": str(ctx.exchange_id or "").strip().lower() or "woox",
                "updated_at": now_iso,
                "worker_id": ctx.worker_id,
                "status": str((ctx.account_row or {}).get("status") or "") if isinstance(ctx.account_row, dict) else None,
                "risk_blocked": 1 if bool((acct_block or {}).get("blocked")) else 0,
                "risk_reason": (acct_block or {}).get("reason"),
                "positions_summary_json": json.dumps({"symbol": ctx.symbol, "long_qty": long_qty, "short_qty": short_qty}),
                "open_orders_count": open_orders_count,
                "margin_ratio": None,
                "wallet_balance": None,
                "last_error": None,
                "exchange_state": (health or {}).get("exchange_state"),
                "last_exchange_error_at": (health or {}).get("last_exchange_error_at"),
            }
            if _should_write(_ACCOUNT_SNAPSHOT_CACHE, int(acct_id), acct_snap):
                try:
                    upsert_account_snapshot(ctx.conn, acct_snap)
                except sqlite3.OperationalError as exc:
                    msg = str(exc).lower()
                    if "locked" in msg or "busy" in msg:
                        try:
                            ctx.emit(
                                "warn",
                                "snapshot_write_failed",
                                "Snapshot write failed (db locked)",
                                {"error": str(exc), "account_id": int(acct_id)},
                            )
                        except Exception:
                            pass
                    else:
                        raise
    except Exception:
        pass

    set_bot_status(ctx.conn, int(ctx.bot_id), status=str(ctx.bot.get("status") or "running"), heartbeat_msg=next_action, heartbeat_at=_now_iso())
    update_job(ctx.conn, ctx.job_id, status="running", message=next_action, progress=0.0)


def bar_tick(ctx: BotTickContext, broker: WooXBroker, bar: Candle) -> None:
    """Bar cadence work: strategy evaluation + intent creation/activation (idempotent)."""

    runtime = ctx.runtime
    last_ts = runtime.get("last_bar_ts_ms") or runtime.get("last_candle_ts_ms")
    candle_ts_ms = _to_ms(bar.timestamp)
    if last_ts is not None:
        try:
            if int(candle_ts_ms) <= int(last_ts):
                return
        except Exception:
            return

    bar_index = int(runtime.get("bar_index", 0) or 0)

    # Bootstrap/persist strategy candle history so indicators match an always-running bot.
    history = _runtime_candle_history(runtime)
    if not history:
        # Estimate warmup from config + timeframe.
        base_min = 1.0
        try:
            tf_val = str(ctx.timeframe or "1m")
            if tf_val.endswith("m"):
                base_min = float(int(tf_val[:-1]))
            elif tf_val.endswith("h"):
                base_min = float(int(tf_val[:-1]) * 60)
        except Exception:
            base_min = 1.0
        if base_min <= 0:
            base_min = 1.0

        def _bars_needed(interval_min: int, length: int) -> int:
            step = max(1, int(round(max(int(interval_min or 1), 1) / float(base_min))))
            return step * max(int(length or 1), 1)

        cfg = ctx.config_obj
        max_req = max(
            _bars_needed(cfg.trend.ma_interval_min, int(getattr(cfg.trend, "ma_len", 1) or 1)),
            _bars_needed(cfg.bbands.interval_min, int(getattr(cfg.bbands, "length", 1) or 1)),
            _bars_needed(cfg.macd.interval_min, int(getattr(cfg.macd, "slow", 26) or 26) + int(getattr(cfg.macd, "signal", 9) or 9)),
            _bars_needed(cfg.rsi.interval_min, int(getattr(cfg.rsi, "length", 14) or 14) + 1),
            _bars_needed(cfg.trend.ma_interval_min, int(getattr(cfg.exits, "tp_atr_period", 1) or 1) + 1),
        )
        warmup_limit = min(max(int(max_req), 500), 250_000)
        history = _fetch_recent_closed_candles(ctx.exchange_id, ctx.symbol, ctx.timeframe, limit=warmup_limit)

    history.sort(key=lambda c: c.timestamp)
    try:
        if history and _to_ms(history[-1].timestamp) == candle_ts_ms:
            history = history[:-1]
    except Exception:
        pass

    # Setup guard context for broker operations on this bar.
    risk_cfg = ctx.config_dict.get("_risk", {}) if isinstance(ctx.config_dict, dict) else {}
    try:
        price_estimate = float(runtime.get("last_price") or bar.close or 0.0)
    except (TypeError, ValueError):
        price_estimate = float(bar.close or 0.0)
    guard_ctx = TradingGuardContext(
        conn=ctx.conn,
        emit_fn=lambda lvl, et, msg, payload=None: ctx.emit(lvl, et, msg, payload or {}),
        bot_row=ctx.bot,
        job_row=ctx.job,
        global_settings=ctx.global_settings,
        risk_cfg=risk_cfg if isinstance(risk_cfg, dict) else {},
        broker=broker,
        price_estimate=price_estimate,
        runtime=runtime if isinstance(runtime, dict) else None,
        account_row=ctx.account_row if isinstance(ctx.account_row, dict) else None,
    )
    token = _GUARD_CTX.set(guard_ctx)

    # Apply any pending dynamic intent activation/parking based on this bar's reference price.
    try:
        intent_rows = list_bot_order_intents(ctx.conn, int(ctx.bot_id), statuses=["parked", "active"], kind="dynamic_limit", limit=2000)
        _rehydrate_intent_orders_into_broker(broker, intent_rows)
        _apply_dynamic_order_intents(
            conn=ctx.conn,
            broker=broker,
            intents=intent_rows,
            ref_price=float(bar.close or 0.0),
            bot_row=ctx.bot,
            job_row=ctx.job,
            global_settings=ctx.global_settings,
            dry_run=ctx.dry_run,
            emit=lambda lvl, et, msg, payload=None: ctx.emit(lvl, et, msg, payload),
        )
    except Exception:
        pass

    strategy = DragonDcaAtrStrategy(config=ctx.config_obj)
    try:
        _restore_strategy_state(strategy, runtime.get("strategy_state"))
    except Exception:
        pass
    try:
        if history:
            strategy.prime_history(history)
    except Exception:
        pass
    guarded_broker = GuardedStrategyBroker(broker, ctx.bot, ctx.job, ctx.global_settings)
    try:
        strategy.on_bar(bar_index, bar, guarded_broker)
    except KillSwitchBlocked as exc:
        ctx.emit("warn", "kill_switch_block", "Blocked by kill switch", {"action": exc.action, "dry_run": ctx.dry_run})
    except AccountRiskBlocked as exc:
        payload = dict(exc.payload or {})
        payload.setdefault("action", exc.action)
        payload.setdefault("reason", exc.reason)
        payload.setdefault("dry_run", ctx.dry_run)
        ctx.emit("warn", "account_risk_block", "Blocked by account risk", payload)
    except RiskLimitTrip:
        _GUARD_CTX.reset(token)
        return

    # Persist updated candle history + minimal strategy state back into runtime.
    try:
        hist_after = list(getattr(strategy.state, "price_history", []) or [])
        max_keep = min(max(len(hist_after), 500), 250_000)
        _set_runtime_candle_history(runtime, hist_after, max_len=max_keep)
    except Exception:
        pass
    try:
        runtime["strategy_state"] = _serialize_strategy_state(strategy)
    except Exception:
        pass

    runtime["last_candle_ts_ms"] = int(candle_ts_ms)
    runtime["last_bar_ts_ms"] = int(candle_ts_ms)
    runtime["bar_index"] = int(bar_index + 1)
    ctx.config_dict["_runtime"] = runtime
    try:
        _save_bot_config(ctx.conn, int(ctx.bot_id), ctx.config_dict)
    except Exception:
        pass

    ctx.emit("info", "bar", "Processed bar", {"ts_ms": int(candle_ts_ms), "close": float(bar.close or 0.0), "dry_run": ctx.dry_run})
    _GUARD_CTX.reset(token)


def _process_job(conn: sqlite3.Connection, job: Dict[str, Any], *, dry_run: bool, worker_id: str) -> None:
    bot_id = job.get("bot_id")
    if bot_id is None:
        update_job(conn, job.get("id"), status="error", error_text="live_bot job missing bot_id", finished_at=_now_iso())
        return

    bot = get_bot(conn, int(bot_id))
    if not bot:
        update_job(conn, job.get("id"), status="error", error_text="bot not found", finished_at=_now_iso())
        return

    # Load account context (label/status/risk limits) for guardrails.
    try:
        bot_user_id = str(bot.get("user_id") or "admin@local")
        ctx_row = get_bot_context(conn, bot_user_id, int(bot_id))
        account_row = ctx_row.get("account") if isinstance(ctx_row, dict) else None
    except Exception:
        account_row = None

    desired_status = bot.get("desired_status") or "running"
    job_status = job.get("status")
    job_id = job.get("id")

    global_settings = get_app_settings(conn)
    live_trading_enabled = bool((global_settings or {}).get("live_trading_enabled", False))

    def _emit(level: str, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
        enriched = dict(payload or {})
        enriched.setdefault("bot_id", int(bot_id))
        enriched.setdefault("job_id", job_id)
        enriched.setdefault("timestamp", _now_iso())
        add_bot_event(conn, int(bot_id), level, event_type, message, enriched)

    # Lease renewal: if we can't renew, another worker reclaimed it or the job changed.
    # Stop immediately to avoid duplicate active processing.
    try:
        if not _maybe_renew_job_lease(
            conn=conn,
            job=job,
            worker_id=str(worker_id),
            lease_s=float(JOB_LEASE_S),
            renew_every_s=float(JOB_LEASE_RENEW_EVERY_S),
            emit=lambda lvl, et, msg, payload=None: _emit(lvl, et, msg, payload),
        ):
            try:
                set_bot_status(conn, int(bot_id), heartbeat_msg="Job lease lost")
            except Exception:
                pass
            return
    except Exception:
        # Renewal failures should not crash the worker loop.
        pass

    if job_status == "queued":
        update_job(conn, job_id, status="running", started_at=_now_iso(), message=f"Starting live bot ({worker_id})", worker_id=worker_id)
        bot_status = "paused" if desired_status == "paused" else "running"
        set_bot_status(conn, int(bot_id), status=bot_status, heartbeat_msg="Bot starting")
        job_status = "running"

    # Stop conditions
    if job_status == "cancel_requested" or desired_status == "stopped":
        set_bot_status(conn, int(bot_id), status="stopped", desired_status="stopped", heartbeat_msg="Stopped by request")
        update_job(conn, job_id, status="finished", finished_at=_now_iso(), message="Stopped by user")
        return

    # Pause handling
    desired_action_early = (bot.get("desired_action") or "").strip().lower()
    if desired_status == "paused" and desired_action_early != "flatten_now":
        set_bot_status(conn, int(bot_id), status="paused", heartbeat_msg="Paused")
        update_job(conn, job_id, status="running", message="Paused", progress=0.0)
        return

    # Global live trading enabled gate (separate from kill switch).
    # When disabled, worker must not process bars or interact with broker/exchange.
    if not live_trading_enabled:
        config_dict = _load_config(bot)
        runtime = _ensure_runtime(config_dict.setdefault("_runtime", {}))
        if not bool(runtime.get("live_trading_disabled_warned", False)):
            _emit(
                "warn",
                "live_trading_disabled_block",
                "Worker blocked by global live trading toggle",
                {"worker_id": worker_id},
            )
            runtime["live_trading_disabled_warned"] = True
            config_dict["_runtime"] = runtime
            try:
                _save_bot_config(conn, int(bot_id), config_dict)
            except Exception:
                pass

        status_now = str(bot.get("status") or "running")
        set_bot_status(
            conn,
            int(bot_id),
            status=status_now,
            heartbeat_msg="Blocked: live trading disabled",
            heartbeat_at=_now_iso(),
        )
        update_job(conn, job_id, status="running", message="Blocked: live trading disabled", progress=0.0)
        return

    config_dict = _load_config(bot)
    runtime = _ensure_runtime(config_dict.setdefault("_runtime", {}))
    last_ts = runtime.get("last_candle_ts_ms")
    bar_index = int(runtime.get("bar_index", 0) or 0)
    run_id = str(job.get("run_id")) if job.get("run_id") is not None else None

    exchange_id = bot.get("exchange_id") or "woox"
    symbol = bot.get("symbol") or "PERP_BTC_USDT"
    timeframe = bot.get("timeframe") or "1m"

    # Strategy decisions are handled in bar_tick() on new closed candles.
    # Here we only need a config snapshot for broker settings.
    try:
        config_obj = _dragon_config_from_snapshot(config_dict)
    except Exception:
        config_obj = _dragon_config_from_snapshot({})

        base_url = os.environ.get("WOOX_BASE_URL", "https://api.woox.io")

        def _fail_job(code: str, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
            _emit(
                "error",
                event_type,
                message,
                {
                    "exchange_id": exchange_id,
                    "symbol": symbol,
                    "user_id": bot.get("user_id"),
                    **(payload or {}),
                },
            )
            # `last_error` is treated as a machine-friendly code; the human-readable guidance is stored
            # on jobs.error_text and bots.heartbeat_msg.
            set_bot_status(conn, int(bot_id), status="error", last_error=code, heartbeat_msg=message)
            update_job(conn, job_id, status="error", error_text=message, finished_at=_now_iso())

        def _fail_creds(message: str, payload: Optional[Dict[str, Any]] = None) -> None:
            _fail_job("credential_missing", "credential_missing", message, payload)

        def _fail_account(message: str, payload: Optional[Dict[str, Any]] = None) -> None:
            _fail_job("account_unavailable", "account_unavailable", message, payload)

        api_key: str
        api_secret: str
        if dry_run:
            api_key = "stub"
            api_secret = "stub"
        else:
            user_id = (str(bot.get("user_id") or "")).strip()
            if not user_id:
                _fail_account("Bot is missing user_id; cannot resolve trading account.")
                return

            ctx = get_bot_context(conn, user_id, int(bot_id))
            bot_ctx = ctx.get("bot") or {}
            account = ctx.get("account")
            if not account:
                _fail_account(
                    "Trading account is not set for this bot. Select an account in the UI and restart the bot.",
                    {"account_id": bot_ctx.get("account_id"), "credentials_id_legacy": bot_ctx.get("credentials_id")},
                )
                return
            account_status = str(account.get("status") or "").strip().lower()
            if account_status != "active":
                _fail_account(
                    f"Trading account is not active (status={account_status}). Enable the account or select another one.",
                    {"account_id": account.get("id"), "account_label": account.get("label")},
                )
                return
            cred_id_raw = account.get("credential_id")
            try:
                cred_id = int(cred_id_raw) if cred_id_raw is not None else None
            except (TypeError, ValueError):
                cred_id = None
            if cred_id is None:
                _fail_creds(
                    "Trading account has no credential_id set.",
                    {"account_id": account.get("id"), "account_label": account.get("label")},
                )
                return

            cred_row = get_credential(conn, user_id, int(cred_id))
            if not cred_row:
                _fail_creds(
                    "Credentials not found for this user (or access denied).",
                    {"credential_id": cred_id, "account_id": account.get("id")},
                )
                return
            api_key = str(cred_row.get("api_key") or "").strip()
            enc = str(cred_row.get("api_secret_enc") or "").strip()
            if not api_key or not enc:
                _fail_creds(
                    "Credentials record is incomplete (missing api_key/api_secret).",
                    {"credential_id": cred_id, "account_id": account.get("id")},
                )
                return
            try:
                api_secret = decrypt_str(enc)
            except CryptoConfigError:
                _fail_creds("Failed to decrypt API secret. Check DRAGON_MASTER_KEY.")
                return

        client: Any
        if dry_run:
            class _DryClient:
                def __init__(self):
                    self._orders: List[Dict[str, Any]] = []

                def set_position_mode(self, mode: str):
                    return {}

                def get_futures_leverage(self, symbol: str, *, margin_mode=None, position_mode=None):
                    # Pretend we can confirm hedge mode via leverage endpoint.
                    return {"positionMode": "HEDGE_MODE"}

                def get_positions(self, symbol: str):
                    # Return both hedge legs with zero holdings to satisfy strict hedge-mode checks.
                    return {
                        "rows": [
                            {"symbol": symbol, "positionSide": "LONG", "holding": 0, "averageOpenPrice": 0},
                            {"symbol": symbol, "positionSide": "SHORT", "holding": 0, "averageOpenPrice": 0},
                        ]
                    }

                def get_orders(self, symbol: str, status: str = "INCOMPLETE"):
                    return {"rows": list(self._orders)}

                def place_order(self, body: Dict[str, Any]):
                    oid = body.get("clientOrderId", f"dry-{len(self._orders) + 1}")
                    order = {
                        "orderId": f"dry-{oid}",
                        "clientOrderId": oid,
                        "status": "INCOMPLETE",
                        "side": body.get("side"),
                        "positionSide": body.get("positionSide"),
                        "price": body.get("price"),
                        "quantity": body.get("quantity"),
                        "type": body.get("type"),
                    }
                    self._orders.append(order)
                    return {"orderId": order["orderId"], "clientOrderId": oid}

                def cancel_order(self, order_id, symbol: str, client_order_id=None):
                    for ord_obj in self._orders:
                        if ord_obj.get("orderId") == order_id or ord_obj.get("clientOrderId") == client_order_id:
                            ord_obj["status"] = "CANCELLED"
                    return {}

                def get_orderbook(self, symbol: str, depth: int = 1):
                    return {"bids": [[100.0, 1]], "asks": [[101.0, 1]]}

                def get_income_history(self, symbol: str, *, start_time_ms=None, end_time_ms=None, limit=None):
                    return {"rows": []}

                def get_funding_history(self, symbol: str, *, start_time_ms=None, end_time_ms=None, limit=None):
                    return {"rows": []}

                def set_leverage(self, symbol: str, leverage: int):
                    return {}

            client = _DryClient()
        else:
            client = WooXClient(api_key=api_key, api_secret=api_secret, base_url=base_url)
            try:
                # Source of truth is bot.account_id -> trading_accounts.credential_id.
                touch_credential_last_used(conn, str(bot.get("user_id") or "").strip(), int(cred_id))
            except Exception:
                pass

        def _extract_positions_rows(payload: Any) -> List[Dict[str, Any]]:
            if isinstance(payload, dict):
                raw_rows = payload.get("positions") or payload.get("rows") or payload.get("data") or []
                if isinstance(raw_rows, list):
                    return [r for r in raw_rows if isinstance(r, dict)]
                return []
            if isinstance(payload, list):
                return [r for r in payload if isinstance(r, dict)]
            return []

        account_id_for_cache = 0
        try:
            if not dry_run and isinstance(locals().get("account"), dict) and locals().get("account").get("id") is not None:  # type: ignore[union-attr]
                account_id_for_cache = int(locals().get("account").get("id"))  # type: ignore[union-attr]
        except Exception:
            account_id_for_cache = 0

        def _detect_hedge_mode() -> tuple[bool, str, Dict[str, Any]]:
            """Detect hedge-mode using leverage endpoint first, then positions.

            Returns (ok, reason, evidence).
            - ok=True: confirmed hedge mode
            - ok=False, reason='one_way': confirmed one-way mode
            - ok=False, reason='unknown': cannot confirm from API responses (often when flat)
            """
            cache_key: Optional[tuple[int, str]] = None
            if int(account_id_for_cache) > 0:
                cache_key = (int(account_id_for_cache), str(symbol))

            now_s = time.time()
            if cache_key is not None:
                cached = _HEDGE_MODE_CHECK_CACHE.get(cache_key)
                if isinstance(cached, dict):
                    checked_at = float(cached.get("checked_at_s") or 0.0)
                    ttl_s = float(cached.get("ttl_s") or 0.0)
                    if checked_at > 0 and ttl_s > 0 and (now_s - checked_at) <= ttl_s:
                        ok_cached = bool(cached.get("ok"))
                        reason_cached = str(cached.get("reason") or "unknown")
                        evidence_cached = cached.get("evidence") if isinstance(cached.get("evidence"), dict) else {}
                        return (
                            ok_cached,
                            reason_cached,
                            {
                                **evidence_cached,
                                "cache": {
                                    "hit": True,
                                    "checked_at_s": checked_at,
                                    "ttl_s": ttl_s,
                                    "account_id": cache_key[0],
                                    "symbol": cache_key[1],
                                },
                            },
                        )

            evidence: Dict[str, Any] = {"symbol": symbol, "account_id": int(account_id_for_cache)}

            def _pos_mode_from_payload(obj: Any) -> Optional[str]:
                if isinstance(obj, dict):
                    # Common shapes: {positionMode: ...} or {data:{positionMode:...}}
                    pm = obj.get("positionMode")
                    if pm is not None:
                        return str(pm)
                    data = obj.get("data")
                    if isinstance(data, dict) and data.get("positionMode") is not None:
                        return str(data.get("positionMode"))
                return None

            def _needs_position_mode_probe(exc: Exception) -> bool:
                msg = str(exc or "").lower()
                if not msg:
                    return False
                if "positionmode" not in msg and "position_mode" not in msg:
                    return False
                return any(tok in msg for tok in ["missing", "required", "invalid", "parameter", "param"])

            def _cache_store(ok_val: bool, reason_val: str, ev: Dict[str, Any], ttl_s: float) -> None:
                if cache_key is None:
                    return
                _HEDGE_MODE_CHECK_CACHE[cache_key] = {
                    "ok": bool(ok_val),
                    "reason": str(reason_val),
                    "evidence": dict(ev),
                    "checked_at_s": float(now_s),
                    "ttl_s": float(ttl_s),
                }

            # 1) Preferred signal: /v3/futures/leverage (positionMode)
            leverage_attempts: list[dict[str, Any]] = []

            probe_needed = False
            try:
                lev = client.get_futures_leverage(symbol, margin_mode="CROSS")
                leverage_attempts.append(
                    {
                        "step": "single",
                        "ok": True,
                        "keys": sorted(list(lev.keys())) if isinstance(lev, dict) else None,
                    }
                )
                pm_raw = _pos_mode_from_payload(lev)
                if pm_raw is not None:
                    pm = str(pm_raw).strip().upper()
                    evidence["leverage_positionMode"] = pm
                    if pm == "HEDGE_MODE":
                        evidence["leverage_attempts"] = leverage_attempts
                        _cache_store(True, "hedge_confirmed", evidence, _HEDGE_MODE_CHECK_TTL_OK_S)
                        return True, "hedge_confirmed", evidence
                    if pm in {"ONE_WAY", "ONEWAY", "ONE_WAY_MODE", "BOTH"}:
                        evidence["leverage_attempts"] = leverage_attempts
                        _cache_store(False, "one_way", evidence, _HEDGE_MODE_CHECK_TTL_OK_S)
                        return False, "one_way", evidence
                    # Unknown value -> fall back to positions.
                else:
                    probe_needed = True
            except Exception as exc:
                leverage_attempts.append({"step": "single", "ok": False, "error": str(exc)})
                probe_needed = _needs_position_mode_probe(exc)

            if probe_needed:
                for i, pos_mode_query in enumerate(("HEDGE_MODE", "ONE_WAY")):
                    if i > 0:
                        time.sleep(_HEDGE_MODE_PROBE_COOLDOWN_S)
                    try:
                        lev = client.get_futures_leverage(symbol, margin_mode="CROSS", position_mode=pos_mode_query)
                        leverage_attempts.append(
                            {
                                "step": "probe",
                                "query": pos_mode_query,
                                "ok": True,
                                "keys": sorted(list(lev.keys())) if isinstance(lev, dict) else None,
                            }
                        )
                        pm_raw = _pos_mode_from_payload(lev)
                        if pm_raw is None:
                            continue
                        pm = str(pm_raw).strip().upper()
                        evidence["leverage_positionMode"] = pm
                        evidence["leverage_query"] = pos_mode_query
                        evidence["leverage_attempts"] = leverage_attempts
                        if pm == "HEDGE_MODE":
                            _cache_store(True, "hedge_confirmed", evidence, _HEDGE_MODE_CHECK_TTL_OK_S)
                            return True, "hedge_confirmed", evidence
                        if pm in {"ONE_WAY", "ONEWAY", "ONE_WAY_MODE", "BOTH"}:
                            _cache_store(False, "one_way", evidence, _HEDGE_MODE_CHECK_TTL_OK_S)
                            return False, "one_way", evidence
                    except Exception as exc:
                        leverage_attempts.append({"step": "probe", "query": pos_mode_query, "ok": False, "error": str(exc)})

            evidence["leverage_attempts"] = leverage_attempts

            # 2) Fallback signal: get_positions(symbol)
            try:
                payload = client.get_positions(symbol)
                rows = _extract_positions_rows(payload)
            except Exception as exc:
                evidence["positions_error"] = str(exc)
                return False, "unknown", evidence

            observed: List[str] = []
            for row in rows:
                pos_side = str(row.get("positionSide") or row.get("position_side") or row.get("side") or "").upper().strip()
                if pos_side:
                    observed.append(pos_side)
            observed_set = {s for s in observed if s in {"LONG", "SHORT", "BOTH"}}
            evidence["observed_sides"] = sorted(list(observed_set))
            evidence["raw_position_sides"] = observed[:20]

            if "BOTH" in observed_set:
                _cache_store(False, "one_way", evidence, _HEDGE_MODE_CHECK_TTL_OK_S)
                return False, "one_way", evidence
            if "LONG" in observed_set and "SHORT" in observed_set:
                _cache_store(True, "hedge_confirmed", evidence, _HEDGE_MODE_CHECK_TTL_OK_S)
                return True, "hedge_confirmed", evidence

            # Flat/empty/only one side -> unknown (do not trade)
            _cache_store(False, "unknown", evidence, _HEDGE_MODE_CHECK_TTL_UNKNOWN_S)
            return False, "unknown", evidence

        ok, reason, evidence = _detect_hedge_mode()
        try:
            health = runtime.setdefault("health", {}) if isinstance(runtime, dict) else {}
            health["hedge_mode"] = {
                "ok": bool(ok),
                "reason": str(reason or "unknown"),
                "checked_at": _now_iso(),
            }
            runtime["health"] = health
            config_dict["_runtime"] = runtime
            _save_bot_config(conn, int(bot_id), config_dict)
        except Exception:
            pass
        if not ok:
            if reason == "one_way":
                guidance = (
                    "This account appears to be in one-way mode. Live trading requires Hedge/Dual-side mode so LONG and SHORT legs are tracked "
                    "independently. In WooX futures/perps settings, switch Position Mode to Hedge (Dual Side), then restart the bot."
                )
                _fail_job("hedge_mode_required", "hedge_mode_required", guidance, evidence)
                return
            guidance = (
                "Unable to confirm hedge mode from API responses while flat. Please ensure WooX Position Mode is set to Hedge (Dual Side) and retry."
            )
            _fail_job("hedge_mode_unknown", "hedge_mode_unknown", guidance, evidence)
            return

        # Optional futures leverage apply (once per bot unless leverage changes)
        try:
            _maybe_apply_futures_leverage(
                client=client,
                symbol=symbol,
                config_dict=config_dict,
                runtime=runtime,
                dry_run=dry_run,
                emit=_emit,
            )
        except Exception as exc:
            _emit("warn", "leverage_apply_warning", "Leverage apply step failed", {"error": str(exc)})

        hedge_event_emitted = False

        def _broker_event(level: str, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
            nonlocal hedge_event_emitted
            if str(event_type or "").strip().lower() == "hedge_mode_required":
                hedge_event_emitted = True
            _emit(level, event_type, message, payload or {})

        broker = WooXBroker(
            client,
            symbol=symbol,
            bot_id=f"live-{bot_id}",
            account_id=int(account_row.get("id")) if isinstance(account_row, dict) and account_row.get("id") is not None else None,
            prefer_bbo_maker=config_obj.general.prefer_bbo_maker,
            bbo_level=config_obj.general.bbo_queue_level,
            primary_position_side=((config_dict.get("_trade") or {}).get("primary_position_side") or "LONG") if isinstance(config_dict, dict) else "LONG",
            require_hedge_mode=True,
            auto_sync=False,
        )
        broker.set_event_logger(_broker_event)

        # Rehydrate persisted order intents (parked/active dynamic orders) into broker so:
        # - strategy pending_*_order_id references stay valid across restarts
        # - sync_orders can match clientOrderId to stable local_id
        intent_rows = []
        try:
            intent_rows = list_bot_order_intents(conn, int(bot_id), statuses=["parked", "active"], kind="dynamic_limit", limit=2000)
            _rehydrate_intent_orders_into_broker(broker, intent_rows)
        except Exception:
            intent_rows = []
        try:
            broker.sync_positions()
            broker.sync_orders()
        except WooXBrokerError as exc:
            # Broker-level hedge checks are a secondary safety net; worker enforces a stricter gate earlier.
            message = str(exc) or "WooX hedge mode required"
            if not hedge_event_emitted:
                _emit("error", "hedge_mode_required", message, {"symbol": symbol})
            set_bot_status(conn, int(bot_id), status="error", last_error="hedge_mode_required", heartbeat_msg=message)
            update_job(conn, job_id, status="error", error_text=message, finished_at=_now_iso())
            return

        # --- Split loop scheduling (fast tick vs bar tick) ----------------
        tick_ctx = BotTickContext(
            conn=conn,
            bot=bot,
            job=job,
            bot_id=int(bot_id),
            job_id=int(job_id),
            worker_id=str(worker_id),
            dry_run=bool(dry_run),
            global_settings=global_settings or {},
            account_row=account_row if isinstance(account_row, dict) else None,
            config_dict=config_dict,
            runtime=runtime,
            config_obj=config_obj,
            exchange_id=str(exchange_id),
            symbol=str(symbol),
            timeframe=str(timeframe),
            run_id=run_id,
            client=client,
            emit=lambda lvl, et, msg, payload=None: _emit(lvl, et, msg, payload),
        )

        sched = _BOT_SCHED.setdefault(int(bot_id), {"next_fast_s": 0.0, "next_bar_check_s": 0.0})
        now_s = time.time()
        fast_s = float(_fast_interval_s(global_settings or {}))
        bar_check_s = min(2.0, fast_s)

        if now_s >= float(sched.get("next_fast_s") or 0.0):
            try:
                fast_tick(tick_ctx, broker, datetime.now(timezone.utc))
            except Exception as exc:
                err_text = _exc_text()
                _emit("error", "fast_tick_exception", "Fast tick failed", {"error": str(exc), "traceback_tail": err_text[-2000:]})
            sched["next_fast_s"] = float(now_s + fast_s)

        if now_s >= float(sched.get("next_bar_check_s") or 0.0):
            try:
                candle = _fetch_latest_closed_candle(exchange_id, symbol, timeframe)
            except Exception:
                candle = None
            if candle is not None:
                try:
                    bar_tick(tick_ctx, broker, candle)
                except Exception as exc:
                    err_text = _exc_text()
                    _emit("error", "bar_tick_exception", "Bar tick failed", {"error": str(exc), "traceback_tail": err_text[-2000:]})
                    # Non-fatal: keep job running, but surface the error in bot health.
                    try:
                        update_bot(conn, int(bot_id), last_error="bar_tick_exception")
                        set_bot_status(conn, int(bot_id), status=str(bot.get("status") or "running"), heartbeat_msg="Bar tick error")
                        update_job(conn, job_id, status="running", message="Bar tick error", progress=0.0)
                    except Exception:
                        pass
            sched["next_bar_check_s"] = float(now_s + bar_check_s)

        # This function is called frequently by the worker poll loop.
        # We intentionally do only a small slice of work per invocation.
        return

        # Mark price preference order:
        # 1) ccxt ticker (markPrice/last)
        # 2) orderbook mid (best bid/ask)
        # 3) candle close
        mark = None
        try:
            t = load_ccxt_ticker(exchange_id, symbol) or {}
            info = t.get("info") if isinstance(t, dict) else None
            if isinstance(info, dict):
                mark = (
                    _safe_float(info.get("markPrice"))
                    or _safe_float(info.get("mark_price"))
                    or _safe_float(info.get("mark"))
                )
            if mark is None and isinstance(t, dict):
                mark = (
                    _safe_float(t.get("mark"))
                    or _safe_float(t.get("last"))
                    or _safe_float(t.get("close"))
                )
        except Exception:
            mark = None

        if mark is None:
            try:
                best = broker.get_best_bid_ask()
                if best:
                    mark = (float(best[0]) + float(best[1])) / 2.0
            except Exception:
                mark = None

        if mark is None:
            mark = float(candle.close or 0.0)

        runtime["last_price"] = mark
        runtime["last_mark_ts"] = _now_iso()

        risk_cfg = config_dict.get("_risk", {}) if isinstance(config_dict, dict) else {}

        # Setup guard context for all broker operations in this loop.
        try:
            price_estimate = float(runtime.get("last_price") or candle.close or 0.0)
        except (TypeError, ValueError):
            price_estimate = float(candle.close or 0.0)
        guard_ctx = TradingGuardContext(
            conn=conn,
            emit_fn=_emit,
            bot_row=bot,
            job_row=job,
            global_settings=global_settings,
            risk_cfg=risk_cfg if isinstance(risk_cfg, dict) else {},
            broker=broker,
            price_estimate=price_estimate,
            runtime=runtime if isinstance(runtime, dict) else None,
            account_row=account_row if isinstance(account_row, dict) else None,
        )
        token = _GUARD_CTX.set(guard_ctx)

        # --- Funding (best-effort; perps only) -------------------------
        try:
            since_ms = runtime.get("last_funding_ts_ms")
            try:
                since_ms_int = int(since_ms) if since_ms is not None else None
            except (TypeError, ValueError):
                since_ms_int = None

            funding_payload = None
            try:
                funding_payload = client.get_income_history(symbol, start_time_ms=since_ms_int, limit=50)
            except Exception:
                try:
                    funding_payload = client.get_funding_history(symbol, start_time_ms=since_ms_int, limit=50)
                except Exception as exc:
                    funding_payload = None
                    _emit("warn", "funding_fetch_failed", "Funding endpoint failed", {"error": str(exc)})

            funding_rows = _parse_funding_rows(funding_payload)
            max_seen = since_ms_int
            for row in funding_rows:
                ts_ms = _funding_row_ts_ms(row)
                if ts_ms is None:
                    continue
                if since_ms_int is not None and ts_ms <= since_ms_int:
                    continue
                amt = _funding_row_amount(row)
                if amt is None:
                    continue
                ref_id = _funding_row_ref_id(row)
                event_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
                inserted = add_ledger_row(
                    conn,
                    bot_id=int(bot_id),
                    event_ts=event_ts,
                    kind="funding",
                    symbol=symbol,
                    funding=float(amt),
                    ref_id=str(ref_id) if ref_id is not None else None,
                    meta={"raw": row},
                )
                if inserted:
                    _emit("info", "funding_recorded", "Funding recorded", {"amount": float(amt), "ref_id": ref_id, "ts_ms": ts_ms})
                max_seen = ts_ms if max_seen is None else max(max_seen, ts_ms)
            if max_seen is not None:
                runtime["last_funding_ts_ms"] = int(max_seen)
        except Exception as exc:
            _emit("warn", "funding_warning", "Funding processing failed", {"error": str(exc)})

        desired_action = (bot.get("desired_action") or "").strip().lower()
        if desired_action == "flatten_now":
            if desired_status == "stopped":
                _emit(
                    "warn",
                    "flatten_blocked_stopped",
                    "Flatten blocked: desired_status=stopped",
                    {"dry_run": dry_run, "desired_status": desired_status},
                )
                update_bot(conn, int(bot_id), desired_action=None)
                set_bot_status(conn, int(bot_id), status="stopped", desired_status="stopped", heartbeat_msg="Flatten blocked (stopped)")
                update_job(conn, job_id, status="running", message="Flatten blocked (bot stopped)", progress=0.0)
                _GUARD_CTX.reset(token)
                return
            try:
                _emit("warn", "flatten_started", "Flatten started (guarded)", {"dry_run": dry_run})
                broker.sync_orders()

                # Cancel exchange-visible open orders
                to_cancel: List[int] = []
                for oid, ord_obj in getattr(broker, "open_orders", {}).items():
                    status = getattr(ord_obj, "status", None)
                    status_name = _status_name(status)
                    if status_name not in {"OPEN", "PARTIAL"}:
                        continue
                    activation = getattr(ord_obj, "activation_state", None)
                    if activation == OrderActivationState.PARKED or str(activation).upper() == "PARKED":
                        continue
                    ext_id = getattr(ord_obj, "external_id", None)
                    client_oid = getattr(ord_obj, "client_order_id", None)
                    if not (ext_id or client_oid):
                        continue
                    to_cancel.append(int(oid))

                cancelled: List[int] = []
                for oid in to_cancel:
                    safe_cancel_order(
                        bot_row=bot,
                        job_row=job,
                        global_settings=global_settings,
                        action="flatten_cancel_order",
                        delegate=lambda oid=oid: broker.cancel_order(int(oid)),
                    )
                    cancelled.append(int(oid))

                _emit(
                    "info",
                    "flatten_cancel_summary",
                    "Cancelled open orders",
                    {"cancel_attempted": len(to_cancel), "cancelled": len(cancelled), "order_ids": cancelled[:50], "dry_run": dry_run},
                )

                broker.sync_positions()
                best = None
                try:
                    best = broker.get_best_bid_ask()
                except Exception:
                    best = None
                bid = float(best[0]) if best else price_estimate
                ask = float(best[1]) if best else price_estimate

                # Close LONG leg
                long_pos = broker.get_position(Side.LONG)
                long_holding = float(getattr(long_pos, "holding", 0.0) or 0.0) if long_pos is not None else 0.0
                if long_holding > 0:
                    safe_place_order(
                        bot_row=bot,
                        job_row=job,
                        global_settings=global_settings,
                        action="flatten_close_long",
                        qty=long_holding,
                        price_estimate=bid,
                        position_side="LONG",
                        reduce_only=True,
                        delegate=lambda: broker.place_market(
                            order_side="SELL",
                            position_side="LONG",
                            price=bid,
                            size=long_holding,
                            note="flatten_now",
                            reduce_only=True,
                        ),
                    )
                    _emit("warn", "flatten_position_close", "Closed LONG leg", {"size": long_holding, "price": bid, "dry_run": dry_run})

                # Close SHORT leg
                short_pos = broker.get_position(Side.SHORT)
                short_holding = float(getattr(short_pos, "holding", 0.0) or 0.0) if short_pos is not None else 0.0
                if short_holding > 0:
                    safe_place_order(
                        bot_row=bot,
                        job_row=job,
                        global_settings=global_settings,
                        action="flatten_close_short",
                        qty=short_holding,
                        price_estimate=ask,
                        position_side="SHORT",
                        reduce_only=True,
                        delegate=lambda: broker.place_market(
                            order_side="BUY",
                            position_side="SHORT",
                            price=ask,
                            size=short_holding,
                            note="flatten_now",
                            reduce_only=True,
                        ),
                    )
                    _emit("warn", "flatten_position_close", "Closed SHORT leg", {"size": short_holding, "price": ask, "dry_run": dry_run})

                broker.sync_positions()
                remaining = {
                    "LONG": float(getattr(broker.get_position(Side.LONG), "holding", 0.0) or 0.0) if broker.get_position(Side.LONG) else 0.0,
                    "SHORT": float(getattr(broker.get_position(Side.SHORT), "holding", 0.0) or 0.0) if broker.get_position(Side.SHORT) else 0.0,
                }
                _emit("warn", "flatten_completed", "Flatten completed (guarded)", {"remaining": remaining, "dry_run": dry_run})
                update_bot(conn, int(bot_id), desired_action=None)
                post_status = "paused" if desired_status == "paused" else "running"
                set_bot_status(conn, int(bot_id), status=post_status, heartbeat_msg="Flatten completed")
                update_job(conn, job_id, status="running", message="Flatten completed", progress=0.0)
                _GUARD_CTX.reset(token)
                return
            except KillSwitchBlocked as exc:
                _emit("warn", "flatten_failed", "Flatten blocked by kill switch", {"action": exc.action, "dry_run": dry_run})
                update_bot(conn, int(bot_id), desired_action=None)
                # Intentional: do NOT set bot/job to error
                set_bot_status(conn, int(bot_id), status=("paused" if desired_status == "paused" else "running"), heartbeat_msg="Blocked by kill switch")
                update_job(conn, job_id, status="running", message="Flatten blocked by kill switch", progress=0.0)
                _GUARD_CTX.reset(token)
                return
            except RiskLimitTrip as exc:
                _GUARD_CTX.reset(token)
                return
            except Exception as exc:
                err_text = _exc_text()
                _emit(
                    "error",
                    "flatten_failed",
                    "Flatten failed",
                    {"error": str(exc), "traceback_tail": err_text[-2000:], "dry_run": dry_run},
                )
                set_bot_status(conn, int(bot_id), status="error", last_error=err_text, heartbeat_msg="Flatten failed")
                update_job(conn, job_id, status="error", error_text=err_text, finished_at=_now_iso())
                _GUARD_CTX.reset(token)
                return

        balance_override = None
        if isinstance(risk_cfg, dict):
            balance_override = risk_cfg.get("balance_override") or risk_cfg.get("balance")
        if balance_override is None:
            env_balance = os.environ.get("DRAGON_LIVE_BALANCE") or os.environ.get("WOOX_BALANCE_OVERRIDE")
            if env_balance is not None:
                balance_override = env_balance
        caps = _broker_caps(broker)
        try:
            if balance_override is not None and caps.has_balance:
                if not _safe_set_float(broker, "balance", balance_override):
                    raise ValueError("balance override rejected")
        except (TypeError, ValueError):
            logger.warning("Invalid balance override provided; leaving balance at default")
            _emit(
                "warn",
                "config_override_invalid",
                "Invalid balance override provided; leaving balance at default",
                {"value": balance_override},
            )

        fee_override = None
        if isinstance(risk_cfg, dict):
            fee_override = risk_cfg.get("fee_rate")
        if fee_override is None:
            env_fee = os.environ.get("DRAGON_FEE_RATE")
            if env_fee is not None:
                fee_override = env_fee
        try:
            if fee_override is not None and caps.has_fee_rate:
                if not _safe_set_float(broker, "fee_rate", fee_override):
                    raise ValueError("fee_rate override rejected")
        except (TypeError, ValueError):
            logger.warning("Invalid fee override provided; leaving fee_rate at default")
            _emit(
                "warn",
                "config_override_invalid",
                "Invalid fee override provided; leaving fee_rate at default",
                {"value": fee_override},
            )

        sizing_mode = None
        try:
            sizing_mode = _safe_initial_entry_sizing_mode((config_dict.get("general") or {}).get("initial_entry_sizing_mode") if isinstance(config_dict, dict) else None)
        except Exception:
            sizing_mode = None
        if float(_safe_getattr(broker, "balance", 0.0) or 0.0) <= 0 and sizing_mode != InitialEntrySizingMode.FIXED_USD:
            warn_msg = "Broker balance is zero; strategy will skip entries. Set DRAGON_LIVE_BALANCE or risk.balance_override."
            _emit("warn", "balance", warn_msg, {})

        # Risk guardrails
        max_orders = risk_cfg.get("max_open_orders")
        if max_orders is not None and len(getattr(broker, "open_orders", {})) > int(max_orders):
            msg = f"Risk trip: open_orders>{max_orders}"
            _emit("error", "risk", msg, {"open_orders": len(getattr(broker, 'open_orders', {}))})
            set_bot_status(conn, int(bot_id), status="error", last_error=msg, heartbeat_msg=msg)
            update_job(conn, job_id, status="error", error_text=msg, finished_at=_now_iso())
            return
        max_pos = risk_cfg.get("max_position_size_per_side")
        if max_pos is not None and getattr(broker, "position", None):
            pos_size = float(getattr(broker.position, "size", 0.0) or 0.0)
            if abs(pos_size) > float(max_pos):
                msg = f"Risk trip: pos_size>{max_pos}"
                _emit("error", "risk", msg, {"position_size": pos_size})
                set_bot_status(conn, int(bot_id), status="error", last_error=msg, heartbeat_msg=msg)
                update_job(conn, job_id, status="error", error_text=msg, finished_at=_now_iso())
                return
        max_notional = risk_cfg.get("max_notional_per_side")
        if max_notional is not None and getattr(broker, "position", None):
            pos_size = float(getattr(broker.position, "size", 0.0) or 0.0)
            notional = abs(pos_size * candle.close)
            if notional > float(max_notional):
                msg = f"Risk trip: notional>{max_notional}"
                _emit("error", "risk", msg, {"notional": notional})
                set_bot_status(conn, int(bot_id), status="error", last_error=msg, heartbeat_msg=msg)
                update_job(conn, job_id, status="error", error_text=msg, finished_at=_now_iso())
                return

        order_snapshot_before = _order_state_snapshot(broker)
        # Apply any pending dynamic intent activation/parking based on this bar's reference price.
        try:
            _apply_dynamic_order_intents(
                conn=conn,
                broker=broker,
                intents=intent_rows,
                ref_price=float(candle.close or 0.0),
                bot_row=bot,
                job_row=job,
                global_settings=global_settings,
                dry_run=dry_run,
                emit=_emit,
            )
        except Exception:
            pass

        strategy = DragonDcaAtrStrategy(config=config_obj)
        try:
            _restore_strategy_state(strategy, runtime.get("strategy_state"))
        except Exception:
            pass
        try:
            if history:
                strategy.prime_history(history)
        except Exception:
            pass
        guarded_broker = GuardedStrategyBroker(broker, bot, job, global_settings)
        try:
            strategy.on_bar(bar_index, candle, guarded_broker)
        except KillSwitchBlocked as exc:
            _emit("warn", "kill_switch_block", "Blocked by kill switch", {"action": exc.action, "dry_run": dry_run})
        except AccountRiskBlocked as exc:
            payload = dict(exc.payload or {})
            payload.setdefault("action", exc.action)
            payload.setdefault("reason", exc.reason)
            payload.setdefault("dry_run", dry_run)
            _emit("warn", "account_risk_block", "Blocked by account risk", payload)
        except RiskLimitTrip:
            _GUARD_CTX.reset(token)
            return

        # Strategy may have created new dynamic intents; reload and apply activation/parking
        # before syncing orders so snapshots reflect the latest exchange state.
        try:
            intent_rows = list_bot_order_intents(conn, int(bot_id), statuses=["parked", "active"], kind="dynamic_limit", limit=2000)
            _rehydrate_intent_orders_into_broker(broker, intent_rows)
            _apply_dynamic_order_intents(
                conn=conn,
                broker=broker,
                intents=intent_rows,
                ref_price=float(candle.close or 0.0),
                bot_row=bot,
                job_row=job,
                global_settings=global_settings,
                dry_run=dry_run,
                emit=_emit,
            )
        except Exception:
            pass

        broker.sync_orders()
        order_snapshot_after = _order_state_snapshot(broker)

        # Best-effort: terminalize intents when their mapped order reaches a terminal status.
        try:
            for row in list(intent_rows or []):
                intent_key = str(row.get("intent_key") or "")
                if not intent_key:
                    continue
                try:
                    local_id = int(row.get("local_id"))
                except (TypeError, ValueError):
                    continue
                ord_obj = getattr(broker, "open_orders", {}).get(local_id)
                if ord_obj is None:
                    continue
                status = getattr(ord_obj, "status", None)
                status_name = _status_name(status)
                if status_name in {"FILLED", "CANCELLED", "REJECTED"}:
                    update_bot_order_intent(
                        conn,
                        bot_id=int(bot_id),
                        intent_key=intent_key,
                        status=status_name.lower(),
                    )
                else:
                    # Keep exchange ids up to date for active intents
                    ext_id = getattr(ord_obj, "external_id", None)
                    client_oid = getattr(ord_obj, "client_order_id", None)
                    if ext_id or client_oid:
                        update_bot_order_intent(
                            conn,
                            bot_id=int(bot_id),
                            intent_key=intent_key,
                            external_order_id=ext_id,
                            client_order_id=client_oid,
                        )
        except Exception:
            pass

        # Persist updated candle history back into runtime for next cycle.
        try:
            hist_after = list(getattr(strategy.state, "price_history", []) or [])
            # Strategy already trims internally; still guard with a cap.
            max_keep = max(len(hist_after), 500)
            max_keep = min(max_keep, 250_000)
            _set_runtime_candle_history(runtime, hist_after, max_len=max_keep)
        except Exception:
            pass

        # Persist minimal strategy state to keep pending order ids stable across restarts.
        try:
            runtime["strategy_state"] = _serialize_strategy_state(strategy)
        except Exception:
            pass

        def _on_fill(payload: Dict[str, Any], state: Dict[str, Any], prev_state: Dict[str, Any]) -> None:
            client_oid = (
                payload.get("client_order_id")
                or state.get("client_order_id")
                or state.get("external_id")
                or state.get("order_id")
            )
            if not client_oid:
                return
            prev_filled = float(prev_state.get("filled_size") or 0.0) if prev_state else 0.0
            curr_filled = float(state.get("filled_size") or 0.0)
            if curr_filled <= prev_filled:
                return
            progress = runtime.setdefault("order_fill_progress", {})
            last_recorded = float(progress.get(client_oid) or 0.0)
            delta = max(0.0, curr_filled - last_recorded)
            if delta <= 0:
                return
            progress[client_oid] = curr_filled
            try:
                price_val = float(state.get("avg_fill_price") or state.get("price") or 0.0)
            except (TypeError, ValueError):
                return
            if price_val <= 0:
                return
            position_side = (state.get("position_side") or payload.get("position_side") or "").upper()
            if not position_side:
                return
            order_action = (state.get("order_action") or state.get("side") or payload.get("side") or "").upper()
            reduce_only = _is_reduce_only(order_action, position_side)
            note_val = state.get("note") or payload.get("note")
            dca_flag = _is_dca(note_val)
            closed_row = _apply_fill_to_runtime(
                runtime,
                position_side,
                order_action,
                delta,
                price_val,
                is_dca=dca_flag,
                reduce_only=reduce_only,
                note=note_val,
                symbol=symbol,
                exchange_id=exchange_id,
                run_id=run_id,
            )
            fill_row = {
                "bot_id": int(bot_id),
                "run_id": run_id,
                "symbol": symbol,
                "exchange_id": exchange_id,
                "position_side": position_side,
                "order_action": order_action,
                "client_order_id": client_oid,
                "external_order_id": state.get("external_id"),
                "filled_qty": delta,
                "avg_fill_price": price_val,
                "fee_paid": None,
                "fee_asset": None,
                "is_reduce_only": reduce_only,
                "is_dca": dca_flag,
                "note": note_val,
                "event_ts": state.get("updated_at") or _now_iso(),
            }
            add_bot_fill(conn, fill_row)

            # Ledger rows: fill + fee (idempotent)
            fill_ref = None
            ext_id = state.get("external_id")
            if ext_id:
                fill_ref = f"fill-{ext_id}-{client_oid}-{state.get('updated_at') or ''}-{price_val}-{delta}"
            else:
                fill_ref = f"fill-{client_oid}-{state.get('updated_at') or ''}-{price_val}-{delta}"

            add_ledger_row(
                conn,
                bot_id=int(bot_id),
                event_ts=fill_row["event_ts"],
                kind="fill",
                symbol=symbol,
                side=order_action,
                position_side=position_side,
                qty=float(delta),
                price=float(price_val),
                ref_id=fill_ref,
                meta={"client_order_id": client_oid, "external_order_id": ext_id, "note": note_val},
            )

            # Fee: use exchange fee if present, else estimate from configured fee_rate
            fee_amt = None
            fee_meta = {"client_order_id": client_oid, "external_order_id": ext_id}
            if state.get("fee_paid") is not None:
                try:
                    fee_amt = float(state.get("fee_paid"))
                except (TypeError, ValueError):
                    fee_amt = None
            if fee_amt is None:
                try:
                    fee_rate = float(_safe_getattr(broker, "fee_rate", 0.0) or 0.0)
                except (TypeError, ValueError):
                    fee_rate = 0.0
                fee_amt = abs(float(delta) * float(price_val)) * fee_rate
                fee_meta["fee_estimated"] = True
                fee_meta["fee_rate"] = fee_rate

            add_ledger_row(
                conn,
                bot_id=int(bot_id),
                event_ts=fill_row["event_ts"],
                kind="fee",
                symbol=symbol,
                side=order_action,
                position_side=position_side,
                qty=float(delta),
                price=float(price_val),
                fee=float(fee_amt or 0.0),
                ref_id=f"fee-{fill_ref}",
                meta=fee_meta,
            )
            if closed_row:
                closed_row["bot_id"] = int(bot_id)
                create_position_history(conn, closed_row)

                # Realized PnL ledger row (best-effort)
                try:
                    realized_val = float(closed_row.get("realized_pnl") or 0.0)
                except (TypeError, ValueError):
                    realized_val = 0.0
                add_ledger_row(
                    conn,
                    bot_id=int(bot_id),
                    event_ts=_now_iso(),
                    kind="adjustment",
                    symbol=symbol,
                    pnl=realized_val,
                    ref_id=f"pnl-{fill_ref}",
                    meta={"source": "position_close", "closed_row": closed_row},
                )

        _emit_order_diffs(order_snapshot_before, order_snapshot_after, _emit, _on_fill)

        _emit(
            "info",
            "bar",
            "Processed bar",
            {"ts_ms": candle_ts_ms, "close": candle.close, "dry_run": dry_run},
        )

        runtime["last_candle_ts_ms"] = candle_ts_ms
        runtime["bar_index"] = bar_index + 1
        config_dict["_runtime"] = runtime

        # Persist exchange positions snapshot (best-effort).
        # This is intentionally the raw WooX `get_positions()` payload so the UI can
        # render exactly what the venue reports (both LONG/SHORT).
        positions_snapshot: Dict[str, Any] = {}
        try:
            payload = client.get_positions(symbol)
            positions_snapshot = payload if isinstance(payload, dict) else {"raw": payload}
        except Exception as exc:
            _emit("warn", "positions_snapshot_failed", "Failed to fetch positions snapshot", {"error": str(exc)})
            positions_snapshot = {}

        positions_normalized = {}
        try:
            positions_normalized = normalize_positions_snapshot(positions_snapshot, _safe_float(runtime.get("last_price")))
        except Exception as exc:
            _emit("warn", "positions_normalize_failed", "Failed to normalize positions snapshot", {"error": str(exc)})
            positions_normalized = {}

        # Persist bot summary fields
        legs = runtime.get("legs", {})
        dca_fills_current = sum(int(v.get("dca_fills", 0) or 0) for v in legs.values() if float(v.get("size", 0.0) or 0.0) > 0)
        # Prefer ledger sums for realized/fees/funding totals (fail-soft)
        fees_total = 0.0
        funding_total = 0.0
        realized_total = 0.0
        try:
            ledger_totals = sum_ledger(conn, int(bot_id))
            fees_total = float(ledger_totals.get("fees_total", 0.0) or 0.0)
            funding_total = float(ledger_totals.get("funding_total", 0.0) or 0.0)
            realized_total = float(ledger_totals.get("realized_total", 0.0) or 0.0)
        except Exception as exc:
            # Only warn once per bot to avoid spamming events every loop.
            if not bool(runtime.get("ledger_totals_warning_emitted")):
                runtime["ledger_totals_warning_emitted"] = True
                _emit(
                    "warn",
                    "ledger_totals_unavailable",
                    "Ledger totals unavailable; defaulting fees/funding/realized to 0",
                    {"error": str(exc)},
                )

        # realized_pnl is the gross realized PnL; fees/funding are separate summary fields
        realized_pnl = realized_total
        mark_price = runtime.get("last_price")
        unrealized_pnl = _compute_unrealized(runtime, mark_price)

        # Futures-friendly ROI% on margin (best-effort).
        roi_pct_on_margin = None
        try:
            from project_dragon.metrics import compute_roi_pct_on_margin, get_effective_leverage

            net_after_costs = (float(realized_pnl or 0.0) + float(unrealized_pnl or 0.0)) - float(fees_total or 0.0) + float(
                funding_total or 0.0
            )
            notional = None
            try:
                totals = (positions_normalized or {}).get("totals") if isinstance(positions_normalized, dict) else None
                if isinstance(totals, dict):
                    notional = _safe_float(totals.get("notional"))
            except Exception:
                notional = None
            if notional is None:
                mp = _safe_float(mark_price)
                if mp is not None:
                    try:
                        legs_for_notional = runtime.get("legs", {}) if isinstance(runtime, dict) else {}
                        n_sum = 0.0
                        has_any = False
                        if isinstance(legs_for_notional, dict):
                            for leg_state in legs_for_notional.values():
                                if not isinstance(leg_state, dict):
                                    continue
                                sz = _safe_float(leg_state.get("size"))
                                if sz is None or abs(sz) < 1e-12:
                                    continue
                                n_sum += abs(float(sz)) * float(mp)
                                has_any = True
                        notional = float(n_sum) if has_any and n_sum > 0 else None
                    except Exception:
                        notional = None

            lev = get_effective_leverage(config_dict)
            roi_pct_on_margin = compute_roi_pct_on_margin(net_pnl=net_after_costs, notional=notional, leverage=lev)
        except Exception:
            roi_pct_on_margin = None
        open_orders_payload = _open_orders_snapshot(order_snapshot_after)
        position_json = json.dumps({k: v for k, v in legs.items()})
        open_orders_json = json.dumps(open_orders_payload)
        # PnL reconciliation warnings (best-effort; do not fail the loop)
        try:
            # Attach computed unrealized (from our runtime legs) to normalized payload for UI.
            if isinstance(positions_normalized, dict):
                positions_normalized.setdefault("computed", {})
                positions_normalized["computed"]["unrealized_pnl"] = float(unrealized_pnl or 0.0)
                positions_normalized["computed"]["mark_price"] = _safe_float(mark_price)
            pnl_diff = reconcile_pnl(
                {"fees_total": fees_total, "funding_total": funding_total, "realized_total": realized_total},
                positions_normalized if isinstance(positions_normalized, dict) else {},
            )
            try:
                thresh = float((risk_cfg or {}).get("pnl_reconcile_threshold") or 5.0)
            except (TypeError, ValueError):
                thresh = 5.0
            diff_u = pnl_diff.get("diff_unreal")
            diff_r = pnl_diff.get("diff_realized")
            should_warn = False
            if diff_u is not None and abs(float(diff_u)) > thresh:
                should_warn = True
            if diff_r is not None and abs(float(diff_r)) > thresh:
                should_warn = True
            if should_warn:
                _emit(
                    "warn",
                    "pnl_reconcile_warn",
                    "PnL reconciliation mismatch",
                    {"threshold": thresh, "diffs": pnl_diff},
                )
        except Exception:
            pass

        update_bot(
            conn,
            int(bot_id),
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            fees_total=fees_total,
            funding_total=funding_total,
            roi_pct_on_margin=roi_pct_on_margin,
            dca_fills_current=dca_fills_current,
            position_json=position_json,
            positions_snapshot_json=json.dumps(positions_snapshot),
            positions_normalized_json=json.dumps(positions_normalized),
            open_orders_json=open_orders_json,
            mark_price=mark_price,
        )

        if guard_ctx.kill_blocked_this_bar:
            heartbeat_msg = "Blocked by kill switch"
        else:
            mark_disp = f"{float(mark_price):.4f}" if _safe_float(mark_price) is not None else "n/a"
            heartbeat_msg = f"{symbol} {timeframe} ts={candle_ts_ms} close={candle.close} mark={mark_disp}"
        set_bot_status(
            conn,
            int(bot_id),
            status="running",
            heartbeat_msg=heartbeat_msg,
            heartbeat_at=_now_iso(),
        )
        if dry_run:
            _emit("info", "dry_run", "Processed bar (no orders)", {"ts_ms": candle_ts_ms})
        _save_bot_config(conn, int(bot_id), config_dict)
        update_job(conn, job_id, status="running", message=heartbeat_msg, progress=0.0)
        _GUARD_CTX.reset(token)
    except Exception as exc:  # pragma: no cover - live path
        err_text = _exc_text()
        err_msg = f"Live bot error: {exc}"
        logger.exception(err_msg)
        _emit("error", "exception", err_msg, {"error": str(exc), "traceback_tail": err_text[-2000:]})
        set_bot_status(conn, int(bot_id), status="error", last_error=err_text, heartbeat_msg="Error")
        update_job(conn, job_id, status="error", error_text=err_text, finished_at=_now_iso())


def _poll_once(worker_id: str, *, dry_run: bool) -> None:
    with profile_span("worker.poll_once", meta={"dry_run": bool(dry_run)}):
        with _get_conn() as conn:
            global_settings = get_app_settings(conn)
            live_trading_enabled = bool((global_settings or {}).get("live_trading_enabled", False))

            now_dt = datetime.now(timezone.utc)
            now_iso = now_dt.isoformat()

            # 1) Conservative stale reclaim: only `running` jobs with expired lease.
            # Skip reclaim when global trading is disabled (keeps behavior conservative).
            reclaimed_job: Optional[Dict[str, Any]] = None
            if live_trading_enabled:
                try:
                    if conn.row_factory is None:
                        conn.row_factory = sqlite3.Row
                    stale_row = conn.execute(
                        """
                        SELECT *
                        FROM jobs
                        WHERE job_type = 'live_bot'
                          AND status = 'running'
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at < ?
                        ORDER BY lease_expires_at ASC
                        LIMIT 1
                        """,
                        (now_iso,),
                    ).fetchone()
                    if stale_row:
                        stale_job = dict(stale_row)
                        stale_job_id = int(stale_job.get("id"))
                        stale_bot_id = stale_job.get("bot_id")
                        old_claimed_by = stale_job.get("claimed_by") or stale_job.get("worker_id")
                        try:
                            old_reclaims = int(stale_job.get("stale_reclaims") or 0)
                        except Exception:
                            old_reclaims = 0

                        if reclaim_stale_job(conn, job_id=stale_job_id, new_worker_id=str(worker_id), lease_s=float(JOB_LEASE_S)):
                            reclaimed_job = get_job(conn, stale_job_id)
                            if reclaimed_job and stale_bot_id is not None:
                                try:
                                    add_bot_event(
                                        conn,
                                        int(stale_bot_id),
                                        "warn",
                                        "job_lease_reclaimed",
                                        "Reclaimed stale job lease",
                                        {
                                            "job_id": stale_job_id,
                                            "bot_id": int(stale_bot_id),
                                            "old_claimed_by": old_claimed_by,
                                            "claimed_by": str(worker_id),
                                            "stale_reclaims": old_reclaims + 1,
                                            "timestamp": _now_iso(),
                                        },
                                    )
                                except Exception:
                                    pass
                                try:
                                    set_bot_status(
                                        conn,
                                        int(stale_bot_id),
                                        heartbeat_msg="Reclaimed stale job lease",
                                        heartbeat_at=_now_iso(),
                                    )
                                except Exception:
                                    pass

                            # Reset lease tracking for reclaimed job (new lease_version).
                            if reclaimed_job and reclaimed_job.get("id") is not None:
                                jid = int(reclaimed_job.get("id"))
                                try:
                                    _JOB_EXPECTED_LEASE_VERSION[jid] = int(reclaimed_job.get("lease_version") or 0)
                                except Exception:
                                    _JOB_EXPECTED_LEASE_VERSION[jid] = 0
                                _JOB_LAST_LEASE_RENEW_S[jid] = 0.0
                except Exception:
                    reclaimed_job = None

            # 2) Claim one queued live_bot job with a lease.
            claimed_job: Optional[Dict[str, Any]] = None
            if live_trading_enabled:
                try:
                    claimed_job = claim_job_with_lease(conn, worker_id=str(worker_id), lease_s=float(JOB_LEASE_S))
                    if claimed_job and claimed_job.get("id") is not None:
                        jid = int(claimed_job.get("id"))
                        try:
                            _JOB_EXPECTED_LEASE_VERSION[jid] = int(claimed_job.get("lease_version") or 0)
                        except Exception:
                            _JOB_EXPECTED_LEASE_VERSION[jid] = 0
                        _JOB_LAST_LEASE_RENEW_S[jid] = 0.0
                except Exception:
                    claimed_job = None

            # 3) Process jobs owned by this worker (including reclaimed/claimed).
            jobs_to_run: List[Dict[str, Any]] = []
            if reclaimed_job:
                jobs_to_run.append(reclaimed_job)
            if claimed_job:
                jobs_to_run.append(claimed_job)

            try:
                if conn.row_factory is None:
                    conn.row_factory = sqlite3.Row
                owned_rows = conn.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE job_type = 'live_bot'
                      AND status IN ('queued', 'running', 'cancel_requested')
                      AND (
                        claimed_by = ?
                        OR (claimed_by IS NULL OR claimed_by = '') AND worker_id = ?
                      )
                    ORDER BY COALESCE(updated_at, created_at) DESC
                    LIMIT 50
                    """,
                    (str(worker_id), str(worker_id)),
                ).fetchall()
                for r in owned_rows:
                    j = dict(r)
                    # Ensure we keep the *first-seen* lease_version for this job.
                    try:
                        jid = int(j.get("id"))
                        if jid not in _JOB_EXPECTED_LEASE_VERSION:
                            _JOB_EXPECTED_LEASE_VERSION[jid] = int(j.get("lease_version") or 0)
                    except Exception:
                        pass
                    jobs_to_run.append(j)
            except Exception:
                pass

    # Deduplicate by job_id and run slices.
    seen: set[int] = set()
    for job in jobs_to_run:
        try:
            jid = int(job.get("id"))
        except Exception:
            continue
        if jid in seen:
            continue
        seen.add(jid)

        # Clean up lease caches for non-running terminal states (best-effort).
        st = str(job.get("status") or "").strip().lower()
        if st not in {"queued", "running", "cancel_requested"}:
            _JOB_EXPECTED_LEASE_VERSION.pop(jid, None)
            _JOB_LAST_LEASE_RENEW_S.pop(jid, None)
            continue

        with _get_conn() as conn:
            _process_job(conn, job, dry_run=dry_run, worker_id=worker_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Project Dragon live worker")
    parser.add_argument("--dry-run", action="store_true", help="Do not place/cancel orders; log only.")
    args = parser.parse_args()

    worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
    logger.info("Starting Dragon live worker (dry_run=%s) worker_id=%s DB=%s", args.dry_run, worker_id, _db_path)
    try:
        while True:
            _poll_once(worker_id, dry_run=args.dry_run)
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Live worker stopped by user")


if __name__ == "__main__":
    main()
