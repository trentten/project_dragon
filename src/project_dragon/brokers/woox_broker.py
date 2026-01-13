from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from project_dragon.domain import Order, OrderActivationState, OrderStatus, OrderType, Position, PositionSide, Side, Trade
from project_dragon.api_resilience import CircuitOpenError, RateLimitTimeout, call_api
from .woox_client import WooXClient, WooXAPIError
from .woox_models import WooXPosition

logger = logging.getLogger(__name__)


class WooXBrokerError(RuntimeError):
    pass


@dataclass
class DynamicOrder:
    local_id: int
    order_side: str
    position_side: str
    price: float
    size: float
    activation_pct: float
    reduce_only: bool = False
    post_only: bool = False
    note: str = ""
    exchange_order_id: Optional[str] = None
    active: bool = False


class WooXBroker:
    """
    Thin WooX V3 Perps adapter with hedge-mode support.
    """

    def __init__(
        self,
        client: WooXClient,
        symbol: str,
        bot_id: Optional[str] = None,
        account_id: Optional[int] = None,
        prefer_bbo_maker: bool = True,
        bbo_level: int = 1,
        primary_position_side: str | Side = "LONG",
        require_hedge_mode: bool = True,
        auto_sync: bool = True,
    ) -> None:
        self.client = client
        self.symbol = symbol
        try:
            self.account_id: int = int(account_id) if account_id is not None else 0
        except Exception:
            self.account_id = 0
        bot_id_safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in (bot_id or "bot")) or "bot"
        self.bot_id = bot_id_safe
        self.client_id_prefix = f"DRAGON-{self.bot_id}"
        self.prefer_bbo_maker: bool = prefer_bbo_maker
        self.bbo_level: int = max(1, min(5, int(bbo_level or 1)))
        self.hedge_enabled = False
        self.require_hedge_mode: bool = bool(require_hedge_mode)
        # Live strategy expects a cash-like balance and fee_rate similar to BrokerSim; default to 0.0 until set by caller
        self.balance: float = 0.0
        self.fee_rate: float = 0.0
        self.positions: Dict[str, WooXPosition] = {}
        self.primary_position_side = self._normalize_primary_position_side(primary_position_side)
        self.position = Position()  # primary leg convenience (back-compat)
        self.open_orders: Dict[int, Order] = {}
        self.dynamic_orders: Dict[int, DynamicOrder] = {}
        self.ws_best_bid: Optional[float] = None
        self.ws_best_ask: Optional[float] = None
        self._local_order_id = 1
        self._event_logger: Optional[Callable[[str, str, str, Dict[str, Any]], None]] = None
        self._ensure_hedge_mode()
        if auto_sync:
            self.sync_positions()
            self.sync_orders()

    def _api_call(self, *, op_name: str, fn: Callable[[], Any], retry: bool = True, cost: float = 1.0) -> Any:
        def _on_failure(exc: BaseException, attempt: int) -> None:
            # Keep this lightweight; worker also emits higher-level health/snapshot events.
            self._emit_event(
                "warn",
                "broker_call_failed",
                "WooX API call failed",
                {
                    "op_name": op_name,
                    "attempt": attempt,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )

        try:
            return call_api(
                int(self.account_id),
                fn,
                op_name=op_name,
                retry=bool(retry),
                cost=float(cost),
                timeout_s=1.0,
                on_failure=_on_failure,
            )
        except (CircuitOpenError, RateLimitTimeout) as exc:
            self._emit_event(
                "warn",
                "broker_call_blocked",
                "WooX call blocked by resilience layer",
                {
                    "op_name": op_name,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            raise

    def _normalize_primary_position_side(self, value: str | Side) -> str:
        if isinstance(value, Side):
            return "LONG" if value == Side.LONG else "SHORT"
        s = str(value or "").strip().upper()
        if s in {"LONG", "SHORT"}:
            return s
        return "LONG"

    def _ensure_hedge_mode(self) -> None:
        try:
            self.client.set_position_mode("HEDGE_MODE")
            self.hedge_enabled = True
        except WooXAPIError as exc:  # pragma: no cover - live-only
            logger.warning("Hedge mode set failed (assuming already set): %s", exc)
            self.hedge_enabled = True

    def _new_local_order_id(self) -> int:
        oid = self._local_order_id
        self._local_order_id += 1
        return oid

    # --- Positions ------------------------------------------------------
    def sync_positions(self) -> None:
        data = self._api_call(op_name="get_positions", fn=lambda: (self.client.get_positions(self.symbol) or {}), retry=True, cost=1.0)

        # Expected shapes include:
        # - {"positions": [...]}
        # - {"rows": [...]}
        # - {"data": [...]}  (already a list)
        # - [...]             (data itself is a list)
        rows: list[dict[str, Any]] = []
        if isinstance(data, dict):
            raw_rows = data.get("positions") or data.get("rows") or data.get("data") or []
            if isinstance(raw_rows, list):
                rows = [r for r in raw_rows if isinstance(r, dict)]
            else:
                rows = []
        elif isinstance(data, list):
            rows = [r for r in data if isinstance(r, dict)]
        else:
            rows = []

        parsed: Dict[str, WooXPosition] = {}
        saw_both = False
        raw_position_sides: list[str] = []
        for row in rows:
            pos_side = str(row.get("positionSide") or "").upper().strip()
            if pos_side:
                raw_position_sides.append(pos_side)

            if pos_side == "BOTH":
                saw_both = True

            if self.require_hedge_mode and saw_both:
                message = "WooX account not in hedge mode (positionSide=BOTH). Enable hedge mode and restart bot."
                self._emit_event(
                    "error",
                    "hedge_mode_required",
                    message,
                    {
                        "symbol": self.symbol,
                        "raw_position_sides": raw_position_sides,
                    },
                )
                raise WooXBrokerError(message)

            # Normalize legacy one-way BOTH into LONG/SHORT using sign of holding.
            if pos_side == "BOTH":
                try:
                    holding_raw = float(row.get("holding", 0) or 0)
                except (TypeError, ValueError):
                    holding_raw = 0.0
                if holding_raw > 0:
                    pos_side = "LONG"
                elif holding_raw < 0:
                    pos_side = "SHORT"
                else:
                    continue
                row = dict(row)
                row["positionSide"] = pos_side

            pos = WooXPosition.from_api(row)
            side_key = (pos.position_side or pos_side or "").upper().strip()
            if side_key not in {"LONG", "SHORT"}:
                continue
            parsed[side_key] = pos

        self.positions = parsed

        # Convenience primary-leg alias for back-compat with strategy code.
        primary = self.primary_position_side
        if primary == "LONG":
            long_pos = parsed.get("LONG")
            if long_pos and float(long_pos.holding_abs or 0.0) != 0.0:
                self.position.side = PositionSide.LONG
                self.position.size = float(long_pos.holding_abs)
                self.position.avg_price = float(long_pos.average_open_price or 0.0)
            else:
                self.position.reset()
        elif primary == "SHORT":
            short_pos = parsed.get("SHORT")
            if short_pos and float(short_pos.holding_abs or 0.0) != 0.0:
                self.position.side = PositionSide.SHORT
                self.position.size = float(short_pos.holding_abs)
                self.position.avg_price = float(short_pos.average_open_price or 0.0)
            else:
                self.position.reset()
        else:
            self.position.reset()

    def get_position(self, side: Side) -> Optional[WooXPosition]:
        key = "LONG" if side == Side.LONG else "SHORT"
        return self.positions.get(key)

    def get_leg_position(self, side: str) -> Optional[WooXPosition]:
        side_key = str(side or "").strip().upper()
        if side_key not in {"LONG", "SHORT"}:
            return None
        return self.positions.get(side_key)

    def get_primary_leg_position(self) -> Optional[WooXPosition]:
        return self.positions.get(self.primary_position_side)

    def _to_iso(self, value: Any) -> Optional[str]:
        try:
            if value is None:
                return None
            ts_float = float(value)
            if ts_float > 1e12:  # ms vs s
                ts_float /= 1000.0
            return datetime.fromtimestamp(ts_float, tz=timezone.utc).isoformat()
        except Exception:
            return None

    # --- Orders ---------------------------------------------------------
    def sync_orders(self) -> None:
        try:
            data = self._api_call(
                op_name="get_orders",
                fn=lambda: (self.client.get_orders(self.symbol, status="INCOMPLETE") or {}),
                retry=True,
                cost=1.0,
            )
        except WooXAPIError as exc:  # pragma: no cover - live-only
            logger.error("get_orders failed: %s", exc)
            self._emit_event("error", "sync_warning", "get_orders failed", {"error": str(exc)})
            raise
        rows = data if isinstance(data, list) else data.get("rows") or data.get("data") or []
        status_map = {
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELLED,
            "INCOMPLETE": OrderStatus.OPEN,
            "OPEN": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIAL,
            "PARTIAL": OrderStatus.PARTIAL,
            "REJECTED": OrderStatus.REJECTED,
            "FAILED": OrderStatus.REJECTED,
        }
        existing_by_ext: Dict[str, Order] = {}
        existing_by_client: Dict[str, Order] = {}
        for ord_obj in self.open_orders.values():
            ext = getattr(ord_obj, "external_id", None)
            if ext:
                existing_by_ext[ext] = ord_obj
            cid = getattr(ord_obj, "client_order_id", None)
            if cid:
                existing_by_client[cid] = ord_obj

        seen_external: set[str] = set()
        seen_client: set[str] = set()
        for row in rows:
            ext_id = str(row.get("orderId")) if row.get("orderId") is not None else None
            client_oid = str(row.get("clientOrderId")) if row.get("clientOrderId") is not None else None
            side_str = str(row.get("side", "")).upper()
            position_side_str = str(row.get("positionSide", "")).upper()
            side_enum = Side.LONG if position_side_str == "LONG" else Side.SHORT
            price_raw = row.get("price") or row.get("dealPrice")
            try:
                price = float(price_raw) if price_raw is not None else float("nan")
            except (TypeError, ValueError):
                price = float("nan")
            size_raw = row.get("quantity") or row.get("origQty") or row.get("size")
            try:
                size = float(size_raw) if size_raw is not None else 0.0
            except (TypeError, ValueError):
                size = 0.0
            filled_raw = (
                row.get("filledQty")
                or row.get("filledQuantity")
                or row.get("executedQty")
                or row.get("dealQuantity")
                or row.get("cumQuantity")
            )
            try:
                filled_size = float(filled_raw) if filled_raw is not None else 0.0
            except (TypeError, ValueError):
                filled_size = getattr(existing_by_client.get(client_oid, None) or existing_by_ext.get(ext_id, None), "filled_size", 0.0)
            avg_fill_raw = row.get("avgPrice") or row.get("dealAvgPrice") or row.get("fillPrice")
            try:
                avg_fill_price = float(avg_fill_raw) if avg_fill_raw is not None else float("nan")
            except (TypeError, ValueError):
                avg_fill_price = float("nan")
            status_str = str(row.get("status", "INCOMPLETE")).upper()
            mapped_status = status_map.get(status_str, OrderStatus.OPEN)
            updated_at_iso = self._to_iso(row.get("updatedTime") or row.get("timestamp") or row.get("transactTime"))
            order_obj = None
            if client_oid and client_oid in existing_by_client:
                order_obj = existing_by_client[client_oid]
            elif ext_id and ext_id in existing_by_ext:
                order_obj = existing_by_ext[ext_id]
            if order_obj is None:
                local_id = self._new_local_order_id()
                order_obj = Order(
                    id=local_id,
                    side=side_enum,
                    type=OrderType.LIMIT,
                    price=price if not math.isnan(price) else float("nan"),
                    size=size,
                    status=mapped_status,
                    note="exchange",
                )
                self.open_orders[local_id] = order_obj
            order_obj.side = side_enum
            if price_raw is not None and not math.isnan(price):
                order_obj.price = price
                setattr(order_obj, "last_seen_price", price)
            if size_raw is not None:
                order_obj.size = size
            order_obj.status = mapped_status
            order_obj.filled_size = filled_size
            if not math.isnan(avg_fill_price):
                setattr(order_obj, "avg_fill_price", avg_fill_price)
            if position_side_str:
                setattr(order_obj, "position_side", position_side_str)
            if side_str:
                setattr(order_obj, "order_action", side_str)
            if client_oid:
                setattr(order_obj, "client_order_id", client_oid)
            if ext_id:
                setattr(order_obj, "external_id", ext_id)
            if updated_at_iso:
                setattr(order_obj, "updated_at", updated_at_iso)
            exch_type = row.get("type") or row.get("orderType")
            if exch_type:
                setattr(order_obj, "exchange_type", str(exch_type).upper())
            reason = row.get("rejectMsg") or row.get("errorCode") or row.get("message")
            if reason:
                setattr(order_obj, "status_reason", str(reason))
            if ext_id:
                seen_external.add(ext_id)
            if client_oid:
                seen_client.add(client_oid)
            dyn = self._find_dynamic_by_external(ext_id)
            if dyn:
                dyn.exchange_order_id = ext_id

        for oid, ord_obj in list(self.open_orders.items()):
            status = getattr(ord_obj, "status", OrderStatus.OPEN)
            if status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED):
                continue

            activation_state = getattr(ord_obj, "activation_state", None)
            if activation_state == OrderActivationState.PARKED:
                # Local-only intent that is not yet eligible to rest on exchange
                continue

            ext = getattr(ord_obj, "external_id", None)
            cid = getattr(ord_obj, "client_order_id", None)

            expected_on_exchange = bool(ext) or (cid and activation_state == OrderActivationState.ACTIVE)
            if not expected_on_exchange:
                continue

            if (cid and cid in seen_client) or (ext and ext in seen_external):
                setattr(ord_obj, "miss_count", 0)
                continue

            miss_count = int(getattr(ord_obj, "miss_count", 0) or 0) + 1
            setattr(ord_obj, "miss_count", miss_count)
            if miss_count in (1, 3):
                self._emit_event(
                    "warn",
                    "sync_warning",
                    "Order missing from exchange snapshot",
                    {"client_order_id": cid, "external_id": ext, "miss_count": miss_count},
                )

            if miss_count < 3:
                continue

            verified = False
            if cid and hasattr(self.client, "get_order"):
                try:
                    ord_data = self._api_call(
                        op_name="get_order",
                        fn=lambda: (self.client.get_order(None, self.symbol, client_order_id=cid) or {}),
                        retry=True,
                        cost=1.0,
                    )
                    row = ord_data.get("data") if isinstance(ord_data, dict) else None
                    if isinstance(ord_data, dict) and not row:
                        row = ord_data.get("result") or ord_data
                    if isinstance(row, dict):
                        returned_cid = str(row.get("clientOrderId")) if row.get("clientOrderId") is not None else None
                        returned_ext = str(row.get("orderId")) if row.get("orderId") is not None else None
                        if returned_cid == cid or (ext and returned_ext == ext):
                            verified = True
                            if returned_ext:
                                setattr(ord_obj, "external_id", returned_ext)
                            status_str = str(row.get("status", "INCOMPLETE")).upper()
                            ord_obj.status = status_map.get(status_str, ord_obj.status)
                            filled_raw = row.get("filledQty") or row.get("filledQuantity") or row.get("executedQty") or row.get("dealQuantity") or row.get("cumQuantity")
                            try:
                                ord_obj.filled_size = float(filled_raw) if filled_raw is not None else ord_obj.filled_size
                            except (TypeError, ValueError):
                                pass
                            setattr(ord_obj, "miss_count", 0)
                            continue
                except WooXAPIError as exc:  # pragma: no cover - live-only
                    self._emit_event("warn", "sync_warning", "Direct get_order failed", {"client_order_id": cid, "error": str(exc)})

            if verified:
                continue

            ord_obj.status = OrderStatus.CANCELLED
            self._emit_event(
                "warn",
                "order_missing_cancelled",
                "Order cancelled after repeated missing snapshots",
                {"client_order_id": cid, "external_id": ext, "miss_count": miss_count},
            )

    def _find_local_order_by_external(self, external_id: Optional[str]) -> Optional[Order]:
        if not external_id:
            return None
        for order in self.open_orders.values():
            if getattr(order, "external_id", None) == external_id:
                return order
        return None

    def _find_dynamic_by_external(self, external_id: Optional[str]) -> Optional[DynamicOrder]:
        if not external_id:
            return None
        for dyn in self.dynamic_orders.values():
            if dyn.exchange_order_id == external_id:
                return dyn
        return None

    def get_order_by_client_order_id(self, client_order_id: str) -> Optional[Order]:
        if not client_order_id:
            return None
        for order in self.open_orders.values():
            if getattr(order, "client_order_id", None) == client_order_id:
                return order
        return None

    def set_event_logger(self, logger_fn: Callable[[str, str, str, Dict[str, Any]], None]) -> None:
        self._event_logger = logger_fn

    def _emit_event(self, level: str, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
        if self._event_logger:
            try:
                self._event_logger(level, event_type, message, payload or {})
            except Exception:  # pragma: no cover - defensive
                logger.exception("Event logger failed for %s", event_type)

    # --- Placement ------------------------------------------------------
    def _normalize_side(self, value: str | Side) -> str:
        if isinstance(value, Side):
            return "BUY" if value == Side.LONG else "SELL"
        return str(value).upper()

    def _normalize_position_side(self, value: str | Side) -> str:
        if isinstance(value, Side):
            return "LONG" if value == Side.LONG else "SHORT"
        return str(value).upper()

    def _client_order_id(self, local_id: int) -> str:
        # Deterministic per local id so cancel/query remains stable
        return f"{self.client_id_prefix}-{local_id}"

    def get_best_bid_ask(self) -> Optional[tuple[float, float]]:
        if self.ws_best_bid is not None and self.ws_best_ask is not None:
            return (self.ws_best_bid, self.ws_best_ask)

        try:
            orderbook = self._api_call(
                op_name="get_orderbook",
                fn=lambda: (self.client.get_orderbook(self.symbol, depth=1) or {}),
                retry=True,
                cost=1.0,
            )
        except WooXAPIError as exc:  # pragma: no cover - live-only
            logger.warning("get_orderbook failed for %s: %s", self.symbol, exc)
            return None
        except (CircuitOpenError, RateLimitTimeout):
            return None

        row = None
        if isinstance(orderbook, dict):
            row = orderbook.get("data") or orderbook.get("result") or orderbook
        elif isinstance(orderbook, list) and orderbook:
            row = orderbook[0]

        if not isinstance(row, dict):
            return None

        bids = row.get("bids") or row.get("bid") or []
        asks = row.get("asks") or row.get("ask") or []
        bid_price = None
        ask_price = None
        if isinstance(bids, list) and bids:
            top_bid = bids[0]
            if isinstance(top_bid, (list, tuple)) and len(top_bid) >= 1:
                bid_price = top_bid[0]
        if isinstance(asks, list) and asks:
            top_ask = asks[0]
            if isinstance(top_ask, (list, tuple)) and len(top_ask) >= 1:
                ask_price = top_ask[0]

        try:
            bid = float(bid_price)
            ask = float(ask_price)
        except (TypeError, ValueError):
            return None

        if bid <= 0 or ask <= 0:
            return None

        self.ws_best_bid = bid
        self.ws_best_ask = ask
        return (bid, ask)

    def _active_duplicate(self, client_oid: Optional[str], note: Optional[str]) -> Optional[Order]:
        for order in self.open_orders.values():
            if getattr(order, "status", OrderStatus.OPEN) not in (OrderStatus.OPEN, OrderStatus.PARTIAL):
                continue
            if client_oid and getattr(order, "client_order_id", None) == client_oid:
                return order
            if note and getattr(order, "note", None) == note:
                if not getattr(order, "external_id", None) and not getattr(order, "client_order_id", None):
                    # Allow parked/local placeholders to be activated later
                    continue
                if getattr(order, "activation_state", None) == OrderActivationState.PARKED and not getattr(order, "external_id", None):
                    continue
                return order
        return None

    def place_market(
        self,
        order_side: str | Side,
        position_side: str | Side,
        price: float,
        size: float,
        note: str = "",
        reduce_only: bool = False,
    ) -> Trade:
        order_side_str = self._normalize_side(order_side)
        position_side_str = self._normalize_position_side(position_side)
        body = {
            "symbol": self.symbol,
            "side": order_side_str,
            "positionSide": position_side_str,
            "type": "MARKET",
            "quantity": size,
            "reduceOnly": reduce_only,
        }
        self._api_call(op_name="place_order_market", fn=lambda: self.client.place_order(body), retry=True, cost=1.0)
        self.sync_positions()
        trade = Trade(
            index=None,
            timestamp=datetime.now(timezone.utc),
            side=Side.LONG if order_side_str == "BUY" else Side.SHORT,
            price=price,
            size=size,
            pnl=0.0,
            note=note,
        )
        setattr(trade, "position_side", position_side_str)
        return trade

    def place_limit(
        self,
        order_side: str | Side,
        position_side: str | Side,
        price: float,
        size: float,
        note: str = "",
        reduce_only: bool = False,
        post_only: bool = False,
        post_only_adjusted: bool = True,
        local_id: Optional[int] = None,
    ) -> int:
        order_side_str = self._normalize_side(order_side)
        position_side_str = self._normalize_position_side(position_side)
        if local_id is None:
            local_id = self._new_local_order_id()
        client_oid = self._client_order_id(local_id)
        existing_live = self._active_duplicate(client_oid, note)
        if existing_live and (getattr(existing_live, "external_id", None) or getattr(existing_live, "client_order_id", None)):
            self._emit_event(
                "warn",
                "sync_warning",
                "Duplicate order suppressed",
                {
                    "client_order_id": getattr(existing_live, "client_order_id", None),
                    "note": note,
                    "order_id": getattr(existing_live, "external_id", None),
                },
            )
            return existing_live.id
        order_type = "POST_ONLY" if post_only else "LIMIT"
        body = {
            "symbol": self.symbol,
            "side": order_side_str,
            "positionSide": position_side_str,
            "type": order_type,
            "price": price,
            "quantity": size,
            "reduceOnly": reduce_only,
            "clientOrderId": client_oid,
        }
        if note:
            body["orderTag"] = note
        if post_only:
            body["postOnlyAdjusted"] = post_only_adjusted
        else:
            body["timeInForce"] = "GTC"
        resp = self._api_call(op_name="place_order_limit", fn=lambda: self.client.place_order(body), retry=True, cost=1.0)
        ext_id = str(resp.get("orderId")) if isinstance(resp, dict) else None
        client_oid = (
            str(resp.get("clientOrderId"))
            if isinstance(resp, dict) and resp.get("clientOrderId") is not None
            else client_oid
        )
        order_obj = self.open_orders.get(local_id)
        if order_obj is None:
            order_obj = Order(
                id=local_id,
                side=Side.LONG if position_side_str == "LONG" else Side.SHORT,
                type=OrderType.LIMIT,
                price=price,
                size=size,
                status=OrderStatus.OPEN,
                note=note,
            )
            self.open_orders[local_id] = order_obj
        order_obj.side = Side.LONG if position_side_str == "LONG" else Side.SHORT
        order_obj.price = price
        order_obj.size = size
        order_obj.status = OrderStatus.OPEN
        order_obj.note = note
        setattr(order_obj, "position_side", position_side_str)
        setattr(order_obj, "order_action", order_side_str)
        setattr(order_obj, "exchange_type", order_type)
        if ext_id:
            setattr(order_obj, "external_id", ext_id)
        setattr(order_obj, "client_order_id", client_oid)
        return local_id

    def place_bbo_queue_limit(
        self,
        order_side: str | Side,
        position_side: str | Side,
        size: float,
        bid_ask_level: int = 1,
        note: str = "",
        reduce_only: bool = False,
        local_id: Optional[int] = None,
    ) -> int:
        order_side_str = self._normalize_side(order_side)
        position_side_str = self._normalize_position_side(position_side)
        lvl = max(1, min(5, int(bid_ask_level or self.bbo_level or 1)))
        order_type = "BID" if order_side_str == "BUY" else "ASK"
        if local_id is None:
            local_id = self._new_local_order_id()
        client_oid = self._client_order_id(local_id)
        existing_live = self._active_duplicate(client_oid, note)
        if existing_live and (getattr(existing_live, "external_id", None) or getattr(existing_live, "client_order_id", None)):
            self._emit_event(
                "warn",
                "sync_warning",
                "Duplicate order suppressed",
                {
                    "client_order_id": getattr(existing_live, "client_order_id", None),
                    "note": note,
                    "order_id": getattr(existing_live, "external_id", None),
                },
            )
            return existing_live.id
        body = {
            "symbol": self.symbol,
            "side": order_side_str,
            "positionSide": position_side_str,
            "type": order_type,
            "bidAskLevel": lvl,
            "quantity": size,
            "reduceOnly": reduce_only,
            "clientOrderId": client_oid,
        }
        if note:
            body["orderTag"] = note
        self._emit_event(
            "info",
            "maker_intent",
            "Placing maker BBO order",
            {
                "note": note,
                "side": order_side_str,
                "position_side": position_side_str,
                "bbo_level": lvl,
                "reduce_only": reduce_only,
                "size": size,
            },
        )
        try:
            resp = self._api_call(op_name="place_order_bbo", fn=lambda: self.client.place_order(body), retry=True, cost=1.0)
        except WooXAPIError as exc:
            bid_ask = self.get_best_bid_ask()
            if not bid_ask:
                raise WooXAPIError(
                    f"BBO place failed and no bid/ask available for fallback: {exc}",
                    payload=getattr(exc, "payload", None),
                    status_code=getattr(exc, "status_code", None),
                )
            fallback_raw = bid_ask[0] if order_side_str == "BUY" else bid_ask[1]
            try:
                fallback_price = float(fallback_raw)
            except (TypeError, ValueError):
                raise WooXAPIError(
                    "BBO place failed and fallback bid/ask was invalid",
                    payload=getattr(exc, "payload", None),
                    status_code=getattr(exc, "status_code", None),
                )
            if fallback_price <= 0:
                raise WooXAPIError(
                    "BBO place failed and fallback bid/ask was non-positive",
                    payload=getattr(exc, "payload", None),
                    status_code=getattr(exc, "status_code", None),
                )
            logger.warning(
                "BBO place failed for %s L%d side=%s pos_side=%s reduce_only=%s, falling back to post-only limit at %s: %s",
                self.symbol,
                lvl,
                order_side_str,
                position_side_str,
                reduce_only,
                fallback_price,
                exc,
            )
            self._emit_event(
                "warn",
                "maker_fallback_post_only",
                "BBO place failed; falling back to post-only limit",
                {
                    "note": note,
                    "side": order_side_str,
                    "position_side": position_side_str,
                    "bbo_level": lvl,
                    "fallback_price": fallback_price,
                    "reduce_only": reduce_only,
                    "size": size,
                },
            )
            return self.place_limit(
                order_side=order_side_str,
                position_side=position_side_str,
                price=fallback_price,
                size=size,
                note=note,
                reduce_only=reduce_only,
                post_only=True,
                post_only_adjusted=True,
                local_id=local_id,
            )
        ext_id = str(resp.get("orderId")) if isinstance(resp, dict) else None
        client_oid = (
            str(resp.get("clientOrderId"))
            if isinstance(resp, dict) and resp.get("clientOrderId") is not None
            else client_oid
        )
        order_obj = self.open_orders.get(local_id)
        if order_obj is None:
            order_obj = Order(
                id=local_id,
                side=Side.LONG if position_side_str == "LONG" else Side.SHORT,
                type=OrderType.LIMIT,
                price=float("nan"),
                size=size,
                status=OrderStatus.OPEN,
                note=note,
            )
            self.open_orders[local_id] = order_obj
        order_obj.side = Side.LONG if position_side_str == "LONG" else Side.SHORT
        order_obj.price = float("nan")
        order_obj.size = size
        order_obj.status = OrderStatus.OPEN
        order_obj.note = note or f"BBO L{lvl}"
        setattr(order_obj, "position_side", position_side_str)
        setattr(order_obj, "order_action", order_side_str)
        setattr(order_obj, "bbo_level", lvl)
        setattr(order_obj, "exchange_type", order_type)
        logger.info("Placed BBO order %s L%d side=%s pos_side=%s size=%s", self.symbol, lvl, order_side_str, position_side_str, size)
        if ext_id:
            setattr(order_obj, "external_id", ext_id)
        setattr(order_obj, "client_order_id", client_oid)
        return local_id

    def place_dynamic_limit(
        self,
        order_side: str | Side,
        position_side: str | Side,
        price: float,
        size: float,
        activation_pct: float,
        note: str = "",
        reduce_only: bool = False,
        post_only: bool = False,
        local_id: Optional[int] = None,
    ) -> int:
        existing_live = self._active_duplicate(None, note)
        if existing_live:
            self._emit_event(
                "warn",
                "sync_warning",
                "Duplicate dynamic order suppressed",
                {
                    "client_order_id": getattr(existing_live, "client_order_id", None),
                    "note": note,
                    "order_id": getattr(existing_live, "external_id", None),
                },
            )
            return existing_live.id
        if local_id is None:
            local_id = self._new_local_order_id()
        order_side_str = self._normalize_side(order_side)
        position_side_str = self._normalize_position_side(position_side)
        dyn = DynamicOrder(
            local_id=local_id,
            order_side=order_side_str,
            position_side=position_side_str,
            price=price,
            size=size,
            activation_pct=activation_pct,
            reduce_only=reduce_only,
            post_only=post_only,
            note=note,
        )
        self.dynamic_orders[local_id] = dyn
        order_obj = Order(
            id=local_id,
            side=Side.LONG if position_side_str == "LONG" else Side.SHORT,
            type=OrderType.LIMIT,
            price=price,
            size=size,
            activation_band_pct=activation_pct,
            target_price=price,
            activation_state=OrderActivationState.PARKED,
            note=note,
        )
        setattr(order_obj, "position_side", position_side_str)
        setattr(order_obj, "order_action", order_side_str)
        setattr(order_obj, "exchange_type", "LIMIT")
        self.open_orders[local_id] = order_obj
        return local_id

    def cancel_order(self, order_id: int) -> None:
        order = self.open_orders.get(order_id)
        if not order:
            return
        ext_id = getattr(order, "external_id", None)
        client_oid = getattr(order, "client_order_id", None)
        if ext_id or client_oid:
            try:
                self._api_call(
                    op_name="cancel_order",
                    fn=lambda: self.client.cancel_order(ext_id, self.symbol, client_order_id=client_oid),
                    retry=True,
                    cost=1.0,
                )
            except WooXAPIError as exc:  # pragma: no cover - live-only
                logger.warning("Cancel failed for %s/%s: %s", ext_id, client_oid, exc)
            except (CircuitOpenError, RateLimitTimeout) as exc:
                logger.warning("Cancel blocked for %s/%s: %s", ext_id, client_oid, exc)
        order.status = OrderStatus.CANCELLED

    def get_order(self, order_id: int) -> Optional[Order]:
        return self.open_orders.get(order_id)

    # --- Dynamic order lifecycle managed by runner ----------------------
    def activate_dynamic(self, dyn: DynamicOrder) -> None:
        if dyn.active:
            return
        order_side = self._normalize_side(dyn.order_side)
        position_side = self._normalize_position_side(dyn.position_side)
        order_id = dyn.local_id
        if self.prefer_bbo_maker:
            self.place_bbo_queue_limit(
                order_side=order_side,
                position_side=position_side,
                size=dyn.size,
                bid_ask_level=self.bbo_level,
                note=dyn.note,
                reduce_only=dyn.reduce_only,
                local_id=order_id,
            )
        else:
            self.place_limit(
                order_side=order_side,
                position_side=position_side,
                price=dyn.price,
                size=dyn.size,
                note=dyn.note,
                reduce_only=dyn.reduce_only,
                post_only=dyn.post_only,
                local_id=order_id,
            )
        dyn.active = True
        dyn.exchange_order_id = getattr(self.open_orders.get(order_id), "external_id", None)
        order = self.open_orders.get(order_id)
        if order is not None:
            order.activation_state = OrderActivationState.ACTIVE
            order.status = OrderStatus.OPEN

    def cancel_dynamic(self, dyn: DynamicOrder) -> None:
        if dyn.exchange_order_id:
            for oid, ord_obj in self.open_orders.items():
                if getattr(ord_obj, "external_id", None) == dyn.exchange_order_id:
                    try:
                        self._api_call(
                            op_name="cancel_order",
                            fn=lambda: self.client.cancel_order(
                                dyn.exchange_order_id,
                                self.symbol,
                                client_order_id=getattr(ord_obj, "client_order_id", None),
                            ),
                            retry=True,
                            cost=1.0,
                        )
                    except WooXAPIError as exc:  # pragma: no cover - live-only
                        logger.warning("Cancel failed for %s: %s", dyn.exchange_order_id, exc)
                    except (CircuitOpenError, RateLimitTimeout) as exc:
                        logger.warning("Cancel blocked for %s: %s", dyn.exchange_order_id, exc)
                    ord_obj.activation_state = OrderActivationState.PARKED
                    ord_obj.status = OrderStatus.OPEN
                    setattr(ord_obj, "external_id", None)
                    break
        local = self.open_orders.get(dyn.local_id)
        if local is not None:
            local.activation_state = OrderActivationState.PARKED
            local.status = OrderStatus.OPEN
            setattr(local, "external_id", None)
        dyn.active = False
        dyn.exchange_order_id = None

    # --- Order helpers for hedge-mode correctness ---------------------
    def open_long_limit(self, price: float, size: float, note: str = "", post_only: bool = False) -> int:
        return self.place_limit("BUY", "LONG", price, size, note=note, reduce_only=False, post_only=post_only)

    def close_long_limit(self, price: float, size: float, note: str = "", post_only: bool = False) -> int:
        return self.place_limit("SELL", "LONG", price, size, note=note, reduce_only=True, post_only=post_only)

    def open_short_limit(self, price: float, size: float, note: str = "", post_only: bool = False) -> int:
        return self.place_limit("SELL", "SHORT", price, size, note=note, reduce_only=False, post_only=post_only)

    def close_short_limit(self, price: float, size: float, note: str = "", post_only: bool = False) -> int:
        return self.place_limit("BUY", "SHORT", price, size, note=note, reduce_only=True, post_only=post_only)

    def open_long_market(self, size: float, price: float, note: str = "") -> Trade:
        return self.place_market("BUY", "LONG", price=price, size=size, note=note, reduce_only=False)

    def close_long_market(self, size: float, price: float, note: str = "") -> Trade:
        return self.place_market("SELL", "LONG", price=price, size=size, note=note, reduce_only=True)

    def open_short_market(self, size: float, price: float, note: str = "") -> Trade:
        return self.place_market("SELL", "SHORT", price=price, size=size, note=note, reduce_only=False)

    def close_short_market(self, size: float, price: float, note: str = "") -> Trade:
        return self.place_market("BUY", "SHORT", price=price, size=size, note=note, reduce_only=True)

    def sync(self) -> None:
        self.sync_positions()
        self.sync_orders()
