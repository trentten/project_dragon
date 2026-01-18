from __future__ import annotations

from datetime import datetime, timezone

from project_dragon.live_worker import _apply_fill_to_runtime, _ensure_runtime
from project_dragon.storage import (
    add_bot_fill,
    create_bot,
    create_position_history,
    init_db,
    open_db_connection,
)


def _iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_sequence() -> None:
    init_db()
    with open_db_connection() as conn:
        bot_id = create_bot(
            conn,
            name="pnl-test",
            exchange_id="testex",
            symbol="SYNTH/USD",
            timeframe="1m",
            config={},
            status="running",
            desired_status="running",
            heartbeat_msg="pnl test",
        )

    # Runtime state mirrors live_worker expectations
    runtime = _ensure_runtime({})
    progress = runtime.setdefault("order_fill_progress", {})
    run_id = "test-run"
    symbol = "SYNTH/USD"
    exchange_id = "testex"

    # (order_key, position_side, order_action, qty, price, note)
    fills = [
        ("cid-long-1", "LONG", "BUY", 1.0, 100.0, "entry"),
        ("cid-long-2", "LONG", "BUY", 1.0, 90.0, "dca"),
        ("cid-long-3", "LONG", "SELL", 1.0, 95.0, "partial reduce"),
        ("cid-long-4", "LONG", "SELL", 1.0, 110.0, "final reduce"),
        ("cid-short-1", "SHORT", "SELL", 1.0, 120.0, "entry short"),
        ("cid-short-2", "SHORT", "SELL", 1.0, 130.0, "dca short"),
        ("cid-short-3", "SHORT", "BUY", 0.5, 125.0, "partial cover"),
        ("cid-short-4", "SHORT", "BUY", 1.5, 115.0, "final cover"),
    ]

    min_sizes = {"LONG": 0.0, "SHORT": 0.0}

    def record_fill(order_key: str, side: str, action: str, qty: float, price: float, note: str) -> None:
        prev = float(progress.get(order_key) or 0.0)
        delta = qty - prev
        if delta <= 0:
            return
        progress[order_key] = qty
        closed = _apply_fill_to_runtime(
            runtime,
            side,
            action,
            delta,
            price,
            is_dca="dca" in note.lower(),
            reduce_only=bool((side == "LONG" and action == "SELL") or (side == "SHORT" and action == "BUY")),
            note=note,
            symbol=symbol,
            exchange_id=exchange_id,
            run_id=run_id,
        )
        add_bot_fill(
            conn,
            {
                "bot_id": bot_id,
                "run_id": run_id,
                "symbol": symbol,
                "exchange_id": exchange_id,
                "position_side": side,
                "order_action": action,
                "client_order_id": order_key,
                "external_order_id": None,
                "filled_qty": delta,
                "avg_fill_price": price,
                "is_reduce_only": (action == "SELL" and side == "LONG") or (action == "BUY" and side == "SHORT"),
                "is_dca": "dca" in note.lower(),
                "note": note,
                "event_ts": _iso(),
            },
        )
        if closed:
            closed["bot_id"] = bot_id
            create_position_history(conn, closed)
        leg_state = runtime["legs"][side]
        min_sizes[side] = min(min_sizes[side], float(leg_state.get("size", 0.0) or 0.0))

    # Apply fills twice to ensure delta tracking prevents duplicates
    for _ in range(2):
        for order_key, side, action, qty, price, note in fills:
            record_fill(order_key, side, action, qty, price, note)

    # Assertions
    cur = conn.execute("SELECT COUNT(*) FROM bot_fills WHERE bot_id=?", (bot_id,))
    fill_count = cur.fetchone()[0]
    assert fill_count == len(fills), f"expected {len(fills)} fills, got {fill_count}"

    cur = conn.execute("SELECT COUNT(*) FROM bot_positions_history WHERE bot_id=?", (bot_id,))
    pos_count = cur.fetchone()[0]
    assert pos_count == 2, f"expected 2 closed positions, got {pos_count}"

    legs = runtime.get("legs", {})
    """Deprecated: PnL sanity check (Postgres-only project)."""

    from __future__ import annotations


    def main() -> int:
        print("This example was removed. Project Dragon is Postgres-only.")
        return 0


    if __name__ == "__main__":
        raise SystemExit(main())
    print("PnL sanity check passed")
