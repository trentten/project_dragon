from __future__ import annotations

import sqlite3

from project_dragon import api_resilience
from project_dragon.live_worker import ExchangeDegradedBlocked, TradingGuardContext, _GUARD_CTX, safe_place_order


class _BrokerStub:
    open_orders = {}

    def get_position(self, _side):  # pragma: no cover
        return None


def test_safe_place_order_blocks_entries_when_exchange_open() -> None:
    acct_id = 999

    # Force circuit open using the global singleton defaults (open_after_failures=5).
    api_resilience.record_success(acct_id)
    for _ in range(5):
        api_resilience.record_failure(acct_id, error_type="TimeoutError")

    assert api_resilience.get_state(acct_id).value in {"OPEN", "HALF_OPEN"}

    conn = sqlite3.connect(":memory:")
    ctx = TradingGuardContext(
        conn=conn,
        emit_fn=lambda *_args, **_kwargs: None,
        bot_row={"id": 1, "user_id": "admin@local", "status": "running"},
        job_row={"id": 1},
        global_settings={"live_kill_switch_enabled": False},
        risk_cfg={},
        broker=_BrokerStub(),
        price_estimate=100.0,
        runtime={},
        account_row={"id": acct_id, "user_id": "admin@local", "status": "active", "risk_block_new_entries": 1},
    )

    token = _GUARD_CTX.set(ctx)
    try:
        try:
            safe_place_order(
                bot_row=ctx.bot_row,
                job_row=ctx.job_row,
                global_settings=ctx.global_settings,
                action="place_limit",
                qty=1.0,
                price_estimate=100.0,
                position_side="LONG",
                reduce_only=False,
                delegate=lambda: 1,
            )
            assert False, "Expected ExchangeDegradedBlocked"
        except ExchangeDegradedBlocked:
            pass

        # Reduce-only is allowed to attempt.
        out = safe_place_order(
            bot_row=ctx.bot_row,
            job_row=ctx.job_row,
            global_settings=ctx.global_settings,
            action="close_limit",
            qty=1.0,
            price_estimate=100.0,
            position_side="LONG",
            reduce_only=True,
            delegate=lambda: 1,
        )
        assert out == 1
    finally:
        _GUARD_CTX.reset(token)
        api_resilience.record_success(acct_id)
