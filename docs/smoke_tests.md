# Project Dragon – Live Trading Smoke Tests (WooX Perps)

This checklist is designed to validate safety gates and basic live loop behavior with **small size** and ideally `--dry-run` first.

## Prereqs
- `DRAGON_MASTER_KEY` set (Fernet key)
- A trading **Account** exists (Tools → Accounts) and is `active`
- Bot is created with an Account selected
- Worker is running:
  - Dry-run: `PYTHONPATH=src python -m project_dragon.live_worker --dry-run`
  - Live: `PYTHONPATH=src python -m project_dragon.live_worker`

## 0) Worker cadence sanity (fast tick vs bar tick)
- Pick a slower timeframe (e.g. `15m` or `1h`) so “waiting for bar” lasts long enough to observe.
- Expected: bot `heartbeat_at` updates roughly every ~3s (or your configured fast interval) even while waiting for the next bar.
- Expected: bot overview `Next` column shows something sensible (e.g. `Running`, `Risk blocked`, `Kill switch`).
- Expected: strategy decisions do **not** run repeatedly during the same closed candle.

## 0.5) Durable snapshots (bot + account)
- Expected: the worker upserts durable snapshots roughly every ~10s (or faster when state changes) while a bot is running.
- Expected: Live Bots overview continues to show `Health` / `Next` / account risk info based on the **latest snapshot** even if the worker goes offline.
- Expected: Accounts page shows a `Snap` timestamp and snapshot-derived columns for the latest known account state.
- If you intentionally create DB contention (e.g., two workers + UI open):
  - Expected: trading loop continues; snapshots are best-effort.
  - Expected event (best-effort): `snapshot_write_failed` warnings may appear, but bot/job should not flip to `error` just because snapshots failed.

## 1) Global processing gate
- Settings → `live_trading_enabled` = OFF
  - Expected: worker does not claim new `live_bot` jobs
  - Expected: already-running bots continue heartbeating but emit `live_trading_disabled_block` and skip broker actions
- Turn `live_trading_enabled` = ON
  - Expected: worker claims queued jobs and starts updating bot heartbeat

## 2) Global kill switch
- Ensure global kill switch is ON
  - Expected: bot stays `running`, but any place/cancel attempts are blocked
  - Expected events: `kill_switch_block` / `kill_switch_block`-style warnings
- Turn kill switch OFF (only after you trust the loop)

## 3) Hedge-mode enforcement (hard block)
- With a WooX account in **one-way mode**:
  - Expected: bot/job transitions to `error`
  - Expected event: `hedge_mode_required`
- With a WooX account in **hedge mode**:
  - Expected: bot starts normally

## 4) Account status blocks new entries (non-fatal)
- Set Account status to anything other than `active`
  - Expected: bot remains `running`
  - Expected: new *entry* order placements are blocked (reduce-only allowed)
  - Expected: heartbeat shows `Blocked by account`
  - Expected events: `account_risk_block`
  - Expected: the block appears quickly (≤ fast interval), not only on candle close

## 5) Account loss limits block new entries (non-fatal)
- Tools → Accounts → set:
  - `Block new entries when limits tripped` = ON
  - `Max daily loss (USD)` = small value (for a test account)
- Create a negative PnL scenario (or temporarily insert a small negative `bot_ledger` adjustment for testing)
  - Expected: `account_risk_block` events when attempting new entries
  - Expected: bot keeps running and continues syncing orders/positions
  - Expected: the block appears quickly (≤ fast interval), not only on candle close

## 5.5) Exchange degraded blocks new entries (non-fatal)

This validates the in-process API resilience layer (rate limiting/retries/circuit breaker) and the worker’s safe degrade mode.

  - `tests/test_api_resilience.py`
  - `tests/test_exchange_degraded_entry_block.py`

### 5.6) Multi-worker job lease reclaim (crash recovery)

Goal: if a worker dies mid-job, another worker can safely reclaim the job after the lease expires.

- Set a short lease for testing:
  - `export DRAGON_JOB_LEASE_S=15`
  - `export DRAGON_JOB_LEASE_RENEW_EVERY_S=5`
- Start worker A and wait for it to claim a `live_bot` job.
- Kill worker A (e.g., `kill -9 <pid>`).
- Wait `> DRAGON_JOB_LEASE_S`.
- Start worker B.

Expected:
- Worker B emits a bot event `job_lease_reclaimed`.
- Bot heartbeat briefly shows "Reclaimed stale job lease".
- No duplicate active processing: order placement remains idempotent via clientOrderId / intents.

## 6) Flatten Now (per-bot)
- On a running bot, request `Flatten Now` (requires typing `FLATTEN`)
  - Expected: cancels exchange-visible open orders
  - Expected: closes LONG and SHORT legs using reduce-only market orders
  - Expected: `flatten_*` events and `desired_action` cleared

## 7) Flatten ALL bots in Account
- Tools → Accounts → `Flatten ALL bots in account` (type `FLATTEN`)
  - Expected: all bots under the account get `desired_action=flatten_now`
  - Expected: each bot performs guarded flatten, then clears `desired_action`

## 8) Restart-safety: order intents
- With dynamic activation enabled (entry/DCA/TP activation pct > 0):
  - Start a bot; confirm it creates parked intents.
  - Restart the worker.
  - Expected: pending order ids remain stable; no duplicate “new” dynamic intents created for the same target.
  - Expected: when the market approaches the target, the intent activates exactly once.
  - Expected: bar_tick runs once per closed candle; restarting the worker does not re-run the same closed candle twice.

## 9) Credentials rotation sanity
- Rotate the underlying credential for an Account (if supported by your workflow)
  - Expected: worker can still decrypt and connect
  - Expected: no secrets are printed/logged

## Notes
- Always start with tiny sizing and `--dry-run` until you trust the behavior.
- If anything is blocked, inspect bot events first (Live → Bot detail → Events).
