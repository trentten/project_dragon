# Project Dragon – Trading Bot Manager

## Update: v0.2.0 (Postgres-only + UI performance + grid UX)

### Status
Shipped to dev/prod with a **Postgres-only** backend and a large Streamlit/AG Grid UX & performance pass.

### Major changes

#### Database & migrations (Postgres-only)
- **Removed SQLite support** end-to-end. Runtime now requires `DRAGON_DATABASE_URL` pointing at `postgres://` / `postgresql://`.
- Standardized driver on **psycopg v3** (with graceful fallback messaging if drivers missing).
- Added **one-shot migrations entrypoint**: `python -m project_dragon.db_migrate`.
  - Compose/Portainer runs migrations *once* via a dedicated `db_migrate` service, then starts UI/workers.
  - Added advisory lock around migrations to prevent concurrent runners.
- Hardened DB session behaviour:
  - Read-only query helpers avoid DDL and avoid leaving **idle-in-transaction** sessions.
  - Added per-session timeouts / rollback-on-error guards to prevent aborted transactions and hanging UI pages.
- Migrated large JSON blobs toward **JSONB** with dual-write + JSONB-preferred reads (text fallback where needed).
- Added/updated key indexes (including JSONB-derived indexes) and schema safety rails.

#### Candle cache + indicator warmup
- Unified candle loading to use an **analysis-window cache** shared by workers + UI.
- Ensured MA/HTF indicators have the required **warmup / prime history** (e.g., 6h MA over a 1h backtest window) so:
  - MAs plot across the full chart, and
  - signal generation aligns with expectations (TradingView parity improved).

#### AG Grid: server-side paging/filtering/sorting
- Implemented **server-side paging/filter/sort** for large tables:
  - Results
  - Runs Explorer
  - Sweeps (+ Sweeps → Runs)
  - Assets (Settings)
- Added filter translation allow-lists and tests to prevent SQL injection and schema drift.

#### UI/UX polish & consistency
- Theme tokens standardised (dark UI):
  - Background `#1c1c1d`
  - Cards `#262627`
  - Card border `#323233`
  - Inputs `#333334`
  - Accent `#F28C28`
- Standardised common display formatting via shared helpers:
  - Dates as **DD/MM/YY**
  - Duration + **Avg Position Time** in a consistent **D/H/M** format across grids.
- Grid layout quality:
  - Persisted column widths/layout via “Save layout” (and applied widths deterministically on render).
  - User-friendly column headings (vs raw DB names), consistent pills for timeframe/direction, conditional formatting restored.
  - Fixed hover highlight consistency and removed noisy/unused debug UI.
- Added editable **Shortlist** + **Shortlist Note** fields surfaced consistently in Runs Explorer + Run Details (and shared across views).
- Icons:
  - Standardised crypto symbol icons in grids and backtest selectors (shared “asset icon” path).
  - Filled Tabler icon usage restored in prod.

### Bugs squashed (high-signal list)
- Fixed Postgres migration issues (duplicate columns, placeholder mismatch, DDL in read-only transactions).
- Fixed sweeps that were interrupted by restart not marking **Done** on completion.
- Fixed rerun sweep UI error (`ValueError: x is not in iterable`) by stabilising widget state/options.
- Fixed Streamlit duplicate element key error in Stop Loss/ATR widgets.
- Fixed “pipeline aborted” / aborted-transaction issues by adding rollback + better logging in job operations.
- Fixed Create Bot path where `df` could be unbound; improved defensive handling.
- Fixed settings/assets page hanging by removing long-lived transactions and moving DDL to migrator.

### Deployment notes (Portainer / QNAP / VM)
- Keep Postgres internal to the compose network in prod (no host port binding).
- For local dev/Codespaces, optionally bind Postgres to `127.0.0.1:5432` via `docker-compose.dev.yml`.

### Next focus (post v0.2.0)
- Continue consolidating exchange/symbol sources (eliminate duplicate “woo/woox” and duplicate symbols).
- Expand server-side filtering to additional admin/ops grids as needed.
- Add more structured “saved periods” UX + defaults and make run naming more informative.

*Last updated: 2026-02-01*


## Full plan (living document)

### What this project is
Project Dragon is a **local-first trading bot manager** that supports:
- Backtesting + parameter sweeps
- Creating “Live bots” from backtest runs (config snapshots)
- A live worker that executes strategies safely with strong guardrails
- A Streamlit UI that acts as the control room

Today we’re **WooX Perps-focused**, but the design is intentionally extensible to more exchanges (and later, other asset classes like stocks).

### Guiding principles
- **Safety-first defaults** (live trading disabled by default; kill switch; risk limits)
- **Traceability** (every live bot links back to run_id / sweep)
- **Idempotent + restart-safe** (job locking; clientOrderId-first; ledger idempotency)
- **Multi-user ready** (header-based identity; encrypted credentials per user)
- **Minimal UI, maximal clarity** (overview table → focused bot detail)

### Current scope (v1)
**Backtesting**
- Single backtest and parameter sweeps (inline per-field sweep controls)
- Results + sweeps browsing

**Live trading**
- Bot storage (bots + bot_run_map)
- Worker-driven execution (bar-close driven)
- WooX broker with hedge-mode support + maker execution (BBO queue + fallback)
- PnL + ledger + events + positions snapshots

**Security / multi-user**
- Header-based identity (proxy header) + per-user encrypted credentials
- Global gate: live_trading_enabled
- Kill switch: global + per-bot
- Guarded flatten-now

### Near-term roadmap (recommended order)
**Phase 1 — Reliability + correctness**
1) **Hedge-mode enforcement**: detect non-hedge and fail fast with a clear error/event.
2) **Account model**: add trading_accounts and link bots → account → credential (supports multiple WooX accounts per user).
3) **Account-level risk limits**: daily loss / drawdown caps across all bots in an account.
4) **Reconciliation hardening**: persist order intents / lifecycle so restarts never double-send.

**Phase 2 — UX polish**
1) Backtest page UX refinement (direction radios, Exits SL/TP conditional fields, per-indicator rows, tooltips).
2) Live overview health indicators and stronger at-a-glance position summary.
3) Better “Create bot from run” flow (templates + defaults).

**Phase 3 — Scaling + deployment**
1) Harden Postgres operations for multi-user / multi-worker needs.
2) Run Streamlit behind a reverse proxy (auth → header identity).
3) Container deployment (worker + UI) with secrets management.

### Architecture overview
**Components**
- `storage.py`: Postgres schema + CRUD + migrations
- `streamlit_app.py`: UI (Backtest/Results/Sweeps/Live/Tools)
- `live_worker.py`: claims jobs, runs bots, writes heartbeats/events/ledger
- `woox_client.py` / `woox_broker.py`: exchange adapter + reconciliation
- `data_online.py`: candle fetching + ccxt exchange mapping

**Core flows**
1) Backtest → stores run config + results
2) Create bot from run_id → stores config snapshot + links bot_run_map + enqueues live_bot job
3) Worker claims job → bar loop runs strategy → broker places/cancels orders → events/ledger updated → UI reflects state

### Data model (high level)
- `users`: identity scope
- `exchange_credentials`: encrypted API keys per user
- `bots`: live bots (+ snapshots, PnL summary, desired_status/action)
- `jobs`: background tasks (backtest/sweep/live_bot)
- `bot_run_map`: traceability (bot ↔ run_id)
- `bot_events`: audit log / debugging
- `bot_ledger`: idempotent fills/fees/funding/pnl adjustments
- `bot_fills`, `bot_positions_history`: optional richer history tables
- `candles_cache`: local candle cache

### Operational safety checklist
- Keep `live_trading_enabled` OFF until ready
- Keep kill switch ON while validating
- Dry-run first
- Start tiny sizing
- Prefer maker settings only once reconciliation is stable

---

## Update: WooX BBO Queue Maker Orders (v1 Enhancement)

### Status

✅ Implemented in code (WooXBroker + WooXClient):

- **V3 trade endpoints** (`/v3/trade/order`, `/v3/trade/orders`).
- **Signature/body parity** (signed and sent using the exact same serialized bytes).
- **clientOrderId-first** workflow (place/cancel/query by clientOrderId; still stores exchange orderId when returned).
- **BBO Queue maker orders** (`type=BID/ASK` + `bidAskLevel=1..5`, no price).
- **Maker-safe fallback** to `type=POST_ONLY` at top-of-book using an orderbook helper.
- Offline payload sanity check script added (`examples/woox_payload_check.py`).

### Overview

Project Dragon’s WooX Perps adapter now supports **Best Bid/Offer (BBO) Queue maker orders** to minimize taker fees. Users can configure **queue levels 1–5** directly from the bot configuration or UI.

### What are BBO Queue Orders?

WooX supports placing orders at specific levels of the order book via the **BBO Queue** mechanism. These orders rest on the maker side (i.e., inside the book) and earn maker fees when filled. Each level represents how deep into the order book the order is placed:

| Queue Level | Description            | Maker Likelihood | Fill Speed |
| ----------- | ---------------------- | ---------------- | ---------- |
| **1**       | Closest to top of book | Moderate         | Fastest    |
| **2**       | Slightly deeper        | Higher           | Slower     |
| **3**       | Mid-depth              | Very high        | Slower     |
| **4**       | Deep                   | Near-certain     | Very slow  |
| **5**       | Deepest                | Highest          | Rare fills |

### Implementation Summary

#### Important API correctness notes (WooX V3)

- **Trading endpoints** for place/cancel/get order are under `` (and list orders under ``) in the official docs. citeturn24view0turn24view1turn24view2
- WooX signs requests as: `timestamp + method + request_path + request_body` (request\_path includes the querystring when present). Make sure the **exact** query ordering and **exact** request-body bytes you send are what you sign. citeturn24view4
- **Post-only** is represented by setting `type = "POST_ONLY"` on the order payload (not a generic `postOnly=true` flag). citeturn24view5
- Prefer sending `clientOrderId` (e.g., reuse our local\_id) so we can cancel/query orders deterministically without relying only on exchange `orderId`. citeturn24view0turn24view1


#### `WooXBroker`

A new method has been added:

```python
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
```

**Key logic:**

- Enforces correct WooX semantics:
  - `type = "BID"` for BUY orders.
  - `type = "ASK"` for SELL orders.
  - Includes `bidAskLevel` (1–5).
- Does **not** include a manual `price` – WooX will derive the correct book level internally.
- Preserves hedge-mode correctness with proper `positionSide` and `reduceOnly` flags.

**Error handling:**

- If WooX rejects `BID`/`ASK` types, the method logs a warning and automatically falls back to a `post-only limit` order at the same intent.

#### Bot Configuration

Each bot gains two new configuration options:

```yaml
prefer_bbo_maker: true
bbo_queue_level: 1   # 1–5
```

- `prefer_bbo_maker` toggles whether the bot should use BBO Queue maker orders when possible.
- `bbo_queue_level` defines how deep into the order book to rest the order.

#### Streamlit UI

Add new settings under **Order Settings**:

- Toggle: “Prefer maker via BBO Queue (WooX only)”
- Dropdown: “BBO Queue Level” (1–5)

### Live Behaviour

When active:

- All **limit entries**, **DCA orders**, and **take-profits** can use the BBO mechanism if `prefer_bbo_maker=True`.
- Dynamic activation respects maker preference:
  - When an order moves from PARKED → ACTIVE, `activate_dynamic()` will place via **BBO Queue** (or fall back) while reusing the original local ID.

### Safety & Fallbacks

- If BBO placement fails or the level becomes invalid, the broker falls back to `post_only` limit orders to retain maker intent.
- If both fail, a standard limit order may be used as a last resort (with warning logged).

### Example Config

```python
DragonAlgoConfig(
    prefer_bbo_maker=True,
    bbo_queue_level=2,
    entry_activation_pct=0.8,
    dca_activation_pct=0.5,
    tp_activation_pct=1.0,
)
```

### Summary

✅ Supports maker-fee-efficient execution using WooX BBO Queue orders.\
✅ Configurable queue depth (1–5) per bot.\
✅ UI toggle for user control.\
✅ Full fallback logic to post-only limits when BBO unavailable.

---

## Update: Live Trading (WooX Perps) – Bot Lifecycle + Job Control (v1)

### Status

✅ Implemented in code (Streamlit UI + storage + worker):

- `bots`, `bot_run_map`, `bot_events` tables (Postgres) + CRUD helpers.
- `jobs` now has `bot_id`, `updated_at`, `worker_id` (for locking) + indexes.
- Streamlit **Live Bots** tab:
  - Create bot from a `run_id` (loads config snapshot, allows edits, creates bot + job + links to run).
  - Controls: Pause / Resume / Stop (writes `desired_status`).
  - Bot Events expander for quick debugging.
- `live_worker.py` (bar-driven runner):
  - Polls/claims `live_bot` jobs (durable locking via `worker_id`).
  - Honours `desired_status` (`running|paused|stopped`).
  - Fetches latest **closed** candle via ccxt each bar.
  - Calls `DragonDcaAtrStrategy.on_bar(...)` per new bar.
  - Uses `WooXBroker` (hedge mode supported).
  - Persists runtime state into bot config (`_runtime.last_candle_ts_ms`, `_runtime.bar_index`).
  - Heartbeats + bot/job status transitions.
  - Error handling: marks bot + job **error** and records a bot\_event.

### Safety / Ops features

✅ Implemented:

- `--dry-run` mode (skips real WooX order placement/cancel; logs bot\_events instead).
- Risk guardrails via config `_risk`:
  - `max_open_orders`
  - `max_position_size_per_side`
  - `max_notional_per_side`
  - Guard trip => bot/job **error** + bot\_event
- Durable job locking:
  - A worker claims a job (stores `worker_id`) and only the claiming worker runs it.

### Reconciliation and audit trail (v1)

✅ Implemented:

- WooX order reconciliation prioritises **clientOrderId** (stable mapping back to our local IDs).
- Tracks **partial / rejected** states (adds `OrderStatus.PARTIAL` and `OrderStatus.REJECTED`).
- Captures filled size + average fill price and attaches timestamps where available.
- Emits bot events on:
  - order status transitions (open → partial → filled / cancelled / rejected)
  - order lifecycle changes (activation state changes, missing snapshot warnings, forced cancellation)
- Offline reconciliation demos:
  - `examples/woox_payload_check.py` validates payload shapes for BID/ASK and POST\_ONLY + fallback behavior.
  - `examples/woox_reconcile_check.py` replays snapshots to validate event capture.

### Missing-from-snapshot policy (important)

✅ Implemented in `WooXBroker.sync_orders`:

- **PARKED** intents are treated as *local-only* and are never auto-cancelled.
- For orders expected to exist on exchange (have `external_id` or ACTIVE `client_order_id`):
  - If an order is missing from the exchange snapshot, increment `miss_count` on the same Order object.
  - Emit `sync_warning` only on **miss\_count 1 and 3** to reduce noise.
  - After **3 consecutive misses**, optionally verify via `get_order(clientOrderId)`; if still missing, mark cancelled and emit `order_missing_cancelled`.
- Identifiers are not overwritten with `None` (prevents “losing” IDs during sparse API responses).
- Price/size only update when present (prevents clobbering good local data with empty fields).

### Notes about hedge mode (v1)

- WooX hedge mode is enabled at the account level.
- Our current strategy state machine is still primarily “single-position”.
- Recommended v1 approach for true LONG+SHORT simultaneously:
  - Run **two bots** for the same symbol (one long-only, one short-only), or
  - Backlog: refactor strategy to maintain **two independent legs** (LONG and SHORT) in one bot.

### How to run the worker

From repo root:

```bash
PYTHONPATH=src python -m project_dragon.live_worker
```

Dry-run:

```bash
PYTHONPATH=src python -m project_dragon.live_worker --dry-run
```

### Next steps

- Live smoke test with **very small size**:
  - Validate pause/resume/stop.
  - Confirm candle “new bar” detection is correct for each timeframe.
  - Verify maker path (BBO vs POST\_ONLY fallback) and cancellation behaviour.
- Backlog: WebSocket ingestion later (private fills/order updates + public candles), but keep v1 bar-driven runner as baseline.

---

**Next steps (implementation backlog):**

- Price-driven runner (tick-based) for faster reaction (backlog).
- Hot-reload strategy params safely (backlog).
- “Flatten now” guarded action (cancel all + reduce-only market close).
- Reconciliation: robust partial-fill handling + consistent PnL/funding tracking (perps).

---

## UI Cleanup Plan (after Safety + PnL steps)

### Goals

- Make **Live trading** feel like a simple “control room”: a clean bot list, then a focused bot detail view.
- Keep navigation fast even with many bots/runs/jobs.
- Ensure every live bot remains **traceable** back to its originating `run_id` (and sweep if applicable).

### Proposed Layout (Streamlit)

#### A) Live Bots – Overview (default view)

A single, compact table with:

- **Name**
- **Exchange**
- **Symbol**
- **Timeframe**
- **Status** (and desired\_status if different)
- **Realized PnL**
- **Unrealized PnL**
- **Last heartbeat**
- **Last event / last error**
- Actions: **Open**, **Pause/Resume**, **Stop**, **Flatten** (guarded), **Dry-run toggle indicator**

Recommended UX:

- Filters above table: exchange, symbol search, status, only-errors.
- Sort toggles: newest updated, highest PnL, biggest drawdown.
- Conditional formatting: green/red PnL; yellow for stale heartbeat; red for error.

#### B) Bot Detail – “Bot page”

Use query params for deep links: `?bot_id=123`. Sections (top-to-bottom):

1. **Header**: Name, symbol, timeframe, status/desire, worker\_id, heartbeat.
2. **PnL panel**: realized/unrealized, today’s PnL, fees, funding (once added).
3. **Charts**:
   - Price chart + orders/fills markers (optional v1)
   - Equity / realized PnL curve (v1 once ledger exists)
4. **Controls (guarded)**:
   - Resume / Pause / Stop
   - Flatten Now (guarded confirmation)
   - Kill-switch status indicator
5. **Config**:
   - Show “effective config” + runtime overlays (`_runtime`, `_risk`, maker settings)
   - Allow editing only **runtime-safe fields** (ex: risk limits, activation band, maker preference)
   - Track a config revision history (optional; backlog)
6. **Events / Logs**:
   - Paginated bot\_events table with severity filter
   - “Copy diagnostics” button (prints bot\_id, run\_id, symbol, timeframe, last 50 events)
7. **Traceability**:
   - Linked **Backtest Run** card (run\_id) + open results
   - Linked **Sweep** card (if applicable)

#### C) Backtests/Results integration (small but valuable)

- In **Backtest Results** detail page: show “**Create bot from this run**” button.
- In **Results table**: add a small indicator column “**Live bot**” if any bot\_run\_map links exist (optional; backlog if slow).

### Data Requirements

To display realized/unrealized PnL cleanly:

- Prefer a local **fills/ledger** table (best long-term) and compute:
  - realized PnL, fees, funding, trade count, win rate
- Short-term v1 fallback (if ledger isn’t ready):
  - unrealized from positions endpoint, realized from account/positions if available; still store locally when possible.

### Backlog UI Enhancements

- Bot grouping (paired long/short “hedge set”).
- Bulk actions (pause all, stop all, flatten all).
- Presets and templates (“create bot from template”).
- Dedicated “Worker status” page.

---

## Note: Grid Filtering Performance (v1)

- Current UI uses a shared Streamlit filter bar (dropdowns/ranges/search) above grids and applies filtering in-memory (pandas) after a single DB query.
- If a grid grows beyond ~5–20k rows, switch backend filtering to SQL (`WHERE` + `LIMIT/OFFSET` pagination) to keep UI responsive.
- Keep the filter UI identical; only swap the data loader from pandas filtering to server-side filtering (same `filters` dict shape).


---

## Update: Multi-user deployment foundations (v1)

### Status

✅ Implemented (Streamlit + DB + worker):

- **Encrypted exchange credentials** stored in Postgres per-user (Fernet).
- **Env-only master key**: `DRAGON_MASTER_KEY` must be a valid Fernet key.
- **Identity scoping**:
  - Reads user identity from a reverse-proxy header (default `X-Forwarded-User`, configurable via `DRAGON_AUTH_HEADER`).
  - Fallback to `DRAGON_USER_EMAIL`, then `admin@local`.
  - Parses formats like `Name <email@domain>` and comma-separated lists.
- **DB schema additions**:
  - `users`, `exchange_credentials`
  - `bots.user_id`, `bots.credentials_id` + indexes + backfill
- **Worker uses stored credentials**:
  - Loads and decrypts credentials by `(bot.user_id, bot.credentials_id)`.
  - On missing/decrypt failure: emits `credential_missing`, sets bot/job to error without leaking secrets.
- **Credentials UI**:
  - Tools → **Credentials** page (create/list/delete).
  - Bot creation requires selecting a credential.
  - Bot detail allows changing credential.
- **Stored-credential connectivity test tool**:
  - Tools → Credentials → “Test WooX connectivity” (calls get\_positions / get\_orders / orderbook; returns only non-sensitive fields).

### Env requirements (runtime)

- `DRAGON_MASTER_KEY=<Fernet.generate_key()>`
- Optional:
  - `DRAGON_AUTH_HEADER=X-Forwarded-User` (or your proxy header)
  - `DRAGON_USER_EMAIL=you@example.com` (fallback)

---

## Update: Live trading safety gates (v1)

### Global "live trading enabled" gate

✅ Implemented:

- New app setting: `live_trading_enabled` (default **False**).
- Settings UI toggle: “Enable Live Trading (Worker Processing)”.
- Worker behavior when OFF:
  - Does not claim new queued `live_bot` jobs.
  - For already-running jobs: heartbeat shows blocked, emits `live_trading_disabled_block` once per bot, skips strategy/broker work.
  - Pause/stop still works.

### Kill switch (global + per-bot)

✅ Implemented:

- Global kill switch setting (separate from live\_trading\_enabled).
- Per-bot kill switch stored under `config_json._risk.kill_switch`.
- Worker guard blocks **all** broker place/cancel actions when kill switch active.
  - Emits `kill_switch_block` events and increments `bots.blocked_actions_count`.
  - Flatten requests are blocked safely (no error), desired action cleared.

### Guarded “Flatten Now”

✅ Implemented:

- `bots.desired_action` supports `flatten_now`.
- UI requires typing `FLATTEN` to confirm.
- Worker executes cancel + reduce-only close flow (dry-run supported).
- Emits detailed events (`flatten_*`) and clears `desired_action` on completion.

---

## Update: PnL + ledger + positions (v1)

### PnL summary + history

✅ Implemented:

- Bot-level summary fields on `bots`:
  - realized/unrealized PnL, fees\_total, funding\_total, dca\_fills\_current, mark\_price
- Worker derives mark price with priority:
  - ccxt ticker → orderbook mid → candle close (persisted to `bots.mark_price`)
- Bot overview and bot detail show:
  - Realized, Unrealized, Fees, Funding, Net After Costs (ROI uses net)

### Unified ledger

✅ Implemented:

- `bot_ledger` table with idempotent inserts (`bot_id, kind, ref_id` unique when ref\_id present).
- Worker writes ledger rows for:
  - fills + fees (fee estimated if missing)
  - funding ingestion (best effort)
  - realized PnL adjustments (best effort when closing position)
- Bot detail includes **Ledger** tab with totals + recent rows.

### Positions snapshot

✅ Implemented:

- Worker persists raw WooX `get_positions(symbol)` snapshots to `bots.positions_snapshot_json` best-effort.
- Bot detail **Positions** tab renders:
  - Exchange positions table (by side + TOTAL row)
  - Open orders grouped summary (Active/Parked/Total)
  - Strategy runtime legs under expander

---

## Update: UI navigation + persistence improvements (v1)

### Sidebar navigation

✅ Implemented:

- Sidebar menu groups:
  - Backtesting: Backtest / Results / Sweeps
  - Live: Live Bots
  - Tools: Jobs / Candle Cache / Settings / Credentials
- Removed run settings from sidebar (moved to Settings).
- Deep-link routing fixed for Jobs → Open sweep/run/bot.

### Live Bots overview

✅ Implemented:

- Single sortable table using `st.data_editor`.
- Open is a LinkColumn deep-linking to bot detail.
- No row selection / no “open selected bot”.
- Overview columns include PnL, ROI, DCA status, kill switch indicators.

### Bot detail dashboard layout

✅ Implemented:

- Header + chart + KPI blocks + compact controls bar.
- Tabs grouped (Orders & Fills as sub-tabs).
- Danger zone expander contains kill switch, flatten, blocked reset.
- Health box shows heartbeat freshness, last\_error, recent event severity.

### Page input persistence

✅ Implemented:

- Backtest page widget inputs persist in session state until changed.
- “Show all runs” on Results/Sweeps persists.

---

## Update: Backtest page refactor (v1)

✅ Implemented:

- Two-column layout with bordered “cards”.
- Inline per-field Fixed/Sweep controls (no sweep selectors at top).
- Grouped into clearer sections; better use of page width.

### Proposed follow-up UX improvements (queued)

- Trade direction as radio (Long-only / Short-only).
- Re-order left-column sections: Core/Position → Entry & Indicators → DCA → Exits.
- Exits split into SL vs TP with conditional fields (hide fixed TP when ATR TP enabled).
- Each indicator on its own row with settings beside it; add hover help.

---

## Update: Futures leverage support (WooX) (optional)

✅ Implemented:

- `WooXClient.set_leverage()`.
- Worker applies leverage once per bot if enabled via config:
  - `_futures.apply_leverage_on_start`, `_futures.leverage`
  - Stores `_runtime.applied_leverage` + timestamp.
  - Dry-run emits `leverage_set_dry_run`.
  - Clamps using `_risk.max_leverage`.
- UI fields added in Bot Config + Create-bot panel.

---

## Hedge mode support status

✅ Current posture:

- Primary recommended approach for simultaneous LONG + SHORT is **two bots**.
- Broker normalizes legacy `positionSide=BOTH` into LONG/SHORT by sign of holding.
- Bot creation stores `_trade.primary_position_side` for single-leg strategy compatibility.

✅ Pending safety check:

- Add a broker/account sanity check: if WooX account is not in hedge mode (or API reports one-way), fail fast with a clear error and event.

---

## Roadmap: Next best steps

### Reliability + correctness

- Hedge-mode enforcement check (fail fast, clear UI error).
- Tighten position/PnL calculations using “hybrid” exchange numbers where trusted, with consistent mark-price fallback.
- Add lightweight websocket layer (later): faster fills/order updates, reduces polling and reconciliation gaps.

### UX & workflow

- Backtest page UX polish (radio direction, exits split, indicator rows, tooltips).
- Sweep settings remain inline per-field; add small sweep summary strip.
- Results ↔ Live linking: “Create bot from this run” in Results detail.

### Multi-user hardening (future)

- Put Streamlit behind a reverse proxy (Caddy/Nginx/Traefik) providing auth headers.
- Continue hardening Postgres when multiple workers/users become real.

---

## Quick operational checklist

1. Start Streamlit

- Ensure `DRAGON_MASTER_KEY` is set.
- Ensure reverse proxy sets auth header OR set `DRAGON_USER_EMAIL`.
- Create credentials in Tools → Credentials.

2. Run worker (dry-run first)

```bash
PYTHONPATH=src python -m project_dragon.live_worker --dry-run
```

3. Enable processing

- Settings → toggle `live_trading_enabled` ON (still safe with kill switch).

4. Safety

- Keep global kill switch ON while validating charts/events/positions.
- Only disable kill switch once confident, with small sizing.

## Update: v0.2.0 (Postgres-only + UI performance + grid UX)

### Status
Shipped to dev/prod with a **Postgres-only** backend and a large Streamlit/AG Grid UX & performance pass.

### Major changes

#### Database & migrations (Postgres-only)
- **Removed SQLite support** end-to-end. Runtime now requires `DRAGON_DATABASE_URL` pointing at `postgres://` / `postgresql://`.
- Standardized driver on **psycopg v3** (with graceful fallback messaging if drivers missing).
- Added **one-shot migrations entrypoint**: `python -m project_dragon.db_migrate`.
  - Compose/Portainer runs migrations *once* via a dedicated `db_migrate` service, then starts UI/workers.
  - Added advisory lock around migrations to prevent concurrent runners.
- Hardened DB session behaviour:
  - Read-only query helpers avoid DDL and avoid leaving **idle-in-transaction** sessions.
  - Added per-session timeouts / rollback-on-error guards to prevent aborted transactions and hanging UI pages.

#### UI & Grid system
- Consolidated AG Grid column definitions and renderers across Results / Runs / Sweeps.
- Standardized pill renderers for **direction** and **timeframe**.
- Unified date and duration formatting (single DHM formatter used everywhere).
- Asset icon rendering routed through shared helpers.
- Shortlist + Shortlist Note standardized, editable, and persisted consistently across grids.

---

## Update: UI Stabilisation & Admin Recovery (Post v0.2.0)

### Status
✅ **Completed and validated**. This phase focused on correctness, stability, and admin recoverability after recent refactors.

### Completed work

#### UI Context & wiring
- Introduced `UIContext` + `build_ui_context()` to centralise shared UI state and helpers.
- Wired into `streamlit_app.py` with **no behaviour change** and no heavy import-time work.
- Serves as a foundation for future refactors; no page extraction performed yet.

#### Settings → Assets (fully restored)
- CCXT asset sync now persists correctly with deterministic diagnostics (exchange id, markets fetched, rows upserted, DB identity).
- **Exchange ID contract locked (Option A):** all assets, categories, and memberships are stored under the **UI exchange_id** (e.g. `woox`). CCXT IDs are used only for fetching.
- Fixed empty list failures:
  - SQL placeholder bug in `list_categories`.
  - Missing imports causing silent NameErrors and greyed-out controls.
  - Category refresh so newly created categories appear immediately.
- Category membership editing and Enable/Disable actions restored.

#### Runs / Results / Sweeps grids
- Restored editable **Shortlist** and **Shortlist Note** in Runs Explorer (friendly headers + persistence).
- Asset display parity enforced across Results / Runs / Sweeps via shared helpers.
- Removed legacy/duplicate fields and duplicate UI renderers.
- Consolidated duration and Avg Position Time formatting to a single canonical path.

#### Jobs & Sweeps cancellation
- Fixed `NameError: set_job_cancel_requested`.
- Cancel now works from Jobs and Sweeps pages.
- Added smoke test coverage for cancelling jobs and sweeps.

#### Import hygiene (stability decision)
- UI-layer standardized on module import: `from project_dragon import storage`.
- All UI call sites reference `storage.*` to avoid missing-import regressions during refactors.
- No measurable performance impact.

### Locked design decisions
- **Exchange ID handling:** UI exchange_id is the source of truth; CCXT IDs are internal only.
- **Refactor posture:** stability first; no structural extraction until the system is quiet and stable.

### Deferred to backlog
- Phase 3 structural refactor:
  - Extract large page sections from `streamlit_app.py` into existing UI modules.
  - Further consolidate grid helpers and UI components.
  - Split `storage.py` by concern (migrations, queries, admin helpers).
- Performance & ops:
  - Postgres query profiling under load.
  - Worker polling optimisation.
- UX polish:
  - Backtest page refinements.
  - Results ↔ Live linking improvements.

### Current posture
- Core admin flows stable.
- Assets, categories, jobs, sweeps, and cancellation verified working.
- Phase 3 explicitly paused and documented.

