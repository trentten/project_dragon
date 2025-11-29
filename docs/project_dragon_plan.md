Project Dragon – Trading Bot Manager

0. Document Purpose

Living design doc for Project Dragon (Trent + ChatGPT).

Tracks vision, scope, assumptions, research, and design.

We will update this as we learn more and refine the architecture.

Change tracking will be done via a simple Change Log section at the end.

1. Vision & Goals

1.1 Vision

Build a modular trading bot manager in Python (“Project Dragon”) that can:

Design and configure strategies/bots.

Backtest them robustly across multiple parameter ranges.

Compare and review results with proper metrics and charts.

Deploy chosen configurations as live bots.

Monitor and manage live bots (PnL, positions, health, logs).

1.2 Primary Goals (v1 timeframe)

Robust backtesting core

Single-asset, single-strategy backtests.

Realistic simulation (fees, slippage model, order types).

Clear performance metrics (Sharpe, Sortino, Win %, Profit Factor, etc.).

Strategy lab

Run parameter sweeps (grid search first).

Store each run with parameters + metrics.

UI to sort, filter, and shortlist best configs.

Live bot manager (v1-lite)

Deploy a strategy configuration as a live bot.

Connect to at least one exchange/broker.

Dashboard of running bots, with basic health + PnL.

Port “Dragon DCA ATR” strategy

Re-implement Trent’s Haas-based DCA + ATR trailing-exit bot.

Use it as the reference strategy for engine and UI.

1.3 Non-goals (for now)

High-frequency / ultra-low-latency trading.

Options, perps funding-arb, or complex multi-leg spreads.

Multi-asset portfolio optimization engine.

AI/ML optimizers beyond simple generational search.

2. Initial Scope Outline

2.1 v0 – Proof of Concept

Load historical OHLCV data from CSV.

Run one strategy (Dragon DCA ATR simplified) over one symbol.

Produce:

Trade list.

Equity curve.

Basic metrics (P/L, Win %, Max DD, Profit Factor).

Display results in a simple Streamlit app.

2.2 v1 – Usable Tool

Add parameter sweeps (brute-force grid).

Persist backtest runs (e.g. SQLite/Parquet/CSV).

Add richer metrics and charts.

Implement one live exchange adapter.

Basic bot monitoring dashboard.

2.3 Future (v2+ Ideas)

Walk-forward and out-of-sample testing.

Generational/AI-based parameter search.

Strategy portfolios (multiple bots per symbol / multiple symbols).

Alerting (Discord/Telegram).

Role-based configuration ("research", "production").

3. Design Principles

Separation of concerns

Strategy logic vs execution vs risk management vs UI.

Exchange-agnostic core

Engine talks to an abstract broker interface.

Deterministic backtests

Same data + config = same result.

Honest simulations

Include fees, slippage, realistic order fills.

Safety first

Explicit risk limits and guardrails.

Extensibility

Adding new strategies or exchanges should not require rewrites.

4. Research & Best Practices (Summary)

4.1 Data & Backtesting

Use clean, well-sourced historical data.

Avoid common backtest biases:

Look-ahead bias.

Data snooping / overfitting.

Survivorship bias.

Always separate:

In-sample (for design).

Out-of-sample (for validation).

Forward test / paper trading.

Model transaction costs and approximate slippage.

Consider simple walk-forward procedures for serious strategies.

4.2 Risk Management

Position sizing rules (e.g. max % of equity per bot/trade).

Global caps:

Max concurrent bots.

Max exposure per asset and overall.

Drawdown limits and circuit breakers:

Per-bot max DD.

Initial per-bot target max DD: ~20% (configurable).

Account-level kill switch.

Clear definition of:

Stop loss, take profit, trailing exits.

What happens on gaps, extreme volatility, or exchange issues.

4.3 Architecture & Implementation

Modular architecture:

Core engine (backtest + live).

Strategy layer.

Broker adapters (exchanges/Haas/etc.).

Data layer.

UI layer.

Strong configuration management:

Strategy configs versioned.

Ability to reproduce a live bot from a saved config + code version.

Logging and observability:

Structured logs for decisions and orders.

Per-bot metrics and health indicators.

4.4 Monitoring & Operations

Real-time monitoring for live bots:

PnL, exposure, open orders, error rates.

Alerting for:

Repeated API failures.

Unusual behaviour (no trades when expected, too many trades, etc.).

Tools for safe interventions:

Pause/stop a bot.

Flatten all positions.

4.5 Security & Safety

4.6 Order & Execution Model (Planned)

Support for multiple order types in both backtest and live modes:

Market orders.

Limit orders.

Maker-style limit orders (price offset to favour maker fills when possible).

Dynamic limit orders:

Ability to specify a target price plus an activation band (e.g. "activate when within X% of last price").

Until activation, orders can remain parked or unsubmitted.

Once active, they behave like normal limit/maker orders.

Execution simulation will model:

Fill probabilities for each order type based on candle high/low/close.

Distinction between maker/taker fees where supported by the venue.

4.5 Security & Safety

API key management:

Read/write keys only where needed.

IP whitelisting when possible.

Avoid storing secrets in plain text.

Separate test/play accounts from real size.

Always support paper trading / sandbox mode.

5. Open Questions / Options

We will refine these with Trent’s preferences.

Primary market(s)

Crypto only? Which exchanges?

Any interest in FX/CFD/other later?

Holding period focus

Intra-day vs swing vs multi-day DCA.

Infrastructure

Run primarily on QNAP / home server?

Cloud VPS later for resilience?

Data source for backtests

Exchange-native historical data downloads.

Third-party data service.

Minimum viable monitoring

What must the initial bot dashboard show for you to feel safe running real money?

5.1 Decisions so far

Primary markets / venues

Crypto via Woox.

Stocks via Moomoo.

Strategy styles

Core: DCA/swing strategies (like Dragon DCA ATR).

Future: room for scalping / faster intraday strategies.

Risk tolerance (starting point)

Target per-bot max drawdown: ~20% while we learn and tune.

Infrastructure preference

Run first on QNAP / home server, with option to move to VPS later for resilience.

Minimum viable live monitoring

For each running bot, dashboard should show at minimum:

Bot/strategy name and strategy version / ID.

Symbol / market.

Current position (side, size, average entry).

Open PnL and cumulative PnL.

Duration running (start time and elapsed time).

Time of last trade.

Recent/repeated error indicators.

6. Initial Amendments / Ideas (Draft)

6.0 Settings Granularity Committed for v0/v1

For Project Dragon v0/v1 we will support, at an engine/config level:

Separate dynamic activation percentages for:

Initial entry orders.

DCA ladder orders.

Take-profit / exit orders.

Separate timeframes/intervals per indicator, including at least:

Trend MA interval.

BBands interval.

MACD interval.

RSI interval.

These can still be surfaced as "Advanced" fields in the UI, but they are first-class citizens in the configuration model from the start.

Order-type flexibility & dynamic limits

Strategies can choose between market, limit, and maker-style orders explicitly.

Introduce a configurable dynamic limit behaviour:

Each strategy can specify a target price plus an activation band (e.g. "activate when within X% of the desired level").

Until the market is within that band, the order is considered parked (not resting on the book in live mode; not eligible for fills in backtests).

Once price moves inside the activation band, the order becomes active and behaves like a normal limit/maker order.

If price moves back outside the activation band before the order is filled, the engine should cancel/park the order again.

Activation bands can be configured separately for entry, DCA, and TP orders via the DynamicActivationConfig (entry_pct, dca_pct, tp_pct).

These behaviours must be:

Available consistently in both backtests and live trading.

Visible in the UI (e.g. show whether an order is dynamic/parked/active, and what activation band is configured).

Potential upgrades to the initial idea, inspired by research and experience:

Explicit risk module

Separate risk-engine that can veto trades or reduce size.

Configurable rules: max daily loss, max DD, etc.

Config + code version pinning

Every backtest and live bot run stores:

Strategy name + version.

Parameter snapshot.

Git commit hash (when using Git).

Scenario tags

Allow tagging backtests as:

Bull, bear, chop, high-vol, low-vol.

Later, compare strategy robustness across regimes.

Walk-forward-friendly design

Architect the backtest API so it’s easy to run rolling optimizations.

Diagnostics-first UI

When a bot misbehaves, it should be easy to see:

What it "thought" (signals).

Which rules fired (entries/exits).

Why a trade was or wasn’t taken.

These are drafts — we will keep or discard after discussion.

6.7 UI / UX Overview (Backtesting, Results, Live Trading)

Core UI surfaces

Backtesting workspace

Purpose: configure and run single backtests or parameter sweeps for a given strategy (starting with Dragon DCA ATR).

Key elements:

Strategy selector (e.g. DragonDcaAtr).

Config editor (form bound to DragonAlgoConfig):

General settings (max entries, order types, cooldowns, dynamic activation %, etc.).

Exit settings (SL/TP, ATR TP options, trailing parameters).

DCA settings.

Indicator settings (intervals/lengths for MA/BB/MACD/RSI).

Backtest range selection (symbol, timeframe, start/end).

Run controls:

"Run backtest" (single config).

"Run sweep" (grid search over selected parameters).

Status indicator while backtests are in progress.

Backtest results & analysis

Purpose: compare, filter, and inspect completed backtests.

Key elements:

Results table (one row per backtest run) with:

Strategy name + version.

Symbol / timeframe.

Date range.

Key metrics: net P/L, max DD, Sharpe, Sortino, Win %, Profit Factor, CPC, etc.

Config summary (or a link to view full config).

Flags/tags (e.g. shortlisted / deployed).

Table features:

Sorting on any metric.

Filtering by ranges (e.g. DD < X, Sharpe > Y).

Text search on tags/notes.

Detail view for a single run (when a row is clicked):

Equity curve chart (equity vs time or bar index).

Drawdown chart.

Price chart with overlays:

Strategy indicators (MA, BB, etc.) where practical.

Entry/exit markers (initial entry, DCA levels, TP/SL hits).

Optional average entry line.

Trades table:

Timestamp, side, price, size.

Realized PnL, cumulative PnL.

Note (e.g. Initial-Long, DCA-L2, TP_ATR).

Config snapshot (read-only view of the DragonAlgoConfig used for this run).

Actions:

"Shortlist" (mark as a candidate for deployment).

"Deploy as live bot" (when live trading is available).

Live trading dashboard

Purpose: monitor and manage running bots.

Key elements:

Bots list (one row per live bot):

Bot name/label.

Strategy name + version.

Exchange / venue (Woox, Moomoo, etc.).

Symbol / market.

Status (running/paused/stopped/error).

Current position: side, size, average entry.

Unrealized PnL and realized PnL.

Bot-level equity / PnL summary.

Started at / uptime.

Last trade time.

Table features:

Filter by status, symbol, venue.

Sort by PnL, DD, uptime, etc.

Bot detail view (when a row is clicked):

Equity curve for this bot since start.

Current open orders (including whether they are dynamic/parked/active, with activation bands shown).

Recent trades list.

Config snapshot (strategy + parameters used at launch).

Controls:

Pause / resume.

Stop and flatten.

(Future) Edit certain safe parameters.

Health & safety indicators:

Per-bot drawdown vs configured limit.

API error rates / connection issues.

Visual alerts for bots hitting DD limits or error conditions.

UI technology notes (for implementer)

v0/v1 UI can be built in Streamlit for speed of development:

One multi-page app with:

"Backtesting" page (config + run + immediate results).

"Results" page (historical backtest browser and detailed run view).

"Live Bots" page (dashboard + bot detail views).

Charts: use Plotly or Matplotlib via Streamlit for equity/price/trades overlays.

Longer term, a more custom web UI (React, etc.) can replace Streamlit while reusing the same Python back-end.

7. Next Actions

Trent: answer / comment on Open Questions.

ChatGPT: propose a concrete data model and engine interface based on confirmed choices.

Then: start designing the backtest engine for Dragon DCA ATR.

8. Data Layer & Persistence (Draft)

High-level approach

v0 can run purely from CSV/Parquet OHLCV files and in-memory results.

v1 should introduce a small SQL data layer (likely SQLite initially, with an easy path to Postgres later) for:

Indexing and querying backtest runs.

Storing summary metrics and metadata.

(Optional) Caching candle data locally.

8.1 Candle data

For now, Project Dragon can assume flat files (CSV/Parquet) per symbol + timeframe are the primary source of truth for OHLCV.

If/when a SQL table is introduced for candles, a minimal schema could be:

candles table (per exchange or per data source):

id (PK)

symbol (text)

timeframe (text, e.g. 1m, 5m, 1h)

timestamp (datetime, UTC)

open, high, low, close (real)

volume (real)

Unique index on (symbol, timeframe, timestamp).

Recommendation:

Start with files for v0/v1.

Add a candle table later if data reuse, cross-strategy queries, or volume grows enough to justify it.

8.2 Backtest runs & results

SQL is much more valuable for backtest metadata than for raw candles. Suggested tables:

backtest_runs:

id (PK)

created_at (datetime, UTC)

symbol (text)

timeframe (text)

strategy_name (text, e.g. DragonDcaAtr)

strategy_version (text or int)

config_json (text) – full serialized DragonAlgoConfig for reproducibility.

start_time, end_time (datetime, backtest range)

Key summary metrics as columns, e.g.:

net_profit

max_drawdown

sharpe

sortino

win_rate

profit_factor

cpc_index

etc.

backtest_trades:

id (PK)

run_id (FK → backtest_runs.id)

trade_index (int, sequential for each run)

timestamp (datetime)

side (text: long/short)

price (real)

size (real)

realized_pnl (real)

note (text – e.g. Initial-Long, DCA-L3, TP_ATR, etc.).

(Optional) backtest_equity_points:

id (PK)

run_id (FK)

index or timestamp

equity

Design intent:

Backtest runs are first-class objects that can be searched, sorted, and filtered by:

Strategy version.

Config parameters.

Key metrics (e.g. Sharpe > X, DD < Y, WinRate > Z).

All the raw detail (trades + equity curve) is available for drill-down when a run is opened in the UI.

8.3 Live bots (future)

Future table sketches (not required for v0, but useful to keep in mind):

live_bots:

id (PK)

name / bot_label

exchange / venue

symbol

strategy_name

strategy_version

config_json

status (running/paused/stopped)

started_at, stopped_at

initial_equity, current_equity

live_bot_events / live_bot_logs:

bot_id (FK)

timestamp

event_type (trade, error, restart, risk_cutoff, etc.)

payload_json (for flexible details).

SQL is helpful but not mandatory for v0. The guidance for the next AI:

Use simple file-based storage for raw candles initially.

Consider SQLite for backtest_runs / backtest_trades once we start doing serious parameter sweeps and need fast querying.

9. Implementation Status & Handover Notes

This section is meant as a handover for whoever (or whichever AI) continues implementation.

9.1 Current repo state (as of 2025-11-29)

GitHub repo (name): project_dragon.

Language: Python.

Current core modules under src/project_dragon/:

domain.py – core types (Candle, Order, Position, Trade, BacktestResult).

config_dragon.py – DragonAlgoConfig and related dataclasses, including DynamicActivationConfig (entry_pct/dca_pct/tp_pct).

broker_sim.py – simple long-only broker simulator with:

Market and limit orders.

Fee model.

Single net position.

engine.py – BacktestEngine and BacktestConfig:

Runs a Strategy over an iterable of Candle objects.

Calls strategy.on_bar(index, candle, broker) each bar.

strategy_dragon.py – DragonDcaAtrStrategy skeleton with basic state object (DragonDcaAtrState).

Example script:

examples/run_backtest_example.py:

Generates synthetic candles.

Instantiates DragonDcaAtrStrategy(dragon_config_example).

Runs BacktestEngine.run(...) and prints high-level results.

9.2 What currently works

The package imports correctly when PYTHONPATH=src is set.

python -m examples.run_backtest_example runs end-to-end in a Codespace:

Candles are generated.

BacktestEngine iterates over them.

BrokerSim processes orders.

Strategy hooks are called.

The current strategy implementation is intentionally minimal (no full Haas logic yet). It may:

Open simple initial longs.

Optionally add basic DCA rungs depending on the latest implementation.

It does not yet implement full ATR TP / trailing exit behaviour.

9.3 Next implementation tasks for the Dragon strategy

Indicator & signal layer

Implement MA/BB/MACD/RSI calculations over historical candles.

Mirror the Haas intervals (MA, BB, MACD, RSI have separate *_interval values).

Implement the "indicator consensus" logic as used in the Haas script.

Wire this into _handle_flat_state(...) so entries respect both trend and signal consensus.

DCA ladder (full port)

Port the Haas DCA ladder logic into methods like:

_update_initial_entry(...)

_process_initial_entry_fill(...)

_manage_dca(...)

Respect config flags:

use_avg_entry_for_dca_base

lock_atr_on_entry_for_dca (cached ATR-based deviations vs live deviations).

Ensure behaviour matches the original Haas script for a given config and data.

Take-profit and stop-loss logic

Implement a full update_tp(...) equivalent:

Support both fixed % TP and ATR-on-entry distance.

Implement tpInitDist and dcaRecalcTP semantics (re-anchoring TP after DCA fills).

Implement stop-loss and trailing exit logic similar to updateTrailingExit(...) in the Haas script.

Ensure TP/SL orders integrate with the dynamic limit behaviour (activation bands) where appropriate.

Dynamic limit orders (engine + broker)

Extend BrokerSim (and later live adapters) to support a dynamic limit order wrapper:

Store the logical target price and activation band.

Only submit/keep the order active when within the band.

Cancel/park the order when outside the band.

Expose this via a clean interface so strategies can request:

"place dynamic entry limit" (using DynamicActivationConfig.entry_pct).

"place dynamic DCA limit" (using DynamicActivationConfig.dca_pct).

"place dynamic TP limit" (using DynamicActivationConfig.tp_pct).

Metrics & reporting

Implement calculation of standard metrics on BacktestResult:

Net profit, max drawdown.

Sharpe, Sortino, Profit Factor.

Win %, Common Sense Ratio, CPC, etc.

Store these metrics in a way that can be saved to backtest_runs (SQL or file-based).

Streamlit or web UI prototype

Build a minimal dashboard that can:

Run a single backtest with a chosen DragonAlgoConfig.

Plot equity curve + drawdown.

Show a trades table.

Show key metrics.

Later, expand into a backtest run browser with sorting/filtering.

9.4 Design notes for the next AI

Keep the separation of concerns in mind:

Strategy logic should not depend on Streamlit or SQL directly.

BrokerSim and real exchange adapters should share a common interface.

Data loading (CSV/SQL/API) should live in a dedicated data module.

When expanding the strategy, try to:

Port Haas functions one at a time.

Keep names as close as possible to the original (e.g. updateTP, manageDCA) for easier cross-referencing.

Add docstrings referencing the original Haas behaviour.

10. Change Log

2025-11-26: Initial document created with vision, scope, research summary, open questions, and draft amendments.

2025-11-29: Added data layer & persistence outline, dynamic limit semantics, and implementation handover notes for the next AI/maintainer.

