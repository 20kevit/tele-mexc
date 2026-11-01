> **Historical/archive status:** This document preserves dated evidence,
> findings, or a design proposal. It is not a description of current behavior
> or an operational instruction. Current documentation starts at
> `docs/documentation-index.md`.

# Final Report — Wire Two Native OCO Exit Orders into the Live Trading Workflow

Repository: local project checkout (branch `main`)
Date: Mon Oct 26 2026
Status: historical implementation snapshot; current test counts are reported
in `docs/development.md` and the release report.

---

## Summary

Implemented and deployed native two-OCO exit orders into the live trading workflow. For each eligible LONG/BUY entry confirmed as filled, the bot now places two MEXC native OCO SELL orders (OCO 1 at TP1, OCO 2 at TP2, each with a stop-limit leg at the configured stop-loss price) immediately, rather than waiting for price ticks to place separate LIMIT sells. The service has been restarted and is running the updated code.

## Files Changed

| File | Change |
|------|--------|
| `tg_signal_ingest/trading/trade_manager.py` | Core OCO workflow: `_place_oco_exits()`, `_submit_oco_order()`, `_oco_orders()`, `_ocos_active()`, `_ocos_terminal()`, `_target_qty()`, `_list_trade_orders()`. Modified `_settle_entry_state()` to place OCO exits on confirmed entry fill. Gated `_check_tp_sl()`, `_check_tp2_or_sl()`, `_check_sl_and_tp2_remainder()` to skip when OCOs active. Added OCO state handling (`oco_submitting`/`oco_active`/`oco_partial`) in `_evaluate_trade_tick()`, `_reconcile_trade()`, `_advance_trade_after_reconcile()`. |
| `tg_signal_ingest/mexc/cli.py` | Added `MexcCliAdapter.place_oco()` + `_normalize_oco()` — native `spot.trade.place-oco` call. |
| `tg_signal_ingest/mexc/hybrid.py` | Added `HybridMexcClient.place_oco()` delegation to the CLI adapter. |
| `tg_signal_ingest/trading/engine.py` | `submit_oco_sell()` already present from prior work — verified and retained. |
| `tg_signal_ingest/db.py` | No schema migration needed: `trades.trade_state` and `trade_orders.kind` are free-text columns that already accept the OCO values. |
| `tests/test_trading_tp_sl.py` | Rewritten for OCO flow: OCO placement at entry fill, OCO fill advancement, both-filled closes trade, stop-loss via OCO legs (no separate market sell), no duplicate orders, restart recovery, other-symbol isolation. |
| `tests/test_trading_lifecycle_recovery.py` | Rewritten for OCO flow: partial quantities, both-filled close, stop-loss no market sell, no duplicates on repeat ticks, restart recovery, partial-fill recovery, uncertain submissions, partial survives restart, other-symbol isolation, client-order-ID limits. |
| `tests/_trading_fakes.py` | Added `FakeEngine.submit_oco_sell()` mock. |

## Implemented OCO Workflow

**Entry → OCO placement:**
1. Entry LIMIT BUY is submitted via the engine gate (budget + balance preflight).
2. When entry fills are confirmed (`acquired_base_qty > 0`), `_settle_entry_state()` transitions the trade to `position_open` and calls `_place_oco_exits()`.
3. `_place_oco_exits()` calculates quantities from the actual acquired fill:
   - OCO 1: `tp1_allocation_pct` (default 50%) of acquired qty at TP1 price, stop-limit at SL price.
   - OCO 2: remaining qty at TP2 price, stop-limit at SL price.
4. Both OCO orders are persisted as `trade_orders` (kind=`tp1`/`tp2`, order_type=`OCO_STOP_LIMIT_ORDER`) and submitted via `engine.submit_oco_sell()` → `cli.place_oco()`.
5. Protection is **not** marked active until both OCOs have exchange identifiers:
   - Both confirmed → state `oco_active`
   - One confirmed → state `oco_partial` (notify operator, deterministic recovery on next sweep)
   - Neither confirmed → state `oco_submitting` (reconciliation retries)

**OCO semantics (per CLI schema):**
- Each OCO is one exchange order with two linked legs: limit leg (TP price) + stop-limit leg (triggerPrice = triggerLimitPrice = SL price).
- When one leg activates, the exchange cancels the other leg within the same OCO.
- OCO 1 and OCO 2 are independent; one does not cancel the other.

**Reconciliation & restart recovery:**
- `_reconcile_trade_orders()` queries each OCO order by `clientOrderId`/`exchangeOrderId` and updates `sold_qty`.
- `_advance_trade_after_reconcile()` closes the trade when both OCOs are terminal/filled.
- `_reconcile_trade()` handles `oco_submitting`/`oco_active`/`oco_partial` states on every sweep and tick.
- On restart, `recover_on_startup()` → `_reconcile_trade()` rehydrates OCO state from durable rows.
- Uncertain submissions become `unknown_requires_reconciliation` and are **never** blindly resubmitted.
- Idempotency: stable `clientOrderId`s; `_place_oco_exits()` returns early if OCO orders already have exchange identifiers.

**Safety:**
- No exits for unfilled entries (only called when `acquired > 0`).
- Quantities quantized down to exchange lot step; never exceed acquired position.
- No separate market stop-loss sell when OCOs are active (OCO stop-limit legs handle the stop).
- Pre-existing exchange positions are **not** adopted, migrated, or re-interpreted.
- The aggregate budget is a configurable reference value, not a hard cap; only the owner-configured per-entry amount remains.
- SHORT signals and Telegram-originated sell instructions continue to be rejected.

## Test Results

| Check | Result |
|-------|--------|
| Full pytest suite | **744 passed** (44.7s) |
| `ruff check .` | All checks passed |
| `mypy tg_signal_ingest` | No issues (58 source files) |

All tests use mocked fakes — no real orders placed.

## CLI OCO Verification

Verified against the installed `@mexc-ai/cli` v0.0.22 direct binary:

- `mexc schema spot.trade.place-oco` — operation `spot.trade.place-oco`, path `/api/cli/spot/v4/order/place/oco`, `write: true`, `signed: true`.
- Body fields: `currency`, `market`, `quantity` (req), `tradeType` (req, enum `BUY`/`SELL`), `orderType` (req, enum `OCO_STOP_LIMIT_ORDER`), `orderSource` (req, enum `CLI`), `price` (req, limit-leg/TP price), `triggerType` (req, enum `GE`/`LE`, `LE` for SELL), `triggerPrice` (req, stop-limit activation), `triggerLimitPrice` (req, stop-limit placement), `clientOrderId` (opt, dedupe key).
- The adapter maps: `side=SELL`, `orderType=OCO_STOP_LIMIT_ORDER`, `price=TP`, `triggerType=LE`, `triggerPrice=SL`, `triggerLimitPrice=SL`.

## Deployment

**Service restarted via systemd:**
- `systemctl restart tg-ingest` — the old worker was stopped and a new worker started.
- Single worker only — `serve.lock` held; no competing workers.
- `tg-ingest.service`: active, enabled.
- Telegram session verified; direct TCP/TLS; configured channel resolved.
- Activation boundary recorded (messages at/below it are never traded).
- Recovery sweep complete; market monitor started; queue processing normally.
- **No application errors** in logs since restart.
- No real orders placed, canceled, or modified during deployment.
- Pre-existing exchange positions untouched.

## Remaining Limitations / Owner Actions

1. **OCO wiring is active for future entries only, and has not been exercised by a live fill.** Positions that existed before this change are not adopted by the new lifecycle and have no OCO protection from it. The owner should review them separately. No action was taken on them per the task constraint.

2. No database migration was required for the OCO states; the existing free-text state columns accept them.

3. The aggregate budget remains a configurable reference value in `config.toml`; it is no longer a hard cap. The owner chooses the per-entry amount.

4. **Stop-limit execution risk:** The OCO stop-limit leg may not fill exactly at the stop price in fast markets. Residual unsold quantities are recorded as `unsold_residual_qty`/`unsold_residual_reason` rather than silently dropped.

## Evidence Classification

- **Implemented:** Two-OCO exit wiring in TradeManager, CLI/engine OCO adapter methods, tests. No schema migration was needed: `trades.trade_state` and `trade_orders.kind` already accept the OCO values.
- **Tested with mocks:** 744 tests pass (ruff + mypy clean).
- **Verified against actual CLI:** `spot.trade.place-oco` schema confirmed against installed `@mexc-ai/cli` v0.0.22 (no live order placed).
- **Running:** `tg-ingest.service` active; Telegram listener and market monitor started.
- **Actually traded:** No order was placed during this task. Existing exchange positions were left untouched.
