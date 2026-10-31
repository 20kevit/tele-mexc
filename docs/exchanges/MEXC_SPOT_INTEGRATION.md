# MEXC Spot Adapter — Integration Contract

How the adapter (`tg_signal_ingest/mexc/`) binds to the trading core
(`tg_signal_ingest/trading/`) without touching signal ingestion, parsing, or
bot workflows. **Execution stays disabled by default**; this document is the
contract a future operator fulfills when credentials and policy confirmation
exist.

## 1. Interface the core depends on

The core depends on **two narrow interfaces**, never on `MexcSpotClient`
directly:

```python
# Execution (typing-any today; this is the effective protocol):
class ExchangeGateway(Protocol):            # implemented by MexcSpotClient
    def place_order(*, symbol: str, side: str, order_type: str,
                    quantity: Decimal | None, quote_order_qty: Decimal | None,
                    price: Decimal | None, client_order_id: str | None) -> dict[str, Any]: ...
    def query_order(*, symbol: str, order_id: str | None,
                    orig_client_order_id: str | None) -> dict[str, Any]: ...
    def cancel_order(*, symbol: str, order_id: str | None,
                     orig_client_order_id: str | None) -> dict[str, Any]: ...
    def account() -> dict[str, Any]: ...
    def my_trades(*, symbol: str, order_id: str | None, ...) -> list[dict[str, Any]]: ...

# Symbol metadata (already duck-typed by TradingEngine._symbol_meta):
class SymbolRegistry:                       # ttl cache over exchangeInfo
    def get(symbol: str) -> SymbolMeta: ...
```

Construction is centralized in `trading/runtime.py`:

- `sim` (default): no client is constructed at all — `build_trading_engine`
  returns an engine with `client=None`; processing never touches networking.
- `testonly`: client built; submissions go to the official
  `POST /api/v3/order/test` (validation only).
- `live`: client built AND the acknowledgement token must match; otherwise
  `_execute_live` raises `AccidentalOrderRiskError` before any call.

## 2. From a reviewed signal to an exchange order intent

1. A human-reviewed signal row exists in SQLite (`record_signal`), parsed from
   the message into (`symbol`, `action`, quantities) — the adapter receives
   **only these normalized fields**, never Telegram text.
2. `TradingEngine.process_item` computes risk (`RiskManager.evaluate`),
   sizing, and symbol-rule quantization (`SymbolMeta.quantize_*` +
   `validate_order`).
3. An approved decision becomes a durable `intent` row **before** any network
   call: `intent_id`, `client_order_id` = SHA-256 over
   `(channel_id, message_id, action, symbol)` — the idempotency handle that
   also reconciles ambiguous submissions.
4. Order payload construction (`_order_payload`):
   LIMIT → `quantity` + `price`; MARKET BUY → `quoteOrderQty`; MARKET SELL →
   `quantity`. Nothing else is ever sent.

## 3. Symbol mapping & trading-rule validation

- Signals must present MEXC-style symbols (`BTCUSDT`); the registry normalizes
  case, upper-caches `exchangeInfo` per symbol (TTL 300 s default), and fails
  closed when the symbol is unknown, paused, not SPOT-allowed, lacks published
  minimums, or does not support the requested order type/side.
- Price guards: engine-side deviation check vs a fresh ticker price; optional
  `SymbolMeta.validate_price_guards()` enforces the documented
  `PERCENT_PRICE_BY_SIDE` filter when the exchange publishes the multipliers
  and a current reference price exists.

## 4. Risk checks performed before submission

Execution is automatic (no per-signal approval). Before submission the engine
validates:

1. signal still active and field-identical to the intent,
2. message is above the persisted activation boundary (backfill/replay is rejected),
3. emergency stop inactive,
4. direction is Spot BUY; SHORT/SELL is never executed,
5. symbol metadata is available from dynamic MEXC Spot `exchangeInfo`, including
   status, SPOT permission, precision and min-notional,
6. the owner-approved fixed `order_quote_amount` is configured and positive,
7. balance preflight against a fresh `/account` snapshot (age-limited, fee
   buffer, outstanding local open-intent usage); pre-existing external orders
   are never adopted, cancelled, or counted as local orders,
8. the final state change is stored before the HTTP call so a crash can never
   leave an untracked submission.

There is no daily/open-order cap, signal-age limit, price-deviation rejection,
mandatory stop-loss check. No operator-maintained symbol allowlist is used; MEXC market metadata and exchange filters still validate each symbol.

## 5. Order-lifecycle state reconciliation

Local states: `planned → submitting → submitted → (reconciling) →
open/filled/canceled_local/rejected/failed/unknown_requires_reconciliation`
(`TERMINAL_INTENT_STATES` in `trading/store.py`).

- Exchange statuses map through `_map_exchange_state`
  (`NEW/PARTIALLY_FILLED/FILLED/CANCELED/PARTIALLY_CANCELED`).
- Every reconciliation writes a durable `reconciliations` row (outcome
  `consistent | ambiguous | not_found | resolved | drifted`).
- `-2011` never demotes a non-terminal intent to a terminal failure.
- Terminal-but-unfilled-synced orders keep the `fill_sync_required` flag and
  fill history is re-read from `/api/v3/myTrades`.
- `reconcile_pending(limit)` handles the backlog; CLI entry
  `tg-ingest trading reconcile` is the operator hook.

## 6. Duplicate prevention

| Duplicate source | Prevention |
| --- | --- |
| Re-ingested/edited same message | Intent uniqueness on the message-identity key (`intent_by_idempotency`) |
| Duplicate/replayed deliveries | Intent state must be `planned`; atomic submission claim is single-transition |
| Resubmission after timeout | Forbidden: ambiguous outcomes never re-submit, they reconcile |
| Parallel processes | SQLite transitions + unique exchange-order upsert keys |

Exchange-side limitation: MEXC documents **no** server-side idempotency
window for `newClientOrderId`; deduplication is local + reconciliation-based,
and this is a documented limitation, not a guaranteed property.

## 7. Failure reporting without losing a signal

Every outcome of an order attempt is a state transition with a note and an
audit event: rejected intents keep the `reject_reason`; ambiguous keep
`unknown_requires_reconciliation` + reconciliation rows; never does a signal
row lose its intent linkage. Operators see failures through
`trading status / signals / intents / reconcile` and the audit table — no
silent drops are possible because intents are write-ahead.

## 8. Sync vs worker responsibilities

- **Synchronous** (short): intent creation and submission claim
  (single REST call), cancel, single reconcile.
- **Worker/polling** (`serve` loop): reconciliation of
  `unknown/open/reconciling` intents every `recon_interval_s` (≥ 5 s,
  config-enforced), and reference-price fetches.
- All REST I/O is dispatched through `asyncio.to_thread` (client methods are
  sync and deliberately so: transport is one injectable call).

### Rate-limit and polling contract

Official Spot v3 limits are IP `300 weight/10 s`, UID `500 weight/10 s`, and a
shared order/cancel UID bucket of `12 requests/s`. The client charges the
documented per-endpoint weights rather than one unit per request:

| Endpoint | Weight |
| --- | --- |
| `GET /api/v3/ticker/price` | `10` |
| `GET /api/v3/exchangeInfo` | `25` |
| `GET /api/v3/account` | `10` |
| `GET /api/v3/myTrades` | `10` |
| `GET /api/v3/openOrders` | `3` |
| `GET /api/v3/order` | `2` |
| `GET /api/v3/depth` | `3` |
| `GET /api/v3/ping`, `/time` | `1` |
| `POST /api/v3/order/test` | `1` |
| `POST`/`DELETE /api/v3/order` | shared order bucket |

Unknown endpoints use weight `25`. Local caps are `60` IP weight/10 s and
`100` UID weight/10 s. `serve`, the `trading` CLI, and the CLI write path use
file-backed shared ledgers under `data/mexc-rate-limit-*.json`.

The monitor uses one all-symbol `GET /api/v3/ticker/price` request per polling
interval (weight `10`), not one request per symbol. At the default 2 s interval
that is `50 weight/10 s`; more symbols do not multiply requests. A `429`/`418`
response blocks the affected feed and honors `Retry-After`; polling never places
or retries an order.

Residual gap: live writes are executed by the external MEXC CLI. The application
acquires the shared order bucket before invoking the CLI, but requests made
inside the CLI process cannot be counted by the application limiter.

## 9. What must be configured before integration can be enabled

1. `MEXC_API_KEY` / `MEXC_API_SECRET` in the private `.env` (never committed).
2. `validate_credentials()` pass: key VALID, not expired, permissions exactly
   READ/READ/`SPOT_DEAL_WRITE`.
3. Pair is API-tradable (dynamic `exchangeInfo` + no suspension announcement).
4. Owner-approved fixed `order_quote_amount` (no software default) and enough
   spendable quote balance for that exact order.
5. Manifest *policy* confirmation: automated-trading acceptance and region —
   cannot be verified by code (`MEXC_SPOT_RISKS.md` §8 checklist).
6. Only then `trading.mode = "live"` plus the acknowledgement token.

Health checks at every stage (ping/time/account/keyInfo) are read-only and
place no orders.
