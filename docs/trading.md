# Trading Pipeline

MEXC Spot order execution, from stored Telegram text to durable order state.
Architecture context: `docs/architecture.md`; schema: `docs/data-model.md`.

## Audit note (2026-10-29)

See [independent audit](INDEPENDENT_AUDIT_2026-10-29.md) for evidence and limits.
OCO rows in `submitting`, `cancel_requested`, or
`unknown_requires_reconciliation` are not confirmed protection, even when an
older exchange order ID is present. They remain reconciliation-only for
placement purposes; this classification never authorizes resubmission. An
otherwise healthy sibling means partial, not full, protection.

## Non-negotiable behavior

- Spot **BUY only** for signals. SHORT/SELL is parsed for reporting and then
  rejected. Sell orders exist only for TP1/TP2/stop-loss exits.
- No futures, leverage, margin, withdrawals, or unofficial endpoints.
- One fixed owner-configured quote amount per BUY. Signal deposit/balance/
  leverage text is never used for sizing.
- One intent per Telegram message; conflicting edits cannot create a second.
- No blind retry after an uncertain submission.
- Historical, backfilled, and at/below-boundary messages never trade.

## Modes

| Mode | Behavior | Requirements |
| --- | --- | --- |
| `sim` (default) | Parse, risk-check, plan, then immediately mark the intent filled locally with `sim-...` | No MEXC credentials or network |
| `testonly` | Call official `POST /api/v3/order/test`; never reaches the matching engine | Credentials + positive `order_quote_amount` |
| `live` | Submit a real Spot BUY; reconcile and record fills | Credentials + positive amount + exact live token |

`live_enabled_token` must be exactly
`I_UNDERSTAND_REAL_MONEY_AND_RISKS`; anything else prevents startup in live mode.

## Execution backends (REST reads + CLI writes)

Two transports are used deliberately, not as competing integrations:

| Concern | Backend | Reason |
| --- | --- | --- |
| Exchange metadata, balances, order/fill history | REST `MexcSpotClient` | Verified Spot v3 endpoints; already the accounting source of truth |
| Live BUY/SELL placement, cancel, single-order query | `@mexc-ai/cli` adapter | Only supported way to attach TP/SL to a new order |
| `testonly` validation | REST `POST /api/v3/order/test` | Official validation endpoint; no matching-engine order |

The CLI adapter (`mexc/cli.py`) is argv-only, bounded by a timeout, and never
starts a shell. It uses the CLI's own encrypted profile
(`~/.mexc/profiles`) and never receives `MEXC_API_KEY`/`MEXC_API_SECRET`.
Reads remain on REST so there is one market-data/account implementation.

`[mexc_cli]` in `config/config.toml`:

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | live mode refuses `false`; a missing binary fails closed |
| `binary` | auto | explicit path; else `MEXC_CLI_BINARY`, the known per-platform path, then `PATH` |
| `profile` | active profile | optional profile name passed as `--profile` |
| `timeout_s` | `20.0` | subprocess timeout in `(0, 120]` |
| `min_version` | `0.0.22` | minimum accepted CLI version |
| `confirm_noninteractive_writes` | auto | live mode arms `--yes` when absent; explicit `false` keeps live writes fail-closed; non-live modes default `false` |

Writes always pass `--no-retry`; the adapter never retries a placement. The
`clientOrderId` is reused verbatim. A CLI `duplicate` response means the earlier
attempt landed, so it becomes `unknown_requires_reconciliation` and is
reconciled, never resubmitted. A CLI write without explicit confirmation is
rejected locally and no request reaches the exchange.

Live mode is already an explicit owner authorization: the service cannot answer
the CLI's terminal prompt, so when `confirm_noninteractive_writes` is absent the
loader arms `--yes` for live writes. Setting it to `false` is honored as a
fail-closed override, but then every live write is rejected locally. Set it to
`false` only when intentionally disabling live writes; otherwise leave it unset
or set `true`.

### CLI credential profile (owner-only)

The CLI accepts the standard MEXC website-issued **Open API Access Key +
Secret Key** — the same key pair the REST client uses. “Open API” is the key
type; it is not a second kind of credential. The CLI stores that pair in its own
encrypted profile. This application never reads, copies, prints or replaces it.

In the owner's terminal (interactive prompt; do not put secrets in shell
history or process arguments):

```bash
mexc profile set <name>     # prompts for Access Key + Secret Key
mexc profile use <name>
mexc profile list           # no credentials needed
```

Grant Spot read + Spot trade only (no withdrawal). Preflight fails closed when
no profile exists, the configured profile is missing, no profile is active, or
the CLI profile reads a different Spot account than the REST key. The CLI's
write permission cannot be proven without placing an order; it is enforced by
MEXC at submission time and by the profile's key permissions.

`700001 API-key format invalid` is an exchange rejection of the stored Access
Key, not a CLI or server failure: the key was copied incompletely, is not the
active Open API key for that account, or has been deleted. Re-enter it with
`mexc profile set <name>` in the owner's terminal. `700007`/`401` are separate
permission failures and require the key's Spot permissions to be corrected.

## Configuration (`[trading]` in `config/config.toml`)

Environment overrides: `TRADING_MODE`, `TRADING_LIVE_TOKEN`,
`TRADING_ORDER_QUOTE_AMOUNT`, `TRADING_AGGREGATE_BUDGET`,
`MEXC_CLI_BINARY`, `MEXC_CLI_PROFILE`.

| Key | Default | Validation / meaning |
| --- | --- | --- |
| `mode` | `sim` | `sim`, `testonly`, or `live` |
| `live_enabled_token` | unset | exact acknowledgement token for live |
| `order_quote_amount` | `3.0` | positive finite per-order quote; 3 USDT default; precedence is env > owner Telegram menu (DB) > `[trading]` > default |
| `aggregate_budget_quote` | `50.0` | cumulative spend reference for reporting; must be >0; never caps per-order size and is not enforced as a ceiling |
| `order_type_policy` | `limit` | `limit` or `market`; explicit signal semantics still apply |
| `remaining_balance_reserve_pct` | `0.0` | `0 ≤ value < 100` |
| `balance_fee_buffer_pct` | `0.2` | `0 ≤ value ≤ 5` |
| `balance_max_age_s` | `5.0` | `(0, 300]`; stale account snapshots fail closed |
| `recon_interval_s` | `30.0` | live reconciliation period, minimum 5 s |
| `emergency_stop` | `false` | blocks new intents and pre-submit revalidation |
| `entry_trigger_offset_ticks` | unset | unset = direct entry; otherwise integer ≥1 enables conditional entry |
| `entry_trigger_stale_threshold_s` | `30.0` | positive; older price ticks are ignored |
| `tp1_allocation_pct_of_acquired` | `50.0` | 0–100 of actual acquired quantity |
| `tp2_allocation_pct_of_remaining` | `100.0` | 0–100 of quantity left after TP1 |
| `stop_loss_policy` | `market_sell` | only accepted value |
| `stop_loss_max_retries` | `3` | parsed, currently unused |
| `stop_loss_retry_backoff_base_s` | `1.0` | positive; parsed, currently unused |
| `stop_loss_trigger_offset_ticks` | `5` | stop-loss activation trigger = exit price + this many symbol ticks (0 disables); exit price itself unchanged |
| `monitor_poll_interval_s` | `2.0` | positive |
| `monitor_max_retries` | `5` | integer ≥1 |
| `monitor_backoff_base_s` | `1.0` | positive |

There is no daily-order cap, open-order cap, signal-age limit, price-deviation
threshold, mandatory stop-loss rule, or manual symbol allowlist. The
`trading_daily_spend` table is maintained but no cap reads it.

## Parsing and executable signals

`trading/parser.py` is deterministic and independent of the offline analyzer.

| Kind | Meaning |
| --- | --- |
| `alert` | New actionable BUY/SELL signal |
| `update` | Edited/updated actionable signal |
| `cancel` | Cancel instruction; may cancel prior matching intents |
| `result` | TP hit/entry filled/position closed follow-up; never executable |
| `info` | Ordinary channel text |
| `ambiguous` | Direction detected but required fields/format are not safe |

- LONG is treated as BUY; SHORT/SELL is retained but rejected by risk.
- Futures/perpetual/margin/leverage wording makes the signal non-Spot and
  ambiguous.
- Missing/unsafe entry, symbol, or required fields fail closed.
- Explicit market signals are blocked when `order_type_policy = "limit"`.
- No OCR: media without usable caption text is not actionable.

## Risk and sizing

`trading/risk.py` rejects:

- emergency stop;
- non-`alert`/`update` kinds;
- missing/ambiguous direction;
- any action other than BUY;
- missing symbol, non-positive entry/stop, inverted entry range;
- missing fixed quote amount;
- unsupported symbol/order type, precision, min/max notional.

Order type: an explicit signal type wins unless policy blocks it; otherwise an
entry price implies LIMIT, and no entry price uses MARKET only when policy is
`market`.

Quantity is `order_quote_amount / price`, then quantized **down** to the symbol
lot step and validated against `exchangeInfo` minimum/maximum and price guards.
The engine re-validates immediately before submission.

## Replay boundary and idempotency

The activation boundary is `app_state['trading.activation_boundary_message_id']`.
At each `serve` start, **before live handlers are registered**, the engine
snapshots the selected channel's newest stored message id and persists
`max(existing boundary, snapshot)`. The boundary is forward-only.

In `testonly`/`live`:

- a message at or below the boundary is rejected as historical;
- `source='backfill'` and `source='recovery'` rows are non-tradeable regardless
  of their message id;
- a live row delivered after the snapshot, including while the recovery sweep
  is running, remains above the boundary and can trade.

The persisted boundary can therefore be lower than the newest recovered/backfill
row. That is intentional: source is the primary guard for downtime traffic, and
the boundary protects against identity/replay confusion.

Idempotency:

```
sha256(f"{channel_id}:{message_id}:{action}:{symbol}")
client_order_id = "tsi" + key[:28]
intent_id       = "intent-" + client_order_id
```

If a message already has an intent with a different action/symbol, the edit is
superseded and audited; no second order is placed. A same-version duplicate
delivery does not supersede the signal and cannot resubmit a terminal intent.
A newer edit may supersede the old signal but still cannot create a second
intent for the same message.

Terminal `rejected`/`failed` intent transitions update an active
(`parsed`/`validated`/`planned`) signal to `rejected` in the same transaction.
They never overwrite `superseded`, `canceled`, or `deleted` signals.

## MEXC rate limits and market polling

Official Spot v3 limits (verified 2026-10-27):

| Bucket | Official limit |
| --- | --- |
| IP weight | `300` per `10 s` |
| UID weight | `500` per `10 s` |
| Order + cancel | `12` requests per second, shared UID bucket |

Endpoint weights used by this service:

| Endpoint | Weight |
| --- | --- |
| `GET /api/v3/ticker/price` | `10` (IP) |
| `GET /api/v3/exchangeInfo` | `25` (IP) |
| `GET /api/v3/account` | `10` (IP) |
| `GET /api/v3/myTrades` | `10` (IP) |
| `GET /api/v3/openOrders` | `3` (IP) |
| `GET /api/v3/order` | `2` (IP) |
| `GET /api/v3/depth` | `3` (IP) |
| `GET /api/v3/ping`, `/time` | `1` (IP) |
| `POST /api/v3/order/test` | `1` (IP) |
| `POST`/`DELETE /api/v3/order` | shared order bucket |

Unknown endpoints are charged a conservative default weight of `25`.

Local protection:

- Configured caps are `max_ip_weight_per_10s=60` and
  `max_uid_weight_per_10s=100`; these are deliberately far below the official
  limits, not additional trading limits.
- `serve`, the `trading` CLI, and other local processes share file-backed
  sliding-window ledgers: `data/mexc-rate-limit-ip.json`,
  `data/mexc-rate-limit-uid.json`, and `data/mexc-rate-limit-order.json`.
  Do not delete these files while a process is running.
- Signed requests are timestamped only after any `Retry-After` backoff and the
  shared rate gate have completed. The gate can legitimately wait a full
  `10 s` window; stamping before that wait would exceed `recvWindow` and produce
  `700003` even with a correctly synchronized host clock.
- Live CLI writes acquire the shared order bucket before invoking the MEXC CLI.
  The external CLI's internal requests cannot be observed by this limiter; that
  residual gap is documented in `docs/exchanges/MEXC_SPOT_INTEGRATION.md`.

Market polling:

- `GET /api/v3/ticker/price` without a symbol returns all tickers for one
  weight-`10` request. The monitor uses that batch path, so the number of open
  symbols does not multiply requests.
- At `monitor_poll_interval_s=2.0`, one batch every 2 s is `0.5 req/s`, or
  `50 weight/10 s` — below the configured `60` cap. Larger symbol sets add no
  requests. The shared gate throttles intervals that would exceed the cap.
- HTTP `429`/`418` responses stop the affected feed and honor `Retry-After`
  with bounded jitter. The polling feed does not retry orders; it only reads
  prices. Order submission remains governed by the no-retry
  `unknown_requires_reconciliation` path.

## Budget ledger

For `testonly`/`live`, intent creation and budget reservation are one atomic
transaction. The reserved amount is:

```
quote_amount * (1 + balance_fee_buffer_pct / 100)
```

`trading_budget_ledger` states:

| State | Effect |
| --- | --- |
| `reserved` | Counts against the cap |
| `uncertain` | Frozen; still counts; never auto-released |
| `settled` | Reservation removed; actual spend and fees retained |
| `released` | No charge; used for rejected/failed/never-sent intents |

- Filled: settle actual `cumulativeQuoteQty` (or fill totals), plus real fees;
  when fees are not yet visible, the configured buffer is retained.
- Canceled: retain actual partial spend and fees.
- Rejected/failed/canceled-locally: release.
- The summary is `limit - active_reserved - settled_spend`; restart never
  resets the ledger.
- `testonly` creates and then releases a reservation around the test order, so
  it does not consume the live budget.

## Execution

| Mode | Flow |
| --- | --- |
| `sim` | `planned → submitting → filled`, exchange id `sim-<client_order_id>`, no network |
| `testonly` | `planned → submitting → submitted → canceled_local`, exchange status `TEST_ONLY_ACCEPTED` |
| `live` | Budget ensure → fresh balance preflight → `submitting` → exchange → `submitted`/uncertain/rejected → reconciliation |

Live balance preflight:

- Fetch the account snapshot on every submission.
- Reject if older than `balance_max_age_s`, if `canTrade` is false, or if the
  asset is missing.
- Required = order notional × fee factor (BUY) or base quantity × fee factor
  (SELL).
- Subtract outstanding local open intents for the same symbol; also keep
  `remaining_balance_reserve_pct` free.
- A timestamp rejection (`700003`/`10073`) on the read-only account query is
  retried once with a fresh signature. A second failure rejects before any
  order call; other balance errors are never retried.
- Any failure rejects before an exchange call.

Intent states (DB check constraint): `planned`, `submitting`, `submitted`,
`reconciling`, `open`, `filled`, `canceled`, `failed`, `rejected`,
`unknown_requires_reconciliation`, `canceled_local`.

### Entry fills and native OCO exits

The current live backend (`HybridMexcClient`) reports
`supports_attached_protection = False`: entries are not sent with attached
take-profit/stop-loss triggers. Instead, once the entry intent is terminal and
`fill_sync_required` is clear, `TradeManager._place_oco_exits()` places two
native MEXC OCO SELL orders against the **actual acquired base quantity**:

- OCO 1: `tp1_allocation_pct` (default 50%) at TP1, with a stop-limit leg whose
  *execution price* is the configured stop-loss and whose *activation trigger*
  is `stop_loss + (stop_loss_trigger_offset_ticks × symbol_tick_size)` (default
  five ticks above the exit price), so the stop can activate earlier as the
  market falls.
- OCO 2: the remaining allocation at TP2, with the same split between the
  stop-limit execution price (configured stop-loss, unchanged) and the
  activation trigger (five ticks above it).
- Each OCO is one exchange order with two linked legs; when one leg activates,
  the exchange cancels the other leg inside that OCO. The two OCO orders do not
  cancel each other.
- The activation trigger and the stop-limit execution price are distinct
  exchange fields (`triggerPrice` / `triggerLimitPrice` on `spot.trade.place-oco`).
  The configured stop-loss exit price itself is never changed by the offset.

Protection is reported as active only after every required leg has an exchange
identifier. The states are:

| Trade state | Meaning |
| --- | --- |
| `oco_submitting` | at least one leg's outcome is uncertain; reconcile, never resubmit |
| `oco_partial` | one leg confirmed, the other uncertain or failed; position is only partly protected |
| `oco_active` | all required OCO legs confirmed by the exchange |
| `needs_reconciliation` | no leg could be confirmed, or the exit ladder/allocation is invalid |

While either OCO leg is live or uncertain, local TP1/TP2/SL ticks are
suppressed so the same position cannot be sold twice. A partially confirmed
position is reported honestly and its missing leg is recovered on a later
sweep; a confirmed leg is never resubmitted. An uncertain submission is
queried by `clientOrderId`/exchange id and is never replaced with a new order
until its state is resolved.

The older `order_protection` attached-order path remains in the code for
backends that advertise support, but the current hybrid backend does not use
it. Its verification table (`unverified` → `active` only after an exchange echo)
applies only to that path.

## Reconciliation

`reconcile_pending()` selects live intents in non-terminal/open states or with
`fill_sync_required = 1`.

| Exchange result | Local effect |
| --- | --- |
| `NEW` / `PARTIALLY_FILLED` | `open` |
| `FILLED` | `filled`; terminal fill sync requested |
| `CANCELED` / `PARTIALLY_CANCELED` | `canceled`; partial spend retained |
| Unknown status | State unchanged, outcome `ambiguous` |
| Network/5xx/malformed query | Outcome `ambiguous`; state unchanged |
| `-2011` not found | Non-terminal intent becomes/stays `unknown_requires_reconciliation`; never terminalized, never retried |

Fills are read from `myTrades` and inserted idempotently by
`(exchange_order_id, exchange_trade_id)`. If persisted quantity is below the
exchange's executed quantity, `fill_sync_required` stays set and later sweeps
retry. A terminal order is never un-filled by a contradictory exchange report;
the discrepancy is recorded.

`trading reconcile` and the `live` background worker use the same path.
Cancellation from a Telegram cancel/delete also reconciles afterward rather than
assuming success.

## Trade lifecycle (`TradeManager`)

Only `live` intents get a `trades` row. Direct entry executes immediately;
conditional entry defers the entry intent until a price trigger.

- **Entry:** limit BUY at the signal entry price; never converts to market.
- **Conditional entry:** if market < entry and offset ≥1, wait for
  `entry_price - offset_ticks × tick_size`, then submit the limit BUY through the
  engine's budget/balance gate. Missing/stale price fails closed.
- **TP1:** first target; limit SELL of
  `tp1_allocation_pct_of_acquired` of actual filled quantity.
- **TP2:** second target; limit SELL of `tp2_allocation_pct_of_remaining` of what
  remains after TP1.
- **Stop-loss:** parsed `sl_price`; one per trade. SL wins when SL and TP are
  crossed in the same tick. It cancels entry remainder and open TPs, reconciles
  fills, then market-sells the remaining bot-owned quantity.
- **Partial fills:** tracked per order; partial SL remains `exit_pending` for
  reconciliation.
- **Quantities:** quantized down; never exceeds acquired minus outstanding sells.
  If a target quantity is below the lot/minimum, that target is marked complete
  with a residual reason.

Trade states: `pending_entry`, `entry_active`, `position_open`,
`oco_submitting`, `oco_active`, `oco_partial`, `tp1_active`, `tp1_completed`,
`tp2_active`, `tp2_completed`, `stop_loss_executing`, `exit_pending`, `closed`,
`needs_reconciliation`.

Trade-order states: `pending`, `submitting`, `open`, `partially_filled`,
`filled`, `cancel_requested`, `canceled`, `rejected`,
`unknown_requires_reconciliation`.

### Partial fills, rounding and restart behavior

- Entry fills are read back from the exchange (`reconcile_intent`) before any
  OCO quantity is computed. If that query is ambiguous, the trade is held and
  retried on the next tick/sweep instead of acting on a stale local quantity.
- OCO allocations are computed once from the **actual** acquired base and
  persisted. A restart reuses `tp1_allocated_qty`/`tp2_allocated_qty`; it never
  silently shrinks or re-expands an existing allocation.
- Each OCO leg is idempotent by kind and stable `clientOrderId`. A confirmed or
  filled leg is never resubmitted. A `pending` leg was never sent and may be
  safely rebased; a `submitting` or `unknown_requires_reconciliation` leg is
  queried by client id and is never replaced until its state is resolved.
- Quantities are quantized down; prices are quantized to the symbol tick. A
  target below the exchange lot/minimum is recorded in
  `unsold_residual_qty`/`unsold_residual_reason`, and the trade is marked
  `needs_reconciliation` rather than being presented as protected.
- A missing OCO leg is retried on a later sweep up to
  `stop_loss_max_retries`; a rejection is recorded. A canceled leg is not
  recreated automatically, because doing so could oversell an already
  partially exited position.
- The OCO stop-limit leg may not fill at the exact stop price in a fast market.
  Any residual remains recorded and is not silently dropped.
- On restart, `serve` runs `recover_on_startup()` and periodically
  `reconcile_all_open()`. `stop_loss_executing` and `exit_pending` resume
  cancellation/fill accounting before selling any remainder. A market exit that
  partially filled, then canceled while offline, is completed with a new
  order id for the remaining quantity.
- A TP state that was persisted before its allocation/order was written is
  completed on recovery. A `pending` row (created locally, never sent) is
  resumed, and its quantity is rebased against current fills so a late fill
  cannot make it oversell. A `pending` stop-loss row is canceled locally and
  replaced by a new exit for the confirmed remainder.
- All exit monitoring is active even when `entry_trigger_offset_ticks` is unset
  (conditional entry disabled). New trade symbols are subscribed to the monitor
  after startup, and the REST fallback fetches prices through the MEXC client.

### Remaining limitations

- `stop_loss_max_retries` caps how many definitive stop-loss/TP rejections are
  retried. `stop_loss_retry_backoff_base_s` is still parsed but not applied;
  recovery cadence comes from market ticks and the reconciliation worker.
- The CLI has no direct lookup by `clientOrderId`; it resolves the id against the
  first page (up to 100 rows) of `current-orders`, then `history-orders`. An
  order outside that window stays `unknown_requires_reconciliation`; it is not
  resubmitted blindly.
- Attached TP/SL is only available on `spot trade place` (limit). Market entries
  rely on local management.
- Exchange-side protection is `active` only when the order detail echoes the
  requested trigger prices. Triggered attached-order listing behavior has not
  been validated with real credentials here.
- A residual below the exchange minimum cannot be sold. It is recorded rather
  than silently dropped; the trade closes with `unsold_residual_qty` set.
- The account-identity preflight compares `GET /api/v3/account` balances read
  through the CLI profile and the REST key. Re-run preflight after any account
  activity changes balances.
- Native two-OCO exits use the CLI operation `spot.trade.place-oco`, which is
  present in the installed `@mexc-ai/cli` v0.0.22 schema. The adapter and the
  mocked lifecycle tests are implemented, but no live OCO order has been placed
  from this repository. Treat live OCO behavior as unverified until a real
  fill produces a real exit and its exchange detail is checked.
- The CLI command wrapper in `$PATH` can be broken while the per-platform binary
  still works; `mexc/cli.py` discovers the known direct binary first. Owners
  should run `trading preflight --network`, which uses the same discovery path.
- `docs/TRADING_DECISIONS.md` records the earlier decision made before the CLI
  exposed `place-oco`; that section is superseded by the shipped OCO flow.

## Notifications

Owner/developer notices are written to `trading_notifications` before delivery
and retried independently of order execution. The outbox key is
`(event_key, role)`; long text is split into chunks. A notification failure can
never resubmit an order. If `CONTROL_BOT_TOKEN` is set, `serve` sends notices to
the configured owner/developer ids; otherwise they stay pending in the outbox.

## Inspect and operate

```bash
.venv/bin/tg-ingest trading status
.venv/bin/tg-ingest config check
.venv/bin/tg-ingest trading preflight
.venv/bin/tg-ingest trading preflight --network
.venv/bin/tg-ingest trading signals --limit 20
.venv/bin/tg-ingest trading intents --limit 20
.venv/bin/tg-ingest trading parse <message_id>
.venv/bin/tg-ingest trading reconcile
.venv/bin/tg-ingest trading trades [--state <state>]
.venv/bin/tg-ingest trading trade <trade-id>
.venv/bin/tg-ingest trading verify
.venv/bin/tg-ingest trading test-order --symbol BTCUSDT --side BUY --quote 10
```

`verify`, `test-order`, and `reconcile` touch MEXC. `test-order` is refused
unless mode is `testonly` or `live`. Never run them without explicit
authorization. `preflight` performs local binary/version/schema and CLI-profile
checks; with `--network` it also runs the CLI doctor, one signed read-only
`current-orders` query, a signed `spot asset account` read, REST key validation,
and compares the CLI and REST account balances. It never places, cancels or
modifies an order.

### CLI protection: verified limits

- The CLI has no lookup by `clientOrderId`; it is resolved against the first page
  (up to 100 rows) of `current-orders` then `history-orders` and matching the
  field. An order outside that window is reported `not_found` and stays
  uncertain rather than being resubmitted.
- `spot.trade.place-oco` supports a native SELL OCO pair (limit TP leg plus
  stop-limit SL leg). `spot.trade.place` still supports one attached TP/SL on a
  limit order, but the hybrid backend does not use that path.
- `oco_active` means every required OCO leg has an exchange order id. It does
  not mean the exchange has been observed to trigger either leg. Triggered
  OCO/exit behavior has not been validated with real credentials here.
- Non-interactive writes pass the CLI's verified global `--yes` flag. Live mode
  arms it automatically when `confirm_noninteractive_writes` is absent; an
  explicit `false` keeps writes fail-closed. A confirmation-required response
  remains a terminal local rejection and contributes no spend.


## Live-enablement checklist (manual, separate approval)

1. Run `trading preflight --network`; confirm the CLI profile is saved/active,
   the CLI and REST keys read the same Spot account, and exit monitoring paths
   are validated with a separate explicit approval.
2. Use a MEXC key with Spot read + Spot trade only; enable IP allowlist; no
   withdrawal permission.
3. Set `order_quote_amount` to an owner-approved positive value.
4. Set `aggregate_budget_quote` to an owner-approved positive value (default 50 USDT; no hard ceiling).
5. Run with `testonly` first; inspect intents, balances, precision, and logs.
6. Set `mode = "live"` and the exact `live_enabled_token`.
7. Leave `mexc_cli.confirm_noninteractive_writes` unset (live arms `--yes`) or
   set it to `true`; never leave an explicit `false` when live writes are
   intended.
8. Restart once; confirm the activation boundary advanced.
9. Submit one minimal test signal only after a separate explicit approval.
10. Monitor `trading status`, `trading intents`, `trading trades`, and the
    notification outbox.

### OCO certainty follow-up review

Uncertainty takes precedence over a filled or working row for the same target;
multiple overlapping working rows require reconciliation. `filled` means a
completed allocation with sufficient recorded execution quantity, never active
protection. An `oco_active` summary requires working quantity to cover the
remaining recorded position (completed allocations do not add coverage). Two
completed allocations finalize only through the existing residual-position
check. These checks change status certainty, not submission authorization.
