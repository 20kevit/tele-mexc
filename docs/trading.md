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

## Execution model

There is one execution behaviour: automatic MEXC Spot BUY on a valid signal.
There is no simulated, test-only, or live product mode and no per-signal
approval.

| Requirement | Behavior |
| --- | --- |
| Owner acknowledgement | `execution_acknowledgement = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"` is required to construct the pipeline |
| MEXC CLI backend | `[mexc_cli] enabled = true` and a healthy CLI profile are required for Spot writes |
| Sizing | One fixed `order_quote_amount` chosen by the owner; signal text never changes it |
| Submission boundary | Pause, emergency stop, Telegram/MEXC/channel readiness and reconciliation blockers are checked atomically with the `planned -> submitting` claim |

`control_setup.paused` is authoritative. `/pause` takes an exclusive kernel
`flock`, waits for any in-flight submission to finish, writes the pause state,
and only then acknowledges. `/resume` checks readiness again (including
emergency stop and unresolved reconciliation) and refuses to clear the pause
when any prerequisite is missing. The shared lock is
`<db_path>.submission.lock`; the kernel releases it on a crash.

`order_intents.mode` remains a historical database column. New intents are
written as `live`; legacy `sim`/`testonly` rows are never executable and are
rejected locally if a stale queue item tries to resume them.

## Execution backends (REST reads + CLI writes)

Two transports are used deliberately, not as competing integrations:

| Concern | Backend | Reason |
| --- | --- | --- |
| Exchange metadata, balances, single-order query and fills | REST `MexcSpotClient` | Spot v3 read and accounting implementation |
| Live BUY/SELL and OCO placement, cancellation | `@mexc-ai/cli` adapter | Live write backend |

The low-level REST `POST /api/v3/order/test` primitive remains available for
contract tests, but it is not a product mode and has no CLI command.

The CLI adapter (`mexc/cli.py`) is argv-only, bounded by a timeout, and never
starts a shell. It uses the CLI's own encrypted profile
(`~/.mexc/profiles`) and never receives `MEXC_API_KEY`/`MEXC_API_SECRET`.
Reads remain on REST so there is one market-data/account implementation.

`[mexc_cli]` in `config/config.toml`:

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | required for the automatic Spot pipeline; a missing binary fails closed |
| `binary` | auto | explicit path; else `MEXC_CLI_BINARY`, the known per-platform path, then `PATH` |
| `profile` | active profile | optional profile name passed as `--profile` |
| `timeout_s` | `5.0` | subprocess timeout in `(0, 60]` |
| `min_version` | `0.0.22` | minimum accepted CLI version |
| `confirm_noninteractive_writes` | `true` | arms the CLI `--yes`; explicit `false` keeps writes fail-closed |

Writes always pass `--no-retry`; the adapter never retries a placement. The
`clientOrderId` is reused verbatim. A CLI `duplicate` response means the earlier
attempt landed, so it becomes `unknown_requires_reconciliation` and is
reconciled, never resubmitted. A CLI write without explicit confirmation is
rejected locally and no request reaches the exchange.

Automatic execution is the product behaviour: the service cannot answer the
CLI's terminal prompt, so non-interactive writes are armed by default. Setting
`confirm_noninteractive_writes` to `false` is honored as a fail-closed override;
every write is then rejected locally. Leave it unset or set it to `true`.

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

Environment overrides: `TRADING_EXECUTION_ACKNOWLEDGEMENT`,
`TRADING_ORDER_QUOTE_AMOUNT`,
`MEXC_CLI_BINARY`, `MEXC_CLI_PROFILE`.

| Key | Default | Validation / meaning |
| --- | --- | --- |
| `execution_acknowledgement` | unset | exact `I_UNDERSTAND_REAL_MONEY_AND_RISKS`; unset/wrong refuses pipeline construction |
| `order_quote_amount` | `3.0` | positive finite per-order quote; 3 USDT default; precedence is env > owner Telegram menu (`control_settings.order_quote_amount`) > `[trading]` > default. A legacy `buy_quote_usd` value is read only when the canonical key is absent. |
| Order type | LIMIT only | `order_type_policy`, `TRADING_ORDER_TYPE_POLICY`, and MARKET execution are rejected |
| `balance_fee_buffer_pct` | `0.2` | `0 ≤ value ≤ 5`; pre-trade fee cover because the exact fee is only known after fills |
| `balance_max_age_s` | `5.0` | `(0, 300]`; rejects a stale account snapshot before submission |
| `recon_interval_s` | `30.0` | reconciliation period, minimum 5 s |
| `emergency_stop` | `false` | blocks new intents and pre-submit revalidation |
| `entry_trigger_offset_ticks` | `3` | integer ≥1; conditional LIMIT entry waits for price to cross `Entry - offset × tick_size`. Missing resolves to 3; it cannot disable conditional entry or fall back to direct entry |
| `entry_trigger_stale_threshold_s` | `60.0` | positive; a conditional-entry order is not triggered from an older price tick |
| `tp1_allocation_pct_of_acquired` | `50.0` | 0–100 of actual acquired quantity |
| `tp2_allocation_pct_of_remaining` | `100.0` | 0–100 of quantity left after TP1 |
| `stop_loss_max_retries` | `3` | integer ≥1; caps definitive TP/SL rejection retries |
| `stop_loss_trigger_offset_ticks` | `3` | stop-loss activation trigger = exit price + this many symbol ticks (0 disables); exit price itself unchanged |
| `monitor_poll_interval_s` | `2.0` | positive |
| `monitor_max_retries` | `5` | integer ≥1 |
| `monitor_backoff_base_s` | `1.0` | positive |

Legacy `mode`, `live_enabled_token`, `max_quote_per_order`, daily/open-order
caps, signal-age/price-deviation caps, `allowed_symbols`, and `allowlist_mode`
are rejected at load time. They are not silently ignored because that would
change financial behaviour.

### Retained safety parameters

| Setting | Why it is retained |
| --- | --- |
| `execution_acknowledgement` | One-time global construction gate proving the owner intends automatic real Spot execution. It is not per-order approval. |
| `balance_fee_buffer_pct` | The exact exchange fee is only visible after fills; the pre-trade requirement adds a small documented buffer so the quote/base balance is sufficient. |
| `balance_max_age_s` | Prevents a decision based on a stale account snapshot. It is a freshness check, not portfolio polling. |
| `entry_trigger_stale_threshold_s` | Prevents a conditional entry from triggering on an old market tick. |
| `entry_trigger_offset_ticks` | Conditional-entry trigger distance; default exactly 3 ticks, still owner-adjustable. |
| `stop_loss_trigger_offset_ticks` | Controls the OCO stop activation trigger relative to the configured stop exit price. |


There is no daily-order cap, open-order cap, signal-age limit, price-deviation
threshold, or symbol allowlist. MEXC market metadata and order filters remain
enforced. The historical `trading_daily_spend` table is retained for compatibility but is no longer written or read.

## Parsing and executable signals

`trading/parser.py` is the only production parser.

| Kind | Meaning |
| --- | --- |
| `alert` | New actionable BUY/SELL signal |
| `update` | Edited/updated actionable signal |
| `cancel` | Cancel instruction; may cancel prior matching intents |
| `result` | TP hit/entry filled/position closed follow-up; never executable |
| `info` | Ordinary channel text |
| `ambiguous` | Direction detected but required fields/format are not safe |

- LONG is treated as BUY; SHORT/SELL is retained but rejected by risk.
- Futures/perpetual/margin/leverage wording is ingested but never enables
  Futures execution.  A LONG signal with a Spot-compatible symbol and complete
  Entry/SL/TP1/TP2 fields is executed as a Spot BUY; leverage/deposit text is
  ignored for sizing.
- Missing/unsafe entry, symbol, or required fields fail closed.
- A signal marked MARKET is converted to LIMIT only when it has a valid Entry price; otherwise it is rejected. MARKET orders are never submitted.
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

Order type is always LIMIT. A MARKET signal is converted to LIMIT when an Entry
price exists; without one it fails closed. Configuration cannot re-enable
MARKET.

Quantity is `order_quote_amount / price`, then quantized **down** to the symbol
lot step and validated against `exchangeInfo` minimum/maximum and price guards.
The engine re-validates immediately before submission.

## Replay boundary and idempotency

The activation boundary is `app_state['trading.activation_boundary_message_id']`.
At each `serve` start, **before live handlers are registered**, the engine
snapshots the selected channel's newest stored message id and persists
`max(existing boundary, snapshot)`. The boundary is forward-only.

Always:

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

Server-clock synchronization and timestamp policy:

- Official MEXC rule (verified): a signed request is accepted when
  `timestamp < serverTime + 1000` and `serverTime - timestamp <= recvWindow`;
  `recvWindow` defaults to `5000 ms` and may not exceed `60000 ms`.
- A `ServerClock` anchors `GET /api/v3/time` to the process monotonic clock,
  samples three observations, keeps the lowest-RTT anchor, and uses the
  **median offset** of the low-RTT samples (half-RTT compensation). The
  result is `server_time_at_anchor + monotonic_elapsed`; a wall-clock jump
  after the anchor cannot move the signed time.
- Samples whose RTT exceeds `max_rtt_ms` are excluded from the offset
  calculation. Samples whose offset differs from the round median by more
  than `max_offset_deviation_ms` are treated as outliers and dropped, and the
  final offset is clamped to `±max_offset_ms` so one bogus `serverTime`
  cannot move the clock by hours.
- Signed, read-only requests (`account`, `myTrades`, `openOrders`, `order`)
  may refresh the clock and retry **once** after `700003`/`10073`. Writes are
  never retried automatically; an ambiguous write outcome is reconciled, not
  resent.
- Signed writes refuse to leave the process without a recent, low-RTT sample
  (`MexcClockUnavailable`). No order reaches the exchange on this path, so it
  is treated as a safe local rejection, never an uncertain submission.
- Production (`build_mexc_client` with no injected `clock`) uses the RTT-aware
  `ServerClock` with defaults: `sample_count=3`, `min_samples_for_write=2`,
  `max_rtt_ms=1500`, `read_max_age_s=30`, `write_max_age_s=10`,
  `max_offset_deviation_ms=5000`, `max_offset_ms=300000`. Refreshes are
  debounced by `min_refresh_interval_s=1`, serialized by a lock, and happen
  lazily when the anchor is older than `read_max_age_s` or immediately before
  a signed write; the timestamp is stamped after any rate-gate wait.
- Tests that inject `clock=lambda: ...` keep the legacy path exactly: no
  server-time sampling, no retry, unchanged timestamps.
- CLI writes (`HybridMexcClient`) are performed by an external process and
  cannot use this client's timestamp; their outcomes remain reconcile-only,
  exactly as before.

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

## Technical reservation ledger

Intent creation and its reservation are one atomic transaction. The reserved
amount is:

```
quote_amount * (1 + balance_fee_buffer_pct / 100)
```

`trading_budget_ledger` is **not a portfolio budget** and has no configurable
spending cap. It prevents duplicate reservations and keeps concurrent intent
creation coherent. States:

| State | Effect |
| --- | --- |
| `reserved` | Active technical reservation |
| `uncertain` | Frozen until reconciliation; never auto-released |
| `settled` | Reservation replaced by actual spend and fees |
| `released` | Rejected/failed/never-sent intent; no active reservation |

- Filled: settle actual `cumulativeQuoteQty` (or fill totals), plus real fees;
  when fees are not yet visible, the configured buffer is retained.
- Canceled: retain actual partial spend and fees.
- Rejected/failed/canceled-locally: release.
- Restart never resets the ledger.

## Execution

Flow: reservation ensure → fresh balance preflight → `submitting` → exchange →
`submitted`/uncertain/rejected → reconciliation. Pause/readiness are checked in
the same transaction as `planned → submitting`.

Live balance preflight:

- Fetch the account snapshot on every submission.
- Reject if older than `balance_max_age_s`, if `canTrade` is false, or if the
  asset is missing.
- Required = order notional × fee factor (BUY) or base quantity × fee factor
  (SELL).
- Subtract outstanding local open intents for the same symbol; the remaining
  free amount must cover the fee-inclusive requirement. No percentage of the
  account is reserved.
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
  three ticks above the exit price), so the stop can activate earlier as the
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
| `unprotected_critical` | position owns base quantity but no exit leg is exchange-confirmed; new BUYs are globally blocked until coverage is verified |
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

## Audit journal

Every trading and exchange event is written to `audit_events` before or inside
the same transaction as the state change it describes. Rows carry
`event_id`, `event_type` (`category.event`), `severity`, `occurred_at_utc`,
`recorded_at_utc`, correlation/causation ids, optional `trade_id`/`intent_id`/
`order_id`/`exchange_order_id`, `symbol`, `reason_code`, `outcome`,
`source_ref`, `evidence_source`, `evidence_limitations`, `payload_json`, and a
bounded, redacted `detail`. `secrets_redacted` is always set; the redaction
layer masks bot tokens, bearer credentials, API keys, secrets, and long blobs.

- `INSERT OR IGNORE` plus a `dedupe_key` makes each audit event idempotent
  across retries and restarts; repeating the same event never duplicates a row.
- `event` keeps the legacy human-readable name; `event_type` is the stable
  machine key (`mexc_request.place_order`, `trade.closed`, `reconcile.outcome`,
  `oco.sibling_canceled`, …).
- Legacy migration-17/pre-18 rows are converged by migration 18: missing
  columns are added, timestamps/ids are backfilled from existing columns, and
  the row is explicitly labelled legacy — never upgraded into an
  exchange-confirmed fact.
- Startup/shutdown, intent lifecycle, protection state changes, OCO group and
  leg transitions, reconciliation outcomes, and notification deliveries all
  emit audit events.

## Exchange evidence

Every order/account exchange call is recorded in `exchange_operations`
**before** the request is sent, and finalized after the response arrives.
A crash between the insert and the response therefore leaves a durable,
correlated record that a write may have been attempted.

- `operation_class` is `read` or `write`; `write_attempted` is set for writes.
- Fields include `endpoint`, `client_order_id`, `exchange_order_id`,
  `request_payload` (redacted), `request_started_at`, `request_finished_at`,
  `response_classification` (`success`/`rejected`/`uncertain`/`rate_limited`/
  `error`), `exchange_code`, `http_status`, `response_payload` (redacted),
  `error_class`, `error_message` (redacted), `rate_limit_wait_ms`, and
  `uncertainty` (`none`/`uncertain`/`reconciled`/`read_only_uncertain`).
- Market-data reads (ticker/depth/klines) are intentionally **not** recorded per
  call: they are high-frequency and carry no order state. Order submission,
  cancellation, and status queries are always recorded.
- An uncertain write stays `uncertain` until a read-only reconciliation proves
  what happened on the exchange. It is never auto-resubmitted with a new client
  id.

## Trade lifecycle (`TradeManager`)

Only `live` intents get a `trades` row. Direct entry executes immediately;
conditional entry defers the entry intent until a price trigger.

- **Entry:** limit BUY at the signal entry price; never converts to market.
- **Conditional entry:** if market < entry and offset ≥1, wait for
  `entry_price - offset_ticks × tick_size`, then submit the limit BUY through the
  engine's reservation/balance gate. Missing/stale price fails closed.
- **TP1:** first target; limit SELL of
  `tp1_allocation_pct_of_acquired` of actual filled quantity.
- **TP2:** second target; limit SELL of `tp2_allocation_pct_of_remaining` of what
  remains after TP1.
- **Stop-loss:** parsed `sl_price`; one per trade. SL wins when SL and TP are
  crossed in the same tick. Protective exits use exchange-side OCO stop-limit
  legs; no MARKET order is submitted.
- **Partial fills:** tracked per order; partial SL remains `exit_pending` for
  reconciliation.
- **Quantities:** quantized down; never exceeds acquired minus outstanding sells.
  If a target quantity is below the lot/minimum, that target is marked complete
  with a residual reason.

Trade states: `pending_entry`, `entry_active`, `position_open`,
`oco_submitting`, `oco_active`, `oco_partial`, `tp1_active`, `tp1_completed`,
`tp2_active`, `tp2_completed`, `stop_loss_executing`, `exit_pending`, `closed`,
`needs_reconciliation`, `unprotected_critical`.

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
### OCO group and leg observation

Each trade gets an explicit `oco_groups` row (`oco_group_id`, `state`, `intended_config`,
`protection_status`) and per-target `oco_legs` rows (`target`, `state`,
`exchange_status`, `filled_qty`, `cancel_reason`, `canceled_by_target`,
`evidence_id`, `last_observed_at`). The group state mirrors the trade state
(`intended`, `submitting`, `active`, `partial`, `triggered`, `completed`,
`uncertain`, `needs_review`).

- Sibling cancellation is **observed**, not invented: when one OCO leg fills and
  its peer is canceled with zero sells, the canceled leg is attributed to the
  filled sibling (`cancel_reason = 'oco_sibling_filled'`,
  `canceled_by_target = <filled target>`). This never resubmits an exit and
  never closes a trade that still owns base quantity.
- Every OCO transition writes an audit event (`oco.intended`, `oco.state`,
  `oco.sibling_canceled`) and a notification (`OCO_PROTECTION_ACTIVE`,
  `OCO_PROTECTION_PARTIAL`, `OCO_PROTECTION_UNCERTAIN`, `OCO_PROTECTION_FAILED`)
  routed by recipient role and the selected minimum severity below.
- `oco_active` means every required leg has an exchange order id; it does not
  mean either leg has been observed triggered.

- Conditional LIMIT entry is always the strategy when market is below Entry;
  the default trigger is exactly 3 exchange ticks below Entry.
- All exit monitoring is active for every trade. New trade symbols are
  subscribed to the monitor after startup, and the REST fallback fetches prices
  through the MEXC client.

### Pre-BUY exit-protection gate and the new-BUY block

A live BUY is rejected *before* any exchange submission unless it can be fully
protected:

- The signal must supply `entry_price`, `tp1_price`, `tp2_price`, and `sl_price`.
- The ladder must be ordered: `stop_loss < entry_price < TP1 <= TP2`.
- Symbol metadata must be available for quantization/validation.
- The execution engine must offer a working OCO submission path.

Any failure rejects the intent locally (`rejected`, category `risk`) with zero
exchange calls and an owner/developer notification.

After an entry fills, `TradeManager._place_oco_exits()` places two native OCO
SELL orders (TP1 + stop-loss, TP2 + stop-loss) against the actual acquired base
quantity. Protection is reported as active only when every required leg is
exchange-confirmed. When no leg can be confirmed, the trade enters
`unprotected_critical`: reconciliation keeps retrying protection idempotently
(never blindly resubmitting an uncertain write), and a **durable global new-BUY
block** is persisted in `app_state`.

| Flag | Meaning |
| --- | --- |
| `trading.new_buys_blocked` | `"1"` while any bot-owned position lacks verified exit coverage |
| `trading.new_buys_block_reason` | Human-readable reason naming the unprotected position(s) |

The block is never cleared by time. It clears only when every open position is
fully sold or has exchange-confirmed working coverage. On every restart,
`recover_on_startup()` rebuilds the block from durable trade state, so a stale
flag cannot hide an unprotected position. The live execution path re-checks the
block immediately before submission, so a retried queue item is also covered.

`BUY_BLOCK_ACTIVE` / `BUY_BLOCK_CLEARED` notifications are sent on state
changes; audit events are written as `trading.buy_block`.

### Remaining limitations

- `stop_loss_max_retries` caps definitive stop-loss/TP rejection retries;
  recovery cadence comes from market ticks and the reconciliation worker.
- The standalone CLI adapter resolves a `clientOrderId` against the first page
  (up to 100 rows) of `current-orders`, then `history-orders`. The live hybrid
  client delegates single-order queries to REST instead. Unresolved orders
  remain uncertain and are not blindly resubmitted.
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
never resubmit an order. Each event carries a severity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, or
`CRITICAL`). The control bot exposes one selectable minimum severity:

| Selected level | Delivered |
| --- | --- |
| DEBUG | DEBUG and above |
| INFO | INFO and above |
| WARNING | WARNING and above |
| ERROR | ERROR and above |
| CRITICAL | CRITICAL only |

Recipient routing is separate from severity: genuine signals go to owner and
developer, routine local rejections/diagnostics go to the developer, and
recovery notices default to the developer. The obsolete per-role/per-category
`notification_preferences` table is historical and is no longer read or written.

### Delivery outcomes and ambiguous recovery

- `sent` — Telegram accepted the message.
- `retry` — transient failure or Telegram `retry_after`; scheduled again after
  the wait (capped at 24h).
- `failed` — definitive rejection; not retried automatically.
- `ambiguous` — the process ended mid-delivery or acceptance could not be
  confirmed; recovered once on startup to `ambiguous` and **never** retried
  automatically.
- `suppressed` — the chat is blocked/deactivated; not retried.

Delivery attempts are recorded in `notification_delivery_attempts` with
outcome, error, telegram message id, retry-after, and an `ambiguous` flag.

### Control-bot integration

The control bot polls `getUpdates`, routes Persian callback data, and on each
`tick` recovers ambiguous notifications older than 120s and flushes the trading
outbox. Notification callbacks (`expand`, `signal_source`, `ack`) are issued as
single-use, time-limited tokens in `notification_callbacks` and are consumed
once per role.

If `CONTROL_BOT_TOKEN` is set, `serve` sends notices to the configured
owner/developer ids; otherwise they stay pending in the outbox. The notifier
tolerates bots that accept only `(chat_id, text)` (no `reply_markup`).

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
.venv/bin/tg-ingest trading notifications [--status pending] [--limit 50]
.venv/bin/tg-ingest trading verify
```

`verify` and `reconcile` touch MEXC. Never run them without explicit
authorization. `preflight` performs local binary/version/schema and CLI-profile
checks; with `--network` it also runs the CLI doctor, one signed read-only
`current-orders` query, a signed `spot asset account` read, REST key validation,
and compares the CLI and REST account balances. It never places, cancels or
modifies an order.

### CLI protection: verified limits

- The standalone CLI adapter resolves `clientOrderId` against only the first
  page (up to 100 rows) of `current-orders`, then `history-orders`. The hybrid
  live client uses REST for single-order queries; unresolved results remain
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
4. Set `execution_acknowledgement = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"`.
   This is a one-time global construction gate, not per-order approval.
5. Leave `mexc_cli.confirm_noninteractive_writes` unset (defaults to `true`) or
   set it to `true`; never leave an explicit `false` when writes are intended.
6. Complete owner setup: Telegram authenticated, MEXC verified, and channel
   selected.
7. Restart once; confirm the activation boundary advanced.
8. Monitor `trading status`, `trading intents`, and the notification outbox.

### OCO certainty follow-up review

Uncertainty takes precedence over a filled or working row for the same target;
multiple overlapping working rows require reconciliation. `filled` means a
completed allocation with sufficient recorded execution quantity, never active
protection. An `oco_active` summary requires working quantity to cover the
remaining recorded position (completed allocations do not add coverage). Two
completed allocations finalize only through the existing residual-position
check. These checks change status certainty, not submission authorization.
