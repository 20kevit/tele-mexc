# Trading Decisions

Canonical, version-controlled record of the intended trading behavior for the
Telegram-to-MEXC spot signal bot. This document is authoritative for the
trading strategy. Executable code and tests (`tg_signal_ingest/`, `tests/`)
outrank it; if code and this document disagree, the code wins and this document
must be updated in the same change.

Related: `docs/trading.md` (canonical trading behavior), `docs/data-model.md`
(schema/migrations), `docs/operations.md` (operator guide), `AGENTS.md`
(safety rules).

Status: current. Gaps and verified limitations are marked explicitly.
Section 5 was revised after the installed CLI gained `spot.trade.place-oco`; the
earlier local-only fallback remains available but is not the shipped live path.

---------------------------------------------------------------
1. Trading scope
---------------------------------------------------------------

- Trade only MEXC **Spot** markets.
- Execute **BUY / LONG entries only**.
- SHORT / SELL instructions from Telegram are parsed for reporting and then
  **rejected**; no exchange sell order is ever originated from a SHORT signal.
- Sell orders exist only as exit legs (take-profit / stop-loss) for a bot-owned
  spot position.
- No futures, margin, leverage, or short selling.
- Never sell unrelated assets already held in the owner's MEXC account.
- No withdrawal, transfer, or unrelated account-management functionality.
- Preserve the existing Telegram signal extraction and analysis functionality.
  Do not redesign it unless a concrete integration defect prevents trading.

---------------------------------------------------------------
2. Position sizing and capital limits
---------------------------------------------------------------

- Default order budget: **3 USDT per entry** (fixed owner-configured quote
  amount, never derived from signal text).
- Preserve the owner-facing setting that allows this amount to be changed
  (`order_quote_amount` in `[trading]`, overridable via the Telegram menu and
  persisted in the DB; precedence is env > Telegram menu/DB > `config.toml` >
  built-in default).
- Preserve the owner-facing aggregate budget setting (`aggregate_budget_quote`),
  default 50 USDT. It is a configurable reference value, not a hard-coded cap;
  the owner may set any positive value. Never silently change it.
- Calculate actual acquired quantity from **confirmed execution fills**,
  accounting for applicable fees and exchange precision.
- All exit quantities are based on the **actual bot-owned position**
  (confirmed fills minus confirmed sells), never on the intended order quantity
  or a percentage of the owner's entire wallet balance.
- Respect MEXC minimum order amounts, quantity increments, price ticks,
  available balances, and other applicable exchange filters.
- Budget reservation: quote amount × (1 + fee buffer) is reserved atomically at
  intent creation; the ledger survives restart and is never reset.

---------------------------------------------------------------
3. Entry strategy (for a signal with entry price E)
---------------------------------------------------------------

1. Current market price above E: submit a **LIMIT BUY at E** using the
   configured quote budget.
2. Current market price below E: calculate the instrument's **actual tick size**
   from exchange metadata and submit the conditional entry trigger at
   `E - offset_ticks × tick_size` (five ticks below E when offset = 5). When the
   trigger activates, submit a **LIMIT BUY at E** (entry price remains a hard
   maximum; never converts to market).
3. Current price equals E: treated as case 1 (market is not below the entry), so
   a LIMIT BUY at E is submitted immediately. Deterministic: price == entry is
   not "below", so no conditional wait.
4. Never hardcode a universal tick size; obtain and validate the tick size per
   instrument from `exchangeInfo` / symbol metadata.
5. Respect order expiration, cancellation, duplicate-signal prevention,
   partial fills, and uncertain submission outcomes.
6. A conditional trigger activating does not guarantee the resulting limit order
   fills; its actual lifecycle is tracked and reconciled.
- Idempotency: `sha256(channel_id:message_id:action:symbol)` → `client_order_id`;
  one Telegram message yields at most one intent; a conflicting edit is refused.
- Backfilled and at/below-boundary messages are stored and audited but never
  traded (activation boundary persists in
  `app_state['trading.activation_boundary_message_id']`).

---------------------------------------------------------------
4. Exit strategy (two native OCO exits — implemented)
---------------------------------------------------------------

For a completed or partially completed entry, calculate the actual filled,
bot-owned quantity and divide it into two portions:

- OCO 1: ~50% of the actual acquired quantity; take-profit limit at TP1;
  stop-loss activation at the signal stop price.
- OCO 2: the remaining ~50%; take-profit limit at TP2; stop-loss activation at
  the same stop price.
- Apply exchange quantity precision and rounding; quantize down.
- Combined exit quantities never exceed the confirmed acquired position.
- Each OCO contains its own take-profit branch and stop-loss branch. When one
  branch activates/executes, the exchange's documented cancellation behavior
  applies to its counterpart. The other OCO remains independently responsible
  for its allocated quantity.
- A stop trigger does not guarantee execution exactly at the stop price.
  Stop-limit execution risk is accepted and documented: a triggered stop may
  not fill in a fast market, leaving residual exposure.
- Partial entry fills: protect only the confirmed acquired quantity. If the full
  exit structure cannot be established, raise an immediate alert and use the
  documented recovery procedure rather than silently leaving an unprotected
  position.

---------------------------------------------------------------
5. Native OCO implementation and verified CLI surface
---------------------------------------------------------------

**Verified against the installed MEXC CLI (`@mexc-ai/cli` v0.0.22, direct
platform binary):**

- `spot.trade.place-oco` exists and is a signed write operation at
  `/api/cli/spot/v4/order/place/oco`.
- Body fields used by the adapter: `currency`, `market`, `quantity`,
  `tradeType` (`SELL`), `orderType` (`OCO_STOP_LIMIT_ORDER`), `orderSource`
  (`CLI`), `price` (TP limit leg), `triggerType` (`LE`), `triggerPrice` and
  `triggerLimitPrice` (stop-loss leg), plus an optional `clientOrderId`.
- `MexcCliAdapter.place_oco()` builds that body; `HybridMexcClient.place_oco()`
  delegates writes, while reads stay on REST.

**Shipped behavior:** after an entry fill is confirmed and fill synchronization
is complete, `TradeManager._place_oco_exits()` submits the two OCO SELL orders
described in section 4. Each leg has a stable `clientOrderId`, is persisted
before submission, and is reconciled rather than blindly resubmitted after an
uncertain outcome. The older local TP/stop-loss manager remains in the tree but
is suppressed while OCO legs are active, and it is not the live hybrid path.

**Documented limitation:** a stop trigger does not guarantee execution at the
stop price; residual exposure is recorded rather than silently dropped
(`unsold_residual_qty` / `unsold_residual_reason`), and trades that cannot be
fully closed are held in `needs_reconciliation` with an alert. No live OCO has
been placed from this repository; the exchange's triggered-OCO behavior remains
unverified.

---------------------------------------------------------------
6. Partial fills and order lifecycle
---------------------------------------------------------------

Handled cases: entry partial fill; entry fills across several executions; entry
open or expires; one OCO/TP accepted while the other fails; exit rejected for
reserved balances or quantity precision; partial TP execution; stop triggered
but not filled; OCO canceled/rejected/expired; service restart with open
orders; request timeout after the exchange may have accepted it.

- No blind resubmission after ambiguous responses; uncertain submissions become
  `unknown_requires_reconciliation` and are reconciled, never retried
  automatically. Exchange `-2011` never terminalizes a non-terminal intent.
- Reconcile actual orders, executions, remaining quantities, and balances
  before recovery decisions.
- Entry fills are read back from the exchange (`reconcile_intent`) before any
  exit quantity is computed; ambiguous queries hold the trade.
- Sold quantities are summed across all orders of a kind so re-armed targets
  cannot oversell.
- On restart, `recover_on_startup()` and `reconcile_all_open()` resume
  cancellation/fill accounting before selling any remainder.

---------------------------------------------------------------
7. Notifications
---------------------------------------------------------------

- Centralized durable outbox (`trading_notifications`) written before delivery;
  retry is independent of order execution and can never resubmit an order.
- Both recipients (owner + developer) are notified for genuine signal and
  trading events; ordinary non-signal diagnostics go to the developer only.
- Configured destinations are reused (`CONTROL_BOT_TOKEN`, owner/developer user
  ids). No usernames, IDs, or contact details are invented; if the developer
  destination is not configured, the missing configuration is identified with
  exact setup instructions.
- Persian for owner-facing messages; clear technical English for
  developer-facing messages.
- Each notification includes timestamp, symbol/signal reference, event and
  status, order id (when available), requested vs actual quantity, price/quote,
  TP/SL levels, and error details/recovery status.
- No API secrets, authentication codes, passwords, session contents, or
  credentials. Private signal contents are not exposed to the developer unless
  explicitly authorized by configuration.
- Undelivered notifications are logged, retried through a bounded durable
  mechanism, and deduplicated so unchanged states are not repeated on every
  polling iteration while no meaningful event is silently discarded.

---------------------------------------------------------------
8. Operational state (observed at time of writing)
---------------------------------------------------------------

- Service `tg-ingest` is running in `mode = "live"` with the explicit live
  token present, `order_quote_amount = 3` USDT, `aggregate_budget_quote = 50`
  USDT. Emergency stop is off.
- Two spot BUY intents are in `filled` state (real exchange orders, fully
  filled); aggregate spend is within the 50 USDT cap (~$6 spent, ~$44
  remaining).
- **No TradeManager trade/exit rows exist for these filled positions, and the
  entry orders carry no attached take-profit/stop-loss triggers.** These two
  live positions therefore currently have no exit protection. This is a known
  gap requiring owner review: per the exit strategy, positions should not be
  left unprotected. No real order will be placed, canceled, or reconciled by
  this document's author to remediate it; the owner must apply the documented
  recovery procedure or enable the TradeManager exit path for future entries.
- All 728 automated tests pass; lint and type checks are part of the dev
  workflow (`docs/development.md`).

---------------------------------------------------------------
9. Change control
---------------------------------------------------------------

- Document implemented behavior only. Mark inference and unresolved gaps
  explicitly.
- If implementation changes a decision recorded here, update this document
  explicitly in the same change; do not allow code and documentation to
  diverge.
- Do not store real secrets in version-controlled documentation.
