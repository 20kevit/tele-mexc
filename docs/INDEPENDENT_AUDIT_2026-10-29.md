# Independent trading-systems audit — 2026-10-29

## A. Executive summary

**Not a production-readiness certification.** Offline inspection and deterministic
experiments found material accounting and observability defects. One narrowly
scoped OCO certainty-classification fix is included; it changes neither exchange
writes nor the approved entry/exit policy. Remaining defects require separate
reviewable fixes. There is no evidence here of profitability.

### Evidence boundary

- Repository `20kevit/tele-mexc`, clean initial tree, branch
  `arena/8b00f400-tele-mexc`, baseline
  `a3a10b56885c5bf1186b6b4fb5f77c8f8cb2b17d`; origin is the corresponding GitHub
  HTTPS remote. The available local log contained one commit.
- Read `AGENTS.md`, README, core architecture/data/operations/trading decisions,
  exchange-integration material and execution/recovery code; inventoried all
  source/test paths and ran the full test suite. **This is not an exhaustive
  line-by-line review of all ~48,000 source/test/documentation lines.** Peripheral
  analysis/management/control behavior and all migration upgrade permutations
  have not been independently certified. Existing reports are not evidence of
  current production state.
- Existing open PR #1 (`arena/bbce7401-tele-mexc`) concerns documentation; it was
  not modified. Session rules require this fixed branch, so a new branch was not
  created despite the mission's general branch-isolation request.
- No production host, private database, credentials, session, account balance,
  open order or historical performance records were accessed. Protected
  PENDLEUSDT/MYXUSDT positions were not touched. Documentation's historical live
  state is **not** a current account observation.
- No exchange request, test order, cancellation, reconciliation command,
  Telegram login, deployment, production migration, setting change or restart
  was performed. Public documentation retrieval is not exchange verification.

## B. Actual execution flow

```mermaid
sequenceDiagram
    participant T as Telegram MTProto
    participant S as IngestService
    participant D as SQLite
    participant E as TradingEngine
    participant M as TradeManager
    participant C as MEXC CLI writes
    participant R as Spot v3 REST reads
    T->>S: new/edit/delete
    S->>D: message snapshot + versioned queue
    S->>E: ProcessingEngine claims work
    E->>D: parsed signal + replay/duplicate decision
    E->>R: metadata / ticker / account (non-sim)
    E->>D: intent + budget ledger (transaction)
    E->>M: decide direct / conditional entry
    Note over M,D: Conditional waits for observed price >= trigger
    E->>D: claim submitting BEFORE request
    E->>C: LIMIT BUY at signal entry (or approved explicit market path)
    C-->>E: acknowledgement OR rejection OR uncertainty
    E->>D: submitted / rejected / unknown; never blind resend
    E->>R: order + myTrades reconciliation
    E->>D: order state, fills, fill-sync flag
    M->>D: terminal entry and synchronized quantity
    M->>C: two durable OCO SELL allocations with shared SL
    Note over C,R: OCO parent/child visibility across CLI and REST NOT VERIFIED
    M->>R: query exit status
    M->>D: sold totals; closed only when no residual, else review
    E->>D: notification outbox
    S->>T: independent best-effort Bot API delivery
```

### Stage-to-code/evidence map

Paths below are under `tg_signal_ingest/`. “Tested” means mocked, not live.

| Stage | Actual source / state and error behavior | Verification / limits |
|---|---|---|
| Receive/persist | `service.py:_register_handlers`, `_store_event_message`; `normalize.py:message_to_record`; `repository.py:upsert_message`; `messages`, `message_edits`, `sync_state` | Repository/service tests; channel identity filtering. No live event verified |
| Edits/duplicates | Versioned queue unique `(channel_id,message_id,item_type,targets_version)`; `processing.py:_handle_item`; engine version/intent checks | Repository/processing/channel-boundary tests; a conflicting edit does not create another intent |
| Downtime/history | `service.py:_arm_trading_boundary`, `sweep_gap`, `refresh_recent_window`; `backfill.py`; `app_state` | Activation/recovery tests. Recovery/backfill is non-tradeable; offline deletions/old edits remain limited |
| Parse/normalize | `trading/parser.py:SignalParser`; `signals` per message version | Signal/channel-format tests; no OCR execution; ambiguous data fails closed |
| Validate/map | `trading/risk.py:RiskManager.evaluate`, engine `_symbol_meta`; `mexc/symbols.py:SymbolRegistry` | BUY-only, no signal-size/leverage use; metadata mocks cannot prove current pair availability |
| Entry timing | `trade_manager.py:on_intent_planned`, `_create_trade`, `_check_entry_trigger` | Conditional-entry tests. Threshold comparison is price >= trigger, not proof of a continuous upward crossing |
| Market data | engine `fetch_market_price(s)`, monitor; manager `_is_stale` | Monitor tests; local receipt freshness is not an exchange-event freshness guarantee; no executable depth recorded by this decision path |
| Size/filters | `trading/sizing.py:apply_sizing`, engine `_quantize_decision`; `SymbolMeta.validate_order` | Decimal, floor quantity/price, min/max published quote constraints; sizing/symbol tests; actual live filter contract unverified |
| Construct/submit | engine `_order_payload`, `_execute_intent`, `_execute_live`; `order_intents`, history/audit/ledger | Durable submitting before external call; live token/balance/filter gates; no test credentials required |
| CLI boundary | `mexc/cli.py:MexcCliAdapter`, `mexc/hybrid.py` | argv-only, timeout, JSON normalization, `--no-retry`; CLI version/profile/schema mocked; installed binary not independently inspected |
| Entry acknowledgement/fills | engine `reconcile_intent`, `_record_fills`, `_resync_terminal_fills`; `exchange_orders`, `order_fills`, `reconciliation_results` | Acknowledgement != fill; terminal fill-sync retries; not-found remains uncertain; recovery tests |
| Entry accounting | manager `_reconcile_entry_fills` | Gross fills and average computed; base-fee defect below |
| OCO construction | manager `_settle_entry_state`, `_place_oco_exits`, `_ensure_oco_leg`, `_submit_oco_order`; `trades`, `trade_orders` | Two portions, each TP and common SL; entry must be terminal and fill-synced. No independently persisted child-branch model |
| Exit/remainder | manager `_reconcile_single_order`, `_refresh_sold_fields`, `_remaining_base`, `_finalize_trade` | Monotonic sold totals in ordinary query path; canceled partial quantities retained; missing/exhausted protection does not prove position closure |
| Recovery | `service.py:_reconciliation_worker`; manager `recover_on_startup`, `reconcile_all_open`, `_reconcile_trade` | Important: lifecycle reconciliation can PLACE pending/rejected exits and retry requested cancels; it is not a pure observation API |
| Notifications/audit | `trading/store.py`, `trading/notify.py`; durable outbox unique `(event_key,role)` | Independent delivery; global lifecycle keys suppress legitimate events (finding below) |
| Shutdown/restart | `service.py:_reap_background_tasks`, `_shutdown_tasks`; `main.py`, `locking.py` | SIGTERM/INT, flock, durable claims; restart tests do not simulate every crash instruction boundary or production process topology |

Actual interfaces: Spot v3 `/api/v3/exchangeInfo`, `/ticker/price`, `/account`,
`GET /order`, `/myTrades`; testonly `POST /order/test`. Live writes use CLI
operations including `spot.trade.place-oco` (documented repository schema path
`/api/cli/spot/v4/order/place/oco`). Hybrid single-order query actually delegates
to REST, contrary to documentation sections that say CLI queries are used.

## C. Ranked findings

### F1 — Gross entry fills treated as sellable net base

**Confirmed defect; high severity; high confidence (conditional on base fees).**
`trade_manager.py:_reconcile_entry_fills` sums `quantity` into
`acquired_base_qty` without subtracting fees charged in the base asset.
`_place_oco_exits` allocates this gross quantity. Synthetic disposable-DB
experiment: 0.1 BTC execution, 0.001 BTC fee => stored acquired **0.1**, net
ownership **0.099**. This can reject protection for insufficient free balance
or consume unrelated holdings if the wallet has the same asset.

Proposed separate fix: net asset ledger from confirmed fills, preserve gross
executed quantity separately; classify missing fee evidence as incomplete.
Test base/quote/third-token fees, multiple executions, cancel-with-partial-fill,
legacy same-asset balances and rounding. Requires compatibility/backfill design
for existing gross rows; do not automatically resize existing orders or adopt
legacy holdings. This implements actual-fill ownership, not a new trading rule.
**Not implemented here:** historical accounting migration needs focused review.

### F2 — Lifecycle notifications deduplicate across different trades

**Confirmed defect; high severity; high confidence.**
`TradingNotifier.notify_trade` and `notify_intent` pass bare keys such as
`TRADE_CLOSED`, `STATE_TRANSITION`, `TP_REJECTED`; store
`enqueue_notification` deduplicates globally by `(event_key,role)` (migration
16). Disposable-DB experiment: notify two different synthetic signals with
`TRADE_CLOSED` => **one row**, not two. Repeated transitions/errors in the same
trade also collide. Owner may miss later unprotected positions or closure.

Proposed separate fix: scope lifecycle keys by trade/intent and durable
transition identity; unchanged polling must remain deduplicated. Test two
trades, repeated distinct transitions, retry/restart, per-role delivery and
migration-16 compatibility. Preserve old outbox rows; no trading-policy effect.
**Not implemented:** event-identity design must handle legitimate repeated
errors without a polling notification storm.

### F3 — Unknown exit state reported as confirmed protection (fixed)

**Confirmed defect; high severity; high confidence.**
`TradeManager._ensure_oco_leg` counted `submitting`/`cancel_requested` as
confirmed and any nonterminal row with an exchange ID as confirmed, including
`unknown_requires_reconciliation`. `_reconcile_trade` repeated this rule and
could leave `oco_active` after a failed query.

Evidence: new parametrized test (three uncertain states × presence/absence of
ID) fails in **five of six** cases before fix; all six pass afterward. Added
restart/reconciliation regression with query failure and retained old ID.

Fix: uncertain states remain uncertain regardless of old ID; only
open/partially-filled/filled evidence counts toward confirmation. Uncertainty
wins over other active rows in that target's classification. With one healthy
sibling, aggregate state becomes `oco_partial`; no placement/cancel/retry is
added. Side effect: more conservative status/alerts. No schema migration.
Rollback is code-only but restores misleading status; no external orders change.

### F4 — CLI OCO group/child accounting contract unresolved

**Strongly supported improvement / unverified integration risk; potentially
critical impact; medium confidence in the failure scenario.**
`submit_oco_sell` reduces response to one order ID/status. `trade_orders` stores
one row per allocation, not separate OCO group, TP child and SL child.
`HybridMexcClient.query_order` uses v3 REST. `_map_order_state` treats CANCELED
as terminal and `_finalize_trade` checks remaining quantity, but there is no
explicit child-fill/sibling-cancel evidence model. Current fakes model each OCO
as one fillable order, so they cannot prove triggered-child reconciliation.

Do **not** claim REST cannot see these orders: that has not been established.
Obtain current official CLI schema and redacted read-only parent/child examples
from an already-existing OCO, then deterministic contract fixtures covering TP
fill, SL trigger, sibling cancel, partial execution, reordered observations,
not-found and restart. Persist independent identities/outcomes if contract
requires it. Migration likely; no automatic order adoption/replacement. This
preserves, rather than replaces, the approved two-OCO design.

### F5 — Third-token fees assigned an unrelated asset price

**Confirmed defect; medium severity; high confidence.**
`TradingStore._fees_for_order` multiplies every non-USDT fee by the traded
pair's fill price. Synthetic BTCUSDT price 100 with MX fee 0.1 => reported
fee_quote **10 USDT**, with no MX conversion evidence. That is not a valid
quote valuation. Manager `fees_paid` also adds raw amounts across currencies.

Proposed separate fix: per-asset fee buckets; base fee × execution price,
quote fee at face value, third-token fee explicitly unresolved until a
provenance-bearing conversion exists. Test mixed currencies and unknown asset.
Old ledger values need interpretation/versioning; no new price feed or strategy
rule should be silently introduced. Not implemented in certainty-only PR.

### F6 — Recovery name/comments imply read-only behavior, but actions exist

**Confirmed behavioral/documentation discrepancy; high operational severity;
high confidence.** `service.py:_reconciliation_worker` says uncertain orders
are queried only, yet manager recovery can call `_place_oco_exits` and
`_reconcile_trade_orders` can retry a cancellation after observing open status.
These may be intended lifecycle actions, not necessarily incorrect trading
policy. Operator diagnostic use must not assume observation-only semantics.

Separate fact refresh from actuation, or clearly label action-capable entry
points and authorize them. Tests should assert zero writes for diagnostic-only
calls. Requires policy/operations review, not an unsolicited change to recovery
or stop-loss behavior. No production command was used in this audit.

### F7 — Offline suite has an unisolated logout test

**Confirmed test defect; low severity; high confidence.**
`tests/test_auth_logout.py:test_logout_refused_while_service_running` supplies a
fixture config but does not patch `_load_or_exit`; it reads ambient repository
config instead. Credential-free baseline and final runs both fail before the
asserted running-service guard. Proposed test-only fix: inject fixture config,
assert no login/logout/network calls. No production behavior change required.
Not bundled into OCO fix. Python 3.12 confirmation still required.

## D. Trading and execution correctness

- Actual default is 3 USDT (`config.py:TradingConfig`); default mode sim.
  Aggregate 50 USDT is a configurable reference; engine explicitly says spend
  tracking is informational. No cap or new strategy filter was introduced.
- BUY/LONG entries only; SHORT/Telegram SELL rejected by risk. Exits are bot
  SELLs. Signal leverage/deposit percentages do not size orders.
- Entry E is a limit-price ceiling. With conditional entry enabled, below-E
  reference creates trigger E minus configured ticks; observation >= trigger
  activates LIMIT BUY at E. A gap above E does not chase with market execution;
  the limit may remain unfilled. No audited universal entry expiry guarantee.
- A sampled comparison cannot establish an intra-poll crossing. Stale local
  ticks are ignored, but delayed live messages have no age filter by approved
  policy. Adding age/slippage thresholds would need explicit approval.
- Decimal floor sizing prevents requested quote overspend from round-up.
  Splitting a small entry creates two smaller exits: entry minimum compliance
  does not guarantee both OCO portions meet minimums. `_validate_sell` reports
  failure, but this is protection risk, not permission to increase stake.
- Native exits use stop-limit with trigger and limit at SL. A gap can leave
  exposure; this is documented accepted policy, not justification for switching
  to market stop-loss. Active entry partial fills can remain unprotected until
  terminal entry/fill synchronization; examine this exposure with owner before
  changing the lifecycle.
- `filled` group accounting is not a verified TP1/TP2/SL outcome classifier.
  Fee defects prevent dependable net position and P&L conclusions.

### OCO observation state table (certainty fix)

| Observed durable state | Protection classification | Permitted response |
|---|---|---|
| pending | never sent | existing controlled submit path only |
| submitting / unknown / cancel_requested | uncertain, even with old ID | query; never infer protection or resend uncertain placement |
| open / partially_filled | confirmed working allocation | monitor actual remaining fills |
| filled with quantity evidence | completed allocation | retain sold evidence; inspect remaining allocation |
| canceled / rejected | not active protection | retain partial sold quantity; operator/recovery policy, not invented fill |
| both allocations terminal, remaining > 0 | not closed | needs_reconciliation |
| both terminal, remaining == 0 | closed | outcome still needs branch/fee evidence |

Current official reference retrieved 2026-10-29:
[MEXC Spot introduction](https://www.mexc.com/api-docs/spot-v3/introduction).
It specifies HMAC-SHA256, millisecond timestamps, default 5000ms window,
`X-MEXC-APIKEY`, HTTP 429 rate limiting and 5xx server errors. The old GitHub
documentation URL redirects there. The attempted account/trade documentation
URL returned 404; this audit therefore **does not certify current endpoint
order types, time-in-force, CLI OCO child semantics, rate weights or
client-ID idempotency guarantees**. Local tests asserting a contract do not
replace authoritative exchange evidence. No code change relies on an assumed
exchange-side duplicate-ID guarantee.

## E. Reliability and recovery

Strengths established by code/tests: intent-before-submit, transactionally
reserved ledger, unique client/intent keys, activation/source guards,
conservative uncertain entry writes, fill synchronization, flock process guards,
Decimal arithmetic, direct-network constraints and separate delivery outbox.

Limits: lifecycle reconciliation is action-capable (F6); in-process locks and
SQLite constraints are not proof against all concurrent external tools;
terminal exit queries do not supply independently modeled branch fills;
notification dedup is too broad (F2). Broad exceptions in legacy exit submit
paths deserve a separate failure-classification audit before declaring every
write fully safe. No comprehensive crash-injection or multi-process exchange
simulation was performed.

## F. Quantitative evidence

No production dataset was read. No win rate, alpha, profitability, spread,
slippage, latency distribution or expected improvement percentage is reported.
Synthetic tests validate state invariants, not execution economics.

Archive timestamps, intent/history timestamps, exchange trade time/price/qty,
quote and fee-asset fields can support descriptive latency/fill/rejection
analysis after validating clock conventions and record completeness.
Acknowledgement latency and first-fill delay need distinct event timestamps;
slippage needs a defined contemporaneous executable reference, not just E;
spread/liquidity need recorded book snapshots. TP/SL attribution needs branch
evidence. Net realized P&L needs F1/F5 repaired and complete exit fills. Budget
spend is neither realized P&L nor current exposure. Offline parser replay is
not a trading backtest and excludes real fill selection/liquidity constraints.

## G. Focused PR and verification

Change files: `trading/trade_manager.py`,
`tests/test_trading_lifecycle_recovery.py`, `docs/trading.md`, this report.
Branch: `arena/8b00f400-tele-mexc`. PR/commit recorded in handoff/Git history.
Reason: F3 only; no other defect silently remediated.

Commands actually run (Python 3.11.2 sandbox; project requires >=3.12):

- `python -m venv .venv`
- `.venv/bin/pip install '.[dev]' --ignore-requires-python` — succeeded;
  temporary tooling workaround, not a supported-runtime certification.
- Baseline `.venv/bin/pytest -q` — 776 passed, 1 failed (F7).
- Pre-fix `.venv/bin/pytest -q tests/test_trading_lifecycle_recovery.py -k uncertain_oco`
  — 5 failed, 2 passed (includes existing matching test), 11 deselected.
- `.venv/bin/pytest -q tests/test_trading_lifecycle_recovery.py tests/test_trading_tp_sl.py`
  — 31 passed, including seven new cases.
- Final `.venv/bin/pytest -q` — 783 passed, same 1 failed (F7).
- `.venv/bin/ruff check .` — passed before/after.
- `.venv/bin/mypy tg_signal_ingest` — passed before/after, 58 files.
- `.venv/bin/ruff format --check` on both changed Python files — fails;
  existing files contain substantial pre-existing formatting differences.
  No broad reformat applied.
- `git diff --check` — passed.
- Separate disposable-DB experiment using existing `make_env`/`setup_trade`
  fakes: update synthetic entry fee to BTC 0.001; reconcile entry facts;
  acquired remains 0.1. Invoke `notify_trade` twice with different signal IDs
  and same TRADE_CLOSED key; one outbox row. Add synthetic MX-fee fill and call
  `_fees_for_order`; fee incorrectly valued at BTC price. Results in F1/F2/F5.

## H. Not implemented

F1/F2/F5 need independent patches, regression matrices and compatibility review.
F4 needs authoritative contract evidence, not speculation. F6 needs an explicit
observation-versus-actuation boundary decision. F7 is independent test isolation.
No strategy experiments, dependency replacement, aggregate cap, auto-replay,
uncertain-placement resubmission or protected-position adoption were added.
Full peripheral-code review and supported Python 3.12 execution remain open.

## I. Production activation considerations — NOT EXECUTED

F3 requires no migration and preserves existing rows/API. Before any separately
approved activation: back up SQLite with the online backup API (do not blindly
copy a live WAL database), capture redacted status, rerun on Python 3.12,
resolve baseline suite isolation, review diff and operator alert expectations.
Confirm existing CLI/REST contract without placing orders. Owner must assess
unprotected positions separately; do not attach exits to legacy holdings.
Deployment/restart/migration requires separate authorization. Reverting code
rolls back certainty classification without external order operations.

## J. Next actions

1. **Correctness:** net base-fee accounting; per-asset fees; scoped lifecycle
   notification identities; obtain OCO parent/child evidence and model outcomes.
2. **Reliability:** observation-only reconciliation interface; crash/failure
   injection for every external write and DB-commit boundary; supported-runtime
   CI; fix ambient-config test isolation.
3. **Optional engineering:** reconcile contradictory documentation, version/pin
   proven CLI contracts, least-privilege service-user review (units currently
   specify root), retention/capacity planning. No permissions changed here.
4. **Strategy experiments (approval required):** expiry, delayed-signal limits,
   spread/liquidity/slippage guards or alternative stops. First collect
   provenance-bearing execution data and evaluate time-aware, fee-adjusted
   outcomes; none is justified as a profitability improvement by this audit.
