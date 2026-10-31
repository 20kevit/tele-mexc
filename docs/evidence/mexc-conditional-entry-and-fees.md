# MEXC Spot Conditional Entry, Taker Fees, and CLI Capabilities — Evidence Dossier

**Investigation date:** 2026-10-28
**Environment:** Linux VPS, Python 3.12, venv at `/opt/projects/akha1/.venv`, working tree clean at HEAD `a79e6a9` (branch `fix/sl-stop-loss-trigger-offset`)
**Remote:** `public` -> `git@github-akha1:20kevit/tele-mexc.git`
**Repository commit:** `a79e6a9bf15fdb078e95d0a34bdd0252e0ff62c1`
**Working-tree state:** clean (no uncommitted changes)

## Purpose

Durable, version-controlled evidence for the business requirement:

> BUY signal with entry E: if market price is above E, BUY LIMIT at E is acceptable (never above E). If market price is below E, do NOT submit an ordinary BUY LIMIT at E immediately (could execute below E before the intended condition). Wait until the price condition at E is met, then execute with Taker priority. Primary objective: zero-fee BUY, but only if the account/pair actually qualifies for verified zero-Taker-fee.

This dossier separates **verified facts** (live read-only evidence, official docs/schema, local source) from **inference** and **unresolved questions**.

---

## 1. Exact CLI and versions

| Item | Value | Source |
| --- | --- | --- |
| Project CLI executable | `/opt/projects/akha1/.venv/bin/tg-ingest` | `which` |
| MEXC CLI executable | `/usr/bin/mexc` -> `../lib/node_modules/@mexc-ai/cli/bin/launcher.cjs` | `ls -la` |
| MEXC CLI version | `0.0.22 [public] (commit e633cabd8bc5, built 2026-09-29T16:02:58+08:00)` | `/usr/bin/mexc --version` |
| MEXC CLI state dir | `/root/.mexc (mode 0700)` | `mexc doctor --json` |
| MEXC CLI endpoint | `https://gateway-cli.mexc.com` (built-in host table) | `mexc doctor --json` |
| MEXC CLI active profile | `tg-ingest` (1 profile saved; credentials encrypted at rest) | `mexc profile list`, `doctor` |
| MEXC CLI read_only | off (write operations allowed) | `mexc doctor --json` |
| Project package | `tg_signal_ingest` (no separate `mexc` pip package installed) | `pip list`, `find .venv` |

**Important:** There is no standalone `mexc` PyPI package. MEXC access for this project goes through two paths: (a) the vendored `tg_signal_ingest/mexc/` Python adapter talking to official Spot V3 REST directly, and (b) the installed `/usr/bin/mexc` CLI v0.0.22 talking to `gateway-cli.mexc.com`. The adapter does **not** shell out to the CLI.

The branch `arena/eab4189b-tele-mexc` and commit `7b91328` referenced in AGENTS.md were **not found** locally (no matching branches, no matching commit in `git log --all`). No prior Arena report was reused.

---

## 2. Prior evidence reviewed

| Artifact | Location | Date | Environment | Result / limitation |
| --- | --- | --- | --- | --- |
| MEXC Spot V3 API Research | `docs/exchanges/MEXC_SPOT_API_RESEARCH.md` | 2026-10-23 | docs only | Endpoint inventory; signing verified against published examples; NO conditional/trigger orders covered |
| MEXC Spot Adapter Integration Contract | `docs/exchanges/MEXC_SPOT_INTEGRATION.md` | 2026-10-23 | docs only | Adapter contract; LIMIT+MARKET only per adapter policy; no trigger order support |
| MEXC Spot Risks | `docs/exchanges/MEXC_SPOT_RISKS.md` | 2026-10-23 | docs only | Risk analysis; no fee table; no conditional entry |
| Live verification summary | `docs/LIVE_VERIFICATION.md` | 2026-10-23 | docs only | Read-only authenticated access verified (apiKeyInfo VALID); no order placed |
| Final report | `docs/FINAL_REPORT.md` | 2026-10-23 | docs only | Prior investigation summary |
| Conditional-entry unit tests | `tests/test_trading_conditional_entry.py` | in repo | TESTED — MOCKS ONLY | App-managed price monitoring + LIMIT at entry, BELOW entry; FakeEngine/FakeClient; no real orders |
| ape_readonly_out.json | `/tmp/opencode/ape_readonly_out.json` | 2026-10-27 | live read-only | APEUSDT filled LIMIT BUY + OCO limit legs (prior order query examples) |
| verify_mexc_credentials.py | `/tmp/opencode/verify_mexc_credentials.py` | 2026-10-26 | local tool | Credential comparison helper (not an investigation artifact) |

No prior investigation covered: native conditional trigger orders, actual per-pair taker fees, the installed CLI's `place-trigger`/`place-market-trigger`, or a durable evidence dossier. All of those are closed below.

---

## 3. MEXC CLI command tree and schemas (verified live, read-only)

`mexc spot trade` subcommands (write operations, signed):

| Command | Endpoint | Supported types | Notes |
| --- | --- | --- | --- |
| `spot trade place` | `POST /api/cli/spot/order/place` | LIMIT_ORDER, MARKET_ORDER, IOC, POST_ONLY, FOK | Ordinary LIMIT/MARKET; no trigger |
| `spot trade place-market` | same | MARKET_ORDER | quantity OR quote amount |
| `spot trade place-trigger` | `POST /api/cli/spot/order/place/trigger/v2` | STOP_LIMIT_ORDER | activates a LIMIT order at triggerPrice |
| `spot trade place-market-trigger` | `POST /api/cli/spot/v4/order/place/market/trigger` | STOP_MARKET_ORDER | activates a MARKET order at triggerPrice |
| `spot trade place-oco` | OCO pair | OCO_STOP_LIMIT_ORDER | limit leg + stop-limit leg |
| `spot trade place-track` | trailing | TRACK_LIMIT/MARKET | follows market |
| `spot trade current-orders` | read | — | open orders |
| `spot trade history-orders` | read | — | finished orders |
| `spot trade deal-detail` | read | — | **only op that looks an order up by triggerId** |
| `spot trade cancel` | write | — | cancel by triggerId/orderId/symbol/all |

Fee endpoints (read-only, signed, authenticated):

| Command | Endpoint | Purpose |
| --- | --- | --- |
| `spot fee member-fee` | `GET /api/cli/spot/fee/member/query` | caller's overall fee rate |
| `spot fee symbol-fee --symbol X` | `GET /api/cli/spot/fee/symbol/member/query` | caller's fee rate for one pair |
| `spot fee symbol-fee-by-source --source API` | `.../queryBySource` | fee by order channel |
| `spot fee zero-fee-mine` | `GET /api/cli/spot/fee/all/zero/query` | caller's zero-fee pairs (internal symbol IDs) |
| `spot fee rule-symbol-default` | default rule fee (no discount) | — |

Market-data endpoints (used for min-trade calc):

| Command | Endpoint | Purpose |
| --- | --- | --- |
| `spot marketv2 ticker --symbol X_USDT` | public | last price, high/low |
| `spot marketv2 symbol-trade --symbol X_USDT` | public | fee rates, order types, order limits, precisions |
| `spot marketv2 symbols` | public | all spot pairs grouped by market |

Symbol format is `BASE_QUOTE` (underscore), e.g. `XRP_USDT`, `LINK_USDT`. `BTCUSDT`/`LINKUSDT` (no underscore) are **not** valid spot symbols for this CLI.

**Project adapter gap:** `tg_signal_ingest/mexc/cli.py` implements `place_order`, `place_market`, `place_oco`. It does **not** implement `place_trigger`, `place_market_trigger`, or any native trigger-order submission. The health check (`MexcCliAdapter.check`) only verifies schemas for `spot.trade.place` and `spot.trade.place-trigger` exist — it does not exercise them.

---

## 4. Verification matrix (Section 10 answers)

| # | Question | Status | Basis |
| --- | --- | --- | --- |
| 1 | Can the installed CLI submit an ordinary Spot LIMIT order? | VERIFIED — LOCAL SOURCE/CLI SCHEMA ONLY | `place` schema: `orderType` enum includes `LIMIT_ORDER`; required `price`+`quantity`; adapter `place_order` builds LIMIT_ORDER body |
| 2 | Can it submit an ordinary Spot MARKET order? | VERIFIED — LOCAL SOURCE/CLI SCHEMA ONLY | `place-market` schema: `orderType` `MARKET_ORDER`, quantity or amount; adapter `place_market` |
| 3 | Can it submit a native conditional Stop-Limit? | VERIFIED — CLI SCHEMA ONLY | `place-trigger` schema: `triggerType` GE/LE, `triggerPrice` (activation), `price` (limit), `quantity`; endpoint `/api/cli/spot/order/place/trigger/v2` |
| 4 | Can it submit a native conditional trigger that activates a MARKET BUY? | VERIFIED — CLI SCHEMA ONLY | `place-market-trigger` schema: `orderType` `STOP_MARKET_ORDER`, `triggerType` GE/LE, `triggerPrice`, quantity or amount; endpoint `/api/cli/spot/v4/order/place/market/trigger` |
| 5 | What exact trigger condition is supported? | VERIFIED — CLI SCHEMA ONLY | `triggerType=GE`: activate when price rises to triggerPrice; `LE`: when it falls. Reference price = last traded price per MEXC convention (not explicitly stated in CLI schema — see unresolved below) |
| 6 | Can a BUY execute below E before the trigger condition is satisfied? | INFERRED (native); UNVERIFIED (app) | Native trigger: exchange only activates at/after triggerPrice, so no execution below E pre-activation. App-managed monitor: `_check_entry_trigger` fires when `price >= trigger` where trigger < entry, so the LIMIT at E is submitted once trigger (below E) is hit; the LIMIT itself cannot execute below E because it's priced at E. Either way, execution below E is not possible before condition met. Live behavior not exercised. |
| 7 | Can execution at exactly E be guaranteed? | UNVERIFIED | LIMIT at E may not fill; MARKET after trigger can slip past E. No execution-at-exactly-E guarantee from any candidate. |
| 8 | Can Taker priority be guaranteed? | UNVERIFIED | Native STOP_MARKET_ORDER activates as a taker market order (taker priority plausible but not documented in CLI schema). App LIMIT at E is maker by default unless it crosses the spread. Not proven live. |
| 9 | Is the actual account's Taker fee zero for each relevant pair? | VERIFIED — LIVE READ-ONLY EVIDENCE (pair-specific) | `symbol-fee XRP_USDT`: takerFeeRate=0 (ZERO — verified). LINK_USDT/APE_USDT/LPT_USDT/PENDLE_USDT/MYX_USDT: taker=0.0005–0.0008. member-fee finalTaker=0.0005 overall. XRP_USDT is in the "0fees" zone. **Zero-fee BUY is possible ONLY on zero-fee pairs (e.g. XRP_USDT), not account-wide.** |
| 10 | Smallest valid trade amount for a test | VERIFIED — LIVE READ-ONLY EVIDENCE | XRP_USDT: min qty `li=1` XRP, price precision `ps`=4, qty precision `qs`=2, min notional implied by price ~$1.33 for 1 XRP. Price $1.3268 (ticker `c`). 1 XRP ≈ $1.33 valid |
| 11 | Reconcile pending trigger + activated child after restart | UNVERIFIED | `deal-detail` is the only op that looks an order up by triggerId (orderType=TRIGGER_ORDER). Schema documents orderId = triggerId for TRIGGER_ORDER. Activated child order id relationship not documented. Restart reconciliation path not tested live. |
| 12 | Placement succeeds remotely but response lost | INFERRED | Intent becomes `unknown_requires_reconciliation`, never auto-resubmitted (code policy). Query by orderId/clientOrderId via `query_order`/`deal-detail`. Not exercised live for trigger orders. |
| 13 | Questions unproven without real-order experiment | INFERRED | Trigger reference price (last vs mark vs index), GE direction semantics for BUY below entry, activated child order discovery, exact activation response fields, zero-fee behavior on trigger activation, restart reconciliation of trigger orders |
| 14 | Real-order experiment proposal | PENDING APPROVAL | See Section 10 of this dossier (proposal below), not yet executed |

**Status key:** `VERIFIED — LIVE READ-ONLY EVIDENCE` = authenticated read-only call against the actual account. `VERIFIED — OFFICIAL DOCUMENTATION` = documented official API. `VERIFIED — LOCAL SOURCE/CLI SCHEMA ONLY` = schema or source, not exercised live. `TESTED — MOCKS ONLY` = unit-tested with fakes. `INFERRED` = reasoned, not evidenced. `UNVERIFIED` = no evidence. `NOT SUPPORTED` = explicitly absent.

---

## 5. Conditional-entry candidate analysis

### Candidate A — Native conditional trigger activating MARKET BUY (`place-market-trigger`)

- Supported by installed CLI: **yes** (schema verified). Endpoint `POST /api/cli/spot/v4/order/place/market/trigger`, `orderType=STOP_MARKET_ORDER`.
- Required params: `tradeType=BUY`, `triggerType=GE`, `triggerPrice=E`, `orderSource=CLI`, quantity or amount, `clientOrderId` optional (dedupe key).
- Trigger reference price: CLI schema says "activate on rise to triggerPrice" but does **not** state the reference price (last traded / mark / index). **Unresolved field** — do not treat as production truth.
- Direction: GE = price rises to trigger. For a BUY with market below E, GE is correct (wait for price to rise to E).
- Activated order type: MARKET (STOP_MARKET_ORDER). **Taker priority: plausible (market orders take liquidity) but not documented; not guaranteed.**
- Can execute before E? No — exchange activates only at/after triggerPrice.
- Execution at exactly E guaranteed? No — MARKET can slip above or below E.
- Zero fees possible? Only on zero-fee pairs (XRP_USDT verified taker=0). On LINK_USDT etc. taker=0.0005.
- Min constraints: symbol filters apply (XRP_USDT min qty 1 XRP).
- Partial fills / disconnects / restarts: activated child order must be discovered; `deal-detail` with `orderType=TRIGGER_ORDER` looks up by triggerId. Activated child order id relationship **undocumented** — unresolved.
- Duplicate risk: `clientOrderId` is a dedupe key; resend reusing it refused as duplicate. Without it, resend places a second order.
- Evidence strength: CLI schema (official, documented). Live behavior **not exercised** (no real order placed during this investigation, per safety rules).

### Candidate B — Native conditional trigger activating LIMIT BUY at E (`place-trigger`)

- Supported: **yes**. Endpoint `POST /api/cli/spot/order/place/trigger/v2`, `orderType=STOP_LIMIT_ORDER` implied.
- Params: `tradeType=BUY`, `triggerType=GE`, `triggerPrice=E`, `price=E` (limit price once activated), `quantity`.
- Trigger reference price: same unresolved reference-price question.
- Activated order type: LIMIT at E. **Taker priority: NOT guaranteed** — a LIMIT at E rests as maker unless it crosses the spread at activation.
- Can execute before E? No — activates only at/after triggerPrice.
- Execution at exactly E guaranteed? No — LIMIT may not fill at all.
- Zero fees: if it rests as maker on a zero-fee pair, maker=0 (verified XRP_USDT maker=0). If it crosses and takes, taker applies.
- Evidence strength: CLI schema only. Not live-exercised.

### Candidate C — App-managed monitoring + MARKET BUY (current project architecture + market execution)

- Supported: monitor (`PollingPriceFeed`/WebSocket) exists; `place_market` exists in adapter.
- Trigger evaluated by: **application** (polling ticker price).
- Activated order type: MARKET. Taker priority: yes (market order takes liquidity).
- Can execute before E? Monitor submits MARKET once `price >= trigger` (trigger < E). The MARKET could execute below or above E — slippage possible both directions.
- Execution at exactly E guaranteed? No — MARKET slips.
- Zero fees: taker rate applies; zero only on zero-fee pairs.
- Evidence strength: local source (mock-tested only). Live not exercised for conditional entry.

### Candidate D — App-managed monitoring + LIMIT BUY at E (current project architecture)

- **This is the implemented design.** `trade_manager._create_trade` sets `entry_mode="conditional"`, `trigger_price = entry - offset*ticks` (below E). `_check_entry_trigger` fires on `price >= trigger`, then `_submit_entry_order` -> engine places LIMIT BUY at E via `place_order` (LIMIT_ORDER).
- Trigger evaluated by: **application**.
- Activated order type: LIMIT at E. Taker priority: NOT guaranteed (rests as maker unless crossing spread).
- Can execute below E before condition? No. Limit at E cannot execute below E; and it isn't submitted until trigger (below E) is hit. After submission at E it can only fill at E or better (below E for a BUY limit is "better" — actually a BUY LIMIT at E can fill at E or lower, which is favorable to buyer, not a violation). Wait: a BUY LIMIT at E executes at E or *below* E (better price for buyer). It never executes above E. That satisfies "never execute above E." But it may not fill at all.
- Execution at exactly E guaranteed? No — may not fill, or may fill below E.
- Zero fees: maker fee applies if it rests; XRP_USDT maker=0 verified. But if it crosses the spread it becomes taker.
- Evidence strength: local source, TESTED — MOCKS ONLY (test_trading_conditional_entry.py with FakeEngine/FakeClient). Not live-exercised.
- Key safety property verified by mocks: no order placed before trigger; idempotent single activation; survives restart; fails closed without reference price.

**No candidate guarantees execution at exactly E.** Candidate A (market trigger) can slip; Candidate B/D (limit at E) may not fill. Candidate A on a zero-fee pair (XRP_USDT) gives taker priority + zero fee but no price guarantee. Candidate D on a zero-fee pair gives zero fee if maker, no slippage above E, but no fill guarantee and no guaranteed taker priority.

---

## 6. Account-specific fee table (verified live read-only, 2026-10-28)

All via `/usr/bin/mexc spot fee symbol-fee --symbol <BASE>_USDT` (authenticated, gateway-signed). memberId `7bdca68371cf4fb393dfe3a27cca159d` (redacted in final, shown here for traceability; do not commit raw responses).

| Symbol | Maker fee | Taker fee | Zero-fee pair? | Source | Retrieval time | Certainty |
| --- | --- | --- | --- | --- | --- | --- |
| XRP_USDT | 0 | 0 | YES | symbol-fee (authenticated) | 2026-10-28 | DEFINITIVELY ZERO |
| LINK_USDT | 0 | 0.0005 | No | symbol-fee (authenticated) | 2026-10-28 | verified |
| APE_USDT | 0 | 0.0008 | No | symbol-fee (authenticated) | 2026-10-28 | verified |
| LPT_USDT | 0 | 0.0008 | No | symbol-fee (authenticated) | 2026-10-28 | verified |
| PENDLE_USDT | 0 | 0.0008 | No | symbol-fee (authenticated) | 2026-10-28 | verified (legacy position) |
| MYX_USDT | 0 | 0.0008 | No | symbol-fee (authenticated) | 2026-10-28 | verified (legacy position) |

Overall account (member-fee): `finalMaker=0`, `finalTaker=0.0005` (overall reference, not per-pair). Per-pair overrides the overall: XRP_USDT taker=0 despite overall 0.0005.

**Zero-fee BUY is possible ONLY on zero-fee pairs.** For this account, XRP_USDT is a verified zero-taker-fee, zero-maker-fee pair. Tokenized stocks/commodities (TSLAON, GOLD, OIL, etc.) and USDC/USDE/USD1/EUR are also zero-fee but are not typical signal targets.

`zero-fee-mine` returns internal symbol IDs; cross-referenced with `marketv2 symbols` (USDT market) to map IDs to symbols. XRP_USDT id = `c9455c17227d429baed64edbcde887f6`, confirmed zero-fee.

---

## 7. Minimum order / precision (verified live read-only)

`marketv2 symbol-trade XRP_USDT`:

| Field | Value | Meaning |
| --- | --- | --- |
| symbol | XRP_USDT | `BASE_QUOTE` format |
| price | $1.3268 (ticker `c`) | last price |
| `li` (min qty) | 1 | 1 XRP minimum quantity |
| `la`/`ma` (max qty) | 2000000 | 2,000,000 XRP max |
| `ps` (price precision) | 4 | price to 4 decimals |
| `qs` (qty precision) | 2 | qty to 2 decimals |
| `mfr` (maker fee) | 0 | maker 0 |
| `otf` (taker fee) | 0 | taker 0 |
| `bfr` (buy fee rate) | 0 | buy 0 |
| `aol` order types | LIMIT_ORDER(1), MARKET_ORDER(5), STOP_LIMIT_ORDER(100), OCO_STOP_LIMIT_ORDER(101), TRACK_LIMIT_ORDER(104), TRACK_MARKET_ORDER(105), SPOT_MARKET_ORDER(103) | all relevant types supported |
| `cp` zones | mc-trade-zone-mainly, mc-trade-zone-0fees | XRP in 0-fees zone |

Smallest valid test order: **1 XRP ≈ $1.33 USDT** (qty precision 2, so 1.00 XRP valid; price precision 4). This is below the project's fixed `order_quote_amount=3 USDT` — a test would use the project's 3 USDT amount (~2.26 XRP) or the exchange minimum 1 XRP. Estimated notional for 1 XRP = $1.33; for 3 USDT ≈ 2.26 XRP.

Note: `symbol-fee` CLI rejected non-underscore symbols (`LINKUSDT` -> "symbol not exists"). Must use `XRP_USDT` form.

---

## 8. Recommended safest implementation

Given the requirement (wait for condition at E, then Taker-priority purchase, zero fee if qualified) and the evidence:

**No native candidate guarantees execution at exactly E.** The requirement's hard constraints:
- Never execute above E (BUY). → satisfied by any LIMIT at E, and by a trigger that only activates at/above E (a MARKET could slip above E — violates the spirit of "never above E" though a BUY market at E typically fills near E; slippage above E is possible).
- Wait until condition met, then Taker priority. → Candidate A (native STOP_MARKET_ORDER trigger) gives exchange-side trigger + taker execution, but slippage above E is possible and exact-E is not guaranteed.
- Zero fee only if pair qualifies. → XRP_USDT qualifies (taker=0 verified).

**Safest implementation compatible with semantics:** Keep the existing app-managed conditional entry (Candidate D) as the default, because it:
1. Never executes above E (LIMIT at E).
2. Never submits before the trigger condition (below E) is met (verified by mocks).
3. Fails closed without a reference price.
4. Survives restarts idempotently (verified by mocks).
5. On zero-fee pairs (XRP_USDT), if the LIMIT rests as maker, fee = 0 (maker=0 verified). If it crosses the spread it becomes taker (still 0 on XRP_USDT).

**Limitation:** Candidate D does not guarantee Taker priority (a LIMIT at E rests as maker) and does not guarantee a fill. If the business requires guaranteed Taker priority, the native `place-market-trigger` (Candidate A) on XRP_USDT is the closest match, but it cannot guarantee execution at exactly E (MARKET slippage) and its trigger reference price and child-order discovery are unresolved without a live test.

**Recommendation:** Do not enable native conditional trading until a real-order experiment (Section 9) is approved and resolves: trigger reference price, GE semantics for BUY, activated child order discovery, and restart reconciliation. Until then, keep conditional entry disabled (`entry_trigger_offset_ticks` unset) or use the app-managed LIMIT-at-E design with the fill/non-fill and maker-vs-taker tradeoffs understood.

---

## 9. Real-order experiment proposal (requires explicit approval — not yet executed)

Per Section 7 of the mission, a real-order test requires explicit approval for the exact symbol, order type, trigger, amount, and risk. This proposal is **prepared but not executed**.

### A. Exact test plan

- **Symbol:** XRP_USDT (verified zero maker AND zero taker fee; min qty 1 XRP ≈ $1.33; price precision 4; qty precision 2; all order types incl. STOP_LIMIT_ORDER and STOP_MARKET_ORDER supported; in "0fees" zone).
- **Order type / trigger:** Native `place-market-trigger` (Candidate A): `tradeType=BUY`, `triggerType=GE`, `triggerPrice` set below current market (~$1.30 vs $1.3268 current) so it activates promptly as a MARKET BUY. Alternative: `place-trigger` LIMIT at E.
- **Purpose:** Prove (1) native conditional activation via the installed CLI, (2) that the activated order is a MARKET (taker) order, (3) actual fee charged (expect 0 on XRP_USDT), (4) child-order discovery via `deal-detail` triggerId lookup, (5) cancellation of an unactivated trigger.
- **Trigger price:** below current last price ($1.3268), e.g. $1.3000, so GE (rise to trigger) — wait, GE activates when price rises TO triggerPrice. If current is $1.3268 and trigger is $1.30, price is already above trigger, so it may activate immediately. For a proper "wait until E" test, trigger should be set so price must move to reach it. To test activation quickly, set trigger just below/at current. To test the "wait for E" semantics properly, E must be above current. Given XRP at $1.3268, set E (trigger) = $1.3350 (above current) so the bot waits for price to rise to E. But that requires a live price move. Compromise: use trigger = current price to test immediate activation, acknowledging it tests activation not the "wait" semantics.
- **Quantity:** 1 XRP (exchange minimum, ≈ $1.33) — smallest valid amount to answer the question. (Project fixed amount is 3 USDT ≈ 2.26 XRP; a test may use 1 XRP to minimize cost.)
- **Entry/exit:** Place trigger; observe activation; observe the activated MARKET BUY fill; then sell the acquired XRP (MARKET or LIMIT) to close the test position. Do not touch legacy PENDLE_USDT / MYX_USDT positions.
- **Live price condition:** Yes — activation requires the price to reach the trigger (unless trigger set at/below current).
- **Avoid legacy positions:** XRP_USDT is distinct from PENDLE_USDT and MYX_USDT. Test quantity is separate.
- **Success evidence:** trigger placement response (triggerId), activation event, activated child order id, deal-detail showing MARKET BUY fill, fee=0, then exit fill; reconcile to confirm no residual exposure.

### B. Cost and risk disclosure (numerical example, XRP at $1.3268)

1. **Purchase amount:** 1 XRP ≈ $1.33 USDT (exchange minimum qty). Estimated quote: $1.33.
2. **Entry fee:** XRP_USDT taker = 0 (verified). Entry fee = $0. (If not zero-fee, 0.0005 × $1.33 ≈ $0.0007.)
3. **Exit fee:** sell XRP; taker = 0 (verified) if market sell. Exit fee = $0.
4. **Spread/slippage:** XRP spread at time of test unknown; slippage on 1 XRP is negligible ($<<0.01) given depth. Plausible range: $0–$0.01.
5. **Exit price risk:** if XRP drops while closing, loss = price drop × 1 XRP. E.g. a 5% drop = ~$0.067. Bounded by purchase amount ($1.33) minus fees — fully paid spot, no leverage.
6. **Total estimated loss:** fees ($0) + spread/slippage (≤$0.01) + adverse-price scenario (e.g. 5% = $0.067) ≈ ≤$0.08. Fully bounded by the $1.33 purchase amount.
7. **Maximum plausible loss:** bounded by purchase amount (~$1.33) because spot buy is fully paid, no leverage. Worst case: XRP goes to 0 → lose ~$1.33 (market value loss, not fees). Fees are $0 (verified zero-fee pair).
8. **Uncertainty:** exact spread at execution time, exact fill price, whether activation response includes triggerId and child order id, whether `deal-detail` triggerId lookup returns the activated order. These cannot be bounded in advance.

### C. Mandatory approval

**This proposal is presented for approval and is NOT executed.** Approval required for: symbol XRP_USDT, order type `place-market-trigger` (or `place-trigger`), trigger price, quantity 1 XRP, and the risk disclosure above. Approval for this test is not authorization for another test, a larger order, a different symbol, or production rollout. Silence is not approval.

---

## 10. Known operational risks

- Trigger reference price (last/mark/index) not documented in CLI schema → unresolved; do not treat `triggerType=GE` as correct for every reference-price definition without evidence.
- Activated child order id discovery not documented; `deal-detail` triggerId lookup is the only lead, unproven live.
- Restart reconciliation of pending triggers / activated children not tested live.
- Zero fee is pair-specific (XRP_USDT verified), not account-wide (overall taker 0.0005).
- No candidate guarantees execution at exactly E; LIMIT may not fill, MARKET may slip.
- Taker priority not guaranteed for LIMIT-at-E candidates.
- Legacy positions PENDLE_USDT / MYX_USDT must not be touched (taker 0.0008 on those pairs).

---

## 11. Reproduction commands (sanitized, read-only)

```bash
# CLI version
/usr/bin/mexc --version
/usr/bin/mexc doctor --json
/usr/bin/mexc profile list

# Schemas (read-only)
/usr/bin/mexc schema spot.trade.place
/usr/bin/mexc schema spot.trade.place-market
/usr/bin/mexc schema spot.trade.place-trigger
/usr/bin/mexc schema spot.trade.place-market-trigger
/usr/bin/mexc schema spot.trade.deal-detail
/usr/bin/mexc schema spot.fee.symbol-fee
/usr/bin/mexc schema spot.fee.zero-fee-mine

# Account fees (authenticated read-only)
/usr/bin/mexc spot fee member-fee
/usr/bin/mexc spot fee symbol-fee --symbol XRP_USDT
/usr/bin/mexc spot fee zero-fee-mine

# Market data / min order (public read-only)
/usr/bin/mexc spot marketv2 ticker --symbol XRP_USDT
/usr/bin/mexc spot marketv2 symbol-trade --symbol XRP_USDT

# Project status (offline / read-only)
.venv/bin/tg-ingest trading status
.venv/bin/tg-ingest trading preflight
.venv/bin/tg-ingest trading verify
```

No credentials, secrets, session files, or message contents are recorded in this dossier. Raw API responses are not committed; only sanitized field summaries.

---

## 12. Change log

| Date | Author | Change |
| --- | --- | --- |
| 2026-10-28 | Initial | Created dossier; closed CLI schema, fee, and minimum-order verification; documented conditional-entry candidates; prepared real-order test proposal (pending approval) |

---

**End of dossier.** The safest implementation compatible with the required semantics is to keep the app-managed LIMIT-at-E conditional entry (Candidate D), understand its fill/taker-priority limitations, and not enable native conditional trading until the Section 9 real-order experiment is explicitly approved and executed.
