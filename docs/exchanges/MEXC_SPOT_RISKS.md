# MEXC Spot Integration — Security & Risk Assessment

**Assessment date: 2026-10-23**, against the same official sources listed in
`MEXC_SPOT_API_RESEARCH.md`. Facts are cited as of that date; policies change
without notice and must be re-checked before any live enablement.

Legend: **[CODE] = mitigated in adapter/trading code (offline-tested);
[OWNER] = requires an account-owner decision or confirmation from MEXC.

## 1. API availability is not trading permission

- **[OWNER]** MEXC documents automated trading as an API use case, but also
  reserves the right to suspend or revoke API access "in cases of abuse"
  (clause repeated across API announcements; User Agreement governs). Whether
  this specific signal-driven workflow is acceptable **must be confirmed by
  the account owner / MEXC support before live mode** — no code can confirm it.
- **[OWNER]** API access is per-pair and can be revoked by announcement
  (documented Jan/Mar 2026 spot-pair API suspensions). Before enabling a pair:
  check https://www.mexc.com/announcements/api-updates for the pair.
- **[CODE]** Fail-closed checks implemented: symbol must appear in
  `exchangeInfo` with `isSpotTradingAllowed=true`, status online, SPOT
  permission, and an exchange-published minimum; otherwise the order is
  rejected locally (`SymbolMeta.validate_order`).
- **[OWNER]** Each API key has a self-configured trading-pair restriction in
  the MEXC web UI; a key-level restriction produces permission errors only at
  submission time.

## 2. Account restrictions, freezing and API revocation

- Risk-control codes `10098 risk control system detected abnormal`,
  `730100 Unusual user status`, `60005 your account is abnormal` and
  `70011 Pair user ban trade apikey` can freeze trading at any time.
- **[CODE]** All four are normalized to typed errors and, for orders, reach
  the durable store as `rejected` with the reason — no retry, no signal loss.
- **[OWNER]** Recovery from an account/risk freeze is an account-owner
  process (web UI / support tickets); the adapter has no remedy.
- **[OWNER]** Do not keep a single point of trade: if the account is
  suspended, orders already on the book remain and must be managed manually
  from the web UI.

## 3. API-key security

- **[CODE]** Secrets live only in the private `.env`, never in source, docs,
  logs or Git (`test_gitignore.py` guards leaks; `.env` base permissions 600).
- **[CODE]** Signed parameter strings and the signature are never logged:
  the transport is additively guarded
  (`request()` deletes the signed query before logging; regression test
  `test_logs_never_contain_key_secret_or_signature`).
- **[CODE]** Credential weakness is detected read-only via
  `validate_credentials()` (status VALID/DELETE/FROZEN, expiry, and required
  permissions READ+READ+`SPOT_DEAL_WRITE`), which **never** requests
  withdrawal capability and **never** calls the whitelist-configuration
  endpoint. Recommend explicitly: create the key with ONLY
  `SPOT_ACCOUNT_READ`, `SPOT_DEAL_READ`, `SPOT_DEAL_WRITE`.
- **[CODE]** A 90-day-expiry key without an IP whitelist is dangerous for
  unattended use (silent expiry between renewals).
- **[OWNER]** Bind the key to the server's outbound IP(s) (≤ 10 per key) and
  renew it ≥ 5 days before the expiry window. Rotation procedure: create a new
  key → update `.env` → restart the service → delete the old key. Never reuse
  production secrets in tests (the suite runs fully credential-free).
- **[OWNER]** If a key is ever exposed: delete it immediately (advice in the
  introduction) and inspect the account's order history for unknown orders.

## 4. Technical failure modes

| Risk | Effect | Mitigation |
| --- | --- | --- |
| Clock drift / signature errors (602/700002, 700003/10073) | All signed calls rejected | **[CODE]** recvWindow guard ≤ 60000 ms with a 5 s default; `clock_skew_ms()` health helper; typed errors surface the cause instead of retry loops. **[OWNER]** keep the host clock NTP-synced |
| Rate limit / WAF (429/418/403, ban 2 min–3 days) | Trading halted | **[CODE]** documented endpoint weights, process-shared ledgers (12 req/s order bucket, ≤ 60 IP weight/10 s, ≤ 100 UID weight/10 s), one batch ticker request per interval, `Retry-After` honoured, 429/418/403 never retried, no fighting the WAF. **[OWNER]** never run another bot from the same IP |
| Network failure / 5XX / malformed response at submit | Order MAY exist with unknown state | **[CODE]** intent becomes `unknown_requires_reconciliation`, never auto-resubmitted; reconciliation queries by `orderId` or client order id |
| Duplicate orders (retry, Telegram edits) | Double spend | **[CODE]** intents persisted in SQLite before submission with the Telegram-message-derived idempotency key; edits cannot create a second order. Limitation: exchanged-side dedup of `newClientOrderId` is NOT documented by MEXC — protection is local + reconciled, not exchange-side |
| Stale price / slippage / spread | Worse-than-expected fill | **[CODE]** signal entry semantics are preserved (a limit entry stays a limit); exchange-published price filters and precision are validated; optional `PERCENT_PRICE_BY_SIDE` guard `validate_price_guards()` fires only when the exchange publishes the filter |
| Partial fills / cancel races (-2011, PARTIALLY_CANCELED) | Wrong bookkeeping | **[CODE]** fills are re-read from `myTrades` when the terminal state includes fills; `-2011` never flips unknown state to terminal automatically |
| Replayed/historical signals | Unintended orders | **[CODE]** persisted forward-only activation boundary plus `source='backfill'/'recovery'` guards reject history; later-cancel precedence and delete-message cancels also fail closed (trading engine suite). There is intentionally no age-based rejection |
| Insufficient balance / locked funds (10101/30004/30005) | Rejected submission | **[CODE]** balance preflight (`trading/balance.py`) with snapshot age limits + fee buffer + outstanding-order accounting, locked (frozen) funds excluded |
| Minimum notional / precision / unsupported pairs | Local rejection before network | **[CODE]** live dynamic `exchangeInfo` lookup per symbol; exchange-published minimums (`baseSizePrecision`, `quoteAmountPrecision(Market)`), precision and order-type support; unknown/paused/not-SPOT symbols fail closed |
| Reconciliation failures / unexpected open orders | Undetected exposure | **[CODE]** reconciliation results are durable rows (`reconciliations` table); non-terminal `unknown` intents stay in the reconcile set indefinitely and never silently vanish. **[CODE]** `open_orders()` exists for operator audit; **[OWNER]** operator should run `trading reconcile` after outages |
| Exchange outage | Submissions fail | **[CODE]** typed server errors; operator-visible ambiguity; health checks never place orders |
| Unexpected extra privileges | Accidental withdrawal/transfer | **[CODE]** adapter implements no wallet endpoints at all; `validate_credentials()` refuses keys lacking SPOT_DEAL_WRITE and warns on suspicious extras only at enablement time. **[OWNER]** never enable withdrawal permission on the trading key |

## 5. Signal-pipeline risks (adapter boundary)

- **[CODE]** The exchange receives **normalized order intents**, never
  Telegram text; parsing ambiguity produces `needs_review`/rejected signals,
  not orders (`trading/parser.py`).
- **[CODE]** Automatic execution: new eligible signals move directly through
  intent planning and submission. The activation boundary, emergency stop,
  fixed owner amount, dynamic exchange rules and fresh balance preflight are
  re-validated immediately before the call. There is no per-signal approval
  gate.
- **[CODE]** Durable binding: `signal_id → intent → exchange_order` rows with
  audit events; a submission failure is written as a rejected/ambiguous intent
  so the signal is visibly resolved, never lost.
- **[OWNER]** Interpreting a signal wrong (targets vs entry, pair variants
  like BTC/USD1 vs BTCUSDT) is a business risk beyond code validation.

## 6. Operational controls (status)

| Control | State |
| --- | --- |
| Execution disabled by default | Yes — `trading.mode = "sim"` (default), no client constructed (`test_sim_mode_builds_no_network_client_and_rejects_live_submission`) |
| Single explicit enable gate | Yes — `mode` + `live_enabled_token = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"`; config refuses to boot in live mode otherwise |
| Kill switch | Yes — emergency stop at evaluation and pre-submission time; operator cancel tooling remains available, but the bot never cancels or adopts pre-existing external orders automatically |
| Order size | One explicit owner-approved fixed `order_quote_amount`; no software default, no signal-derived size, and no arbitrary daily/open-order caps |
| Per-order exchange-rule validation | Yes — registry TTL cache over exchangeInfo + local fail-closed checks |
| Partial fills / cancel races | Yes — reconciliation + fill sync (durable `fill_sync_required` flag) |
| Durable intents & reconciliation | Yes — SQLite (TradingStore) with migrations; idempotency key on Telegram identity |
| Health checks without orders | Yes — ping/time/account/keyInfo are read-only; no write call exists outside the order/cancel paths |
| Log hygiene | Yes — typed errors carry no secrets; signed strings deleted before logging; test-enforced |
| Dry-run vs live distinction | Yes — `sim` never builds a client; `testonly` uses only the official `/order/test` (validation, not a sandbox and not an order); `live` requires the token |

## 7. Recovery procedures (owner runbook)

1. **Kill switch**: set `trading.emergency_stop = true` (or stop the service:
   `systemctl stop tg-ingest`). Both instantly prevent NEW orders.
2. **Cancel exposure**: `DELETE /api/v3/order/all` via the operator CLI path,
   or manually in the web UI; then run `tg-ingest trading reconcile` and
   review `intents`/`reconciliations` tables.
3. **Ambiguous orders**: any intent in `unknown_requires_reconciliation` is
   re-queried (by `orderId`, else `origClientOrderId`) — resolve manually; the
   system never resubmits by itself.
4. **Credential compromise**: delete the key on MEXC, rotate `.env`, audit
   order history, restart.
5. **Ban (418/429 escalation)**: stop all requests (the adapter already
   refuses to send while blocked), do not restart into the ban window, and
   move to the fallback IP only after reading `Retry-After`.

## 8. Requirements before enabling live mode (checklist)

- [ ] **[OWNER]** Confirm automated-trading use is acceptable for the account
      (MEXC terms + support confirmation) — required, outside code scope.
- [ ] **[OWNER]** Confirm the target pairs still allow API trading
      (announcements + `exchangeInfo`) at enablement time.
- [ ] **[OWNER]** Create the key with READ + SPOT_DEAL_WRITE only, IP-bound,
      renewal scheduled.
- [ ] **[OWNER]** Set realistic exposure limits in `[trading]` config.
- [ ] **[CODE]** Run `validate_credentials()` once credentials exist
      (read-only; safe).
- [ ] **[CODE]** `testonly` mode first (official order/test), watching
      signature/timestamp/clock behaviour on the real host clock.
- [ ] **[CODE]** Then, and only then, `mode = "live"` with the
      acknowledgement token.
- No step above may be skipped; the code fails closed on every missing gate,
  but policy confirmation (first two bullets) is not code-verifiable.
