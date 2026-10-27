# MEXC Spot V3 API Research

**Sources checked: 2026-10-23 (all links verified live on this date).**

This document inventories the official MEXC Spot V3 REST API and maps each
endpoint to the adapter implementation in `tg_signal_ingest/mexc/`. Nothing in
this document or the adapter assumes that API availability implies permission:
see "Technically supported vs. currently permitted" below, and
`MEXC_SPOT_RISKS.md` for the full risk analysis.

## 1. Summary of findings

| Topic | Finding |
| --- | --- |
| Base URL | `https://api.mexc.com` (single official REST base URL) |
| API version | Spot V3 (`/api/v3/...`); no conventional sandbox exists — `POST /api/v3/order/test` is validation-only against the LIVE matching-engine configuration, not a sandbox |
| Signing | HMAC-SHA256 of `totalParams = queryString + requestBody` concatenated with no separator; `signature` excluded; lowercase hex; official published examples reproduced in `tests/test_mexc_official_contract.py` |
| Header | `X-MEXC-APIKEY: <access key>`; optional `Content-Type: application/json` (body must still be raw `key=value&...`, never a JSON object; `x-www-form-urlencoded` is rejected with 700013) |
| Timestamp | Milliseconds, required on all signed endpoints; server accepts when `timestamp < serverTime + 1000 && serverTime - timestamp <= recvWindow` |
| recvWindow | Optional, default 5000 ms, documented maximum 60000 ms (see ambiguity A1) |
| Rate limits | Independent buckets: IP-weighted endpoints share 300 weight / 10 s; UID-weighted share 500 / 10 s. Order placement + cancellation share their own UID bucket of 12 requests/s. HTTP 429/418 carry `Retry-After`; continued excess ⇒ IP ban 2 min–3 days |
| Error model | HTTP 4XX request errors, 401 auth/permission, 403 WAF, 429 rate limit, 5XX server; JSON body `{"code", "msg"}`. Codes are documented on the Error Code page and normalized by `mexc/errors.py` |
| Keys | ≤ 30 keys/account; a key WITHOUT an IP whitelist expires after 90 days; whitelist binds ≤ 10 IPs (ambiguity A2); per-key trading-pair restriction configurable in the MEXC web UI |
| Permissions | Endpoint-granular: `SPOT_ACCOUNT_READ`, `SPOT_ACCOUNT_WRITE`, `SPOT_DEAL_READ`, `SPOT_DEAL_WRITE`, `SPOT_TRANSFER_*`, `CONTRACT_*`. Least-privilege for this adapter: READ + `SPOT_DEAL_WRITE` only; never withdraw permissions |
| WebSocket | `wss://wbs-api.mexc.com/ws`; max connection 24 h; ≤ 30 subscriptions/connection; server disconnects after 30 s without a valid subscription and after 60 s without traffic; requires Pong replies. Not implemented (see §4) |
| SDK | Official SDKs (Python, Java, Go, .NET, JS) at `github.com/mexcdevelop/mexc-api-demo`. Decision: NOT used — stdlib urllib transport keeps the vendored dependency surface at zero, the SDK would add a third-party dep plus no network-timeout/idempotency semantics this project needs (see §5) |

## 2. Endpoint inventory

Status legend: **implemented** = typed client method exists and is covered by
offline tests; **documented-not-implemented** = exists in official docs, no
adapter method, no plan, listed for completeness.

### Public market data (no key required)

| Method | Path | Weight | Purpose | Implementation |
| --- | --- | --- | --- | --- |
| GET | `/api/v3/ping` | n/d | Connectivity probe | `MexcSpotClient.ping()` — implemented |
| GET | `/api/v3/time` | n/d | Server time / clock-skew source | `server_time_ms()`, `clock_skew_ms()` — implemented |
| GET | `/api/v3/exchangeInfo` | 25 | Trading rules, symbol status, precisions, min/max amounts, order types, `PERCENT_PRICE_BY_SIDE` filters | `exchange_info()` + `SymbolRegistry` + `SymbolMeta` — implemented |
| GET | `/api/v3/depth` | 3 (max 5000 levels, default 100) | Order-book snapshot | `depth()` + typed `markets.parse_depth()` — implemented |
| GET | `/api/v3/ticker/price` | 10 | Latest price (1 symbol or all) | `ticker_price()` — implemented |
| GET | `/api/v3/trades` | 5 | Recent trades (default 500, max 1000) | Documented-not-implemented (core has no use) |
| GET | `/api/v3/klines` | 1 | Candlesticks (max 500) | Documented-not-implemented (core has no use) |
| GET | `/api/v3/avgPrice`, `/api/v3/ticker/24hr`, `/api/v3/ticker/bookTicker` | n/d | Average price / stats / book ticker | Documented-not-implemented |
| GET | `/api/v3/defaultSymbols` | n/d | Pairs with API access (FAQ) | Documented-not-implemented (allowlist mode covers this) |
| GET | `/api/v3/announcements` | 5 req / 2 s | API announcements (change log 2026-06-16) | Documented-not-implemented |

`exchangeInfo` symbol fields used by the registry (all verified against the
official response sample): `status` (1 online / 2 paused / 3 offline),
`baseAsset`, `baseAssetPrecision`, `quoteAssetPrecision`, `baseSizePrecision`
(minimum order quantity), `quoteAmountPrecision` (minimum notional — note the
odd field name, ambiguity A4), `quoteAmountPrecisionMarket`,
`maxQuoteAmount`, `maxQuoteAmountMarket`, `orderTypes`, `isSpotTradingAllowed`,
`permissions`, `tradeSideType`, and the `filters[]` array with
`PERCENT_PRICE_BY_SIDE` (`bidMultiplierUp`, `askMultiplierDown`).

### Signed reads

| Method | Path | Permission | Weight | Purpose | Implementation |
| --- | --- | --- | --- | --- | --- |
| GET | `/api/v3/account` | SPOT_ACCOUNT_READ | 10 | Balances + `canTrade`/`canWithdraw`/`permissions` | `account()` — implemented |
| GET | `/api/v3/apiKeyInfo` | SPOT_ACCOUNT_READ | 1 | Key status (VALID/DELETE/FROZEN), permissions, expiry, IP whitelist | `api_key_info()` + typed `credentials.validate_credentials()` — implemented (docs added to change log 2026-08-24) |
| GET | `/api/v3/openOrders` | SPOT_DEAL_READ | 3 | Open orders; ≤ 5 comma-separated symbols, ≤ 1000 rows | `open_orders()` — implemented |
| GET | `/api/v3/order` | SPOT_DEAL_READ | 2 | Single order status (7-day window) by `orderId`/`origClientOrderId` | `query_order()` — implemented |
| GET | `/api/v3/allOrders` | SPOT_DEAL_READ | 10 | Order history (default last 24 h, ≤ 7 days, limit ≤ 1000) | Documented-not-implemented (reconcile uses `/order`) |
| GET | `/api/v3/myTrades` | SPOT_ACCOUNT_READ | 10 | Fills for a symbol/order (default 10, max 1000; 1-month window) | `my_trades()` — implemented |
| GET | `/api/v3/uid` | SPOT_ACCOUNT_READ | 1 | Account UID | Documented-not-implemented |

### Signed writes

| Method | Path | Permission | Limit | Purpose | Implementation |
| --- | --- | --- | --- | --- | --- |
| POST | `/api/v3/order` | SPOT_DEAL_WRITE | 12 req/s shared with cancel (UID) | Place LIMIT/MARKET/IOC/FOK/LIMIT_MAKER order; `newClientOrderId` idempotency handle; optional `stpMode` | `place_order()` — implemented; local validation in `SymbolMeta.validate_order()`; LIMIT + MARKET only (adapter policy) |
| POST | `/api/v3/order/test` | (as /order) | 1 | Validation without matching engine (response `{}`). NOT a sandbox and NOT an order | `place_test_order()` — implemented, used by `testonly` mode only |
| DELETE | `/api/v3/order` | SPOT_DEAL_WRITE | 12 req/s shared | Cancel one order by `orderId`/`origClientOrderId` | `cancel_order()` — implemented |
| DELETE | `/api/v3/openOrders` | SPOT_DEAL_WRITE | 12 req/s shared | Cancel all open orders for ONE symbol (incl. OCO); `symbol` required | `cancel_all_orders(symbol=...)` — implemented (fixed: was called without a symbol) |
| DELETE | `/api/v3/order/all` | SPOT_DEAL_WRITE | 12 req/s shared | Cancel ALL orders account-wide (added 2026-05-22) | `cancel_all_account_orders()` — implemented; kill-switch only |

### Deliberately NOT implemented (and why)

- `POST /api/v3/apiKeyInfo` (configures the key's IP whitelist; needs
  SPOT_ACCOUNT_WRITE) — least privilege: the adapter must never be able to
  modify its own key.
- Batch orders `POST /api/v3/batchOrders` (20 orders/req, 2 req/s) — the core
  processes one intent at a time by design.
- Wallet endpoints (withdraw/deposit/transfer), sub-account endpoints, STP
  strategy-group CRUD, MX-deduct toggles, rebate, dust, futures — out of scope,
  and most conflict with the no-withdrawals guarantees in the README.
- WebSocket market streams and user-data streams — the existing core is
  pull-based (asyncio + `asyncio.to_thread` REST) and the scale (single
  channel, few intents/hour) never justifies a socket loop with keep-alive and
  protobuf decoding. Reconciliation already polls `GET /api/v3/order` and
  `GET /api/v3/myTrades`. Revisit only if sub-second fill latency becomes a
  requirement.

## 3. Signature generation (verified against published examples)

Synthetic offline vector (never a live or published credential), apiKey
`<test-api-key>`, secret `<test-secret>`:

| Example | totalParams | Published digest |
| --- | --- | --- |
| 1 & 2 (all params in query OR body) | `symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=11&recvWindow=5000&timestamp=1644489390087` | `0ac3f412a5be580b1ff879fd4194a303bcf0f5d0fabcd07a681e31e42f1563ec` |
| 3 (mixed: query + body) | `symbol=BTCUSDT&side=BUY&type=LIMIT` + `quantity=1&price=11&recvWindow=5000&timestamp=1644489390087` (no `&` between parts) | `1c2f9ed04448f8e704b73ee00cb6ec1ce1bae45c58e2761583f8402ac8322c7a` |

Both digests are reproduced byte-for-byte by `MexcSpotClient._sign()` in
`tests/test_mexc_official_contract.py`. The parameter shapes follow the official
documentation; the key, secret, and digests in this repository are synthetic so
no credential is published. The adapter canonicalizes by sorting
parameters before signing (allowed: the docs specify any order, as long as the
signed order equals the transmitted order) and encodes special characters
(commas etc.) with uppercase percent-encoding during signing.

## 4. Retry / ambiguity policy (implemented behavior)

- **Never retry** on: 429/418/403 (WAF/rate family; the client instead blocks
  new requests for the advertised `Retry-After` — defaulting to 60 s for 403
  which has none), any typed 4XX rejection (`MexcAuthError`,
  `MexcPermissionError`, `MexcSymbolError`, `MexcPrecisionError`,
  `MexcInsufficientBalance`, `MexcOrderRejected`, `MexcOrderNotFound`),
  signature errors (602/700002) or timestamp errors (700003/10073).
- **Ambiguous outcomes** (network failure, 5XX, malformed response): for order
  submission the trading engine moves the durable intent to
  `unknown_requires_reconciliation` and re-queries by client order id — it
  never blindly resubmits (`trading/engine.py`).
- **Idempotency**: `newClientOrderId` is derived from the Telegram
  `(channel, message)` identity, so a re-send of the same intent becomes a
  duplicate-key rejection locally *and* is reconcilable on the exchange side.
  Limitation: MEXC documents no server-side idempotency window for
  `newClientOrderId` — the guarantee is reconciliation, not deduplication at
  the exchange (documented as such; no invented guarantee).
- Clock skew is measurable via `clock_skew_ms()` (unused by default; a
  health-check aid, does NOT submit orders).

## 5. SDK decision

Official SDK exists (`mexcdevelop/mexc-api-demo`, Python demo). Decision:
**direct REST implementation stays.** Reasons: (1) zero added dependencies —
the project pins only telethon; (2) the critical safety requirements
(injectable transport for deterministic offline tests, signed-string redaction
in logs, weighted local rate gates, Decimal formatting of order values) are
adapter-level concerns the SDK does not provide; (3) the SDK wraps the same
endpoints verified above, so swapping later would change no contract.

## 6. Official sources (all checked 2026-10-23)

- Introduction, signing, limits, key rules: https://www.mexc.com/api-docs/spot-v3/introduction
- Error codes: https://www.mexc.com/api-docs/spot-v3/error-code
- Change log: https://www.mexc.com/api-docs/spot-v3/change-log
- New order: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/new-order
- Test new order: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/test-new-order
- Cancel order: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/cancel-order
- Cancel all on symbol: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/cancel-all-open-orders-on-a-symbol
- Cancel all (account): https://www.mexc.com/api-docs/spot-v3/spot-account-trade/cancel-all-orders
- Query order: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/query-order
- Open orders: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/current-open-orders
- All orders: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/all-orders
- Trade list: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/account-trade-list
- Account: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/account-information
- Query APIKey info: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/query-api-key-info
- Query UID: https://www.mexc.com/api-docs/spot-v3/spot-account-trade/query-uid
- Exchange info: https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/exchange-information
- Depth: https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/order-book
- Price ticker: https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/symbol-price-ticker
- Recent trades: https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/recent-trades-list
- Klines: https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/klinecandlestick-data
- Enums: https://www.mexc.com/api-docs/spot-v3/public-api-definitions (order sides BUY/SELL; types LIMIT/MARKET/LIMIT_MAKER/IOC/FOK, STOP_MARKET_ORDER query-only; statuses NEW/FILLED/PARTIALLY_FILLED/CANCELED/PARTIALLY_CANCELED)
- WebSocket market streams: https://www.mexc.com/api-docs/spot-v3/websocket-market-streams/
- API product page & FAQ ("no sandbox", SDK list, pair policy): https://www.mexc.com/mexc-api/
- API announcements: https://www.mexc.com/announcements/api-updates
  (notably: "API Suspension for Selected Spot Trading Pairs" Jan/Mar 2026 —
  per-pair API trading can be disabled; "Introducing API Futures Trading";
  official statements that MEXC may suspend or revoke API access for abuse)

## 7. Unresolved ambiguities

| # | Ambiguity | Adapter handling | Needs confirmation |
| --- | --- | --- | --- |
| A1 | recvWindow maximum: introduction says "maximum is 60 s" and the new-order table says `Max 60000`, but error 700005 text says "must **less than** 60000" | Local guard allows ≤ 60000; default is the officially recommended 5000 | Only if a 700005 occurs in practice |
| A2 | IP whitelist size: FAQ/error 730703 bound 10 IPs; `POST /api/v3/apiKeyInfo` page says up to 20 | Adapter never writes whitelists (POST variant not implemented) | Owner configuring the key |
| A3 | apiKeyInfo response field is `apikey` in the POST sample but `accessKey` in the GET sample | `validate_credentials()` ignores both; only `status`/`permissions`/`remainingValidity` are read | No |
| A4 | `quoteAmountPrecision` is documented as "min order amount" despite the name suggesting precision/step | Treated strictly as the minimum notional (fail-closed); no invented step | No |
| A5 | `remainingValidity` semantics: "-999 for permanent, 0 if expired, greater than 0 if active"; the string enum lives outside the API docs | Parsed as int with exactly these three meanings; anything else fails validation | No |
| A6 | Error code spacing: docs show `70011 Pair user ban trade apikey`; some community references list `700011` | Both are mapped to `MexcPermissionError` defensively | No — normalization is conservative either way |

## 8. Technically supported vs. currently permitted

**Technically supported** (documented, implemented, tested offline): every
endpoint in §2 marked "implemented".

**Permitted under current MEXC policy** (status as of 2026-10-23):

- Automated trading over the Spot API is an explicitly documented use case —
  the introduction names "automated trading applications" and
  https://www.mexc.com/mexc-api/ advertises API trading Spot-wide — so
  **API-driven spot order placement is in principle permitted**.
- BUT: (a) MEXC "reserves the right to suspend or revoke API access in cases
  of abuse" (user-agreement clause repeated across API announcements) and the
  risk-control system can block activity (codes 10098, 730100, 60005,
  `70011 Pair user ban trade apikey`); (b) API availability is per-pair and
  can be switched off by announcements (Jan 2026, Mar 2026 suspensions), even
  while the symbol itself trades on the web UI (`isSpotTradingAllowed`
  covers API trading, but announcements can override per pair);
  (c) each API key additionally has a self-service trading-pair restriction;
  (d) regional availability "may vary by jurisdiction" per the User Agreement.
- Therefore, before live automated execution the account owner must confirm:
  key permissions (READ + SPOT_DEAL_WRITE, no withdrawals), key/pair/API
  regional availability for the chosen pairs, and that the intended
  signal-driven manual-approval workflow does not classify as abuse under the
  current terms. **No adapter code claims to confirm any of this**: it can
  only verify key status/permissions read-only via `validate_credentials()`
  and symbol tradability via `exchangeInfo`, and it fails closed otherwise.
  See `MEXC_SPOT_RISKS.md` §"Policy ambiguities requiring confirmation".

## 9. Implementation status of this adapter

| Capability | Status |
| --- | --- |
| Signed request construction, canonicalization, doc-example regression tests | Implemented + tested offline |
| Public market data (time, exchangeInfo, depth, price ticker) | Implemented + tested offline |
| Account/balances, key validation, open orders, order query, fills | Implemented + tested offline (transport mocked); **live read-only verified 2026-10-23** (masked balance/account check, apiKeyInfo VALID permanent, confirmed candidate symbols; no values disclosed) |
| Order construction + symbol-filter validation (LIMIT/MARKET) | Implemented + tested offline |
| Order submission / cancellation / kill switch | Implemented + tested offline; **disabled by default** (`mode=sim`), live requires an explicit token; additionally, planning a live intent without the owner approval gate is rejected (fail-closed guard, 2026-10-23) |
| Official `POST /api/v3/order/test` exercise | **Verified live 2026-10-23**: one valid MARKET BUY check accepted (`{}`); no order placed; no intent row created |
| Ambiguous-timeout handling, duplicate prevention, reconciliation | Implemented in `trading/engine.py` + `trading/store.py` (SQLite durable intents), covered by that module's test suite |
| WebSocket handling | Not implemented (documented decision, §2) |
| Live connectivity / account validation | **Read-only authenticated access VERIFIED 2026-10-23** (apiKeyInfo/account/exchangeInfo); real order submission/cancellation remains NOT exercised — `mode=sim` and the empty allowlist stay until the owner explicitly changes them |
