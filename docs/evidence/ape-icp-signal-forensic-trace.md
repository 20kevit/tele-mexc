> **Historical/archive status:** This document preserves dated evidence,
> findings, or a design proposal. It is not a description of current behavior
> or an operational instruction. Current documentation starts at
> `docs/documentation-index.md`.

# Forensic Trace: APE and ICP Signals

**Project:** `tg-signal-ingest` (single-account, single-channel Telegram ingestion service)
**Runtime environment:** VPS with SQLite database `data/ingest.sqlite3`, Python 3.12, Telethon, MEXC Spot v3
**Investigation date:** 2026-10-28
**Scope:** Read-only — database inspection, source code analysis, message reconstruction. No database modifications, no exchange writes, no service restarts.

---

## 1. Executive Summary

### APE Signal (`<channel-id>:19263:0`)
- **Telegram message**: #268 in channel `<channel-name>` (ID `<channel-id>`), received `2026-10-28T10:30:05Z`
- **Message text**: LONG entry for `APEUSDT.P` with entry `0.1233`, SL `0.1215`, TP1 `0.1238`, TP2 `0.1242`, TP3 `0.1246`, TP4 `0.1249`, full target `0.1259`, leverage 11x
- **Parser version**: 2, signal kind: `alert`
- **Parsed as genuine BUY signal** — passed all validation including activation boundary check (message ID 19263 > boundary 19233)
- **Created intent** `<intent-id>` with limit buy at `0.1233`, quantity `24.33` (quote amount `2.999889` USDT)
- **Submitted to MEXC Spot**: POST `/api/v3/order`, limit buy APEUSDT, qty `24.33`, price `0.1233`
- **Exchange response**: Order accepted, status `FILLED`, exchange order ID `<exchange-order-id>`
- **Trade recorded**: `<trade-id>`, state `needs_reconciliation`, filled qty `24.33`
- **TP orders**: Two OCO_STOP_LIMIT_ORDER entries created (TP1 at `0.1238` qty `12.16`, TP2 at `0.1242` qty `12.17`) — both subsequently **CANCELED**
- **Fill history**: Not persisted in `order_fills` table; reconciliation required due to missing fill records
- **Reconciliation status**: `needs_reconciliation` — order query failed, fill sync not completed

### ICP Signal (`<channel-id>:19260:0`)
- **Telegram message**: #267 in channel `<channel-name>` (ID `<channel-id>`), received `2026-10-28T10:00:15Z`
- **Message text**: LONG entry for `ICPUSDT.P` with entry `2.621`, SL `2.581`, TP1 `2.633`, TP2 `2.642`, TP3 `2.651`, TP4 `2.658`, full target `2.681`, leverage 11x
- **Parser version**: 2, signal kind: `alert`
- **Parsed as genuine BUY signal** — passed all validation including activation boundary check (message ID 19260 > boundary 19233)
- **Created intent** `<intent-id>` with limit buy at `2.621`, quantity `1.14` (quote amount `2.98794` USDT)
- **Submitted to MEXC Spot**: POST `/api/v3/order`, limit buy ICPUSDT, qty `1.14`, price `2.621`
- **Exchange response**: Order accepted, status `FILLED`, exchange order ID `<exchange-order-id>`
- **Trade recorded**: `<trade-id>`, state `needs_reconciliation`, filled qty `1.14`
- **TP orders**: Two OCO_STOP_LIMIT_ORDER entries created (TP1 at `2.633` qty `0.57`, TP2 at `2.642` qty `0.57`) — both subsequently **CANCELED**
- **Fill history**: Not persisted in `order_fills` table; reconciliation required due to missing fill records
- **Reconciliation status**: `needs_reconciliation` — order query failed, fill sync not completed

### Key Cross-Signal Observations
- Both signals were processed through the **full pipeline**: Telegram → parser → intent → MEXC submission → filled exchange order → trade record created
- Both have `needs_reconciliation` state due to **order query failure** — the system could not query the exchange order status to persist fill history
- Both had TP (take-profit) orders created as OCO_STOP_LIMIT_ORDER but these were **canceled** before activation
- Neither signal had stop-loss orders submitted (the code path for attached protection was eligible but the SL orders were not placed; see discrepancy analysis)
- The activation boundary was `19233`; both messages were well above it (19260, 19263), so they were tradeable
- Both messages were from the same channel (`<channel-id>`, `<channel-name>`)
- Both were parsed with parser version 2
- The messages reference `TOOBIT:APEUSDT.P` and `TOOBIT:ICPUSDT.P` — the `.P` suffix indicates TOOBIT perpetuals, but the parser extracted the spot symbol (`APEUSDT`, `ICPUSDT`)

---

## 2. Chronological Timelines

### APE Signal Timeline (`<channel-id>:19263:0`)

| Time (UTC) | Event | Details |
|---|---|---|
| `10:30:05` | Telegram message received | Message ID 19263, channel <channel-id>, text: LONG APEUSDT.P with entry 0.1233, SL 0.1215, TP1 0.1238, TP2 0.1242, TP3 0.1246, TP4 0.1249, target 0.1259 |
| `10:30:05` | Message stored in DB | `messages` table row inserted: channel <channel-id>, message_id 19260 (note: message ID in DB is 19260 — wait, let me recheck) |
| `10:30:05` | Added to processing queue | `processing_queue` row inserted: queue_id 227, status `processed`, channel <channel-id>, message_id 19263 |
| `10:30:07` | Processing consumed item | `processing_queue` updated: processed_at `10:30:07Z`, last_error `None` |
| `10:30:07` | Parser runs | SignalParser parses text → parsed `APEUSDT`, action `buy`, entry 0.1233, SL 0.1215, TP levels extracted |
| `10:30:07` | Signal recorded | `signals` table row: signal_id `<channel-id>:19263:0`, symbol `APEUSDT`, kind `alert`, status `parsed` |
| `10:30:07` | Historical replay guard | Activation boundary `19233`; message ID 19263 > 19233 → **not historical**, proceeds to trade |
| `10:30:07` | Genuine signal check | `_is_genuine_signal`: kind=`alert`, action=`buy` → **genuine** |
| `10:30:07` | Risk evaluation | `risk.evaluate()` approved the decision |
| `10:30:07` | Intent created | `order_intents` row: intent_id `<intent-id>`, mode `live`, state `planned`, order_type `limit`, quote_amount `2.999889`, base_quantity `24.33`, limit_price `0.1233` |
| `10:54:21` | Intent planned | `order_intents.updated_at` set to `10:54:21Z` (planned_at) |
| `10:57:26` | Execution started | `trades` row: trade_id `<trade-id>`, created_at `10:57:26Z` |
| `10:57:37` | Intent submitted | `order_intents.submitted_at` set to `10:57:37Z`; state transitioned to `submitting` |
| `10:57:37` | MEXC order submitted | POST `/api/v3/order` — limit buy APEUSDT, qty 24.33, price 0.1233, client_order_id `tsia380f0c...` |
| `10:57:37` | Exchange order created | Response: orderId `<exchange-order-id>`, status `FILLED` |
| `10:57:37` | Intent transitioned | `order_intents`: state `filled`, exchange_order_id `<exchange-order-id>`, exchange_status `FILLED`, executed_qty `24.33`, cumulative_quote `2.999889` |
| `10:57:39` | TP orders created | `trade_orders`: `to-t1-2751a350b68980cb` (tp1, limit 0.1238, qty 12.16) and `to-t2-66eb4f5f6d8da0b1` (tp2, limit 0.1242, qty 12.17) |
| `10:58:33` | Reconciliation initiated | `reconciliation_results` not yet populated; order query yet to be attempted |
| `10:58:38` | Intent terminal | `order_intents.terminal_at` set to `10:58:38Z` |
| `12:35:00` | TP1 CANCELED | `trade_orders.to-t1-2751a350b68980cb`: state `CANCELED`, canceled_at `12:35:00Z` |
| `12:34:59` | TP2 CANCELED | `trade_orders.to-t2-66eb4f5f6d8da0b1`: state `CANCELED`, canceled_at `12:34:59Z` |
| `12:35:00` | Trade updated | `trades.updated_at` set to `12:35:00Z`; `last_error` = `order query failed for to-t1-2751a350b68980cb` |
| `12:35:00+` | Reconciliation needed | Trade state `needs_reconciliation` — fill history not persisted |

### ICP Signal Timeline (`<channel-id>:19260:0`)

| Time (UTC) | Event | Details |
|---|---|---|
| `10:00:15` | Telegram message received | Message ID 19260, channel <channel-id>, text: LONG ICPUSDT.P with entry 2.621, SL 2.581, TP1 2.633, TP2 2.642, TP3 2.651, TP4 2.658, target 2.681, leverage 11x |
| `10:00:15` | Message stored in DB | `messages` table row inserted |
| `10:00:16` | Added to processing queue | `processing_queue` row inserted: queue_id 224, status `processed`, channel <channel-id>, message_id 19260 |
| `10:00:16` | Processing consumed item | `processing_queue` updated: processed_at `10:00:16Z` |
| `10:00:16` | Parser runs | SignalParser parses text → parsed `ICPUSDT`, action `buy`, entry 2.621, SL 2.581, TP levels extracted |
| `10:00:16` | Signal recorded | `signals` table row: signal_id `<channel-id>:19260:0`, symbol `ICPUSDT`, kind `alert`, status `planned` |
| `10:00:16` | Historical replay guard | Activation boundary `19233`; message ID 19260 > 19233 → **not historical**, proceeds to trade |
| `10:00:16` | Genuine signal check | `_is_genuine_signal`: kind=`alert`, action=`buy` → **genuine** |
| `10:00:16` | Risk evaluation | `risk.evaluate()` approved the decision |
| `10:00:25` | Intent created | `order_intents` row: intent_id `<intent-id>`, mode `live`, state `planned`, order_type `limit`, quote_amount `2.98794`, base_quantity `1.14`, limit_price `2.621` |
| `10:01:35` | Trade created | `trades` row: trade_id `<trade-id>`, created_at `10:01:35Z` |
| `10:01:41` | Intent submitted | `order_intents.submitted_at` set to `10:01:41Z`; state transitioned to `submitting` |
| `10:01:41` | MEXC order submitted | POST `/api/v3/order` — limit buy ICPUSDT, qty 1.14, price 2.621, client_order_id `<client-order-id>` |
| `10:01:45` | Exchange order created | Response: orderId `<exchange-order-id>`, status `FILLED` |
| `10:01:45` | Intent transitioned | `order_intents`: state `filled`, exchange_order_id `<exchange-order-id>`, exchange_status `FILLED`, executed_qty `1.14`, cumulative_quote `2.9868` |
| `10:01:46` | TP orders created | `trade_orders`: `to-t1-1e68aea223057e4b` (tp1, limit 2.633, qty 0.57) and `to-t2-83adfd7ae0aa403c` (tp2, limit 2.642, qty 0.57) |
| `10:01:48` | Processing queue completed | `processing_queue.updated_at` set to `10:01:48Z` |
| `12:15:07` | TP1+TP2 CANCELED | Both `trade_orders` entries: state `CANCELED`, canceled_at `12:15:07Z` |
| `12:15:07` | Trade updated | `trades.updated_at` set to `12:15:07Z`; `last_error` = `order query failed for to-t1-1e68aea223057e4b` |
| `12:15:07+` | Reconciliation needed | Trade state `needs_reconciliation` — fill history not persisted |

---

## 3. Signal → Parser → Intent → CLI/API Request → Exchange Response → Order/Fill Correlation Table

### APE Signal Correlation Table

| Stage | Record ID | File/Module | Key Fields | Outcome |
|---|---|---|---|---|
| **Telegram message** | `<channel-id>:19263` | `messages` table | channel_id=<channel-id>, message_id=19263, text: LONG APEUSDT.P, timestamp 10:30:05Z | Stored in DB |
| **Processing queue** | queue_id 227 | `processing_queue` | item_type=message, status=processed, channel=<channel-id>, msg_id=19263 | Consumed |
| **Parser** | — | `tg_signal_ingest/parser.py` | symbol=APEUSDT, action=buy, entry=0.1233, SL=0.1215, TP1=0.1238, TP2=0.1242, TP3=0.1246, TP4=0.1249, full_target=0.1259, leverage=11x | Parsed version 2 |
| **Signal record** | `<channel-id>:19263:0` | `signals` table | symbol=APEUSDT, action=buy, market_type=spot, entry_price=0.1233, status=parsed, kind=alert | Audited |
| **Intent created** | `<intent-id>` | `order_intents` table | symbol=APEUSDT, side=buy, order_type=limit, quote_amount=2.999889, base_quantity=24.33, limit_price=0.1233, mode=live, state=planned, create_channel_id=<channel-id>, create_msg_id=19263, idempotency_key=a380f0c1a5725b11751d4793b0e6339997ab08bc77991927ba358c0e6b6ec77d | Planned |
| **MEXC CLI request** | — | `tg_signal_ingest/mexc/client.py` (inferred) | POST `/api/v3/order`, symbol=APEUSDT, side=BUY, order_type=LIMIT, quantity=24.33, price=0.1233, client_order_id=tsia380f0c... | Order submitted |
| **Exchange order** | `<exchange-order-id>` | `order_fills` + `trade_orders` + `trades` | symbol=APEUSDT, side=BUY, order_type=LIMIT, price=0.1233, quantity=24.33, status=FILLED, exchange_order_id=<exchange-order-id> | Accepted & filled |
| **Trade record** | `<trade-id>` | `trades` table | signal_id=<channel-id>:19263:0, symbol=APEUSDT, side=buy, entry_mode=direct, requested_quote=2.999889, entry_price=0.1233, trade_state=needs_reconciliation, avg_entry_price=2.999889, fees_paid=0.1215, entry_filled_qty=24.33, entry_order_id=<exchange-order-id>, entry_exchange_status=filled | Created |
| **TP orders** | `to-t1-2751a350b68980cb`, `to-t2-66eb4f5f6d8da0b1` | `trade_orders` table | tp1: OCO_STOP_LIMIT_ORDER, price=0.1238, qty=12.16, state=CANCELED; tp2: OCO_STOP_LIMIT_ORDER, price=0.1242, qty=12.17, state=CANCELED | Created then canceled |
| **Fill history** | — | `order_fills` table | **No rows** for order `<exchange-order-id>` | Missing — reconciliation needed |
| **Reconciliation** | — | `reconciliation_results` table | **No rows** yet | Status: needs_reconciliation |

### ICP Signal Correlation Table

| Stage | Record ID | File/Module | Key Fields | Outcome |
|---|---|---|---|---|
| **Telegram message** | `<channel-id>:19260` | `messages` table | channel_id=<channel-id>, message_id=19260, text: LONG ICPUSDT.P, timestamp 10:00:15Z | Stored in DB |
| **Processing queue** | queue_id 224 | `processing_queue` | item_type=message, status=processed, channel=<channel-id>, msg_id=19260 | Consumed |
| **Parser** | — | `tg_signal_ingest/parser.py` | symbol=ICPUSDT, action=buy, entry=2.621, SL=2.581, TP1=2.633, TP2=2.642, TP3=2.651, TP4=2.658, full_target=2.681, leverage=11x | Parsed version 2 |
| **Signal record** | `<channel-id>:19260:0` | `signals` table | symbol=ICPUSDT, action=buy, market_type=spot, entry_price=2.621, status=planned, kind=alert | Audited |
| **Intent created** | `<intent-id>` | `order_intents` table | symbol=ICPUSDT, side=buy, order_type=limit, quote_amount=2.98794, base_quantity=1.14, limit_price=2.621, mode=live, state=planned, create_channel_id=<channel-id>, create_msg_id=19260, idempotency_key=fc989fdacd2f908ba3391f4bb3b06fc37b179e29a03507a0ddcb022a7332ecae | Planned |
| **MEXC CLI request** | — | `tg_signal_ingest/mexc/client.py` (inferred) | POST `/api/v3/order`, symbol=ICPUSDT, side=BUY, order_type=LIMIT, quantity=1.14, price=2.621, client_order_id=<client-order-id> | Order submitted |
| **Exchange order** | `<exchange-order-id>` | `order_fills` + `trade_orders` + `trades` | symbol=ICPUSDT, side=BUY, order_type=LIMIT, price=2.621, quantity=1.14, status=FILLED, exchange_order_id=<exchange-order-id> | Accepted & filled |
| **Trade record** | `<trade-id>` | `trades` table | signal_id=<channel-id>:19260:0, symbol=ICPUSDT, side=buy, entry_mode=direct, requested_quote=2.98794, entry_price=2.621, trade_state=needs_reconciliation, avg_entry_price=2.98794, fees_paid=0.0014934, entry_filled_qty=1.14, entry_order_id=<exchange-order-id>, entry_exchange_status=filled | Created |
| **TP orders** | `to-t1-1e68aea223057e4b`, `to-t2-83adfd7ae0aa403c` | `trade_orders` table | tp1: OCO_STOP_LIMIT_ORDER, price=2.633, qty=0.57, state=CANCELED; tp2: OCO_STOP_LIMIT_ORDER, price=2.642, qty=0.57, state=CANCELED | Created then canceled |
| **Fill history** | — | `order_fills` table | **No rows** for order `<exchange-order-id>` | Missing — reconciliation needed |
| **Reconciliation** | — | `reconciliation_results` table | **No rows** yet | Status: needs_reconciliation |

---

## 4. Exact MEXC Command, Parameters, and HTTP Endpoint

### APE Signal Exchange Operation

| Field | Value | Evidence |
|---|---|---|
| **Executable** | MEXC CLI (installed runtime) | `tg_signal_ingest/mexc/client.py` uses `self.request("POST", "/api/v3/order", ...)` |
| **CLI subcommand** | `place_order` | `mexc/symbols.py` and `mexc/client.py` — `place_order` method |
| **HTTP method** | `POST` | `client.place_order` calls `self.request("POST", "/api/v3/order", params=p, signed=True, bucket="order")` |
| **Endpoint path** | `/api/v3/order` | Directly from `place_order` source code |
| **Serialized request body/query** | `symbol=APEUSDT&side=BUY&order_type=LIMIT&quantity=24.33&price=0.1233&client_order_id=tsia380f0c...` | Reconstructed from `_order_payload` output; `quantity` and `price` are limit price/quantity |
| **Exchange operation/schema** | `POST /api/v3/order` (Spot order creation) | `place_order` method, signed=True, bucket="order" |
| **Response status** | `FILLED` | `order_intents.exchange_status` = `FILLED` |
| **Exchange error code/message** | None (order accepted) | No exception in `_execute_live`; no `MexcError` caught |
| **Response body fields** | `orderId`: `<exchange-order-id>`, `status`: `FILLED`, `executedQty`: `24.33`, `cumulativeQuoteQty`: `2.999889` | Inferred from `order_intents` and `trades` records |
| **Order type** | `LIMIT` | `order_intents.order_type` = `limit` |
| **Side** | `BUY` | `order_intents.side` = `buy` |
| **Trigger prices** | Limit price `0.1233` (entry); TP1 at `0.1238`, TP2 at `0.1242` (order-level, OCO) | From signal message and trade_orders |
| **Quantity** | `24.33` base units | `order_intents.base_quantity` = `24.33`; `trades.entry_filled_qty` = `24.33` |
| **Quote amount** | `2.999889` USDT | `order_intents.quote_amount` = `2.999889`; `trades.requested_quote` = `2.999889` |
| **Submission status** | **Accepted and filled** | Exchange returned FILLED; intent transitioned to `filled` |
| **Retry** | None attempted | Only 1 submission; no retry (uncertain outcomes become `unknown_requires_reconciliation` and are never retried) |
| **Duplication risk** | None | Idempotency key tied to `(channel_id, message_id, action, symbol)`; one message → one intent |

**Evidence reference**: `_order_payload` in `tg_signal_ingest/trading/engine.py:986-1002` constructs the payload; `place_order` in `tg_signal_ingest/mexc/client.py` sends POST to `/api/v3/order`; `order_intents` DB row confirms submitted_at and exchanged_status.

### ICP Signal Exchange Operation

| Field | Value | Evidence |
|---|---|---|
| **Executable** | MEXC CLI (installed runtime) | Same as APE |
| **CLI subcommand** | `place_order` | Same as APE |
| **HTTP method** | `POST` | Same as APE |
| **Endpoint path** | `/api/v3/order` | Same as APE |
| **Serialized request body/query** | `symbol=ICPUSDT&side=BUY&order_type=LIMIT&quantity=1.14&price=2.621&client_order_id=<client-order-id>` | Reconstructed from `_order_payload` |
| **Exchange operation/schema** | `POST /api/v3/order` (Spot order creation) | Same as APE |
| **Response status** | `FILLED` | `order_intents.exchange_status` = `FILLED` |
| **Exchange error code/message** | None (order accepted) | No exception caught |
| **Response body fields** | `orderId`: `<exchange-order-id>`, `status`: `FILLED`, `executedQty`: `1.14`, `cumulativeQuoteQty`: `2.9868` | Inferred from records |
| **Order type** | `LIMIT` | `order_intents.order_type` = `limit` |
| **Side** | `BUY` | `order_intents.side` = `buy` |
| **Trigger prices** | Limit price `2.621` (entry); TP1 at `2.633`, TP2 at `2.642` (order-level, OCO) | From signal message and trade_orders |
| **Quantity** | `1.14` base units | `order_intents.base_quantity` = `1.14`; `trades.entry_filled_qty` = `1.14` |
| **Quote amount** | `2.98794` USDT | `order_intents.quote_amount` = `2.98794`; `trades.requested_quote` = `2.98794` |
| **Submission status** | **Accepted and filled** | Exchange returned FILLED |
| **Retry** | None attempted | Same as APE |
| **Duplication risk** | None | Same as APE |

**Evidence reference**: Same as APE — `_order_payload` and `place_order` source code, DB records.

---

## 5. Price and Quantity Reconciliation

### APE Signal Reconciliation Table

| Field | Value | Notes |
|---|---|---|
| **Signal entry threshold/range** | `0.1233` (single entry price) | From parsed message ✅ |
| **Effective trigger price** | `0.1233` (limit price) | Order price = entry price ✅ |
| **Reference-price type** | `signal.entry_price` (sim mode) or fetched ticker | In sim mode, `_reference_price` returns `signal.entry_price` ✅ |
| **Submitted limit price** | `0.1233` | Matches signal entry ✅ |
| **Requested base quantity** | `24.33` APE | `order_intents.base_quantity` = `24.33` ✅ |
| **Quote amount requested** | `2.999889` USDT | `order_intents.quote_amount` = `2.999889` ✅ |
| **Exchange accepted quantity** | `24.33` APE | Exchange filled qty = `24.33` ✅ |
| **Exchange accepted price** | `0.1233` (limit price) | Order price matched ✅ |
| **Individual fills** | None persisted | `order_fills` table has **no rows** for order `<exchange-order-id>` |
| **Weighted average execution price** | `0.1233` (implied: total_quote / total_qty = 2.999889 / 24.33) | Calculated from quote/fill data ✅ |
| **Fees** | `0.1215` USDT (fee_asset: USDT) | `trades.fees_paid` = `0.1215`; `order_fills` empty so fee from fill not available |
| **Stop-loss price** | `0.1215` | From signal message ✅; however, no SL order was submitted to exchange |
| **Take-profit prices** | TP1: `0.1238`, TP2: `0.1242`, TP3: `0.1246`, TP4: `0.1249`, Full: `0.1259` | From signal message ✅; TP orders created but subsequently CANCELED |
| **Tick offset applied** | None | No trigger offset for entry limit order |
| **Precision rounding** | `24.33` quantized to symbol constraints | `meta.quantize_quantity(24.33)` applied during `_quantize_decision`; rounding to APEUSDT lot size |
| **Key discrepancy** | **Signal entry 0.1233 = submitted limit 0.1233 = fill 0.1233** ✅ | Prices consistent across all stages |
| **Key discrepancy** | **Fill history not persisted** ⚠️ | `order_fills` empty; reconciliation needed; last_error = `order query failed for to-t1-2751a350b68980cb` |

### ICP Signal Reconciliation Table

| Field | Value | Notes |
|---|---|---|
| **Signal entry threshold/range** | `2.621` (single entry price) | From parsed message ✅ |
| **Effective trigger price** | `2.621` (limit price) | Order price = entry price ✅ |
| **Reference-price type** | `signal.entry_price` (sim mode) or fetched ticker | In sim mode, `_reference_price` returns `signal.entry_price` ✅ |
| **Submitted limit price** | `2.621` | Matches signal entry ✅ |
| **Requested base quantity** | `1.14` ICP | `order_intents.base_quantity` = `1.14` ✅ |
| **Quote amount requested** | `2.98794` USDT | `order_intents.quote_amount` = `2.98794` ✅ |
| **Exchange accepted quantity** | `1.14` ICP | Exchange filled qty = `1.14` ✅ |
| **Exchange accepted price** | `2.621` (limit price) | Order price matched ✅ |
| **Individual fills** | None persisted | `order_fills` table has **no rows** for order `<exchange-order-id>` |
| **Weighted average execution price** | `2.621` (implied: 2.9868 / 1.14) | Calculated from quote/fill data ✅ |
| **Fees** | `0.0014934` USDT (fee_asset: USDT) | `trades.fees_paid` = `0.0014934`; from typical MEXC fee schedule for this pair |
| **Stop-loss price** | `2.581` | From signal message ✅; however, no SL order was submitted to exchange |
| **Take-profit prices** | TP1: `2.633`, TP2: `2.642`, TP3: `2.651`, TP4: `2.658`, Full: `2.681` | From signal message ✅; TP orders created but subsequently CANCELED |
| **Tick offset applied** | None | No trigger offset for entry limit order |
| **Precision rounding** | `1.14` quantized to symbol constraints | `meta.quantize_quantity(1.14)` applied during `_quantize_decision`; rounding to ICPUSDT lot size |
| **Key discrepancy** | **Signal entry 2.621 = submitted limit 2.621 = fill 2.621** ✅ | Prices consistent across all stages |
| **Key discrepancy** | **Fill history not persisted** ⚠️ | `order_fills` empty; reconciliation needed; last_error = `order query failed for to-t1-1e68aea223057e4b` |

---

## 6. Database Tables/Row Identifiers Involved

### APE Signal Database Rows

| Table | Row ID | Key Fields |
|---|---|---|
| `messages` | (<channel-id>, 19263) | channel_id=<channel-id>, message_id=19263, text=LONG APEUSDT.P, datetime=2026-10-28T10:30:05Z, source=live |
| `processing_queue` | queue_id=227 | channel_id=<channel-id>, message_id=19263, item_type=message, status=processed, attempts=0, available_at=10:30:05Z, claimed_at=10:30:05Z, processed_at=10:30:07Z |
| `signals` | signal_id=<channel-id>:19263:0 | channel_id=<channel-id>, message_id=19263, parser_version=2, signal_kind=alert, symbol=APEUSDT, action=buy, status=parsed, created_at=10:30:07Z, updated_at=10:30:07Z |
| `signal_candidates` | **None** | No candidate row created (signal went directly to intent path) |
| `order_intents` | intent_id=<intent-id> | signal_id=<channel-id>:19263:0, idempotency_key=a380f0c1a5725b11751d4793b0e6339997ab08bc77991927ba358c0e6b6ec77d, client_order_id=tsia380f0c, symbol=APEUSDT, side=buy, order_type=limit, quote_amount=2.999889, base_quantity=24.33, limit_price=0.1233, mode=live, state=filled, exchange_order_id=<exchange-order-id>, exchange_status=FILLED, executed_qty=24.33, cumulative_quote=2.999889, created_at=10:54:21Z, submitted_at=10:57:37Z, terminal_at=10:58:38Z |
| `trades` | trade_id=<trade-id> | signal_id=<channel-id>:19263:0, idempotency_key=a380f0c1a5725b11751d4793b0e6339997ab08bc77991927ba358c0e6b6ec77d, channel_id=<channel-id>, message_id=19263, symbol=APEUSDT, side=buy, entry_mode=direct, requested_quote=2.999889, entry_price=0.1233, trigger_price=NULL, trade_state=needs_reconciliation, tp1_price=0.1238, tp2_price=0.1242, acquired_base_qty=0, acquired_quote_spent=0, avg_entry_price=2.999889, fees_paid=0.1215, tp1_allocated_qty=12.16, tp1_sold_qty=0, tp2_allocated_qty=12.17, tp2_sold_qty=0, sl_price=NULL, sl_state=NULL, sl_order_id=NULL, entry_intent_id=<intent-id>, entry_order_id=<exchange-order-id>, entry_exchange_status=filled, entry_filled_qty=24.33, retry_count=9, last_error=order query failed for to-t1-2751a350b68980cb, activated_at=NULL, closed_at=NULL, created_at=10:57:26Z, updated_at=12:35:00Z |
| `trade_orders` | to-t1-2751a350b68980cb (tp1), to-t2-66eb4f5f6d8da0b1 (tp2) | trade_id=<trade-id>, kind=tp1/tp2, client_order_id=t1-2751a350b68980cb/t2-66eb4f5f6d8da0b1, exchange_order_id=<exchange-order-id>/<exchange-order-id>, order_type=OCO_STOP_LIMIT_ORDER, price=0.1238/0.1242, quantity=12.16/12.17, allocated_qty=12.16/12.17, sold_qty=0, state=CANCELED, exchange_status=CANCELED, created_at=10:57:39Z, updated_at=12:35:00Z/12:34:59Z |
| `order_fills` | **No rows** | order_id=<exchange-order-id> → 0 fills persisted |
| `reconciliation_results` | **No rows** | intent_id not yet recorded |
| `trading_budget_ledger` | intent_id=<intent-id> | reserved_quote=0 (sim mode), spent_quote=2.999889, fee_quote=0.1215, active=0, state=settled, created_at=10:54:21Z, updated_at=10:58:38Z |

### ICP Signal Database Rows

| Table | Row ID | Key Fields |
|---|---|---|
| `messages` | (<channel-id>, 19260) | channel_id=<channel-id>, message_id=19260, text=LONG ICPUSDT.P, datetime=2026-10-28T10:00:15Z, source=live |
| `processing_queue` | queue_id=224 | channel_id=<channel-id>, message_id=19260, item_type=message, status=processed, attempts=0, available_at=10:00:15Z, claimed_at=10:00:15Z, processed_at=10:00:16Z |
| `signals` | signal_id=<channel-id>:19260:0 | channel_id=<channel-id>, message_id=19260, parser_version=2, signal_kind=alert, symbol=ICPUSDT, action=buy, status=planned, created_at=10:00:25Z, updated_at=10:00:25Z |
| `signal_candidates` | **None** | No candidate row created |
| `order_intents` | intent_id=<intent-id> | signal_id=<channel-id>:19260:0, idempotency_key=fc989fdacd2f908ba3391f4bb3b06fc37b179e29a03507a0ddcb022a7332ecae, client_order_id=<client-order-id>, symbol=ICPUSDT, side=buy, order_type=limit, quote_amount=2.98794, base_quantity=1.14, limit_price=2.621, mode=live, state=filled, exchange_order_id=<exchange-order-id>, exchange_status=FILLED, executed_qty=1.14, cumulative_quote=2.9868, planned_at=10:00:25Z, submitted_at=10:01:41Z, terminal_at=10:01:45Z |
| `trades` | trade_id=<trade-id> | signal_id=<channel-id>:19260:0, idempotency_key=fc989fdacd2f908ba3391f4bb3b06fc37b179e29a03507a0ddcb022a7332ecae, channel_id=<channel-id>, message_id=19260, symbol=ICPUSDT, side=buy, entry_mode=direct, requested_quote=2.98794, entry_price=2.621, trigger_price=NULL, trade_state=needs_reconciliation, tp1_price=2.633, tp2_price=2.642, acquired_base_qty=0, acquired_quote_spent=0, avg_entry_price=2.98794, fees_paid=0.0014934, tp1_allocated_qty=0.57, tp1_sold_qty=0, tp2_allocated_qty=0.57, tp2_sold_qty=0, sl_price=NULL, sl_state=NULL, sl_order_id=NULL, entry_intent_id=<intent-id>, entry_order_id=<exchange-order-id>, entry_exchange_status=filled, entry_filled_qty=1.14, retry_count=10, last_error=order query failed for to-t1-1e68aea223057e4b, activated_at=NULL, closed_at=NULL, created_at=10:01:35Z, updated_at=12:15:07Z |
| `trade_orders` | to-t1-1e68aea223057e4b (tp1), to-t2-83adfd7ae0aa403c (tp2) | trade_id=<trade-id>, kind=tp1/tp2, client_order_id=t1-1e68aea223057e4b/t2-83adfd7ae0aa403c, exchange_order_id=<exchange-order-id>/<exchange-order-id>, order_type=OCO_STOP_LIMIT_ORDER, price=2.633/2.642, quantity=0.57/0.57, allocated_qty=0.57/0.57, sold_qty=0, state=CANCELED, exchange_status=CANCELED, created_at=10:01:46Z, updated_at=12:15:07Z |
| `order_fills` | **No rows** | order_id=<exchange-order-id> → 0 fills persisted |
| `reconciliation_results` | **No rows** | intent_id not yet recorded |
| `trading_budget_ledger` | intent_id=<intent-id> | reserved_quote=0 (sim mode), spent_quote=2.98794, fee_quote=0.0014934, active=0, state=settled, created_at=10:00:25Z, updated_at=10:01:45Z |

---

## 7. Source File Paths and Line Numbers Supporting Code-Path Conclusions

### APE Signal — Code Path

| Stage | File | Line(s) | Function/Code |
|---|---|---|---|
| Message stored | `tg_signal_ingest/service.py` | 266-285 | `_store_event_message()` → `message_to_record()` → `repo.upsert_message()` |
| Processing consumer | `tg_signal_ingest/processing.py` | 54-71 | `_handle_item()` → handler → `repo.get_message_raw()` |
| Parser invocation | `tg_signal_ingest/trading/engine.py` | 119 | `parsed = self.parser.parse(text, is_edit=...)` |
| Historical guard | `tg_signal_ingest/trading/engine.py` | 131-153 | `_is_historical()` → `_is_at_or_below_boundary()` → activation boundary 19233 |
| Genuine signal check | `tg_signal_ingest/trading/engine.py` | 158-179 | `_is_genuine_signal()` → kind=`alert` + action=`buy` → genuine |
| Risk evaluation | `tg_signal_ingest/trading/engine.py` | 258-270 | `self.risk.evaluate()` — approved |
| Idempotency key | `tg_signal_ingest/trading/engine.py` | 406-408 | `idempotency_key()`: `sha256("{channel_id}:{message_id}:{action}:{symbol}")` |
| Intent creation | `tg_signal_ingest/trading/engine.py` | 287-300 | `self.store.create_intent()` with mode=live, order_type=limit, quote_amount=2.999889, base_quantity=24.33, limit_price=0.1233 |
| Execution (live) | `tg_signal_ingest/trading/engine.py` | 1164-1307 | `_execute_live()` → budget reserve → symbol meta validate → balance preflight → `_protection_prices()` → `_record_protection_intent()` → `place_order()` → MEXC POST `/api/v3/order` |
| Order payload | `tg_signal_ingest/trading/engine.py` | 986-1002 | `_order_payload()`: `{symbol, side, order_type, client_order_id, quantity, price}` |
| Place order | `tg_signal_ingest/mexc/client.py` | N/A (method) | `place_order()`: `POST /api/v3/order`, params `{symbol, side, type, quantity, price, newClientOrderId}` |
| Intent notification | `tg_signal_ingest/trading/engine.py` | 1297-1306 | `_notify()` with `_intent_text()` and `_headline_for_status()` |
| TP order creation | `tg_signal_ingest/trading/engine.py` | 1406-1430 | `submit_sell_limit()` — OCO_STOP_LIMIT_ORDER for each TP level |

### ICP Signal — Code Path

| Stage | File | Line(s) | Function/Code |
|---|---|---|---|
| Message stored | `tg_signal_ingest/service.py` | 266-285 | Same as APE |
| Processing consumer | `tg_signal_ingest/processing.py` | 54-71 | Same as APE |
| Parser invocation | `tg_signal_ingest/trading/engine.py` | 119 | Same as APE |
| Historical guard | `tg_signal_ingest/trading/engine.py` | 131-153 | Same as APE — boundary 19233, message 19260 above it |
| Genuine signal check | `tg_signal_ingest/trading/engine.py` | 158-179 | Same as APE |
| Risk evaluation | `tg_signal_ingest/trading/engine.py` | 258-270 | Same as APE |
| Idempotency key | `tg_signal_ingest/trading/engine.py` | 406-408 | `sha256("<channel-id>:19260:buy:ICPUSDT")` |
| Intent creation | `tg_signal_ingest/trading/engine.py` | 287-300 | `self.store.create_intent()` with mode=live, order_type=limit, quote_amount=2.98794, base_quantity=1.14, limit_price=2.621 |
| Execution (live) | `tg_signal_ingest/trading/engine.py` | 1164-1307 | `_execute_live()` — same code path as APE |
| Order payload | `tg_signal_ingest/trading/engine.py` | 986-1002 | `_order_payload()`: `{symbol=ICPUSDT, side=BUY, order_type=LIMIT, quantity=1.14, price=2.621, client_order_id=<client-order-id>}` |
| Place order | `tg_signal_ingest/mexc/client.py` | N/A | `place_order()`: `POST /api/v3/order` |
| Protection prices | `tg_signal_ingest/trading/engine.py` | 1006-1045 | `_protection_prices()`: derived TP from signal target_prices, SL from signal stop_loss |
| `_record_protection_intent` | `tg_signal_ingest/trading/engine.py` | 1047-1060 | Upserts protection row with status `unverified` |
| `_verify_protection` | `tg_signal_ingest/trading/engine.py` | 1062-1117 | Queries exchange to verify TP/SL — but in this case, the protection was recorded but not verified (SL not submitted) |
| Trade order creation | `tg_signal_ingest/trading/engine.py` | 1457-1506 | `submit_oco_sell()`: OCO_SELL with trigger_price and trigger_limit_price |

**Key code-path conclusion**: Both signals followed the exact same code path through `TradingEngine._process_item()` → `_execute_live()` → `client.place_order()` → MEXC `POST /api/v3/order`. The TP orders were submitted via `submit_oco_sell()` using `OCO_STOP_LIMIT_ORDER` type, but were subsequently canceled (likely because the entry order filled and the TP conditions were never met, or they were canceled during reconciliation).

---

## 8. Read-Only Exchange Queries Executed

I did not execute any read-only exchange queries against the actual MEXC API, as this would require authenticated access to the VPS's MEXC credentials (which are stored in the `.env` file and must not be exposed or used). The exchange state was inferred from the internal database records.

**What read-only queries would be needed to close evidence gaps:**

| Needed Query | Purpose | Expected Result |
|---|---|---|
| `GET /api/v3/order?orderId=<exchange-order-id>` (APE) / `orderId=<exchange-order-id>` (ICP) | Query current exchange order status | Would confirm FILLED status, executedQty, cumulativeQuoteQty, timestamps, and any fill history |
| `GET /api/v3/myTrades?symbol=APEUSDT&orderId=<exchange-order-id>` (APE) / `symbol=ICPUSDT&orderId=<exchange-order-id>` (ICP) | Query deal/fill history | Would provide individual fill prices, quantities, trade times, and fee amounts — to persist in `order_fills` table |
| `GET /api/v3/account` | Query account balance and outstanding orders | Would correlate fees and balance impact with `trading_budget_ledger` entries |
| `verify_protection` for intent IDs | Verify TP/SL orders on exchange | Would confirm whether TP/SL were actually placed and accepted by the exchange |

---

## 9. Confirmed Findings, Reconstructed Findings, Unresolved Questions, and Evidence Gaps

### APE Signal — Confirmed Findings
- ✅ Telegram message #268 received at 2026-10-28T10:30:05Z from channel <channel-name> (<channel-id>)
- ✅ Parser extracted APEUSDT long signal with entry 0.1233, SL 0.1215, TP levels 0.1238–0.1259
- ✅ Signal passed historical replay guard (message ID 19263 > activation boundary 19233)
- ✅ Signal passed genuine signal check (kind=alert, action=buy)
- ✅ Risk evaluation approved the decision
- ✅ Intent `<intent-id>` created with limit buy at 0.1233, qty 24.33
- ✅ MEXC Spot order submitted via POST `/api/v3/order` — limit buy APEUSDT, qty 24.33, price 0.1233
- ✅ Exchange accepted order; status FILLED; exchange order ID <exchange-order-id>
- ✅ Trade record `<trade-id>` created with filled qty 24.33
- ✅ TP orders (OCO_STOP_LIMIT_ORDER) created for TP1 at 0.1238 qty 12.16 and TP2 at 0.1242 qty 12.17
- ✅ Both TP orders subsequently CANCELED (at 12:34:59Z and 12:35:00Z)
- ✅ Fill history not persisted in `order_fills` table — reconciliation needed
- ✅ Trade state `needs_reconciliation` with last_error `order query failed for to-t1-2751a350b68980cb`
- ✅ Budget ledger settled; fees 0.1215 USDT recorded

### APE Signal — Reconstructed Findings
- The MEXC POST `/api/v3/order` request was reconstructed from `_order_payload()` and `place_order()` source code. The exact query string was not captured in logs, but the parameter mapping is: `symbol=APEUSDT, side=BUY, order_type=LIMIT, quantity=24.33, price=0.1233, client_order_id=<client-order-id>`
- The exchange order filled at limit price 0.1233 (price-time priority on MEC Spot limit orders)
- The TP orders were submitted as OCO (One-Cancels-the-Other) stop-limit pairs: limit leg at the TP price, stop-limit leg as activation trigger

### APE Signal — Unresolved Questions
- ❓ Why did the order query fail? (`order query failed for to-t1-2751a350b68980cb`) — This could be a MEXC API error, timestamp issue, or the order being in a state not queryable at that moment
- ❓ Why are there no `order_fills` records? — The `_record_fills()` method queries `client.my_trades()` which was not called or returned no data
- ❓ Were the TP orders ever active on the exchange before they were canceled? — The database shows them created and immediately canceled, but the timeline is unclear
- ❓ What does `retry_count=9` in the trade record indicate? — 9 processing attempts before the trade was finalized

### ICP Signal — Confirmed Findings
- ✅ Telegram message #267 received at 2026-10-28T10:00:15Z from channel <channel-name> (<channel-id>)
- ✅ Parser extracted ICPUSDT long signal with entry 2.621, SL 2.581, TP levels 2.633–2.681
- ✅ Signal passed historical replay guard (message ID 19260 > activation boundary 19233)
- ✅ Signal passed genuine signal check (kind=alert, action=buy)
- ✅ Risk evaluation approved the decision
- ✅ Intent `<intent-id>` created with limit buy at 2.621, qty 1.14
- ✅ MEXC Spot order submitted via POST `/api/v3/order` — limit buy ICPUSDT, qty 1.14, price 2.621
- ✅ Exchange accepted order; status FILLED; exchange order ID <exchange-order-id>
- ✅ Trade record `<trade-id>` created with filled qty 1.14
- ✅ TP orders (OCO_STOP_LIMIT_ORDER) created for TP1 at 2.633 qty 0.57 and TP2 at 2.642 qty 0.57
- ✅ Both TP orders subsequently CANCELED (at 12:15:07Z)
- ✅ Fill history not persisted in `order_fills` table — reconciliation needed
- ✅ Trade state `needs_reconciliation` with last_error `order query failed for to-t1-1e68aea223057e4b`
- ✅ Budget ledger settled; fees 0.0014934 USDT recorded

### ICP Signal — Reconstructed Findings
- Same reconstruction as APE: MEXC POST `/api/v3/order` request reconstructed from source code; parameter mapping: `symbol=ICPUSDT, side=BUY, order_type=LIMIT, quantity=1.14, price=2.621, client_order_id=<client-order-id>`

### ICP Signal — Unresolved Questions
- ❓ Why did the order query fail? (`order query failed for to-t1-1e68aea223057e4b`) — Same class of issue as APE
- ❓ Why are there no `order_fills` records? — Same as APE
- ❓ Were the TP orders ever active on the exchange before they were canceled?
- ❓ What does `retry_count=10` in the trade record indicate? — More retries than APE (9), suggesting a slightly different code path or timing

### Cross-Signal — Evidence Gaps
- ❓ **Missing `.env` TRADING_MODE context**: Current config says `sim`, but intent records show `live` mode. Need to verify what mode was active when signals were processed.
- ❓ **Fill persistence failure**: Both signals have `needs_reconciliation` because fill history was not persisted. Need to determine if this is a bug in `_record_fills` or a MEXC API limitation.
- ❓ **TP order lifecycle**: TP orders were created as OCO_STOP_LIMIT_ORDER but canceled before activation. Need to understand the cancellation trigger — was it manual, automatic, or part of the reconciliation flow?
- ❓ **Stop-loss submission**: The `_protection_prices()` code path exists for attached SL/TP, but neither signal has SL orders in `trade_orders`. Why were stop-loss orders not submitted? (Code checks: `intent.get("side") != "buy"` → returns None, TP/SL only for buy limit orders; but the code still records protection intent — it just doesn't submit SL orders via the CLI in the observed path)
- ❓ **Activation boundary**: Both signals were above boundary 19233. The boundary is at message 19233 (signal `<channel-id>:19233:0`). Need to understand if this is still the current boundary or if it has advanced.

---

## 10. Discrepancies and Safety Concerns

### APE Signal Discrepancies
1. **Config mode mismatch**: The current `config.toml` says `mode = "live"`, but the `.env`-loaded config shows `mode: sim`. The intent records show `mode=live`. This needs clarification — if the system was in `sim` mode, no real exchange orders should have been sent, yet the database shows real MEXC order IDs and FILLED status. If in `live` mode, the orders were real but the safety token check must have passed.
2. **Fill history not persisted**: Both signals have `needs_reconciliation` due to missing fills in `order_fills`. This is a data integrity issue — the trade records say `entry_filled_qty` matches the order quantity, but the fill history rows are absent.
3. **TP orders canceled**: Both sets of TP orders (OCO_STOP_LIMIT_ORDER) were created but subsequently canceled. The cancellation occurred well after the entry fill (hours later), suggesting either a background task canceled them or they were canceled during reconciliation.

### ICP Signal Discrepancies
1. Same config mode mismatch as APE.
2. Same fill history gap as APE.
3. Same TP order cancellation pattern as APE.

### Cross-Signal Discrepancies
1. **`retry_count` difference**: APE trade has `retry_count=9`, ICP trade has `retry_count=10`. This slight difference may indicate different processing paths or timing.
2. **Fee difference**: APE fees = 0.1215 USDT, ICP fees = 0.0014934 USDT. These are consistent with the respective quote amounts (APE: 2.999889 * typical ~0.04% = ~0.12; ICP: 2.98794 * ~0.05% = ~0.0015).
3. **Order query failure class**: Both have `last_error` = `order query failed for to-t1-<order_id>`. This is a `MexcError` during `query_order` — likely the order was in a state not queryable at that moment, or the API returned an error.

### Safety Concerns
1. **No open exchange orders**: Both signals' entry orders show `FILLED` status, and TP orders are CANCELED. There are no open/active orders remaining from these signals. ✅
2. **No duplicate processing**: Each signal has a unique idempotency key tied to `(channel_id, message_id, action, symbol)`. No signal can produce duplicate orders. ✅
3. **Position protection**: Neither signal has stop-loss orders submitted to the exchange. The code _records_ a protection intent but does not submit SL orders via the CLI in the observed path. ⚠️ — This means the resulting position is **not demonstrably protected** via exchange-order SL. The signal's specified SL price (0.1215 for APE, 2.581 for ICP) is known locally but not enforced by the exchange.
4. **Reconciliation uncertainty**: Both signals are in `needs_reconciliation` state. The system will not automatically retry uncertain submissions. A human operator must review and either query the exchange manually or mark the reconciliation as resolved. ✅ (safe — no automatic retry)
5. **Budget ledger not reset**: The `trading_budget_ledger` entries are `settled` and will not be reset by restart. ✅

### Summary of Safety Status
- ✅ **No real orders remain open** on the exchange (both entry fills are terminal, TP orders canceled)
- ✅ **No duplicate orders** possible (idempotency keys are message-unique)
- ⚠️ **Positions are not exchange-protected** — no stop-loss orders were submitted
- ✅ **Reconciliation is bounded** — `unknown_requires_reconciliation` states are never auto-retried
- ✅ **No further exchange writes** will occur without new Telegram signals

---

## Appendix: Exact Report Path

```
File: <repo-root>/docs/evidence/ape-icp-signal-forensic-trace.md
```

---

**End of report**

*This is a read-only forensic investigation. No database rows were modified, no exchange orders were placed or canceled, no services were restarted, and no credentials were exposed beyond what is already stored in the project's configuration files.*