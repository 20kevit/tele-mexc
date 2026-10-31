# نمونه چرخه حیات سیگنال — از تلگرام تا بسته‌شدن معامله (فارسی)

**هدف:** یک سیگنال کامل LONG را مرحله‌به‌مرحله با وضعیت‌های DB، نامتغیر OCO و گزارش‌های مالک نشان دهد.

---

## ۰. سیگنال ورودی (تلگرام)

```
Channel: @CryptoSignalsVIP (ID 100)
Message 52 — 2026-11-01 12:00 UTC
BUY BTCUSDT
ENTRY: 100
SL: 95
TP1: 110
TP2: 120
```

**پارسر استخراج می‌کند:** `symbol=BTCUSDT, action=BUY (LONG), entry=100, SL=95, TP1=110, TP2=120` → بررسی `SL<Entry<TP1≤TP2` → `95<100<110≤120` ✅

**پایگاه داده:** `signals` سطر `signal_id=100:52:0` با `raw_text`, `normalized_text`, `fingerprint`, `status=parsed`.

---

## ۱. اعتبارسنجی در مرز اجرا (§۴)

`RiskManager.evaluate()` در `engine.py:_process_item` قبل از هر تماس صرافی:

- نماد پشتیبانی‌شده؟ بله (FakeRegistry یا MEXC symbols)
- LONG؟ بله
- entry/SL/TP1/TP2 موجود و مثبت؟ بله
- رابطه قیمتی؟ `95<100<110≤120` ✅
- دقت نماد؟ `quantize down` (مثلاً lot size 0.00001)

**نتیجه:** `decision.approved = true`, `quote_amount=3 USDT` (پیکربندی `order_quote_amount`), `limit_price=100`.

**اگر SL یا TP2 مفقود بود:** `status=rejected`, `rejection_reason="missing stop loss (SL)"`, رویداد `audit_events` دسته `risk`, اعلان به مالک — **هیچ BUY ارسال نمی‌شود**.

---

## ۲. یک BUY برای هر سیگنال (§۲) — Idempotency + هم‌زمانی + موجودی

1. `idempotency_key = sha256("100:52:buy:BTCUSDT") = 48a1cff...` → `clientOrderId = tsi48a1cff...`
2. بررسی **per-signal idempotency**: آیا `order_intents` با همین کلید قبلاً وجود دارد؟ خیر.
3. بررسی **per-symbol concurrency** (`trade_store.py:has_active_trade_for_symbol`): آیا BTCUSDT معامله فعال `pending_entry/entry_active/oco_active...` دارد؟ خیر (معامله قبلی `closed` است).
4. **رزرو موجودی** (`store.py:_reserve_budget` داخل تراکنش): `reserved_quote=3.006` (با بافر کارمزد 0.2%) در `trading_budget_ledger` ثبت می‌شود.

**ایجاد:** `order_intents` سطر `intent_id=intent-tsi48a1...` با `state=planned`, `signals.status=validated`, رویداد `audit_events: intent_planned`.

---

## ۳. ارسال BUY

`engine.py:_execute_intent` → `_execute_live` (یا `sim`):

- **پیش‌پرواز موجودی** (`_balance_preflight`): `available USDT=1000`, `outstanding=3.006`, `required=3` → کافی است.
- **ارسال:** `client.place_order(symbol=BTCUSDT, side=BUY, type=LIMIT, price=100, quote=3)` → `exchange_order_id=ex-1`.

**انتقال حالت:** `planned → submitting → submitted (open)` با تاریخچه `order_status_history`.

**تکوین معامله:** `trade_manager.py:on_intent_planned` یک `trades` سطر `trade_id=tr-48a1cff...` با `trade_state=pending_entry`, `entry_intent_id=intent-tsi...`, `sl_price=95, tp1_price=110, tp2_price=120` می‌سازد.

---

## ۴. پرشدن BUY (یک یا چند Fill)

صرافی پر می‌کند: 0.03 BTC @ 100 (fill1 با fee 0.00003 BTC) + 0.0?? پرشدن دیگر؟ در این نمونه یک fill کامل:

- `order_fills`: `quantity=0.03, quote_amount=3, price=100, fee_asset=BTC, fee_amount=0.00003, exchange_trade_id=t-1`
- `reconcile_intent` → `acquired_base_qty=0.03, acquired_quote_spent=3, avg_entry_price=100`
- `trade_state: pending_entry → entry_active → position_open`
- `sellable = 0.03 - 0.00003 (base fee) - 0 = 0.02997`

---

## ۵. دو گروه مستقل OCO (§۵)

`trade_manager.py:_place_oco_exits` با `acquired=0.03`:

- تخصیص: `TP1 50% of acquired = 0.015` (quantized), `TP2 100% of remainder = 0.01497` (پس از کم‌کردن TP1 و کارمزد)
- **OCO1:** TP1 Limit 110 + SL Stop 95, quantity 0.015, type `OCO_STOP_LIMIT_ORDER`
- **OCO2:** TP2 Limit 120 + SL Stop 95, quantity 0.01497

**نامتغیر:** `OCO1_reserved 0.015 + OCO2_reserved 0.01497 = 0.02997 ≤ sellable 0.02997` ✅ → `trade_state=oco_active`, اعلان `OCO_PROTECTION_ACTIVE`.

**پس از هر ارسال:** `_verify_oco_invariant` دوباره محاسبه می‌شود.

---

## ۶. اجرای TP1

قیمت به 110 می‌رسد → OCO1 TP1 به‌صورت جزئی پر می‌شود (0.015 کامل):

- `trade_orders` OCO1 → `filled`, `sold_qty=0.015`
- `trades.tp1_sold_qty=0.015`
- `sellable = 0.03 -0.00003 -0.015 = 0.01497`
- OCO2 همچنان `open` با 0.01497 — نامتغیر `0.01497 ≤ 0.01497` ✅
- اگر TP1 کامل پر شود و TP2 هنوز باز است: `trade_state=tp1_active → enter_tp2` (در مدل قدیمی) اما در مدل دو-OCO، هر دو OCO از ابتدا فعال هستند، پس فقط `tp1_sold` به‌روز می‌شود.

---

## ۷. اجرای TP2 و بسته‌شدن

قیمت به 120 می‌رسد → OCO2 پر می‌شود (0.01497):

- `trade_orders` OCO2 → `filled`
- `trades.tp2_sold_qty=0.01497`
- `sellable = 0.03 -0.00003 -0.03 = -0.00003 → max(0, ...)=0`
- `remaining_base=0` → `trade_state=closed`, `closed_at` ثبت, اعلان `TRADE_CLOSED`
- **PNL محقق‌شده:** `(0.015*110 + 0.01497*120) - 3 - fees` با `Decimal` و `fee_asset` اصلی.

---

## ۸. سناریو نقض نامتغیر (نمونه)

اگر به‌دلیل کارمزد پایه `sellable` ناگهان 0.029 به 0.028 کاهش یابد اما هر دو OCO هنوز 0.015+0.01497=0.02997 رزرو دارند:

- `_verify_oco_invariant` → `0.02997 > 0.028` → `trade_state=needs_reconciliation`, اعلان `OCO_INVARIANT_VIOLATED`, **هیچ OCO جدیدی ارسال نمی‌شود**, تطبیق لازم است (مثلاً لغو یک OCO یا تأیید اجرای جزئی).

**Market Sell خودکار ارسال نمی‌شود** — فقط پس از تطبیق و تصمیم مالک.

---

## ۹. بازیابی پس از راه‌اندازی مجدد

در راه‌اندازی: `reconcile_all_open` → برای هر `trade` غیرنهایی:

1. هویت‌ها بازیابی (`trade_id`, `exchange_order_id`, `clientOrderId`)
2. سفارش‌های واقعی ربات پرس‌وجو (`query_order` برای هر `trade_order`)
3. fillها تطبیق (`myTrades`)
4. `sellable` و نامتغیر دوباره محاسبه
5. حفاظت راستی‌آزمایی — اگر مفقود/نامطمئن → `needs_reconciliation` + اعلان

**هرگز** پیام‌های تلگرام قدیمی به‌عنوان معامله جدید بازپخش نمی‌شوند (idempotency_key پایدار).

---

## ۱۰. وضعیت‌های نهایی قابل مشاهده برای مالک

| وضعیت | معنی |
|-------|------|
| `rejected` | سیگنال ناقص (دلیل دقیق) — هیچ BUY |
| `planned` | BUY برنامه‌ریزی شد، منتظر ارسال |
| `filled/closed` | معامله کامل شد |
| `oco_active` | دو OCO فعال، پوشش کامل |
| `oco_partial` | فقط یک OCO تأیید شد — پوشش ناقص، نیاز به پیگیری |
| `needs_reconciliation` | نامتغیر نقض یا نتیجه مبهم — نیاز به اقدام دستی/تطبیق |
| `unknown_requires_reconciliation` | ارسال با قطع شبکه — قبل از retry تطبیق شود |

همه وضعیت‌ها در `signals`, `order_intents`, `trades`, `audit_events` با `created_at` قابل حسابرسی هستند.

---

**ارجاع فنی:** `OCO_AND_INVENTORY_INVARIANT_AUDIT.md` (جزئیات Decimal، کارمزد، دقت) و `DEVELOPER_DIAGNOSTICS_GUIDE.md` (شناسه‌ها، کوئری‌ها).
