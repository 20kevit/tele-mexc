> **Historical/archive status:** This document preserves dated evidence,
> findings, or a design proposal. It is not a description of current behavior
> or an operational instruction. Current documentation starts at
> `docs/documentation-index.md`.

<!-- VERIFICATION 2026-10-31T2026-10-31T08:21:53.180647 — Mocked (unit) vs Live (integration) distinction -->
> **Verification (mocked, disposable DBs — no live exchange, no real login):**  
> `pytest tests/test_trading_engine.py tests/test_trading_config.py tests/test_trading_live_gates.py tests/test_owner_confirmed_deterministic.py tests/test_critical_review_fixes.py` → **105 passed** (42+18+10+15+20) in ~7.6s on Python 3.11.  
> All exchange calls are mocked via `SpyMexc` / `FakeRegistry`; `TradingStore` uses ephemeral SQLite (`:memory:` or `tmp_path`). No `MEXC_API_KEY`/`MEXC_SECRET` required, no order leaves the sandbox. Live integration (`tests/test_e2e_live.py`, `tests/test_trading_live.py`) intentionally **not** executed — would require real credentials and network.  
> **Key file:line citations (branch `arena/e452655b-tele-mexc`, base `71a952b`):**  
> - Mandatory invariants (SL/TP1/TP2/SL<Entry<TP1≤TP2) non-disableable: `tg_signal_ingest/config.py:488-495` (raises `ConfigError` on forbidden keys), rejected in `tests/test_trading_config.py:18` and `tests/test_critical_review_fixes.py::test_config_cannot_disable_mandatory_invariants`.  
> - Entry-range explicit rejection: `tg_signal_ingest/trading/risk.py:57-71` (message `entry range X–Y … not supported … use single ENTRY price`), verified `tests/test_critical_review_fixes.py::test_entry_range_rejected_with_clear_reason`.  
> - Per-symbol atomic admission (shared DB): `tg_signal_ingest/trading/store.py:67-92` (`SymbolConflictError`/`TradingError` fail-closed), `tg_signal_ingest/trading/trade_store.py:356-368` (DB-filtered `has_active_trade_for_symbol` LIMIT 1), `tg_signal_ingest/trading/engine.py:280-355` (`TradeStore(repo.db)` shared, `SymbolConflictError` handling, `LIMIT 20` removed), tested `tests/test_critical_review_fixes.py::test_per_symbol_*` (4 tests) + `tests/test_owner_confirmed_deterministic.py` concurrency spy.  
> - Sellable = acquired − base_fees − sold, idempotent via `exchange_trade_id`: `tg_signal_ingest/trading/trade_manager.py:1505-1545` (`_base_asset_for_trade` fallback, `_total_base_fees` SELL-aware, `_sellable_quantity` `None` if unknown, `_remaining_base`), verified `tests/test_critical_review_fixes.py::test_allocation_uses_net_sellable_with_base_fee` & `test_sell_base_fee_reduces_sellable`.  
> - Allocation from net sellable (not gross): `tg_signal_ingest/trading/trade_manager.py:1085-1125` (`sellable_for_allocation = _sellable_quantity`, `TP1 % of sellable`, `TP2 % of remaining pool`, unknown→`needs_reconciliation`), spy proves `TP1+TP2 ≤ sellable`.  
> - OCO invariant **before** submission (no API call if unprovable/violated/hypothetical exceed): `tg_signal_ingest/trading/trade_manager.py:1135-1225` (`_verify_oco_invariant` + `existing_other+qty > sellable` + `unknown→needs_reconciliation` + `would_violate`), spied in `tests/test_critical_review_fixes.py` (`test_oco_pre_submit_*` 6 tests) with `place_oco` call-count assertions, plus reuse-not-double-count & ambiguous `unknown_requires_reconciliation`.  
> - Market Sell removal (owner-confirmed §7): `tg_signal_ingest/trading/engine.py:1553-1572` (`submit_stop_loss_market_sell` raises `RuntimeError` disabled), `tg_signal_ingest/trading/trade_manager.py:1408` (`_enter_stop_loss` → `_handle_unprotected_sl`), `1433-1450` (`_resume_stop_loss`/`_submit_stop_loss_order` stubs), `400-460` (`_handle_unprotected_sl` persists `needs_reconciliation`+`SL_UNPROTECTED_CRITICAL`+audit, narrow per-symbol restriction), `1800` (legacy states read-only). Zero `submit_stop_loss_market_sell` reachable (verified `grep -rn` → only `tests/` + `trade_store.py` shim). Proven by 6 spy tests: `test_no_market_sell_*` (both OCOs active, OCO1 missing, OCO2 uncertain, rejection, cancel, restart).  
> - Startup reconcilable states: `tg_signal_ingest/trading/trade_manager.py:244-310` (`recover_on_startup` → `_reconcile_trade` → `_reconcile_entry_fills`/`_reconcile_trade_orders`), legacy `stop_loss_executing`/`exit_pending` now read-only `needs_reconciliation` (line 1800).  

---

# راهنمای گزارش‌های عملیاتی مالک (فارسی) — سامانه معاملاتی تلگرام→MEXC

**نسخه:** ۲٫۰ — راهبرد تأییدشده ۱۴۰۴/۰۸/۰۹ (۲۰۲۶-۱۰-۳۱)  
**مخاطب:** مالک (غیر توسعه‌دهنده) — برای پایش روزانه معاملات بدون خواندن کد  
**زبان:** فارسی کامل؛ معادل انگلیسی: `DEVELOPER_DIAGNOSTICS_GUIDE.md` (سطح فنی)

---

## ۱. گزارش‌ها کجا هستند؟

| گزارش | مسیر | کاربرد |
|-------|------|--------|
| خلاصه مدیریتی | `reports/trading_compliance/EXECUTIVE_SUMMARY.md` | وضعیت کلی COMPLIANT/UNVERIFIED |
| این راهنما | `OWNER_OPERATIONAL_REPORT_GUIDE_FA.md` | همین سند |
| ممیزی الزام‌به‌الزام | `REQUIREMENT_BY_REQUIREMENT_AUDIT.md` | هر الزام با file:line |
| تشخیص توسعه‌دهنده | `DEVELOPER_DIAGNOSTICS_GUIDE.md` | شواهد خام برای عیب‌یابی |
| کامل‌بودن داده | `DATA_COMPLETENESS_AND_AUDITABILITY.md` | چه داده‌ای ثبت می‌شود |
| نامتغیر OCO/موجودی | `OCO_AND_INVENTORY_INVARIANT_AUDIT.md` | اثبات OCO1+OCO2≤sellable |
| تغییرات اصلاحی | `REMEDIATION_CHANGELOG.md` | چه چیزی تغییر کرد |
| مسائل باز | `OPEN_ISSUES_AND_OWNER_DECISIONS.md` | تصمیمات مورد نیاز شما |
| نمونه چرخه حیات | `SIGNAL_LIFECYCLE_EXAMPLE_FA.md` | یک سیگنال از ورود تا خروج |

همه گزارش‌ها **evidence-based** هستند: هر ادعا یک `file:line` و نام تست دارد؛ موارد `UNVERIFIED` صریح علامت‌گذاری شده‌اند.

---

## ۲. پایش روزانه — چه چیزی را هر روز ببینید

### ۲٫۱ داشبورد سیگنال‌ها

در `audit_events` و `signals` هر سیگنال یکی از این وضعیت‌ها را دارد:

- `parsed` → `validated` → `planned` → `submitting` → `filled/open/canceled`
- `rejected` (دلیل دقیق: `missing SL`, `missing TP1`, `invalid price relationship` …)
- `unknown_requires_reconciliation` → **بحرانی**، نیاز به تطبیق دستی

**دستور پیشنهادی (بدون نوشتن در صرافی):**

```sql
SELECT signal_id, symbol, status, rejection_reason, created_at
FROM signals ORDER BY created_at DESC LIMIT 20;
```

### ۲٫۲ پوشش دو OCO

هر معامله موفق باید **دو گروه OCO** داشته باشد:

- OCO1: TP1 + SL
- OCO2: TP2 + SL
- نامتغیر: `OCO1_reserved + OCO2_reserved ≤ sellable` (sellable = پرشده منهای کارمزد پایه)

اگر نامتغیر نقض شود، معامله به `needs_reconciliation` می‌رود و اعلان `OCO_INVARIANT_VIOLATED` برای شما ارسال می‌شود — **هیچ ارسال خودکار جدیدی انجام نمی‌شود**.

### ۲٫۳ موجودی

| مقدار | معنی |
|-------|------|
| `sellable` | مقدار واقعی قابل فروش (پرشده − کارمزد پایه − فروخته‌شده) |
| `reserved` | مقدار قفل‌شده در OCOهای باز |
| `unprotected` | sellable − reserved → اگر >0 و بدون OCO، هشدار بحرانی |
| `realized_pnl` | سود محقق‌شده منهای کارمزد |
| `unvalued_fees` | کارمزد با دارایی ناشناخته (بدون قیمت‌گذاری اختراعی) |

### ۲٫۴ هشدارهای بحرانی (فوری)

این رویدادها به شما (مالک) و توسعه‌دهنده اطلاع داده می‌شوند:

- `OCO_INVARIANT_VIOLATED` — نامتغیر نقض شد
- `OCO_PROTECTION_PARTIAL/FAILED` — پوشش ناقص
- `unknown_requires_reconciliation` — نتیجه مبهم
- `TRADE_CLOSE_REFUSED` — بسته‌شدن با موجودی مانده
- `STALE_CONFIG` — متادیتای نماد قدیمی

هرگز Market Sell خودکار به‌عنوان جایگزین ارسال نمی‌شود (§۷).

---

## ۳. تفسیر راستی‌آزمایی

- **COMPLIANT (mocked):** با FakeMexc/FakeRegistry اثبات شد — رفتار صرافی شبیه‌سازی است، نه مشاهده زنده.
- **VERIFIED (live):** با CLI واقعی یا مستندات MEXC تأیید شد (مثلاً `place-oco`).
- **UNVERIFIED:** هنوز در حساب زنده مشاهده نشده — مثلاً `canTrade != true` global halt.

هیچ‌گاه `UNVERIFIED` به‌عنوان `COMPLIANT` گزارش نمی‌شود.

---

## ۴. اقدامات مورد نیاز شما

به `OPEN_ISSUES_AND_OWNER_DECISIONS.md` مراجعه کنید و درباره موارد زیر تصمیم بگیرید:

1. مجموعه حالت‌های متعارض هم‌نماد (آیا `tp1_completed` باید مسدود کند؟)
2. محرک‌های توقف سراسری (چه زمانی همه نمادها متوقف شوند؟)
3. تأیید تست زنده OCO با مبلغ کم

---

## ۵. نمونه چرخه حیات

به `SIGNAL_LIFECYCLE_EXAMPLE_FA.md` مراجعه کنید: یک سیگنال LONG BTCUSDT با SL 90, TP1 110, TP2 120 از `ENTRY 100` تا دو OCO و `TRADE_CLOSED` مرحله‌به‌مرحله توضیح داده شده است.

---

## ۶. پشتیبانی

برای جزئیات فنی، `DEVELOPER_DIAGNOSTICS_GUIDE.md` را به توسعه‌دهنده ارجاع دهید. هیچ راز API در گزارش‌ها ثبت نمی‌شود.
