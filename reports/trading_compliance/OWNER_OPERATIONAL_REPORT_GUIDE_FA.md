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
