# F-09 — Notification Coalescing & Suppression Policy

**Status: PROPOSAL ONLY — awaiting explicit owner approval. Nothing in this document is implemented.**

The owner has already decided that notification volume must be reduced
(owner decision recorded 2026-11-12; audit F-09). This document finalizes
the *design*: which event classes stay independent, which coalesce into
summaries, which are suppressed as redundant, and under which safety
exceptions. Implementation must not start until the owner approves this
policy, and must land behind an off-by-default switch (see Rollout).

## Principles

1. **Dispatcher-side only.** All coalescing and suppression runs inside the
   independent notification dispatcher, before per-recipient fan-out.
   Coalescing state is persisted in `dispatcher_state` so a worker restart
   cannot lose or double-flush a pending summary. Trading paths are never
   changed and no synchronous Telegram call is introduced.
2. **Never suppress material risk.** UNKNOWN/uncertain exchange outcomes,
   CRITICAL severities, entry failures, protection failures, reconciliation
   requirements and material trade-state transitions are always delivered
   independently (table below).
3. **Per-recipient preferences win.** The existing
   `notification_preferences` / minimum-severity routing is applied
   independently per recipient as today. This policy only narrows
   *repetition* for classes the owner approves; it never widens delivery,
   and the owner-safety bypass
   (`owner_safety_alert_bypasses_preferences`) stays untouched.
4. **Summaries are real events.** A coalescing summary is itself a
   persisted delivery with a stable identity, so the per-trade timeline and
   delivery history remain complete and auditable.
5. **Deduplication first.** The same `event_id` is never delivered twice to
   the same recipient (already guaranteed by the
   `UNIQUE(event_id, recipient_role, part_no)` ledger); this policy adds no
   second dedup layer.

## Event classes

### Keep independently (never coalesced, never suppressed)

- Every CRITICAL event (SL_UNPROTECTED_CRITICAL, OCO_INVARIANT_VIOLATED,
  TRADE_CLOSE_REFUSED_UNKNOWN, BUY_BLOCK_ACTIVE_*, SUBMISSION_BLOCKED, …).
- Any event describing an UNKNOWN/uncertain exchange outcome
  (`unknown_requires_reconciliation`, ambiguous cancellation, submission
  with no order id).
- Entry failures: rejected/blocked submissions, gate rejections, plan
  failures.
- Protection failures: failed/uncertain OCO submission, unprotected-price
  detection, missing stop child where identity is unresolved.
- Every transition into `needs_reconciliation` or
  `unprotected_critical`.
- Material trade-state transitions: `entry_active`, `oco_active`,
  `oco_partial`, `tp1_active`, `tp1_completed`, `tp2_active`,
  `tp2_completed`, `closed` (per fill tranche), residual-dust records.
- Startup-boundary / historical-replay refusals (one per message id).

### Coalesce into a summary (first occurrence immediate, repeats counted)

Applies only to *repetition of the same* `(aggregate_id, event_type)` while
the trade state has not materially changed. Window: 900 s (aligned with the
existing backoff cap). First occurrence is always delivered immediately;
subsequent occurrences increment a persisted counter; the summary is
flushed when the window closes, when any Keep-independently event fires for
the same trade, or on severity escalation.

| Repetitive class | Summary condition |
|---|---|
| Order-not-found parking (`MEXC_ORDER_NOT_FOUND` backoff repeats) | ≥2 consecutive parked reconciles for the same order |
| OCO stop-child discovery unresolved repeats (`OCO_STOP_CHILD_DISCOVERY`) | ≥2 consecutive unresolved discovery attempts for the same leg |
| Reconciliation query failures (order query errored, will retry) | ≥3 consecutive query failures for the same trade |
| Exchange rate-limit warnings (429/Retry-After honored) | ≥3 within one window |
| TP parent projection noise (same projection repeated on ticks) | ≥3 identical projections within one window |

Representative summaries (Persian, dispatcher style):

```
⏱ tr-a1b2 (BTCUSDT): 3× order-not-found parking در بازه اخیر
(آخرین کد: -2011، count=3) — reconciliation ادامه دارد؛ پارکینگ ≤900s
```

```
⏱ 5× discovery-unresolved روی stop-child ترید tr-a1b2 —
بدون تغییر وضعیت محافظت؛ تلاش بعدی پس از backoff
```

### Suppress as redundant (no owner-visible message at all)

- Repeat delivery of an identical `event_id` to a recipient that already
  has a `sent` delivery row for it (defensive; the ledger unique key
  already prevents this).
- Notifications the recipient's own preferences already disable (existing
  behavior, unchanged).

No other suppression is authorized. In particular there is **no**
severity-based blanket suppression beyond the existing minimum-severity
routing, and **no** time-based muting of any Keep-independently class.

## Safety exceptions (override everything above)

1. Severity escalation inside a window flushes the summary immediately and
   the escalated event is delivered independently.
2. Any Keep-independently event for the same trade flushes that trade's
   pending summaries immediately (the timeline stays gapless around
   material events).
3. Owner CRITICAL routing and the owner-safety bypass are evaluated before
   coalescing, exactly as today.
4. If the dispatcher cannot persist coalescing state, it falls back to
   pass-through delivery (never silent dropping).

## Interplay with the F-08 migration

This policy targets the authoritative event/delivery model
(`audit_events` → `deliveries_v2` flow). It must not be implemented before
F-08 Phase 1 parity tooling exists (it does now:
`tg-ingest notifications parity`), and its effect must be verified with
that read-only report — after enabling, the report must show no gap/orphan
regression, with summaries visible as their own delivery rows.

## Rollout (after owner approval)

1. Add a `control_settings` switch `notification_coalescing_enabled`
   (default **off**) plus per-class keys
   (`notification_coalesce_order_not_found`, `…_discovery`,
   `…_query_failure`, `…_rate_limit`, `…_projection_noise`), all default
   off. No code path changes while off.
2. Implement in the dispatcher behind the switch; unit tests cover each
   class, the flush conditions, and the safety exceptions.
3. Owner flips the switch per class in the control bot; parity report is
   checked after each flip.
4. Rollback = switch off (state rows are inert leftovers; no migration).
