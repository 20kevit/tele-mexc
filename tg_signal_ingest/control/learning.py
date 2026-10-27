"""Signal-format learning from owner-confirmed examples."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..repository import Repository
from ..trading.parser import ParsedSignal, SignalParser
from .store import ControlStore


@dataclass
class Candidate:
    sample: dict[str, Any]
    parsed: ParsedSignal
    message_text: str


def extract_fields(parsed: ParsedSignal) -> dict[str, Any]:
    return {
        "kind": parsed.kind,
        "action": parsed.action,
        "symbol": parsed.symbol,
        "order_type": parsed.order_type,
        "entry_price": str(parsed.entry_price) if parsed.entry_price is not None else None,
        "entry_low_price": str(parsed.entry_low_price) if parsed.entry_low_price is not None else None,
        "entry_high_price": str(parsed.entry_high_price) if parsed.entry_high_price is not None else None,
        "targets": [str(p) for p in parsed.target_prices],
        "stop_loss": str(parsed.stop_loss) if parsed.stop_loss is not None else None,
        "amount_text": parsed.amount_text,
        "reasons": list(parsed.reasons),
        "fingerprint": parsed.fingerprint_sha256,
    }


class LearningService:
    def __init__(self, store: ControlStore, repo: Repository, parser: SignalParser | None = None):
        self.store = store
        self.repo = repo
        self.parser = parser or SignalParser()

    def _next_unsampled(self, channel_id: int) -> dict[str, Any] | None:
        return self.repo.db.conn.execute(
            """
            SELECT m.message_id, m.processing_version, m.text, m.caption
            FROM messages m
            WHERE m.channel_id = ? AND m.deleted = 0
              AND COALESCE(m.text, m.caption) IS NOT NULL
              AND TRIM(COALESCE(m.text, m.caption)) <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM control_learning_samples s
                  WHERE s.channel_id = m.channel_id AND s.message_id = m.message_id
              )
            ORDER BY m.message_id DESC LIMIT 1
            """,
            (int(channel_id),),
        ).fetchone()

    def next_candidate(self) -> Candidate | None:
        setup = self.store.setup()
        channel_id = setup.get("selected_channel_id")
        if not channel_id:
            return None
        row = self._next_unsampled(int(channel_id))
        if row is None:
            return None
        text = row["text"] if row["text"] is not None else row["caption"]
        parsed = self.parser.parse(text, is_edit=int(row["processing_version"]) > 0)
        sample, _created = self.store.add_learning_sample(
            channel_id=int(channel_id),
            message_id=int(row["message_id"]),
            edit_version=int(row["processing_version"]),
            message_text=text,
            extracted=extract_fields(parsed),
            parser_kind=parsed.kind,
            actionable=parsed.actionable,
        )
        return Candidate(sample=sample, parsed=parsed, message_text=text)

    def pending_candidate(self) -> Candidate | None:
        sample = self.store.pending_learning_sample()
        if sample is None:
            return None
        parsed = self.parser.parse(sample.get("message_text") or "")
        return Candidate(sample=sample, parsed=parsed, message_text=sample.get("message_text") or "")

    def decide(self, sample_id: str, approved: bool) -> tuple[bool, str, dict[str, Any] | None]:
        sample = self.store.get_learning_sample(sample_id)
        if sample is None:
            return False, "نمونه یادگیری پیدا نشد.", None
        if sample["decision"] != "pending":
            return False, "این نمونه قبلاً بررسی شده است.", sample
        if approved and not int(sample["actionable"]):
            return (
                False,
                "این پیام سیگنال معتبر و قابل استخراج نیست و نمی‌تواند به‌عنوان نمونه تأیید شود.",
                sample,
            )
        try:
            updated = self.store.decide_learning_sample(
                sample_id, approved,
                reason="owner confirmed extraction" if approved else "owner rejected extraction",
            )
        except ValueError as exc:
            return False, str(exc), sample
        target = self.store.get_int_setting("learning_target", 5)
        streak = int(self.store.setup().get("learning_streak") or 0)
        if approved:
            note = f"تأیید شد. پیشرفت: {streak} از {target} نمونه متوالی."
        else:
            note = "رد شد. شمارنده نمونه‌های متوالی بازنشانی شد."
        return True, note, updated
