"""Developer request, owner authorization, bounded import and human review."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from tests.conftest import FakeMsg
from tests.test_management_bot import (
    DEVELOPER,
    OWNER,
    STRANGER,
    Dialog,
    Entity,
    _bot,
    _callback,
    _message,
)
from tg_signal_ingest.analysis import AnalysisStore
from tg_signal_ingest.control.channels import ChannelChoice

CHANNEL = 77
SIGNAL_TEXT = "BUY BTCUSDT ENTRY 100 TP 120 SL 95"
UPDATE_TEXT = "BUY ETHUSDT ENTRY 10 TP 12 SL 9"
PLAIN_TEXT = "hello everyone"


def _setup(cfg, repo):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    messages = [
        FakeMsg(1, SIGNAL_TEXT),
        FakeMsg(2, UPDATE_TEXT),
        FakeMsg(3, PLAIN_TEXT),
    ]
    control, fake, factory = _bot(
        cfg,
        repo,
        dialogs=[Dialog(Entity(CHANNEL, "Source", date=now), now)],
        messages=messages,
    )
    store = control.store
    store.update_state(telegram_state="authenticated", telegram_label="Operator")
    snapshot = store.create_snapshot(
        [ChannelChoice(CHANNEL, 770, "Source", None, "کانال")],
        operator_id=DEVELOPER,
    )
    store.set_operator_channel(
        DEVELOPER,
        selected_channel_id=CHANNEL,
        selected_channel_title="Source",
        selected_channel_username=None,
        selection_snapshot_id=snapshot,
    )
    return control, fake, factory


def _button(fake, chat_id: int, label: str) -> str:
    for item in reversed(fake.sent_to(chat_id)):
        markup = item.get("reply_markup") or {}
        for row in markup.get("inline_keyboard") or []:
            for button in row:
                if button.get("text") == label:
                    return str(button["callback_data"])
    raise AssertionError(f"button not found for chat {chat_id}: {label}")


class Clicker:
    def __init__(self, control, fake):
        self.control = control
        self.fake = fake
        self.update_id = 10_000

    async def click(self, chat_id: int, label: str) -> None:
        self.update_id += 1
        data = _button(self.fake, chat_id, label)
        await self.control.handle_update(
            _callback(self.update_id, data, user=chat_id)
        )

    async def text(self, chat_id: int, text: str) -> None:
        self.update_id += 1
        await self.control.handle_update(
            _message(self.update_id, text, user=chat_id)
        )


async def _request_self_authorized(control, clicker):
    await control._show_validation_request(DEVELOPER)
    await clicker.click(DEVELOPER, "10 پیام آخر")
    request = control.store.latest_import_request()
    assert request is not None and request["status"] == "approved"
    assert request["decided_by"] == DEVELOPER
    assert request["decided_at"] is not None
    assert control.store.latest_job() is None
    owner_text = "\n".join(str(item["text"]) for item in control.bot.sent_to(OWNER))
    assert "✅ تأیید محدود" not in owner_text
    assert "هیچ اقدام یا تأییدی از طرف مالک لازم نیست" in owner_text
    return request


async def test_complete_self_authorized_validation_flow(cfg, repo, caplog):
    caplog.set_level(logging.DEBUG)
    control, fake, _factory = _setup(cfg, repo)
    clicker = Clicker(control, fake)

    request = await _request_self_authorized(control, clicker)
    assert request["max_messages"] == 10
    assert repo.message_counts(CHANNEL).total == 0

    await clicker.click(DEVELOPER, "▶️ شروع واردسازی محدود")
    task = control._worker_task
    assert task is not None
    await task

    saved = control.store.get_import_request(request["request_id"])
    assert saved is not None
    assert saved["status"] == "review"
    assert saved["imported_min_id"] == 1
    assert saved["imported_max_id"] == 3
    assert saved["candidate_count"] == 3
    assert repo.message_counts(CHANNEL).total == 3
    assert repo.get_message_raw(CHANNEL, 1)["text"] == SIGNAL_TEXT
    assert repo.get_message_raw(CHANNEL, 2)["text"] == UPDATE_TEXT
    assert repo.get_message_raw(CHANNEL, 3)["text"] == PLAIN_TEXT

    await clicker.click(DEVELOPER, "🔎 مرور کاندیدها")
    first = fake.sent_to(DEVELOPER)[-1]["text"]
    assert "متن واقعی پیام ذخیره‌شده" in first
    assert SIGNAL_TEXT in first

    await clicker.click(DEVELOPER, "✅ تأیید")
    await clicker.click(DEVELOPER, "❌ رد")
    await clicker.click(DEVELOPER, "✏️ اصلاح")
    await clicker.text(
        DEVELOPER,
        "classification=new_signal\nsymbol=SOLUSDT\nside=buy\n"
        "entry_price=200\ntargets=210,220",
    )
    await clicker.click(DEVELOPER, "💾 ثبت اصلاح")

    saved = control.store.get_import_request(request["request_id"])
    assert saved is not None
    assert saved["status"] == "completed"
    assert saved["reviewed_count"] == 3
    await clicker.click(DEVELOPER, "وضعیت")
    assert _button(fake, DEVELOPER, "🧪 درخواست تازه")

    analysis = AnalysisStore(repo.db)
    candidates = analysis.list_candidates_in_range(CHANNEL, 1, 3)
    assert [item["message_id"] for item in candidates] == [1, 2, 3]
    reviews = [analysis.list_reviews(item["candidate_id"]) for item in candidates]
    assert [row[0]["decision"] for row in reviews] == ["confirm", "reject", "correct"]
    assert reviews[2][0]["corrected"]["symbol"] == "SOLUSDT"
    assert reviews[2][0]["corrected"]["targets"] == [
        {"index": 1, "price": "210"},
        {"index": 2, "price": "220"},
    ]
    assert candidates[0]["review_status"] == "confirmed"
    assert candidates[1]["review_status"] == "rejected"
    assert candidates[2]["review_status"] == "corrected"
    # Machine candidate fields are immutable; correction is an append-only review.
    assert candidates[2]["symbol"] is None
    assert repo.get_message_raw(CHANNEL, 3)["text"] == PLAIN_TEXT

    queue = repo.db.conn.execute("SELECT COUNT(*) FROM processing_queue").fetchone()[0]
    signals = repo.db.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    intents = repo.db.conn.execute("SELECT COUNT(*) FROM order_intents").fetchone()[0]
    orders = repo.db.conn.execute("SELECT COUNT(*) FROM exchange_orders").fetchone()[0]
    assert queue == signals == intents == orders == 0

    audit_rows = repo.db.conn.execute(
        "SELECT event, detail FROM mgmt_audit ORDER BY rowid"
    ).fetchall()
    details = "\n".join(str(row["detail"] or "") for row in audit_rows)
    assert "validation_import_requested" in {row["event"] for row in audit_rows}
    assert "validation_import_self_authorized" in {row["event"] for row in audit_rows}
    assert "validation_import_started" in {row["event"] for row in audit_rows}
    assert "validation_analysis_ready" in {row["event"] for row in audit_rows}
    assert "candidate_review_recorded" in {row["event"] for row in audit_rows}
    assert "validation_review_completed" in {row["event"] for row in audit_rows}
    self_auth = [
        str(row["detail"] or "")
        for row in audit_rows
        if row["event"] == "validation_import_self_authorized"
    ]
    assert len(self_auth) == 1
    assert "role=developer" in self_auth[0]
    assert "scope=bounded" in self_auth[0]
    for forbidden in (SIGNAL_TEXT, UPDATE_TEXT, PLAIN_TEXT):
        assert forbidden not in details
    for field in ("channel_id=", "operator_id=", "channel=", "operator=", "requested_by="):
        assert field not in details
    logged = "\n".join(record.getMessage() for record in caplog.records)
    for forbidden in (SIGNAL_TEXT, UPDATE_TEXT, PLAIN_TEXT):
        assert forbidden not in logged


async def test_owner_cannot_request_start_or_review_and_developer_cannot_use_owner_path(
    cfg, repo
):
    control, fake, _factory = _setup(cfg, repo)
    clicker = Clicker(control, fake)

    await control._show_validation_request(OWNER)
    assert control.store.latest_import_request() is None
    await control._create_validation_request(OWNER, 10)
    assert control.store.latest_import_request() is None

    request = await _request_self_authorized(control, clicker)
    await control._decide_validation_request(DEVELOPER, request["request_id"], True)
    after = control.store.get_import_request(request["request_id"])
    assert after is not None and after["status"] == "approved"

    await control._start_validation_import(OWNER, request["request_id"])
    assert control.store.latest_job() is None

    await control._review_candidate(
        OWNER, request["request_id"], "cand-not-real", 0, "confirm"
    )
    assert AnalysisStore(repo.db).list_candidates() == []


def _legacy_requested(control):
    details = control.store.selected_channel_details(DEVELOPER)
    return control.store.create_import_request(
        channel_id=CHANNEL,
        channel_title="Source",
        channel_access_hash=details.get("access_hash"),
        selection_snapshot_id=details.get("selection_snapshot_id"),
        max_messages=10,
        requested_by=DEVELOPER,
    )


async def test_legacy_owner_denial_still_works_and_starts_nothing(cfg, repo):
    control, fake, _factory = _setup(cfg, repo)
    clicker = Clicker(control, fake)
    request = _legacy_requested(control)
    assert request["status"] == "requested"

    await control._show_validation_status(OWNER)
    await clicker.click(OWNER, "❌ رد درخواست")
    saved = control.store.get_import_request(request["request_id"])
    assert saved is not None and saved["status"] == "denied"
    assert control.store.latest_job() is None
    assert repo.message_counts(CHANNEL).total == 0
    assert any("رد کرد" in item["text"] for item in fake.sent_to(DEVELOPER))


async def test_self_authorization_does_not_start_and_owner_cannot_start(cfg, repo):
    control, fake, _factory = _setup(cfg, repo)
    clicker = Clicker(control, fake)

    request = await _request_self_authorized(control, clicker)
    assert control.store.latest_job() is None
    assert repo.message_counts(CHANNEL).total == 0

    await control._start_validation_import(OWNER, request["request_id"])
    assert control.store.latest_job() is None
    assert repo.message_counts(CHANNEL).total == 0

    await clicker.click(DEVELOPER, "▶️ شروع واردسازی محدود")
    task = control._worker_task
    assert task is not None
    await task
    saved = control.store.get_import_request(request["request_id"])
    assert saved is not None and saved["status"] == "review"


async def test_stranger_and_group_cannot_use_validation_callbacks(cfg, repo):
    control, fake, _factory = _setup(cfg, repo)
    clicker = Clicker(control, fake)
    request = await _request_self_authorized(control, clicker)

    data = _button(fake, DEVELOPER, "▶️ شروع واردسازی محدود")
    before = len(fake.sent)
    for user, chat_type in ((STRANGER, "private"), (OWNER, "group")):
        await control.handle_update(
            _callback(20_000 + len(fake.sent), data, user=user, chat_type=chat_type)
        )
    assert len(fake.sent) == before
    after = control.store.get_import_request(request["request_id"])
    assert after is not None and after["status"] == "approved"
    assert control.store.latest_job() is None


async def test_legacy_owner_approval_callback_is_single_use(cfg, repo):
    control, fake, _factory = _setup(cfg, repo)
    request = _legacy_requested(control)
    await control._show_validation_status(OWNER)
    data = _button(fake, OWNER, "✅ تأیید محدود")
    await control.handle_update(_callback(30_001, data, user=OWNER))
    await control.handle_update(_callback(30_002, data, user=OWNER))
    after = control.store.get_import_request(request["request_id"])
    assert after is not None and after["status"] == "approved"
    assert control.store.latest_job() is None
    assert any("منقضی" in (text or "") for _cid, text in fake.answered)


async def test_start_rejects_changed_channel_selection(cfg, repo):
    control, fake, _factory = _setup(cfg, repo)
    clicker = Clicker(control, fake)
    request = await _request_self_authorized(control, clicker)

    control.store.set_operator_channel(
        DEVELOPER,
        selected_channel_id=88,
        selected_channel_title="Other",
        selected_channel_username=None,
        selection_snapshot_id=control.store.current_snapshot(DEVELOPER),
    )
    await clicker.click(DEVELOPER, "▶️ شروع واردسازی محدود")

    saved = control.store.get_import_request(request["request_id"])
    assert saved is not None and saved["status"] == "cancelled"
    assert control.store.latest_job() is None
    assert repo.message_counts(CHANNEL).total == 0
