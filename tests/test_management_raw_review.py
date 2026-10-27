"""Developer-only raw review: authorization, ordering, batching, pacing, read-only."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel

from tg_signal_ingest.control.bot_api import BotApiError
from tg_signal_ingest.db import Database
from tg_signal_ingest.management import raw_review as rr
from tg_signal_ingest.management.bot import ManagementBot
from tg_signal_ingest.management.store import ManagementStore
from tg_signal_ingest.session_guard import SessionGuard

OWNER = 4242
DEVELOPER = 5151
STRANGER = 999
CHANNEL_ID = 1234567
TITLE = "Example Signals"
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)

# Tables a raw review must never touch (read-only guarantee).
READ_ONLY_TABLES = (
    "messages",
    "channels",
    "processing_queue",
    "signal_candidates",
    "signals",
    "mgmt_ingestions",
    "mgmt_import_requests",
    "mgmt_audit",
    "mgmt_callbacks",
    "mgmt_review_drafts",
    "mgmt_operator_channels",
    "audit_events",
    "order_intents",
    "exchange_orders",
)


@pytest.fixture(autouse=True)
def _fast_pacing(monkeypatch):
    """Keep the production rate limit visible but skip real waiting in tests."""

    monkeypatch.setattr(rr, "SEND_INTERVAL_S", 0.0)


async def _noop_sleep(_seconds: float) -> None:
    return None


# ------------------------------------------------------------------- harness


class FakeBot:
    def __init__(self, fail_at: set[int] | None = None):
        self.sent: list[dict] = []
        self.attempts = 0
        self.fail_at = fail_at or set()

    def send_message(self, chat_id, text, *, reply_markup=None, **kwargs):
        self.attempts += 1
        if self.attempts in self.fail_at:
            raise BotApiError("Telegram Bot API rejected sendMessage: test failure")
        self.sent.append(
            {
                "chat_id": chat_id,
                "text": text,
                "reply_markup": reply_markup,
                "kwargs": kwargs,
                "message_id": 1000 + self.attempts,
            }
        )
        return {"message_id": 1000 + self.attempts}

    def answer_callback_query(self, callback_id, text=None):
        return True

    def delete_message(self, chat_id, message_id):
        return True

    def set_my_commands(self, commands):
        return True

    def get_updates(self, **_kwargs):
        return []

    def sent_to(self, chat_id):
        return [item for item in self.sent if item["chat_id"] == chat_id]

    @property
    def texts(self) -> list[str]:
        return [item["text"] for item in self.sent]


class Entity(Channel):
    def __init__(self, cid, title, *, username=None, date=None, megagroup=False):
        super().__init__(
            id=cid,
            title=title,
            photo=None,
            date=date or EPOCH,
            broadcast=not megagroup,
            megagroup=megagroup,
            access_hash=cid * 10,
            username=username,
        )


class Dialog:
    def __init__(self, entity, date=None):
        self.entity = entity
        self.name = entity.title
        self.date = date or EPOCH


class Msg:
    def __init__(
        self,
        mid,
        text="hello",
        *,
        date=None,
        edit_date=None,
        media=None,
        action=None,
        forward=None,
        reply_to=None,
        deleted=False,
    ):
        self.id = mid
        self.message = text
        self.date = date or (EPOCH + timedelta(seconds=mid))
        self.edit_date = edit_date
        self.media = media
        self.action = action
        self.forward = forward
        self.reply_to = reply_to
        self.deleted = deleted
        self.photo = None
        self.video = None
        if media == "photo":
            self.photo = object()


class FakeClient:
    def __init__(self, dialogs, messages=(), *, authorized=True, get_messages_error=None):
        self.dialogs = list(dialogs)
        self.messages = list(messages)
        self.authorized = authorized
        self.get_messages_error = get_messages_error
        self.connected = False
        self.disconnects = 0
        self.get_messages_calls: list[dict] = []

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnects += 1
        self.connected = False

    async def is_user_authorized(self):
        return self.authorized

    async def get_entity(self, _peer):
        channel_id = int(getattr(_peer, "channel_id", _peer))
        for dialog in self.dialogs:
            if dialog.entity.id == channel_id:
                return dialog.entity
        raise PermissionError("no entity")

    async def iter_dialogs(self, limit=None):
        for dialog in self.dialogs:
            yield dialog

    async def get_messages(self, entity, limit=1, offset_id=0, min_id=0):
        # Telethon returns the most recent messages, newest first.
        self.get_messages_calls.append({"entity": entity, "limit": limit})
        if self.get_messages_error is not None:
            raise self.get_messages_error
        rows = sorted(self.messages, key=lambda m: m.id, reverse=True)
        rows = [m for m in rows if m.id < (offset_id or 10**18)]
        return rows[:limit]


class Factory:
    def __init__(self, dialogs, messages=(), **kwargs):
        self.dialogs = dialogs
        self.messages = messages
        self.kwargs = kwargs
        self.clients: list[FakeClient] = []

    def __call__(self, _cfg):
        client = FakeClient(self.dialogs, self.messages, **self.kwargs)
        self.clients.append(client)
        return client


def _dialog(cid=CHANNEL_ID, title=TITLE):
    return Dialog(Entity(cid, title))


def _message(update_id, text=None, *, user=DEVELOPER, chat_type="private", message_id=None):
    payload = {
        "message_id": message_id or update_id,
        "from": {"id": user},
        "chat": {"id": user if chat_type == "private" else -100999, "type": chat_type},
    }
    if text is not None:
        payload["text"] = text
    return {"update_id": update_id, "message": payload}


def _bot(cfg, repo, *, messages=(), dialogs=(), fail_at=None, **factory_kwargs):
    cfg.control.owner_user_id = OWNER
    cfg.control.developer_user_id = DEVELOPER
    fake = FakeBot(fail_at=fail_at)
    factory = Factory(list(dialogs), list(messages), **factory_kwargs)
    control = ManagementBot(
        cfg,
        repo,
        fake,
        telethon_factory=factory,
        probe_override=lambda: ("valid", "Owner"),
    )
    control.store.update_state(telegram_state="authenticated", telegram_label="Owner")
    return control, fake, factory


def _select_channel(control, *, operator_id=DEVELOPER, channel_id=CHANNEL_ID, title=TITLE):
    control.store.set_operator_channel(
        operator_id, selected_channel_id=channel_id, selected_channel_title=title
    )


def _batches(fake, chat_id=DEVELOPER) -> list[str]:
    """The outgoing texts between the intro and the summary are the batches."""

    texts = [item["text"] for item in fake.sent_to(chat_id)]
    assert len(texts) >= 2, "intro and summary must bracket the batches"
    return texts[1:-1]


def _table_counts(repo) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in READ_ONLY_TABLES:
        row = repo.db.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        counts[table] = int(row[0])
    return counts


def _bot_ids(summary: str) -> set[int]:
    marker = "شناسه پیام‌های ربات: "
    tail = summary.split(marker, 1)[1].splitlines()[0]
    return {int(part) for part in tail.split(",") if part.strip().isdigit()}


def _recording_sender(sent: list[str] | None = None):
    box = sent if sent is not None else []

    async def _send(text: str):
        box.append(text)
        return {"message_id": 1000 + len(box)}

    return _send


def _opener(factory):
    """Async client opener matching the production ``open_client`` contract."""

    async def _open(_cfg=None):
        return factory(_cfg)

    return _open


# ----------------------------------------------------------- authorization


async def test_raw_command_is_developer_only(cfg, repo):
    messages = [Msg(101, "first"), Msg(102, "second"), Msg(103, "third")]
    control, fake, factory = _bot(cfg, repo, messages=messages, dialogs=[_dialog()])
    _select_channel(control)

    # Owner is refused and the shared session is never opened.
    await control.handle_update(_message(1, "/raw", user=OWNER))
    assert fake.sent_to(OWNER), "owner must receive an explicit refusal"
    assert "توسعه‌دهنده" in fake.sent_to(OWNER)[0]["text"]
    assert factory.clients == []

    # Strangers (private chat) are ignored silently.
    await control.handle_update(_message(2, "/raw", user=STRANGER))
    # The developer in a group chat is ignored as well.
    await control.handle_update(_message(3, "/raw", user=DEVELOPER, chat_type="group"))
    assert len(fake.sent) == 1  # only the owner refusal
    assert factory.clients == []

    # The developer gets the review in their own private chat.
    await control.handle_update(_message(4, "/raw", user=DEVELOPER))
    developer_texts = [item["text"] for item in fake.sent_to(DEVELOPER)]
    assert developer_texts
    assert {item["chat_id"] for item in fake.sent} == {OWNER, DEVELOPER}
    assert len(factory.clients) == 1

    joined = "\n".join(developer_texts)
    assert "مرور خام" in developer_texts[0]  # intro first
    assert "[1/3] #101 —" in joined
    assert "[3/3] #103 —" in joined
    assert joined.index("#101 —") < joined.index("#103 —")  # oldest first
    assert "پایان مرور خام" in developer_texts[-1]  # summary last


async def test_raw_without_selected_channel_never_touches_session(cfg, repo):
    control, fake, factory = _bot(cfg, repo, messages=[Msg(1, "x")], dialogs=[_dialog()])
    await control.handle_update(_message(1, "/raw", user=DEVELOPER))
    assert "ابتدا یک کانال" in fake.sent_to(DEVELOPER)[-1]["text"]
    assert factory.clients == []
    assert fake.sent_to(OWNER) == []


async def test_run_raw_review_without_channel_raises_before_session_use(cfg, repo):
    factory = Factory([_dialog()], [Msg(1, "x")])
    store = ManagementStore(repo.db)
    guard = SessionGuard(cfg)
    sent: list[str] = []
    send = _recording_sender(sent)

    with pytest.raises(rr.NoChannelSelected):
        await rr.run_raw_review(
            store=store,
            guard=guard,
            open_client=_opener(factory),
            send=send,
            operator_id=STRANGER,
        )
    assert factory.clients == []
    assert sent == []


# ------------------------------------------------------ ordering & metadata


async def test_messages_are_delivered_oldest_first_with_timestamps(cfg, repo):
    messages = [
        Msg(201, "first message"),
        Msg(202, "edited message", edit_date=EPOCH + timedelta(seconds=1000)),
        Msg(
            203,
            "forwarded message",
            forward=SimpleNamespace(from_name="Source", channel_id=7, from_id=None, date=EPOCH),
        ),
        Msg(204, "reply message", reply_to=SimpleNamespace(reply_to_msg_id=202)),
        Msg(205, "last message"),
    ]
    control, fake, _ = _bot(cfg, repo, messages=messages, dialogs=[_dialog()])
    _select_channel(control)

    await control.handle_update(_message(1, "/raw", user=DEVELOPER))
    joined = "\n".join(_batches(fake))

    positions = [joined.index(f"[{index}/5] #{200 + index} —") for index in range(1, 6)]
    assert positions == sorted(positions)  # oldest first, ascending order
    assert joined.index("first message") < joined.index("last message")

    expected_stamp = (EPOCH + timedelta(seconds=201)).strftime("%Y-%m-%d %H:%M:%S UTC")
    assert f"[1/5] #201 — {expected_stamp}" in joined  # order + id + timestamp
    assert "ویرایش: " in joined  # edited marker, where available
    assert "بازنشر: Source, channel_id=7" in joined  # forward metadata
    assert "در پاسخ به #202" in joined  # reply metadata


async def test_media_service_and_empty_messages_are_reported_transparently(cfg, repo):
    messages = [
        Msg(301, "کپشن دار 🖼", media="photo"),
        Msg(302, "", media="photo"),
        Msg(303, "", action=SimpleNamespace()),
        Msg(304, ""),
        Msg(305, "", deleted=True),
    ]
    control, fake, _ = _bot(cfg, repo, messages=messages, dialogs=[_dialog()])
    _select_channel(control)

    await control.handle_update(_message(1, "/raw", user=DEVELOPER))
    joined = "\n".join(_batches(fake))
    assert "رسانه: عکس (دانلود نشد)" in joined
    assert "کپشن دار 🖼" in joined  # caption preserved verbatim
    assert "[بدون کپشن]" in joined
    assert "سرویس: SimpleNamespace" in joined  # service message reported, not invented
    assert "[بدون متن]" in joined
    assert "[پیام حذف‌شده یا در دسترس نیست]" in joined


# ------------------------------------------------- batching & formatting


def test_formatting_is_verbatim_and_plain_text():
    text = "*bold* _under_ `code` <b>&amp;</b>\nخط دوم 💰\n\n  قیمت: 100.5 USDT  "
    message = Msg(7, text)
    entry = rr.message_entry(1, 1, message)
    assert entry.body == text  # no strip, no normalization, no truncation

    batches = rr.build_batches([message])
    assert len(batches) == 1
    assert text in batches[0]  # exact original text inside the outgoing batch
    assert rr.utf16_len(batches[0]) <= rr.MAX_BATCH_UTF16


def test_oversized_message_is_split_without_loss():
    body = "".join(
        f"خط شماره {index} با ایموجی 🚀 و قیمت {index}.25 USDT\n" for index in range(80)
    )
    entry = rr.message_entry(3, 10, Msg(9, body))
    parts = rr.render_entry_parts(entry, max_utf16=400)

    assert len(parts) >= 2
    head = len(entry.head_lines())
    for position, part in enumerate(parts, start=1):
        assert rr.utf16_len(part) <= 400
        header = part.split("\n", 1)[0]
        assert f"#{entry.message_id}" in header
        if position == 1:
            assert f"(بخش 1/{len(parts)})" in header
            assert "(ادامه" not in header
        else:
            assert f"(ادامه {position}/{len(parts)})" in header

    # Removing the repeated headers restores the original body exactly.
    reconstructed = "".join("\n".join(part.split("\n")[head:]) for part in parts)
    assert reconstructed == body


def test_batches_stay_within_limit_and_keep_every_message():
    messages = [
        Msg(1000 + index, f"پیام {index} — متن آزمایشی طولانی با ایموجی 💰 " * 3)
        for index in range(1, 31)
    ]
    batches = rr.build_batches(messages, max_utf16=300)
    assert len(batches) > 1
    for batch in batches:
        assert rr.utf16_len(batch) <= 300

    joined = "\n".join(batches)
    positions = []
    for index, message in enumerate(messages, start=1):
        key = f"[{index}/30] #{message.id} —"
        assert joined.count(key) == 1
        positions.append(joined.index(key))
        assert message.message in joined
    assert positions == sorted(positions)


async def test_hundred_recent_messages_are_delivered_in_safe_batches(cfg, repo):
    messages = [
        Msg(1000 + index, f"پیام شماره {index} 💰 قیمت {index}.5 USDT")
        for index in range(1, 101)
    ]
    control, fake, _ = _bot(cfg, repo, messages=messages, dialogs=[_dialog()])
    _select_channel(control)

    await control.handle_update(_message(1, "/raw", user=DEVELOPER))

    sent = fake.sent_to(DEVELOPER)
    assert sent, "developer must receive the review"
    assert {item["chat_id"] for item in fake.sent} == {DEVELOPER}

    intro, batches, summary = sent[0]["text"], [i["text"] for i in sent[1:-1]], sent[-1]["text"]
    assert "100 پیام در" in intro
    assert batches
    for item in sent:
        assert rr.utf16_len(item["text"]) <= 4096  # Telegram hard cap (UTF-16)
        assert "parse_mode" not in item["kwargs"]  # plain text, no Markdown/HTML execution

    joined = "\n".join(batches)
    positions = []
    for index, message in enumerate(messages, start=1):
        key = f"[{index}/100] #{message.id} —"
        assert joined.count(key) == 1
        assert message.message in joined
        positions.append(joined.index(key))
    assert positions == sorted(positions)  # oldest first across batch boundaries
    assert f"{len(batches)}/{len(batches)}" in summary


# ------------------------------------------------------ rate limits & errors


async def test_delivery_is_paced_between_every_outgoing_message(cfg, repo, monkeypatch):
    monkeypatch.setattr(rr, "SEND_INTERVAL_S", 2.5)
    sleeps: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    messages = [
        Msg(400 + index, f"پیام {index} با متن نسبتاً کوتاه برای آزمون")
        for index in range(1, 6)
    ]
    store = ManagementStore(repo.db)
    store.set_operator_channel(
        DEVELOPER, selected_channel_id=CHANNEL_ID, selected_channel_title=TITLE
    )
    factory = Factory([_dialog()], messages)
    guard = SessionGuard(cfg)
    sent: list[str] = []
    send = _recording_sender(sent)

    outcome = await rr.run_raw_review(
        store=store,
        guard=guard,
        open_client=_opener(factory),
        send=send,
        operator_id=DEVELOPER,
        max_utf16=200,
        sleep=_sleep,
    )

    total_sends = 1 + outcome.batch_count + 1  # intro + batches + summary
    assert outcome.batch_count >= 2
    assert len(sent) == total_sends
    assert sleeps == [2.5] * (total_sends - 1)  # paced before every send but the first
    assert outcome.batches_sent == outcome.batch_count
    assert outcome.batches_failed == 0
    assert outcome.intro_message_id == 1001
    assert outcome.summary_message_id == 1000 + total_sends
    assert not guard.locked  # shared session released before the delivery finished


async def test_failed_batch_is_reported_and_later_batches_continue(cfg, repo):
    messages = [
        Msg(1000 + index, f"پیام شماره {index} 💰 قیمت {index}.5 USDT")
        for index in range(1, 101)
    ]
    control, fake, _ = _bot(
        cfg, repo, messages=messages, dialogs=[_dialog()], fail_at={2}
    )
    _select_channel(control)

    await control.handle_update(_message(1, "/raw", user=DEVELOPER))

    sent = fake.sent_to(DEVELOPER)
    intro, batches, summary = sent[0]["text"], [i["text"] for i in sent[1:-1]], sent[-1]["text"]
    assert "100 پیام در" in intro
    assert batches  # batches after the failed one were still attempted
    assert "دسته ناموفق: 1" in summary
    counted = summary.split("دسته‌های ارسال‌شده: ", 1)[1].split("\n", 1)[0]
    delivered, total = (int(part) for part in counted.split("/"))
    assert delivered == total - 1
    bot_ids = _bot_ids(summary)
    assert len(bot_ids) == total - 1
    assert 1002 not in bot_ids  # the failed send is never claimed as delivered
    assert rr.utf16_len(summary) <= 4096


async def test_flood_wait_is_surfaced_without_sending_any_batch(cfg, repo):
    control, fake, factory = _bot(
        cfg,
        repo,
        messages=[Msg(1, "secret signal")],
        dialogs=[_dialog()],
        get_messages_error=FloodWaitError(None, 123),
    )
    _select_channel(control)

    await control.handle_update(_message(1, "/raw", user=DEVELOPER))

    texts = fake.sent_to(DEVELOPER)
    assert len(texts) == 1  # only the user-facing notice, no batch at all
    assert "123" in texts[0]["text"] and "صبر کنید" in texts[0]["text"]
    assert factory.clients and factory.clients[0].disconnects >= 1
    assert int(repo.db.conn.execute("SELECT COUNT(*) FROM mgmt_audit").fetchone()[0]) == 0


async def test_busy_session_is_reported_and_session_is_never_opened(cfg, repo):
    control, fake, factory = _bot(cfg, repo, messages=[Msg(1, "x")], dialogs=[_dialog()])
    _select_channel(control)
    await control.guard.acquire(operator_id=STRANGER)
    try:
        await control.handle_update(_message(1, "/raw", user=DEVELOPER))
    finally:
        control.guard.release(operator_id=STRANGER)

    assert len(fake.sent_to(DEVELOPER)) == 1
    assert "مشغول" in fake.sent_to(DEVELOPER)[0]["text"]
    assert factory.clients == []


async def test_runner_retries_stop_when_session_stays_busy(cfg, repo, monkeypatch):
    monkeypatch.setattr(rr, "SESSION_BUSY_RETRIES", 1)
    monkeypatch.setattr(rr, "SESSION_BUSY_RETRY_DELAY_S", 0.0)
    fake = FakeBot()
    factory = Factory([_dialog()], [Msg(1, "x")])
    store = ManagementStore(repo.db)
    store.set_operator_channel(
        DEVELOPER, selected_channel_id=CHANNEL_ID, selected_channel_title=TITLE
    )
    guard = SessionGuard(cfg)
    await guard.acquire(operator_id=STRANGER)
    try:
        code = await rr._run_once(
            cfg,
            DEVELOPER,
            1,
            bot=fake,
            store=store,
            guard=guard,
            open_client=_opener(factory),
            sleep=_noop_sleep,
        )
    finally:
        guard.release(operator_id=STRANGER)
    assert code == 1
    assert fake.sent == []  # no partial delivery while the session is held
    assert factory.clients == []


# ------------------------------------------------------- one-shot runner


async def test_runner_targets_only_the_developer_private_chat(cfg, repo):
    messages = [Msg(501, "قدیمی‌تر"), Msg(502, "میانی"), Msg(503, "جدیدترین")]
    fake = FakeBot()
    factory = Factory([_dialog()], messages)
    store = ManagementStore(repo.db)
    store.set_operator_channel(
        DEVELOPER, selected_channel_id=CHANNEL_ID, selected_channel_title=TITLE
    )

    code = await rr._run_once(
        cfg,
        DEVELOPER,
        100,
        bot=fake,
        store=store,
        guard=SessionGuard(cfg),
        open_client=_opener(factory),
        sleep=_noop_sleep,
    )

    assert code == 0
    assert fake.sent, "runner must deliver to the developer chat"
    assert {item["chat_id"] for item in fake.sent} == {DEVELOPER}
    joined = "\n".join(_batches(fake))
    assert "[1/3] #501 —" in joined
    assert "[3/3] #503 —" in joined
    assert joined.index("#501 —") < joined.index("#503 —")  # oldest first
    assert "پیام‌های دریافت‌شده: 3" in fake.texts[-1]  # summary carries the counts


def test_runner_reads_the_selected_channel_read_only_and_never_migrates(cfg, repo):
    store = ManagementStore(repo.db)
    store.set_operator_channel(
        DEVELOPER, selected_channel_id=CHANNEL_ID, selected_channel_title=TITLE
    )
    assert rr.resolve_selected_channel(store, DEVELOPER) == (CHANNEL_ID, TITLE)

    db = Database(cfg.db_path, read_only=True)
    try:
        ro_store = ManagementStore(db)
        assert rr.resolve_selected_channel(ro_store, DEVELOPER) == (CHANNEL_ID, TITLE)
        with pytest.raises(RuntimeError):  # no migration on a read-only handle
            db.migrate()
    finally:
        db.close()


async def test_runner_falls_back_to_the_existing_owner_selection(cfg, repo):
    store = ManagementStore(repo.db)
    store.set_operator_channel(
        OWNER, selected_channel_id=CHANNEL_ID, selected_channel_title=TITLE
    )
    factory = Factory([_dialog()], [Msg(1, "x")])
    outcome = await rr.run_raw_review(
        store=store,
        guard=SessionGuard(cfg),
        open_client=_opener(factory),
        send=_recording_sender(),
        operator_id=DEVELOPER,
        fallback_operator_id=OWNER,
    )
    assert (outcome.channel_id, outcome.channel_title) == (CHANNEL_ID, TITLE)


# ------------------------------------------------------ read-only guarantee


async def test_raw_review_writes_nothing_to_db_or_logs(cfg, repo, caplog):
    marker = "SIGNAL_MARKER_abcdef"
    messages = [
        Msg(601, f"ordinary discussion {marker} about the market"),
        Msg(602, "buy ETHUSDT? no, just chat"),
        Msg(603, "سیگنال نیست؛ فقط بحث عادی"),
    ]
    original_texts = [message.message for message in messages]
    control, fake, _ = _bot(cfg, repo, messages=messages, dialogs=[_dialog()])
    _select_channel(control)

    before_counts = _table_counts(repo)
    before_selection = dict(control.store.operator_channel(DEVELOPER))
    caplog.set_level(logging.DEBUG)

    await control.handle_update(_message(1, "/raw", user=DEVELOPER))

    assert fake.sent_to(DEVELOPER), "review must be delivered"
    assert _table_counts(repo) == before_counts  # no rows written anywhere
    assert dict(control.store.operator_channel(DEVELOPER)) == before_selection
    assert control.store.latest_job() is None  # no backfill/live monitoring started
    assert repo.list_channels() == []  # no ingestion channel registered

    # No message content in logs.
    assert marker not in caplog.text
    assert "buy ETHUSDT" not in caplog.text

    # No message content in the database file (or its WAL).
    for path in (cfg.db_path, cfg.db_path.with_name(cfg.db_path.name + "-wal")):
        if path.is_file():
            assert marker.encode("utf-8") not in path.read_bytes()

    # Source messages were not altered in any way.
    assert [message.message for message in messages] == original_texts
    assert [message.id for message in messages] == [601, 602, 603]
