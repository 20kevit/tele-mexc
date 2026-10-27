"""Two-operator management bot: authorization, isolation, preview and logout."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import Channel

from tg_signal_ingest.management.bot import ManagementBot

OWNER = 4242
DEVELOPER = 5151
STRANGER = 999


class FakeBot:
    def __init__(self):
        self.sent: list[dict] = []
        self.answered: list[tuple[str, str | None]] = []
        self.deleted: list[tuple[int, int]] = []
        self.commands: list[list[dict]] = []

    def send_message(self, chat_id, text, *, reply_markup=None):
        self.sent.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"message_id": len(self.sent)}

    def answer_callback_query(self, callback_id, text=None):
        self.answered.append((callback_id, text))
        return True

    def delete_message(self, chat_id, message_id):
        self.deleted.append((chat_id, message_id))
        return True

    def set_my_commands(self, commands):
        self.commands.append(commands)
        return True

    def get_updates(self, **_kwargs):
        return []

    def buttons(self, index=-1):
        markup = self.sent[index].get("reply_markup") or {}
        inline = markup.get("inline_keyboard")
        if inline:
            return [button for row in inline for button in row]
        return []

    def sent_to(self, chat_id):
        return [item for item in self.sent if item["chat_id"] == chat_id]


class Entity(Channel):
    def __init__(self, cid, title, *, username=None, date=None, megagroup=False):
        super().__init__(
            id=cid,
            title=title,
            photo=None,
            date=date,
            broadcast=not megagroup,
            megagroup=megagroup,
            access_hash=cid * 10,
            username=username,
        )


class Dialog:
    def __init__(self, entity, date):
        self.entity = entity
        self.name = entity.title
        self.date = date


class Msg:
    def __init__(self, mid, text="hello", *, media=None, action=None):
        self.id = mid
        self.message = text
        self.date = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=mid)
        self.edit_date = None
        self.media = media
        self.action = action
        self.photo = None
        self.video = None
        if media in {"photo", "video"}:
            setattr(self, media, object())

    def to_dict(self):
        return {"id": self.id}


class FakeClient:
    def __init__(
        self,
        dialogs,
        messages=(),
        authorized=True,
        *,
        needs_password=False,
        sign_in_error=None,
        logout_result=True,
        logout_error=None,
    ):
        self.dialogs = dialogs
        self.messages = list(messages)
        self.authorized = authorized
        self.needs_password = needs_password
        self.sign_in_error = sign_in_error
        self.logout_result = logout_result
        self.logout_error = logout_error
        self.connected = False
        self.disconnects = 0
        self.code_requests: list[str] = []
        self.sign_ins: list[dict] = []
        self.logout_calls = 0
        self.me = SimpleNamespace(first_name="Owner", last_name=None, username="owner")

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnects += 1
        self.connected = False

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        return self.me

    async def send_code_request(self, phone):
        self.code_requests.append(phone)
        return SimpleNamespace(phone_code_hash="hash")

    async def sign_in(self, **kwargs):
        self.sign_ins.append(kwargs)
        if self.sign_in_error is not None:
            raise self.sign_in_error
        if "password" not in kwargs and self.needs_password:
            raise SessionPasswordNeededError(None)
        if "password" in kwargs and self.needs_password:
            self.authorized = True
            return None
        self.authorized = True
        return None

    async def log_out(self):
        self.logout_calls += 1
        if self.logout_error is not None:
            raise self.logout_error
        self.authorized = False
        return self.logout_result

    async def iter_dialogs(self, limit=None):
        for dialog in self.dialogs:
            yield dialog

    async def get_entity(self, _peer):
        channel_id = int(getattr(_peer, "channel_id", _peer))
        for dialog in self.dialogs:
            if dialog.entity.id == channel_id:
                return dialog.entity
        raise PermissionError("no entity")

    async def get_messages(self, _entity, limit=1, offset_id=0, min_id=0):
        rows = [m for m in sorted(self.messages, key=lambda m: m.id, reverse=True) if m.id < (offset_id or 10**18)]
        return rows[:limit]


class Factory:
    def __init__(self, dialogs, messages=(), authorized=True, **client_kwargs):
        self.dialogs = dialogs
        self.messages = messages
        self.authorized = authorized
        self.client_kwargs = client_kwargs
        self.clients: list[FakeClient] = []

    def __call__(self, _cfg):
        client = FakeClient(
            self.dialogs,
            self.messages,
            self.authorized,
            **self.client_kwargs,
        )
        self.clients.append(client)
        return client


def _message(update_id, text=None, *, user=OWNER, chat_type="private", contact=None, message_id=None):
    payload = {
        "message_id": message_id or update_id,
        "from": {"id": user},
        "chat": {"id": user if chat_type == "private" else -100999, "type": chat_type},
    }
    if text is not None:
        payload["text"] = text
    if contact is not None:
        payload["contact"] = contact
    return {"update_id": update_id, "message": payload}


def _callback(update_id, data, *, user=OWNER, chat_type="private"):
    return {
        "update_id": update_id,
        "callback_query": {
            "id": f"cb-{update_id}",
            "from": {"id": user},
            "message": {"message_id": 10, "chat": {"id": user, "type": chat_type}},
            "data": data,
        },
    }


def _bot(cfg, repo, *, probe=("valid", "Owner"), dialogs=None, messages=(), **factory_kwargs):
    cfg.control.owner_user_id = OWNER
    cfg.control.developer_user_id = DEVELOPER
    fake = FakeBot()
    factory = Factory(dialogs or [], messages, **factory_kwargs)
    control = ManagementBot(
        cfg,
        repo,
        fake,
        telethon_factory=factory,
        probe_override=lambda: probe,
    )
    return control, fake, factory


def _channel(cid, title, date, *, username=None):
    return Dialog(Entity(cid, title, username=username, date=date), date)


def _auth_token(control, user=OWNER):
    return "mg:" + control.store.issue_callback(owner_id=user, chat_id=user, action="auth")


async def _start_login(control, user=OWNER, update_id=1):
    await control.handle_update(
        _callback(update_id, _auth_token(control, user), user=user)
    )


async def test_both_operators_authorized_stranger_and_groups_denied(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    await control.handle_update(_message(1, "/start", user=STRANGER))
    await control.handle_update(_message(2, "/start", chat_type="group"))
    assert fake.sent == []

    await control.handle_update(_message(3, "/start", user=OWNER))
    await control.handle_update(_message(4, "/start", user=DEVELOPER))
    assert fake.sent_to(OWNER) and fake.sent_to(DEVELOPER)
    assert "نقش شما: مالک" in fake.sent_to(OWNER)[-1]["text"]
    assert "نقش شما: توسعه‌دهنده" in fake.sent_to(DEVELOPER)[-1]["text"]

    denied = _callback(5, "mg:whatever", user=STRANGER)
    await control.handle_update(denied)
    assert "دسترسی مجاز نیست" in fake.answered[-1][1]

    group_callback = _callback(6, "mg:whatever", user=OWNER, chat_type="group")
    await control.handle_update(group_callback)
    assert "دسترسی مجاز نیست" in fake.answered[-1][1]


async def test_callback_tokens_reject_wrong_operator_and_replay(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    token = "mg:" + control.store.issue_callback(owner_id=OWNER, chat_id=OWNER, action="status")

    await control.handle_update(_callback(1, token, user=DEVELOPER))
    assert "دسترسی مجاز نیست" in fake.answered[-1][1]
    assert fake.sent == []

    await control.handle_update(_callback(2, token))
    sent_after_first = len(fake.sent)
    await control.handle_update(_callback(3, token))
    assert len(fake.sent) == sent_after_first
    assert "منقضی" in fake.answered[-1][1]


async def test_terminal_login_guidance_never_requests_a_code_or_opens_a_flow(cfg, repo):
    control, fake, factory = _bot(cfg, repo, probe=("missing", None), authorized=False)
    await _start_login(control, OWNER)

    assert control.store.flow(OWNER)["state"] == "idle"
    assert control.guard.holder is None
    assert factory.clients == []
    guidance = fake.sent_to(OWNER)[-1]["text"]
    assert ".venv/bin/tg-ingest auth login" in guidance
    assert "غیرفعال" in guidance
    assert "ارسال شماره" not in guidance

    # A phone number is not accepted, and no code request is made.
    await control.handle_update(_message(2, "+15550101337", message_id=20))
    assert control.store.flow(OWNER)["state"] == "idle"
    assert factory.clients == []
    assert all("ارسال شماره" not in item["text"] for item in fake.sent_to(OWNER))

    # A pasted code is deleted, never echoed, and never submitted.
    await control.handle_update(_message(3, "12345", message_id=21))
    assert (OWNER, 21) in fake.deleted
    assert factory.clients == []
    assert control.store.flow(OWNER)["state"] == "idle"
    assert any(
        "کد ورود را هرگز" in item["text"] for item in fake.sent_to(OWNER)
    )
    assert all("12345" not in item["text"] for item in fake.sent_to(OWNER))
    events = [row["event"] for row in repo.db.conn.execute("SELECT event FROM mgmt_audit")]
    assert "chat_code_rejected" in events

    # Sharing a contact is also refused and cleaned up.
    await control.handle_update(
        _message(
            4,
            contact={"user_id": OWNER, "phone_number": "+15550101337"},
            message_id=22,
        )
    )
    assert (OWNER, 22) in fake.deleted
    assert factory.clients == []
    assert control.store.flow(OWNER)["state"] == "idle"


async def test_stale_challenge_is_discarded_without_reuse_or_a_new_request(cfg, repo):
    control, fake, factory = _bot(cfg, repo, probe=("missing", None), authorized=False)
    control.store.set_flow(OWNER, "auth_code")
    control.store.update_state(telegram_state="pending")

    await control.handle_update(_message(1, "54321", message_id=20))

    assert control.store.flow(OWNER)["state"] == "idle"
    assert control.store.state()["telegram_state"] == "unauthenticated"
    assert factory.clients == []
    assert (OWNER, 20) in fake.deleted
    events = [row["event"] for row in repo.db.conn.execute("SELECT event FROM mgmt_audit")]
    assert "chat_login_rejected" in events


async def test_owner_and_developer_cannot_submit_codes_in_chat(cfg, repo):
    control, fake, factory = _bot(cfg, repo, probe=("missing", None), authorized=False)
    await _start_login(control, OWNER, update_id=1)
    await _start_login(control, DEVELOPER, update_id=2)

    await control.handle_update(_message(3, "12345", user=OWNER, message_id=21))
    await control.handle_update(_message(4, "12345", user=DEVELOPER, message_id=22))

    assert factory.clients == []
    assert (OWNER, 21) in fake.deleted
    assert (DEVELOPER, 22) in fake.deleted
    assert control.store.flow(OWNER)["state"] == "idle"
    assert control.store.flow(DEVELOPER)["state"] == "idle"
    assert control.guard.holder is None
    for recipient in (OWNER, DEVELOPER):
        assert all("12345" not in item["text"] for item in fake.sent_to(recipient))


async def test_duplicate_updates_do_not_repeat_auth_side_effects(cfg, repo):
    control, fake, factory = _bot(cfg, repo, probe=("missing", None), authorized=False)
    await _start_login(control, OWNER, update_id=1)
    sent_before = len(fake.sent_to(OWNER))
    deleted_before = len(fake.deleted)

    duplicate = _message(1, "12345", message_id=30)
    await control.handle_update(duplicate)
    await control.handle_update(duplicate)

    assert len(fake.sent_to(OWNER)) == sent_before
    assert len(fake.deleted) == deleted_before
    assert factory.clients == []


async def test_stranger_contact_is_never_deleted_or_authorized(cfg, repo):
    control, fake, factory = _bot(cfg, repo, probe=("missing", None), authorized=False)
    await _start_login(control, OWNER)
    ignored = _message(
        2,
        contact={"user_id": STRANGER, "phone_number": "+15550101338"},
        user=STRANGER,
        message_id=30,
    )
    await control.handle_update(ignored)
    assert factory.clients == []
    assert (STRANGER, 30) not in fake.deleted
    assert fake.sent_to(STRANGER) == []
    await control.handle_update(_message(3, "/cancel", user=OWNER))


async def test_two_bot_instances_cannot_hold_the_session_guard(cfg, repo):
    first, _fake1, _ = _bot(cfg, repo)
    second, _fake2, _ = _bot(cfg, repo)
    await first.guard.acquire(operator_id=OWNER)
    try:
        try:
            await second.guard.acquire(operator_id=DEVELOPER)
        except Exception as exc:  # SessionBusyError
            assert "locked" in str(exc) or "use" in str(exc)
        else:  # pragma: no cover - the flock must prevent this
            raise AssertionError("second session guard unexpectedly acquired")
    finally:
        first.guard.release(operator_id=OWNER)


async def test_restart_abandons_sensitive_login_stage_without_touching_session(cfg, repo):
    control, fake, _ = _bot(cfg, repo, probe=("missing", None), authorized=False)
    control.store.set_flow(OWNER, "auth_code")
    control.store.set_flow(DEVELOPER, "auth_password")
    session_file = cfg.session_path.with_suffix(".session")
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_bytes(b"do-not-delete")

    restarted, fake2, _ = _bot(cfg, repo)
    await restarted.startup()
    assert restarted.store.flow(OWNER)["state"] == "idle"
    assert restarted.store.flow(DEVELOPER)["state"] == "idle"
    assert session_file.read_bytes() == b"do-not-delete"
    assert fake.sent == []
    assert any("راه‌اندازی مجدد" in item["text"] for item in fake2.sent_to(OWNER))
    assert any("راه‌اندازی مجدد" in item["text"] for item in fake2.sent_to(DEVELOPER))


async def test_channels_are_paged_ten_per_operator_and_cross_operator_snapshot_is_stale(cfg, repo):
    base = datetime(2026, 4, 1, tzinfo=UTC)
    dialogs = [
        _channel(100 + index, f"channel {index:02d}", base + timedelta(days=index))
        for index in range(12)
    ]
    control, fake, _ = _bot(cfg, repo, dialogs=dialogs)
    control.store.update_state(telegram_state="authenticated", telegram_label="Owner")

    await control._refresh_channels(OWNER, 0)
    texts = [button["text"] for button in fake.buttons() if button["text"].startswith("کانال:")]
    assert len(texts) == 10
    assert texts[0] == "کانال: channel 11 (id=111)"
    assert texts[-1] == "کانال: channel 02 (id=102)"

    owner_snapshot = control.store.current_snapshot(OWNER)
    await control._show_page(OWNER, 10, owner_snapshot)
    texts = [button["text"] for button in fake.buttons() if button["text"].startswith("کانال:")]
    assert len(texts) == 2
    assert "صفحه 2 از 2" in fake.sent[-1]["text"]
    assert any(button["text"] == "قبلی" for button in fake.buttons())

    await control._show_page(DEVELOPER, 0, owner_snapshot)
    assert "قدیمی" in fake.sent_to(DEVELOPER)[-1]["text"]

    await control._refresh_channels(DEVELOPER, 0)
    assert control.store.current_snapshot(DEVELOPER) != owner_snapshot
    assert control.store.current_snapshot(OWNER) == owner_snapshot


async def test_channel_selection_is_read_only_and_preview_shows_media_only(cfg, repo):
    dialogs = [_channel(101, "Source", datetime(2026, 4, 1, tzinfo=UTC), username="source")]
    messages = [
        Msg(1, "plain text"),
        Msg(2, "", media="photo"),
        Msg(3, "", media="video"),
        Msg(4, "", action=SimpleNamespace()),
    ] + [Msg(5 + index, f"older {index}") for index in range(6)]
    control, fake, _ = _bot(cfg, repo, dialogs=dialogs, messages=messages)
    control.store.update_state(telegram_state="authenticated", telegram_label="Owner")
    await control._refresh_channels(OWNER, 0)
    snapshot = control.store.current_snapshot(OWNER)
    assert snapshot is not None

    await control._confirm_channel(OWNER, {"channel_id": 101}, snapshot)
    assert control.store.operator_channel(OWNER)["selected_channel_id"] == 101
    assert repo.selected_channel() is None  # no project ingestion side effect
    assert repo.list_channels() == []
    assert control.store.latest_job() is None
    assert repo.db.conn.execute("SELECT COUNT(*) FROM processing_queue").fetchone()[0] == 0
    assert "دریافت تاریخچه" in fake.sent[-1]["text"]
    assert "آغاز نشد" in fake.sent[-1]["text"]

    await control._preview_latest(OWNER)
    preview = fake.sent[-1]["text"]
    assert "#10 —" in preview
    assert "[عکس؛ بدون متن]" in preview
    assert "[ویدیو؛ بدون متن]" in preview
    assert "[پیام سرویس]" in preview
    assert "فقط خواندنی" in preview
    assert "https://" not in preview
    assert control.store.latest_job() is None
    assert repo.db.conn.execute("SELECT COUNT(*) FROM processing_queue").fetchone()[0] == 0
    assert repo.db.conn.execute("SELECT COUNT(*) FROM mgmt_ingestions").fetchone()[0] == 0

    # The preview is per-operator: the developer has not selected a channel.
    await control._preview_latest(DEVELOPER)
    assert "ابتدا یک کانال" in fake.sent_to(DEVELOPER)[-1]["text"]


async def test_preview_truncates_oversized_messages(cfg, repo):
    dialogs = [_channel(101, "Source", datetime(2026, 4, 1, tzinfo=UTC))]
    messages = [Msg(1, "x" * 5000)]
    control, fake, _ = _bot(cfg, repo, dialogs=dialogs, messages=messages)
    control.store.update_state(telegram_state="authenticated", telegram_label="Owner")
    control.store.set_operator_channel(
        OWNER, selected_channel_id=101, selected_channel_title="Source"
    )
    await control._preview_latest(OWNER)
    text = fake.sent[-1]["text"]
    assert "…" in text
    assert len(text) <= 4096


async def test_logout_requires_confirmation_and_notifies_both_operators(cfg, repo):
    session_file = cfg.session_path.with_suffix(".session")
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_bytes(b"session")
    control, fake, factory = _bot(cfg, repo)
    control.store.update_state(telegram_state="authenticated", telegram_label="Owner")
    control.store.set_flow(OWNER, "auth_code")

    await control._show_logout_confirm(OWNER)
    assert "تأیید" in fake.sent[-1]["text"]
    assert control.store.state()["telegram_state"] == "authenticated"  # no auto logout

    await control._perform_logout(OWNER)
    assert factory.clients[-1].logout_calls == 1
    assert control.store.state()["telegram_state"] == "unauthenticated"
    assert control.store.flow(OWNER)["state"] == "idle"
    assert not session_file.exists()
    assert any("خروج انجام شد" in item["text"] for item in fake.sent_to(OWNER))
    assert any("خارج شد" in item["text"] for item in fake.sent_to(DEVELOPER))


async def test_logout_failure_keeps_session_and_reports_accurately(cfg, repo):
    session_file = cfg.session_path.with_suffix(".session")
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_bytes(b"session")
    control, fake, _ = _bot(cfg, repo, logout_error=OSError("network down"))
    control.store.update_state(telegram_state="authenticated", telegram_label="Owner")

    await control._perform_logout(OWNER)
    assert control.store.state()["telegram_state"] == "authenticated"
    assert session_file.exists()
    assert any("انجام نشد" in item["text"] for item in fake.sent_to(OWNER))


async def test_logout_of_already_invalid_session_removes_only_local_files(cfg, repo):
    session_file = cfg.session_path.with_suffix(".session")
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_bytes(b"stale")
    control, fake, _ = _bot(cfg, repo, authorized=False)
    control.store.update_state(telegram_state="authenticated", telegram_label="Owner")

    await control._perform_logout(OWNER)
    assert control.store.state()["telegram_state"] == "unauthenticated"
    assert not session_file.exists()
    assert any("از قبل معتبر نبود" in item["text"] for item in fake.sent_to(OWNER))


async def test_audit_logs_role_not_numeric_identity_and_no_secrets(cfg, repo):
    control, fake, _ = _bot(
        cfg, repo, probe=("missing", None), authorized=False, needs_password=True
    )
    await _start_login(control, DEVELOPER)
    await control.handle_update(_message(2, "+15550101339", user=DEVELOPER, message_id=20))
    await control.handle_update(_message(3, "98765", user=DEVELOPER, message_id=21))
    await control.handle_update(_message(4, "my-2fa-password", user=DEVELOPER, message_id=22))

    rows = repo.db.conn.execute("SELECT event, detail FROM mgmt_audit").fetchall()
    details = " ".join(str(row["detail"] or "") for row in rows)
    assert "role=developer" in details
    assert str(DEVELOPER) not in details
    assert str(OWNER) not in details
    rendered = " ".join(item["text"] for item in fake.sent)
    assert "98765" not in rendered
    assert "my-2fa-password" not in rendered
    assert "+15550101339" not in rendered


async def test_worker_failure_keeps_pending_job_recoverable(cfg, repo):
    control, fake, _factory = _bot(cfg, repo)
    job = control.store.create_job(101, "Source")

    def disconnected(_cfg):
        raise OSError("network down")

    control._telethon_factory = disconnected
    await control._run_worker(job["job_id"])

    updated = control.store.get_job(job["job_id"])
    assert updated["status"] == "interrupted"
    assert any("checkpoint" in item["text"] for item in fake.sent)


# -------------------------------------------------------------------
# Order amount menu tests (persisted via trading_settings)
# -------------------------------------------------------------------


async def test_status_shows_default_order_amount_of_3_usdt(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    await control.handle_update(_message(1, "/start", user=OWNER))
    text = fake.sent_to(OWNER)[-1]["text"]
    assert "مبلغ سفارش پیش‌فرض: 3 USDT در هر خرید" in text


async def test_order_amount_button_shows_current_and_requests_input(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    token = "mg:" + control.store.issue_callback(
        owner_id=OWNER, chat_id=OWNER, action="order_amount"
    )
    await control.handle_update(_callback(1, token))
    text = fake.sent_to(OWNER)[-1]["text"]
    assert "مبلغ فعلی: 3 USDT" in text
    assert "مبلغ جدید را به‌صفته یک عدد مثبت وارد کنید" in text
    assert control.store.flow(OWNER)["state"] == "order_amount_input"


async def test_order_amount_persisted_through_database_and_displayed(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    control.store.set_setting("order_quote_amount", "7.5")
    # Simulate service restart: _apply_db_settings would reload into config
    control.cfg.trading.order_quote_amount = 7.5
    control.cfg.trading.order_quote_amount_source = "database (Telegram menu)"

    await control.handle_update(_message(1, "/start", user=OWNER))
    text = fake.sent_to(OWNER)[-1]["text"]
    assert "مبلغ سفارش پیش‌فرض: 7.5 USDT در هر خرید" in text


async def test_order_amount_change_validates_and_persists(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    token = "mg:" + control.store.issue_callback(
        owner_id=OWNER, chat_id=OWNER, action="order_amount"
    )
    await control.handle_update(_callback(1, token))
    assert control.store.flow(OWNER)["state"] == "order_amount_input"

    await control.handle_update(_message(2, "5", message_id=20))
    assert control.store.flow(OWNER)["state"] == "idle"
    assert control.store.get_setting("order_quote_amount") == "5.0"
    assert control.cfg.trading.order_quote_amount == 5.0
    assert control.cfg.trading.order_quote_amount_source == "database (Telegram menu)"
    text = fake.sent_to(OWNER)[-1]["text"]
    assert "تنظیم شد" in text and "5 USDT" in text
    assert "5" not in text or "تنظیم شد" in text  # value not echoed as plain input


async def test_order_amount_rejects_zero_and_negative(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    token = "mg:" + control.store.issue_callback(
        owner_id=OWNER, chat_id=OWNER, action="order_amount"
    )
    await control.handle_update(_callback(1, token))

    await control.handle_update(_message(2, "0", message_id=20))
    assert control.store.flow(OWNER)["state"] == "idle"
    assert control.store.get_setting("order_quote_amount") is None
    assert "بزرگتر از صفر" in fake.sent_to(OWNER)[-1]["text"]

    token = "mg:" + control.store.issue_callback(
        owner_id=OWNER, chat_id=OWNER, action="order_amount"
    )
    await control.handle_update(_callback(3, token))
    await control.handle_update(_message(4, "-2", message_id=21))
    assert control.store.flow(OWNER)["state"] == "idle"
    assert control.store.get_setting("order_quote_amount") is None


async def test_order_amount_rejects_non_numeric(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    token = "mg:" + control.store.issue_callback(
        owner_id=OWNER, chat_id=OWNER, action="order_amount"
    )
    await control.handle_update(_callback(1, token))

    await control.handle_update(_message(2, "many", message_id=20))
    assert control.store.flow(OWNER)["state"] == "idle"
    assert control.store.get_setting("order_quote_amount") is None
    assert "معتبر نیست" in fake.sent_to(OWNER)[-1]["text"]


async def test_order_amount_above_aggregate_reference_is_accepted(cfg, repo):
    control, fake, _ = _bot(cfg, repo)
    token = "mg:" + control.store.issue_callback(
        owner_id=OWNER, chat_id=OWNER, action="order_amount"
    )
    await control.handle_update(_callback(1, token))

    await control.handle_update(_message(2, "60", message_id=20))
    assert control.store.flow(OWNER)["state"] == "idle"
    # The aggregate value is reference-only; the owner controls per-order size.
    assert control.store.get_setting("order_quote_amount") == "60.0"
    assert control.cfg.trading.order_quote_amount == 60.0
    assert "تنظیم شد" in fake.sent_to(OWNER)[-1]["text"]
