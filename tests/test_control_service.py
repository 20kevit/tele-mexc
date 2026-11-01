"""End-to-end control bot behavior with fake Bot API, Telethon and engine."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from tg_signal_ingest.control.service import ControlBot
from tg_signal_ingest.control.store import ControlStore

OWNER = 4242
DEVELOPER = 7777
STRANGER = 999
CHANNEL = -1001234567890


class FakeBot:
    def __init__(self):
        self.sent: list[dict] = []
        self.edited: list[dict] = []
        self.answered: list[str] = []
        self.deleted: list[tuple[int, int]] = []
        self.commands: list[list[dict]] = []

    def send_message(self, chat_id, text, *, reply_markup=None):
        self.sent.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"message_id": len(self.sent)}

    def edit_message_text(self, chat_id, message_id, text, *, reply_markup=None):
        self.edited.append({"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup})
        return {}

    def answer_callback_query(self, callback_id, text=None):
        self.answered.append(callback_id)
        return True

    def delete_message(self, chat_id, message_id):
        self.deleted.append((chat_id, message_id))
        return True

    def set_my_commands(self, commands):
        self.commands.append(commands)
        return True

    def get_updates(self, **_kwargs):
        return []

    def last_buttons(self) -> list[dict]:
        for item in reversed(self.sent):
            markup = item.get("reply_markup") or {}
            if markup.get("inline_keyboard"):
                return [button for row in markup["inline_keyboard"] for button in row]
        return []


class Msg:
    def __init__(self, mid: int, text: str):
        self.id = mid
        self.message = text
        self.date = datetime.now(UTC)
        self.edit_date = None
        self.media = None

    def to_dict(self):
        return {}


class FakeTradingStore:
    def __init__(self, repo):
        self.repo = repo
        self.transitions: list[tuple[str, str]] = []

    def _row(self, sql, params):
        row = self.repo.db.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def get_signal(self, signal_id):
        return self._row("SELECT * FROM signals WHERE signal_id = ?", (signal_id,))

    def get_intent(self, intent_id):
        return self._row("SELECT * FROM order_intents WHERE intent_id = ?", (intent_id,))

    def transition_intent(self, intent_id, state, note=""):
        now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self.repo.db.transaction() as conn:
            conn.execute(
                "UPDATE order_intents SET state = ?, updated_at = ? WHERE intent_id = ?",
                (state, now, intent_id),
            )
        self.transitions.append((intent_id, state))


class FakeEngine:
    def __init__(self, repo):
        self.store = FakeTradingStore(repo)
        self.executed: list[str] = []
        self.result = {
            "state": "filled",
            "exchange_order_id": "ex-1",
            "executed_qty": "0.1",
            "cumulative_quote": "10",
        }

    def execute_intent(self, intent_id):
        self.executed.append(intent_id)
        return dict(self.result)


def _message(update_id: int, text: str, *, user: int = OWNER, message_id: int | None = None):
    mid = message_id or update_id
    return {
        "update_id": update_id,
        "message": {
            "message_id": mid,
            "from": {"id": user},
            "chat": {"id": user, "type": "private"},
            "text": text,
        },
    }


def _callback(update_id: int, data: str, *, user: int = OWNER):
    return {
        "update_id": update_id,
        "callback_query": {
            "id": f"cb-{update_id}",
            "from": {"id": user},
            "message": {"message_id": 99, "chat": {"id": user}},
            "data": data,
        },
    }


def _full_setup(store: ControlStore) -> None:
    store.update_setup(
        telegram_state="authenticated",
        telegram_account_label="Owner",
        mexc_state="verified",
        mexc_label="Spot trade + read",
        selected_channel_id=CHANNEL,
        selected_channel_title="Signals",
    )
    store.refresh_setup_state()


def _bot(cfg, repo, probe=("valid", "Owner")) -> tuple[ControlBot, FakeBot, FakeEngine]:
    cfg.control.owner_user_id = OWNER
    cfg.control.secret_key = "test-control-secret"
    fake = FakeBot()
    control = ControlBot(cfg, repo, fake, probe_override=lambda: probe)
    engine = FakeEngine(repo)
    control.attach_engine(engine)
    return control, fake, engine


def _add_channel(repo) -> None:
    repo.ensure_channel(CHANNEL, 1, "signals", "Signals")
    repo.select_channel(CHANNEL)


def _add_intent(repo, *, intent_id="intent-1", signal_id="sig-1") -> None:
    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    with repo.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO signals
                (signal_id, channel_id, message_id, edit_version, parser_version,
                 signal_kind, symbol, action, market_type, target_prices, stop_loss,
                 order_type, fingerprint_sha256, status, created_at, updated_at)
            VALUES (?, ?, 1, 0, 1, 'alert', 'BTCUSDT', 'buy', 'spot', '110', '95',
                    'limit', 'fp-1', 'planned', ?, ?)
            """,
            (signal_id, CHANNEL, now, now),
        )
        conn.execute(
            """
            INSERT INTO order_intents
                (intent_id, signal_id, idempotency_key, client_order_id, symbol, side,
                 order_type, quote_amount, base_quantity, limit_price, mode, state,
                 create_msg_id, create_channel_id, planned_at, updated_at)
            VALUES (?, ?, ?, ?, 'BTCUSDT', 'buy', 'limit', '10', '0.1', '100', 'sim',
                    'planned', 1, ?, ?, ?)
            """,
            (intent_id, signal_id, f"idem-{intent_id}", f"client-{intent_id}", CHANNEL, now, now),
        )


async def test_start_requires_owner_and_resumes_persisted_setup(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    before = store.setup()
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_message(1, "/start", user=999))
    assert "دسترسی مجاز نیست" in fake.sent[-1]["text"]
    assert store.setup()["effective_state"] == before["effective_state"]

    await control.handle_update(_message(2, "/start"))
    text = fake.sent[-1]["text"]
    assert "وضعیت سیستم" in text
    assert "کانال: Signals" in text
    assert store.setup()["effective_state"] == before["effective_state"]
    assert store.flow()["state"] == "idle"


async def test_duplicate_update_id_is_processed_once(cfg, repo):
    control, fake, _engine = _bot(cfg, repo)
    update = _message(10, "/help")
    await control.handle_update(update)
    await control.handle_update(update)
    assert len(fake.sent) == 1


async def test_existing_valid_session_requires_explicit_adoption(cfg, repo):
    control, fake, _engine = _bot(cfg, repo)
    await control.handle_update(_message(1, "/start"))
    assert "ناسازگاری" in fake.sent[-1]["text"]
    assert ControlStore(repo.db).setup()["telegram_state"] == "unauthenticated"

    await control.handle_update(_callback(2, "m:adopt"))
    assert ControlStore(repo.db).setup()["telegram_state"] == "authenticated"
    assert any("پذیرش" not in item["text"] for item in fake.sent)


class FakeTelethon:
    def __init__(self):
        self.authorized = False
        self.connected = False
        self.disconnects = 0
        self.code_requests: list[str] = []
        self.sign_ins: list[dict] = []
        self.stage = "code"

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnects += 1
        self.connected = False

    async def is_user_authorized(self):
        return self.authorized

    async def send_code_request(self, phone):
        self.code_requests.append(phone)
        return SimpleNamespace(phone_code_hash="hash")

    async def get_me(self):
        return SimpleNamespace(first_name="Owner", last_name=None, username="owner")

    async def sign_in(self, **kwargs):
        self.sign_ins.append(kwargs)
        if "password" in kwargs:
            self.authorized = True
            return None
        from telethon.errors import SessionPasswordNeededError

        raise SessionPasswordNeededError(None)


async def test_chat_login_is_disabled_and_stale_challenge_is_discarded(cfg, repo):
    telethon = FakeTelethon()
    fake = FakeBot()
    cfg.control.owner_user_id = OWNER
    cfg.control.secret_key = "test-control-secret"
    control = ControlBot(
        cfg,
        repo,
        fake,
        telethon_factory=lambda _cfg: telethon,
        probe_override=lambda: ("missing", None),
    )

    await control.handle_update(_message(1, "/start"))
    assert ".venv/bin/tg-ingest auth login" in fake.sent[-1]["text"]
    assert "ترمینال امن SSH" in fake.sent[-1]["text"]

    await control.handle_update(_callback(3, "m:auth"))
    assert ControlStore(repo.db).flow()["state"] == "idle"
    assert telethon.code_requests == []
    guidance = fake.sent[-1]["text"]
    assert ".venv/bin/tg-ingest auth login" in guidance
    assert "غیرفعال" in guidance

    # A stale challenge from an older deployment cannot be reused.
    ControlStore(repo.db).set_flow("auth_code")
    ControlStore(repo.db).update_setup(telegram_state="pending")
    await control.handle_update(_message(4, "12345", message_id=4))
    assert ControlStore(repo.db).flow()["state"] == "idle"
    assert telethon.code_requests == []
    assert telethon.sign_ins == []
    assert fake.deleted[-1] == (OWNER, 4)
    audit = repo.db.conn.execute("SELECT event, detail FROM control_audit").fetchall()
    events = " ".join(str(row["event"] or "") for row in audit)
    persisted = " ".join(str(row["detail"] or "") for row in audit)
    assert "chat_login_rejected" in events
    assert "12345" not in persisted


async def test_legacy_order_approval_callback_is_inert_and_tick_is_safe(cfg, repo):
    """Legacy order-approval callbacks cannot execute orders; tick stays inert."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    _add_intent(repo)
    control, fake, engine = _bot(cfg, repo)

    await control.handle_update(_callback(100, "ap:y:legacy-id:legacy-nonce"))
    assert engine.executed == []
    assert store.setup()["effective_state"] == "automatic"
    await control.tick()
    assert engine.executed == []


async def test_version_command_reports_runtime_info(cfg, repo):
    """The /version command returns non-secret runtime info to the owner."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_message(10, "/version"))
    text = fake.sent[-1]["text"]
    assert "نسخه اجرا" in text
    assert "نسخه برنامه:" in text
    assert "commit SHA:" in text
    assert "شاخه:" in text
    assert "حالت معامله:" not in text
    assert "مجوز لایو:" not in text
    # No secrets leaked.
    assert "api_key" not in text.lower()
    assert "secret" not in text.lower()
    assert "token" not in text.lower()


async def test_version_command_is_owner_only(cfg, repo):
    """Unauthorized users must not receive /version output."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_message(10, "/version", user=999))
    assert "دسترسی مجاز نیست" in fake.sent[-1]["text"]


async def test_pause_and_resume_are_administrative_controls(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, _fake, _engine = _bot(cfg, repo)

    assert store.setup()["effective_state"] == "automatic"
    await control.handle_update(_callback(100, "m:pause"))
    assert store.setup()["effective_state"] == "paused"
    await control.handle_update(_callback(101, "m:resume"))
    assert store.setup()["effective_state"] == "automatic"


def test_control_status_cli_is_offline_and_redacts_secrets(cfg, repo, monkeypatch, capsys):
    from tg_signal_ingest import main as main_module

    cfg.control.bot_token = "123:very-secret-token"
    cfg.control.owner_user_id = OWNER
    cfg.control.secret_key = "control-encryption-secret"
    store = ControlStore(repo.db)
    _full_setup(store)
    monkeypatch.setattr(main_module, "_load_or_exit", lambda **_kwargs: cfg)

    assert main_module.cmd_control_status(None) == 0
    out = capsys.readouterr().out
    assert "state: automatic" in out
    assert f"owner A user id: {OWNER}" in out
    assert "bot token: set" in out
    assert "execution paused: False" in out
    assert "valid signals execute automatically" in out
    assert "very-secret-token" not in out
    assert "control-encryption-secret" not in out


def test_encrypted_mexc_credentials_are_restored_on_restart(cfg, repo):
    from tg_signal_ingest.control.secretbox import SecretBox

    cfg.control.owner_user_id = OWNER
    cfg.control.secret_key = "test-control-secret"
    box = SecretBox(cfg.control.secret_key)
    store = ControlStore(repo.db)
    store.put_secret("mexc_api_key", "restored-key", box)
    store.put_secret("mexc_api_secret", "restored-secret", box)
    assert cfg.mexc.api_key is None

    control, _fake, _engine = _bot(cfg, repo)
    assert cfg.mexc.api_key == "restored-key"
    assert cfg.mexc.api_secret == "restored-secret"
    text = str([dict(row) for row in repo.db.conn.execute("SELECT * FROM control_audit")])
    assert "restored-key" not in text
    assert "restored-secret" not in text
    assert control.cfg.mexc.api_key == "restored-key"


class MexcOk:
    def account(self):
        return {"canTrade": True, "permissions": ["SPOT"], "enableWithdrawals": False}

    def api_key_info(self):
        return {"permissions": ["SPOT"]}


async def test_mexc_flow_stores_encrypted_secret_and_never_echoes_it(cfg, repo):
    from tg_signal_ingest.control.secretbox import SecretBox

    control, fake, _engine = _bot(cfg, repo)
    control._mexc_client_factory = lambda _key, _secret: MexcOk()

    await control.handle_update(_callback(100, "mx:replace"))
    assert ControlStore(repo.db).flow()["state"] == "mexc_key"
    await control.handle_update(_message(101, "new-api-key-123", message_id=501))
    assert ControlStore(repo.db).flow()["state"] == "mexc_secret"
    await control.handle_update(_message(102, "new-api-secret-456", message_id=502))

    setup = ControlStore(repo.db).setup()
    assert setup["mexc_state"] == "verified"
    assert control.cfg.mexc.api_key == "new-api-key-123"
    box = SecretBox(cfg.control.secret_key)
    assert ControlStore(repo.db).get_secret("mexc_api_secret", box) == "new-api-secret-456"
    assert fake.deleted[-2:] == [(OWNER, 501), (OWNER, 502)]
    assert all("new-api-secret-456" not in item["text"] for item in fake.sent)
    rows = repo.db.conn.execute("SELECT * FROM control_secrets").fetchall()
    assert all("new-api-secret-456" not in str(dict(row)) for row in rows)


async def test_disconnect_requires_explicit_second_confirmation(cfg, repo, monkeypatch):
    from tg_signal_ingest import telethon as telethon_module

    calls: list[int] = []

    async def fake_logout(_cfg):
        calls.append(1)

    monkeypatch.setattr(telethon_module, "logout_and_delete_session", fake_logout)
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(100, "au:disconnect"))
    assert calls == []
    assert "تأیید" in fake.sent[-1]["text"]
    assert store.setup()["telegram_state"] == "authenticated"

    await control.handle_update(_callback(101, "au:disconnect2"))
    assert calls == [1]
    assert store.setup()["telegram_state"] == "unauthenticated"


def test_cli_control_runtime_no_longer_installs_approval_hooks(cfg, repo, monkeypatch):
    from tg_signal_ingest import main as main_module

    cfg.control.bot_token = "123:test-token"
    cfg.control.owner_user_id = OWNER
    cfg.control.secret_key = "test-control-secret"
    cfg.control.enabled = True

    def _fake_builder(_cfg, _repo):
        return FakeEngine(_repo)

    monkeypatch.setattr("tg_signal_ingest.trading.build_trading_engine", _fake_builder)
    bot, engine = main_module._build_control_runtime(cfg, repo)
    assert bot._engine is engine
    assert not hasattr(engine, "approval_gate")
    assert not hasattr(engine, "sizing_provider")


def _bot_with_developer(cfg, repo, probe=("valid", "Owner")) -> tuple[ControlBot, FakeBot, FakeEngine]:
    cfg.control.owner_user_id = OWNER
    cfg.control.developer_user_id = DEVELOPER
    cfg.control.secret_key = "test-control-secret"
    fake = FakeBot()
    control = ControlBot(cfg, repo, fake, probe_override=lambda: probe)
    engine = FakeEngine(repo)
    control.attach_engine(engine)
    return control, fake, engine


async def test_developer_receives_read_only_start_and_version(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot_with_developer(cfg, repo)

    # Programmer /start is accepted (not rejected) and shows system status.
    await control.handle_update(_message(1, "/start", user=DEVELOPER))
    text = fake.sent[-1]["text"]
    assert "دسترسی مجاز نیست" not in text
    assert "وضعیت سیستم" in text

    # Programmer /version is accepted and returns runtime info.
    await control.handle_update(_message(2, "/version", user=DEVELOPER))
    text = fake.sent[-1]["text"]
    assert "دسترسی مجاز نیست" not in text
    assert "نسخه اجرا" in text


async def test_developer_cannot_pause_or_resume(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot_with_developer(cfg, repo)
    before = int(store.setup()["paused"])

    # The programmer is not an owner: state-changing callbacks are refused.
    await control.handle_update(_callback(100, "m:pause", user=DEVELOPER))
    assert int(store.setup()["paused"]) == before
    await control.handle_update(_callback(101, "m:resume", user=DEVELOPER))
    assert int(store.setup()["paused"]) == before

    # Text commands that change execution state are refused too.
    await control.handle_update(_message(102, "/pause", user=DEVELOPER))
    assert int(store.setup()["paused"]) == before
    assert "فقط برای مالکان" in fake.sent[-1]["text"]


async def test_second_owner_has_owner_level_controls(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot_with_developer(cfg, repo)
    cfg.control.owner_b_user_id = 8888

    await control.handle_update(_message(1, "/start", user=8888))
    assert "دسترسی مجاز نیست" not in fake.sent[-1]["text"]
    await control.handle_update(_callback(2, "m:pause", user=8888))
    assert int(store.setup()["paused"]) == 1
    await control.handle_update(_callback(3, "m:resume", user=8888))
    assert int(store.setup()["paused"]) == 0


async def test_wrong_chat_and_replayed_callbacks_are_refused(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot_with_developer(cfg, repo)
    before = int(store.setup()["paused"])

    # An owner identity outside its own private chat cannot pause the system.
    forged = _callback(100, "m:pause", user=OWNER)
    forged["callback_query"]["message"]["chat"] = {"id": 999999, "type": "group"}
    await control.handle_update(forged)
    assert int(store.setup()["paused"]) == before

    # Replaying the same Telegram update_id is a no-op.
    pause = _callback(101, "m:pause", user=OWNER)
    await control.handle_update(pause)
    assert int(store.setup()["paused"]) == 1
    await control.handle_update(pause)
    assert int(store.setup()["paused"]) == 1


async def test_stranger_remains_blocked_for_all_control_commands(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot_with_developer(cfg, repo)

    for command in ("/start", "/status", "/version", "/pause", "/resume", "/help"):
        await control.handle_update(_message(10, command, user=STRANGER))
        assert "دسترسی مجاز نیست" in fake.sent[-1]["text"], command

    # Stranger callbacks are rejected too: answered but no status message sent.
    before = len(fake.sent)
    await control.handle_update(_callback(20, "m:status", user=STRANGER))
    assert fake.answered[-1] == "cb-20"
    assert len(fake.sent) == before  # no state-changing response to a stranger


async def test_owner_still_authorized_after_developer_added(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot_with_developer(cfg, repo)

    # Owner access is preserved alongside the developer.
    await control.handle_update(_message(1, "/start", user=OWNER))
    assert "دسترسی مجاز نیست" not in fake.sent[-1]["text"]
    assert "وضعیت سیستم" in fake.sent[-1]["text"]

    await control.handle_update(_callback(2, "m:pause", user=OWNER))
    assert int(store.setup()["paused"]) == 1


async def test_developer_is_blocked_when_not_configured(cfg, repo):
    """If CONTROL_DEVELOPER_USER_ID is not set, only the owner is authorized."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)  # owner only, developer not set

    await control.handle_update(_message(1, "/start", user=DEVELOPER))
    assert "دسترسی مجاز نیست" in fake.sent[-1]["text"]


async def test_learning_workflow_is_removed_from_control_bot(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_message(1, "/start"))
    keyboard = fake.sent[-1].get("reply_markup") or {}
    callbacks = [button["callback_data"] for row in keyboard.get("inline_keyboard", []) for button in row]
    assert not any(callback in ("m:learn", "m:learn_next") or callback.startswith("lr:") for callback in callbacks)
    assert "یادگیری" not in fake.sent[-1]["text"]

    await control.handle_update(_callback(2, "lr:y:legacy-sample"))
    assert any("دکمه نامعتبر" in item["text"] for item in fake.sent)


async def test_notification_severity_is_one_selected_level(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(1, "n:settings"))
    keyboard = fake.sent[-1].get("reply_markup") or {}
    callbacks = [button["callback_data"] for row in keyboard.get("inline_keyboard", []) for button in row]
    assert {f"n:set:{level}" for level in store.NOTIFICATION_LEVELS} <= set(callbacks)
    assert not any(callback.startswith("n:toggle:") for callback in callbacks)

    await control.handle_update(_callback(2, "n:set:WARNING"))
    assert store.notification_min_severity() == "WARNING"
    assert any("WARNING" in item["text"] for item in fake.sent)


async def test_control_buy_amount_writes_the_execution_setting(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(1, "m:buy"))
    await control.handle_update(_message(2, "7.25"))

    assert store.get_setting("order_quote_amount") == "7.25"
    assert control.cfg.trading.order_quote_amount == pytest.approx(7.25)
    assert control.cfg.trading.order_quote_amount_source == "database (Telegram menu)"
    assert store.get_setting("buy_quote_usd") is None
    assert any("ذخیره شد" in item["text"] for item in fake.sent)


async def test_control_buy_amount_invalid_values_do_not_change_execution(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)
    original = control.cfg.trading.order_quote_amount

    await control.handle_update(_callback(1, "m:buy"))
    for value in ("0", "-3", "nan", "inf", "not-a-number"):
        await control.handle_update(_message(2, value))
        assert store.get_setting("order_quote_amount") is None
        assert control.cfg.trading.order_quote_amount == original
        assert any("معتبر" in item["text"] or "بزرگ‌تر" in item["text"] for item in fake.sent)


async def test_control_sizing_ui_has_no_sell_percentage_flow(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(1, "m:sizing"))
    keyboard = fake.sent[-1].get("reply_markup") or {}
    callbacks = [button["callback_data"] for row in keyboard.get("inline_keyboard", []) for button in row]
    assert "m:sell" not in callbacks
    assert not any("فروش" in item["text"] for item in fake.sent[-1:])

    await control.handle_update(_callback(2, "m:sell"))
    assert any("گزینه نامعتبر" in item["text"] for item in fake.sent)


async def test_control_buy_amount_respects_environment_precedence(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    cfg.trading.order_quote_amount = 4.25
    cfg.trading.order_quote_amount_source = "env"
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(1, "m:buy"))
    await control.handle_update(_message(2, "9.5"))

    assert store.get_setting("order_quote_amount") is None
    assert control.cfg.trading.order_quote_amount == 4.25
    assert any("TRADING_ORDER_QUOTE_AMOUNT" in item["text"] for item in fake.sent)
