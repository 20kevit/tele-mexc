"""End-to-end control bot behavior with fake Bot API, Telethon and engine."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from tg_signal_ingest.control.service import ControlBot
from tg_signal_ingest.control.store import ControlStore
from tg_signal_ingest.normalize import message_to_record

OWNER = 4242
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
        self.edited.append(
            {"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup}
        )
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

    def execute_approved_intent(self, intent_id):
        self.executed.append(intent_id)
        return dict(self.result)

    def balance_check(self, _intent):
        return None


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


def _full_setup(store: ControlStore, *, completed: bool = True) -> None:
    store.update_setup(
        telegram_state="authenticated",
        telegram_account_label="Owner",
        mexc_state="verified",
        mexc_label="Spot trade + read",
        selected_channel_id=CHANNEL,
        selected_channel_title="Signals",
        learning_state="completed" if completed else "in_progress",
        learning_streak=5 if completed else 0,
        execution_mode="approval",
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
    assert "یادگیری: 5 از 5" in text
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


async def test_learning_requires_five_valid_examples_and_never_enables_trading(cfg, repo):
    _add_channel(repo)
    for index in range(1, 6):
        repo.upsert_message(
            message_to_record(
                CHANNEL,
                Msg(index, f"BUY BTCUSDT ENTRY 100 TP 110 SL 95 #{index}"),
                "live",
            )
        )
    store = ControlStore(repo.db)
    _full_setup(store, completed=False)
    control, fake, _engine = _bot(cfg, repo)

    for index in range(5):
        await control.handle_update(_callback(100 + index * 2, "m:learn_next"))
        approve = next(btn for btn in fake.last_buttons() if btn["callback_data"].startswith("lr:y:"))
        await control.handle_update(_callback(101 + index * 2, approve["callback_data"]))

    setup = store.setup()
    assert setup["learning_streak"] == 5
    assert setup["learning_state"] == "completed"
    assert setup["execution_mode"] == "approval"
    assert control.should_require_approval({}) is True


async def test_no_signal_approval_prompt_is_created(cfg, repo):
    """Eligible signals are evaluated automatically; tick() must not create
    per-signal approval prompts."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    _add_intent(repo)
    control, fake, engine = _bot(cfg, repo)

    await control.tick()
    # No new approval prompt is created for the planned intent.
    assert store.list_approvals("pending", 1) == []
    assert engine.executed == []
    assert not any(btn["callback_data"].startswith("ap:y:") for btn in fake.last_buttons())


async def test_approval_expiration_does_not_cancel_or_resubmit_intent(cfg, repo):
    """An expired approval record must never cancel or resubmit an intent.
    The trading engine owns the intent lifecycle."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    _add_intent(repo)
    control, fake, engine = _bot(cfg, repo)

    # Manually create a legacy pending approval, then set it to expired.
    approval, _created = store.create_approval(
        intent_id="intent-1", signal_id="sig-1",
        intent_version="v1",
    )
    now = datetime.now(UTC)
    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE control_approvals SET expires_at = ? WHERE approval_id = ?",
            (
                (now - timedelta(seconds=1)).isoformat(timespec="seconds").replace("+00:00", "Z"),
                approval["approval_id"],
            ),
        )
    await control.tick()  # expire_approvals marks it expired, does not touch intent
    assert store.get_approval(approval["approval_id"])["status"] == "expired"
    # Intent state is unchanged: no cancellation, no resubmission.
    assert engine.store.get_intent("intent-1")["state"] == "planned"
    assert engine.executed == []


async def test_legacy_approval_callback_is_rejected(cfg, repo):
    """Legacy ap:y:/ap:n: callbacks no longer execute any order."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    _add_intent(repo)
    control, fake, engine = _bot(cfg, repo)

    await control.handle_update(_callback(100, "ap:y:dummy-approval-id:dummy-nonce"))
    assert "حذف شد" in fake.sent[-1]["text"] or "حذف است" in fake.sent[-1]["text"]
    assert engine.executed == []


async def test_tick_does_not_create_signal_approval_or_send_timeout(cfg, repo):
    """The control bot's tick() must not create per-signal approval prompts
    or send approval-timeout messages.  Signals are auto-executed by the
    trading engine."""
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    _add_intent(repo)
    control, fake, engine = _bot(cfg, repo)

    before_sent = len(fake.sent)
    await control.tick()
    # No new messages (no approval prompt, no timeout).
    assert len(fake.sent) == before_sent
    # No approval records created.
    assert store.list_approvals("pending", 1) == []


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
    assert "حالت معامله:" in text
    assert "مجوز لایو:" in text
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


async def test_auto_mode_toggle_is_administrative_control(cfg, repo):
    """Auto-mode enable/disable remains a genuine administrative control,
    separate from per-signal approval."""
    cfg.trading.order_quote_amount = 10.0
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, fake, engine = _bot(cfg, repo)

    assert store.setup()["execution_mode"] == "approval"
    assert control.should_require_approval({}) is True

    await control.handle_update(_callback(100, "m:auto"))
    await control.handle_update(_callback(101, "m:auto2"))
    assert store.setup()["execution_mode"] == "auto"
    assert control.should_require_approval({}) is False

    await control.handle_update(_callback(102, "m:manual"))
    assert store.setup()["execution_mode"] == "approval"


async def test_auto_mode_requires_two_steps_and_restart_clears_it(cfg, repo):
    cfg.trading.order_quote_amount = 10.0
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    control, _fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(100, "m:auto"))
    assert store.setup()["execution_mode"] == "approval"
    assert control.should_require_approval({}) is True

    await control.handle_update(_callback(101, "m:auto2"))
    assert store.setup()["execution_mode"] == "auto"
    assert control.should_require_approval({}) is False

    await control.handle_update(_callback(102, "m:pause"))
    assert store.setup()["execution_mode"] == "approval"
    assert int(store.setup()["paused"]) == 1
    assert control.should_require_approval({}) is True
    await control.handle_update(_callback(103, "m:auto2"))
    assert store.setup()["execution_mode"] == "approval"

    store.update_setup(paused=0, execution_mode="auto")
    store.refresh_setup_state()
    restarted, _fake2, _engine2 = _bot(cfg, repo)
    assert store.setup()["execution_mode"] == "approval"
    assert restarted.should_require_approval({}) is True


async def test_auto_mode_is_refused_until_learning_and_mexc_are_ready(cfg, repo):
    cfg.trading.order_quote_amount = 10.0
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store, completed=False)
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(100, "m:auto"))
    assert store.setup()["execution_mode"] == "approval"
    assert "پنج نمونه" in fake.sent[-1]["text"]


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
    assert "state: ready" in out
    assert f"owner user id: {OWNER}" in out
    assert "bot token: set" in out
    assert "pending approvals: 0" in out
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
    assert store.setup()["execution_mode"] == "approval"


def test_cli_control_runtime_no_longer_installs_approval_hooks(cfg, repo):
    from tg_signal_ingest import main as main_module

    cfg.control.bot_token = "123:test-token"
    cfg.control.owner_user_id = OWNER
    cfg.control.secret_key = "test-control-secret"
    cfg.control.enabled = True
    cfg.trading.mode = "sim"
    bot, engine = main_module._build_control_runtime(cfg, repo)
    assert bot._engine is engine
    assert not hasattr(engine, "approval_gate")
    assert not hasattr(engine, "sizing_provider")


async def test_auto_mode_is_refused_without_owner_fixed_amount(cfg, repo):
    _add_channel(repo)
    store = ControlStore(repo.db)
    _full_setup(store)
    cfg.trading.order_quote_amount = None
    control, fake, _engine = _bot(cfg, repo)

    await control.handle_update(_callback(100, "m:auto"))
    assert store.setup()["execution_mode"] == "approval"
    assert "مبلغ" in fake.sent[-1]["text"]
