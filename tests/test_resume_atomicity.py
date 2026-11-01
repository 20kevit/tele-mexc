"""Atomic resume: readiness and durable unpause share one lock/transaction."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.control.service import ControlBot
from tg_signal_ingest.control.store import ControlStore
from tg_signal_ingest.repository import Repository
from tg_signal_ingest.trading.gate import GateTimeout, SubmissionGate
from tg_signal_ingest.trading.parser import SignalParser
from tg_signal_ingest.trading.store import TradingStore

OWNER = 100000001
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
ALERT = "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120"


class FakeBot:
    def __init__(self, repo: Repository | None = None):
        self.sent: list[dict] = []
        self.repo = repo

    def send_message(self, chat_id: int, text: str, reply_markup=None):
        paused = None
        if self.repo is not None:
            paused = ControlStore(self.repo.db).setup().get("paused")
        self.sent.append({"chat_id": chat_id, "text": text, "paused_at_send": paused})
        return {"message_id": len(self.sent)}


def _message(update_id: int, text: str, *, user: int = OWNER) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "from": {"id": user},
            "chat": {"id": user, "type": "private"},
            "text": text,
        },
    }


def _control(cfg: AppConfig, repo: Repository, *, bot: FakeBot | None = None) -> tuple[ControlBot, FakeBot]:
    cfg.control.owner_user_id = OWNER
    cfg.control.secret_key = "test-control-secret"
    fake = bot or FakeBot(repo)
    control = ControlBot(
        cfg,
        repo,
        fake,
        probe_override=lambda: ("valid", "Owner"),
    )
    return control, fake


def _paused(repo: Repository) -> int:
    row = repo.db.conn.execute("SELECT paused FROM control_setup WHERE id = 1").fetchone()
    assert row is not None
    return int(row["paused"] or 0)


def _ready(repo: Repository, *, paused: bool = True) -> ControlStore:
    store = ControlStore(repo.db)
    store.update_setup(
        telegram_state="authenticated",
        telegram_account_label="Owner",
        mexc_state="verified",
        mexc_label="Spot",
        selected_channel_id=100,
        selected_channel_title="Signals",
        paused=1 if paused else 0,
        pause_reason="owner paused execution" if paused else None,
    )
    store.refresh_setup_state()
    return store


def _refused(fake: FakeBot) -> bool:
    return any("ادامه اجرا ممکن نیست" in item["text"] for item in fake.sent)


def _succeeded(fake: FakeBot) -> bool:
    return any("اجرای خودکار ادامه یافت" in item["text"] for item in fake.sent)


async def test_authorized_resume_with_complete_readiness_succeeds(cfg, repo):
    _ready(repo)
    control, fake = _control(cfg, repo)

    await control._resume(OWNER)

    assert _paused(repo) == 0
    assert _succeeded(fake)
    assert not _refused(fake)
    success = next(item for item in fake.sent if "اجرای خودکار ادامه یافت" in item["text"])
    # The acknowledgement was produced after the durable commit.
    assert success["paused_at_send"] == 0


async def test_unauthorized_resume_cannot_unpause(cfg, repo):
    _ready(repo)
    control, fake = _control(cfg, repo)

    await control.handle_update(_message(1, "/resume", user=999))

    assert _paused(repo) == 1
    assert not _succeeded(fake)
    assert any("دسترسی مجاز نیست" in item["text"] for item in fake.sent)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("telegram_state", "unauthenticated"),
        ("mexc_state", "not_configured"),
        ("selected_channel_id", None),
    ],
)
async def test_resume_ignores_stale_setup_fields(cfg, repo, field, value):
    _ready(repo)
    with repo.db.transaction() as conn:
        conn.execute(f"UPDATE control_setup SET {field} = ? WHERE id = 1", (value,))
    control, fake = _control(cfg, repo)

    await control._resume(OWNER)

    assert _paused(repo) == 0
    assert _succeeded(fake)


async def test_resume_refuses_emergency_stop(cfg, repo):
    _ready(repo)
    cfg.trading.emergency_stop = True
    control, fake = _control(cfg, repo)

    await control._resume(OWNER)

    assert _paused(repo) == 1
    assert _refused(fake)
    assert "emergency" in fake.sent[-1]["text"].lower()


def _unresolved_intent(repo: Repository, cfg: AppConfig) -> None:
    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=1, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=1,
        edit_version=0,
        idempotency_key="atomic-resume",
        client_order_id="tsi-atomic-resume",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE order_intents SET state='unknown_requires_reconciliation' WHERE intent_id=?",
            (intent["intent_id"],),
        )


async def test_resume_ignores_unresolved_reconciliation_for_other_orders(cfg, repo):
    _ready(repo)
    _unresolved_intent(repo, cfg)
    control, fake = _control(cfg, repo)

    await control._resume(OWNER)

    assert _paused(repo) == 0
    assert _succeeded(fake)


async def test_readiness_change_while_waiting_for_exclusive_lock_is_observed(cfg, repo, monkeypatch):
    _ready(repo)
    control, fake = _control(cfg, repo)

    held = threading.Event()
    release = threading.Event()

    def hold_shared() -> None:
        gate = SubmissionGate(repo.db, cfg, require_control_setup=True)
        with gate.shared():
            held.set()
            assert release.wait(timeout=5)

    holder = threading.Thread(target=hold_shared)
    holder.start()
    assert held.wait(timeout=5)

    original = SubmissionGate.exclusive
    attempted = threading.Event()

    def tracked(self, **kwargs):
        attempted.set()
        return original(self, **kwargs)

    monkeypatch.setattr(SubmissionGate, "exclusive", tracked)
    task = asyncio.create_task(control._resume(OWNER))
    assert await asyncio.to_thread(attempted.wait, 2)

    # The only remaining resume blockers are pause itself and emergency stop.
    cfg.trading.emergency_stop = True
    release.set()
    holder.join(timeout=5)
    await asyncio.wait_for(task, timeout=5)

    assert _paused(repo) == 1
    assert _refused(fake)
    assert "emergency" in fake.sent[-1]["text"].lower()


async def test_transaction_failure_during_unpause_leaves_system_paused(cfg, repo, monkeypatch):
    _ready(repo)
    control, fake = _control(cfg, repo)

    def boom(self, conn, **fields):
        raise RuntimeError("simulated commit failure")

    monkeypatch.setattr(ControlStore, "update_setup_in_conn", boom)

    await control._resume(OWNER)

    assert _paused(repo) == 1
    assert not _succeeded(fake)
    assert any("در حالت توقف باقی ماند" in item["text"] for item in fake.sent)


async def test_no_submission_enters_between_check_and_durable_unpause(cfg, repo, monkeypatch):
    _ready(repo)
    control, fake = _control(cfg, repo)

    inside_tx = threading.Event()
    release_tx = threading.Event()
    original = ControlStore.refresh_setup_state_in_conn

    def hold_tx(self, conn):
        inside_tx.set()
        assert release_tx.wait(timeout=5)
        return original(self, conn)

    monkeypatch.setattr(ControlStore, "refresh_setup_state_in_conn", hold_tx)
    task = asyncio.create_task(control._resume(OWNER))
    assert await asyncio.to_thread(inside_tx.wait, 2)

    entered = threading.Event()
    observed: list[int] = []

    def attempt_submission() -> None:
        gate = SubmissionGate(repo.db, cfg, require_control_setup=True)
        with gate.shared():
            entered.set()
            observed.append(_paused(repo))

    worker = threading.Thread(target=attempt_submission)
    worker.start()
    # The exclusive resume lock is still held: no submission may enter.
    assert not entered.wait(timeout=0.25)

    release_tx.set()
    await asyncio.wait_for(task, timeout=5)
    worker.join(timeout=5)

    assert entered.is_set()
    # The submission entered only after the unpause commit.
    assert observed == [0]
    assert _succeeded(fake)


async def test_lock_timeout_refuses_resume_without_unpausing(cfg, repo, monkeypatch):
    _ready(repo)
    control, fake = _control(cfg, repo)

    def timeout(self, **kwargs):
        raise GateTimeout("simulated in-flight submissions")

    monkeypatch.setattr(SubmissionGate, "exclusive", timeout)

    await control._resume(OWNER)

    assert _paused(repo) == 1
    assert not _succeeded(fake)
    assert any("در حالت توقف باقی ماند" in item["text"] for item in fake.sent)
