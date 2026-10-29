"""Service lifecycle tests: auth-required, channel access, graceful shutdown,
recovery sweep, and live insert path (all with a fake Telethon client)."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from telethon.errors import FloodWaitError

from tests.conftest import FakeMsg
from tg_signal_ingest.backfill import BackfillReport
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.service import IngestService


class FakeClient:
    def __init__(self, authorized=True, entity=None, on_get_entity=None):
        self.authorized = authorized
        self._entity = entity or FakeChannelEntity()
        self._on_get_entity = on_get_entity
        self.connected = False
        self.disconnect_count = 0
        self.handlers = []
        self.running_event = asyncio.Event()
        self.stop_remote = asyncio.Event()

    async def connect(self):
        self.connected = True

    async def is_user_authorized(self):
        return self.authorized

    async def disconnect(self):
        self.disconnect_count += 1
        self.connected = False
        self.stop_remote.set()

    async def get_entity(self, peer):
        if self._on_get_entity:
            self._on_get_entity(peer)
        return self._entity

    async def get_messages(self, ref, limit=..., **kw):
        return []

    def iter_messages(self, ref, **kwargs):
        raise AssertionError("iter_messages not used in default test flow")

    def on(self, *a, **k):
        def decorator(fn):
            self.handlers.append(fn)
            return fn
        return decorator

    def run_until_disconnected(self):
        async def _run():
            self.running_event.set()
            await self.stop_remote.wait()
        return _run()


class FakeChannelEntity:
    id = 100
    access_hash = 123
    title = "Sig Chan"
    username = "sigchan"
    is_user = False
    is_channel = True


def stub_backfill(repo, cfg):
    async def runner(client, repo_arg, cfg_arg, channel_bare_id=None, **kw):
        repo_arg.complete_backfill(channel_bare_id, None)
        return BackfillReport(completed=True)
    return runner


async def run_service(svc, fake_client, timeout=5.0):
    task = asyncio.ensure_future(svc.run())
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(fake_client.running_event.wait(), timeout=timeout)
    return task


@pytest.fixture()
def selected_repo(repo):
    repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
    repo.select_channel(100)
    return repo


class TestServiceLifecycle:
    async def test_auth_missing_returns_code_3(self, cfg, repo):
        client = FakeClient(authorized=False)
        svc = IngestService(cfg, repo, client=client, backfill_runner=stub_backfill(repo, cfg))
        code = await svc.run()
        assert code == 3
        await client.disconnect()

    async def test_channel_inaccessible_returns_code_4(self, cfg, selected_repo):
        def broken(peer):
            raise ValueError("could not find the input entity")
        client = FakeClient(on_get_entity=broken)
        svc = IngestService(cfg, selected_repo, client=client, backfill_runner=stub_backfill(selected_repo, cfg))
        code = await svc.run()
        assert code == 4

    async def test_graceful_shutdown(self, cfg, selected_repo):
        client = FakeClient()
        svc = IngestService(cfg, selected_repo, client=client, backfill_runner=stub_backfill(selected_repo, cfg))
        task = asyncio.ensure_future(svc.run())
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(client.running_event.wait(), timeout=3)
        svc.request_stop()
        code = await asyncio.wait_for(task, timeout=10)
        assert code == 0
        assert client.disconnect_count >= 1

    async def test_pending_processing_requeue_on_startup(self, cfg, selected_repo, test_msgs=None):
        # Simulate crash: stuck processing items get requeued at service start.
        markers: list = []

        class Probe(FakeClient):
            async def connect(self):
                await super().connect()
                markers.append("ran")

        client = Probe()
        svc = IngestService(cfg, selected_repo, client=client, backfill_runner=stub_backfill(selected_repo, cfg))
        task = asyncio.ensure_future(svc.run())
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(client.running_event.wait(), timeout=3)
        svc.request_stop()
        await asyncio.wait_for(task, timeout=10)
        assert svc.repo.queue_counts()["processing"] == 0  # nothing stuck

    async def test_recovery_sweep_catches_gap_messages(self, cfg, selected_repo):
        class SweepingClient(FakeClient):
            def __init__(self):
                super().__init__()
                self.gap_msgs = [
                    FakeMsg(1005, "recovered high 1"),
                    FakeMsg(1006, "recovered high 2"),
                ]

            def iter_messages(self, ref, **kw):
                msgs = self.gap_msgs
                async def gen():
                    for m in msgs:
                        yield m
                return gen()

        # Watermark from a previous run: last live id 1004; gap = 1005, 1006
        svc_repo = selected_repo
        svc_repo.record_live_event(100, 1004)
        svc_repo.upsert_message(message_to_record(100, FakeMsg(1004, "before outage"), "live"))

        client = SweepingClient()
        svc = IngestService(cfg, svc_repo, client=client, backfill_runner=stub_backfill(svc_repo, cfg))
        task = asyncio.ensure_future(svc.run())
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(client.running_event.wait(), timeout=3)
        # ensure the startup sweep completed before stopping
        for _ in range(200):
            if svc_repo.get_message_raw(100, 1006):
                break
            await asyncio.sleep(0.01)
        svc.request_stop()
        await asyncio.wait_for(task, timeout=10)

        stored = svc_repo.get_message_raw(100, 1005)
        assert stored is not None
        assert stored["source"] == "recovery"
        stored2 = svc_repo.get_message_raw(100, 1006)
        assert stored2 is not None
        state = svc_repo.sync_state(100)
        assert state["live_last_seen_message_id"] == 1006
        assert state["live_last_sync_at"] is not None

    async def test_live_message_insert_via_handler(self, cfg, selected_repo):
        client = FakeClient()
        svc = IngestService(cfg, selected_repo, client=client, backfill_runner=stub_backfill(selected_repo, cfg))
        task = asyncio.ensure_future(svc.run())
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(client.running_event.wait(), timeout=3)
        msg = FakeMsg(2000, "breaking signal", media=None)
        await svc._store_event_message(msg, "live")
        stored = svc.repo.get_message_raw(100, 2000)
        assert stored is not None
        assert stored["source"] == "live"
        svc.request_stop()
        await asyncio.wait_for(task, timeout=10)


async def test_recovery_sweep_honors_full_flood_wait(cfg, selected_repo):
    class FloodClient(FakeClient):
        def __init__(self):
            super().__init__()
            self.raised = False

        def iter_messages(self, ref, **kwargs):
            async def gen():
                if not self.raised:
                    self.raised = True
                    raise FloodWaitError(None, 321)
                return
                yield  # pragma: no cover

            return gen()

    client = FloodClient()
    svc = IngestService(
        cfg, selected_repo, client=client, backfill_runner=stub_backfill(selected_repo, cfg)
    )
    waits: list[float] = []

    async def fake_sleep(seconds):
        waits.append(seconds)

    svc._sleep_with_stop = fake_sleep  # type: ignore[method-assign]
    caught = await svc.sweep_gap(1004)
    assert caught == 0
    assert waits and waits[0] >= 321


def test_service_notifications_are_delivered_once_per_recipient(cfg, selected_repo):
    """Service lifecycle events are delivered via the existing notifier and
    deduplicated by (event_key, role) so restarts/retries never duplicate them.
    """

    from tg_signal_ingest.trading.notify import TradingNotifier

    svc = IngestService(cfg, selected_repo, client=FakeClient())

    class FakeBot:
        def __init__(self) -> None:
            self.sent: list[tuple[int, str]] = []

        def send_message(self, chat_id: int, text: str, reply_markup=None) -> dict:
            self.sent.append((int(chat_id), str(text)))
            return {"message_id": len(self.sent)}

    bot = FakeBot()
    svc.trading.notifier = TradingNotifier(svc.trading.store, bot, owner_id=1, developer_id=2)

    # Critical events go to owner + developer and are deduplicated by event key.
    svc._notify_service_event(event_key="svc:start", text="serve started", category="critical")
    svc._notify_service_event(event_key="svc:start", text="serve started", category="critical")

    assert len(bot.sent) == 2  # one per recipient, not four
    assert {chat for chat, _ in bot.sent} == {1, 2}
    assert all("serve started" in text for _, text in bot.sent)

    # Recovery is developer-only by default (owner opts in).
    svc._notify_service_event(event_key="svc:recovery", text="recovery done", category="recovery")
    assert len(bot.sent) == 3
    assert bot.sent[-1] == (2, "recovery done")
