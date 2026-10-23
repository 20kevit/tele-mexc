"""Trading boundary for the structured channel index.

Guarantees exercised here:

* SHORT signals, their follow-ups, and every other structured candidate are
  historical review data: the analysis pipeline (replay + reindex) must
  never create ``signals`` rows, order intents, or exchange orders;
* the channel's ``Deposit`` and ``MAX LEV Suggested`` metadata stays review
  data and never reaches any order-config/trading surface.
"""

from __future__ import annotations

from pathlib import Path

from tests.test_signal_index import (
    CHANNEL,
    FOLLOW_UP_301_ENTRY,
    INITIAL_301,
    INITIAL_303_SHORT,
    _put,
    _rows,
)
from tg_signal_ingest.analysis import ReplayScope, run_replay
from tg_signal_ingest.analysis.reindex import rebuild_signal_index
from tg_signal_ingest.normalize import message_to_record

REINDEX_DIR = Path(__file__).resolve().parents[1] / "tg_signal_ingest" / "analysis"
MANAGEMENT_DIR = Path(__file__).resolve().parents[1] / "tg_signal_ingest" / "management"

TRADING_TABLES = ("processing_queue", "signals", "order_intents", "exchange_orders")


def _assert_trading_tables_empty(repo):
    values = {
        table: _rows(repo.db, f"SELECT COUNT(*) c FROM {table}")[0]["c"]
        for table in TRADING_TABLES
    }
    assert not any(values.values()), f"trading tables touched: {values}"


class TestShortAndFollowUpsCreateNoOrders:
    def test_full_short_lifecycle_never_creates_orders(self, repo):
        _put(repo, 700, INITIAL_303_SHORT)
        _put(repo, 701, "🆔 #303\nSOLUSDT\n📌 ورود SHORT با موفقیت ثبت شد\nقیمت: 200")
        _put(repo, 702, "🆔 #303\nTOOBIT:SOLUSDT.P (SHORT)\n🎯 تارگت 1 با موفقیت خورد!\n💰 2.2 % سود")
        _put(repo, 703, "🆔 #303\nTOOBIT:SOLUSDT.P (SHORT)\n❌ حد ضرر خورد!")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        stats = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)

        # Fully indexed for historical review...
        assert stats.initial_signals == 1
        sig = _rows(repo.db, "SELECT * FROM channel_signals WHERE logical_id = 303")[0]
        assert sig["direction"] == "short"
        assert sig["execution_status"] == "none"
        candidates = _rows(repo.db, "SELECT * FROM signal_candidates")
        assert len(candidates) == 4
        # ...but no trading artefact of any kind.
        _assert_trading_tables_empty(repo)

    def test_long_signal_with_followups_also_creates_no_orders(self, repo):
        _put(repo, 500, INITIAL_301)
        _put(repo, 501, FOLLOW_UP_301_ENTRY)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        assert _rows(repo.db, "SELECT COUNT(*) c FROM channel_signals")[0]["c"] == 1
        _assert_trading_tables_empty(repo)

    def test_dry_run_is_also_order_free(self, repo):
        _put(repo, 500, INITIAL_303_SHORT)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=True)
        _assert_trading_tables_empty(repo)
        assert _rows(repo.db, "SELECT COUNT(*) c FROM channel_signals")[0]["c"] == 0

    def test_plain_v1_signal_still_never_creates_orders(self, repo):
        # Regression guard: unchanged v1 behaviour has no order side effect.
        repo.upsert_message(
            message_to_record(CHANNEL, FakeMsgFor(900, "BUY ETHUSDT ENTRY 30 TP 33 SL 28"), "live"),
            enqueue_processing=False,
        )
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        _assert_trading_tables_empty(repo)


def FakeMsgFor(message_id, text):
    from tests.conftest import FakeMsg

    return FakeMsg(message_id, text)


class TestChannelMetadataNeverReachesTrading:
    def test_deposit_and_leverage_stay_in_candidates_and_index_only(self, repo):
        _put(repo, 500, INITIAL_301)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)

        cand = _rows(
            repo.db,
            "SELECT * FROM signal_candidates ORDER BY parser_version DESC LIMIT 1",
        )[0]
        # Retained as review data on the candidate...
        assert cand["deposit_pct"] == "1.00"
        assert cand["suggested_leverage"] == "8"
        sig = _rows(repo.db, "SELECT * FROM channel_signals")[0]
        # ...and on the logical index row, but nowhere else.
        assert sig["deposit_pct"] == "1.00"
        assert sig["suggested_leverage"] == "8"
        # The execution status of even a perfectly parsed LONG signal is
        # 'none': nothing in this pipeline enables orders or leverage.
        assert sig["execution_status"] == "none"
        _assert_trading_tables_empty(repo)

    def test_analysis_code_never_contains_order_actions(self):
        files = sorted(REINDEX_DIR.glob("*.py"))
        assert files
        for path in files:
            source = path.read_text(encoding="utf-8")
            for token in ("create_order", "set_leverage", "order_market", "api_key"):
                assert token not in source, f"{path.name} contains {token!r}"

    def test_signal_review_module_is_order_free(self):
        review = MANAGEMENT_DIR / "signal_review.py"
        assert review.is_file()
        source = review.read_text(encoding="utf-8")
        for token in ("create_order", "set_leverage", "api_key", "api_secret"):
            assert token not in source, f"{review.name} contains {token!r}"
