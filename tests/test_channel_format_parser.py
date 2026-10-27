"""Tests for the structured channel-format parser (parser version 2).

All examples here are synthetic. They exercise the observed structured shape
(logical ID, exchange-qualified symbol, Jalali date, entry/stop/targets) and
defensive cases: invalid prices, duplicate logical IDs, unconvertible dates,
and direction conflicts must be detected or rejected, never silently accepted.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.analysis import (
    ANALYSIS_PARSER_VERSION,
    MarketType,
    ParseStatus,
    Side,
    SignalClassification,
    parse_message,
)
from tg_signal_ingest.analysis.channel_format import (
    follow_up_event_fa,
    gregorian_to_jalali,
    jalali_to_gregorian,
    parse_structured,
    source_timestamp_from,
)
from tg_signal_ingest.analysis.model import Direction, FollowUpEvent


class TestJalaliConversion:
    """Explicit Jalali calendar conversion; never a silent Gregorian misread."""

    @pytest.mark.parametrize(
        ("jalali", "gregorian"),
        [
            ((1403, 1, 1), (2024, 3, 20)),
            ((1404, 1, 1), (2025, 3, 21)),
            ((1405, 1, 1), (2026, 3, 21)),
            ((1405, 8, 1), (2026, 10, 23)),
            ((1400, 1, 1), (2021, 3, 21)),
            ((1399, 12, 30), (2021, 3, 20)),  # leap-year last day
            ((1403, 12, 30), (2025, 3, 20)),  # leap-year last day
        ],
    )
    def test_known_anchors(self, jalali, gregorian):
        assert jalali_to_gregorian(*jalali) == gregorian
        assert gregorian_to_jalali(*gregorian) == jalali

    def test_round_trip_over_four_centuries(self):
        for jy in range(1100, 1500):
            for jm, jd in ((1, 1), (3, 10), (6, 31), (7, 1), (9, 15), (12, 29)):
                gregorian = jalali_to_gregorian(jy, jm, jd)
                assert gregorian_to_jalali(*gregorian) == (jy, jm, jd)
            # Esfand 30 exists only in leap years.
            try:
                gregorian = jalali_to_gregorian(jy, 12, 30)
            except ValueError:
                continue
            assert gregorian_to_jalali(*gregorian) == (jy, 12, 30)

    @pytest.mark.parametrize(
        "invalid",
        [(1400, 12, 30), (1405, 13, 1), (1405, 6, 32), (1405, 0, 1), (1405, 1, 0)],
    )
    def test_invalid_dates_rejected(self, invalid):
        with pytest.raises(ValueError):
            jalali_to_gregorian(*invalid)

    def test_source_timestamp_conversion(self):
        assert source_timestamp_from("1405/08/01", "08:45") == "2026-10-23T05:15:00Z"
        assert source_timestamp_from("2026/10/23", "08:45") == "2026-10-23T05:15:00Z"
        assert source_timestamp_from(None, "08:45") is None
        assert source_timestamp_from("1405/13/01", "08:45") is None
        assert source_timestamp_from("1405/08/02", "25:00") is None


class TestStructuredFormats:
    """Synthetic cases exercising the structured channel format."""

    def test_initial_signal_parse_via_dispatch(self):
        text = (
            "🆔 #301\n⏰ 1405/08/01\n TIME =08:45\n *LONG*🟢 \n"
            " ⚡️TOOBIT:ABCUSDT.P(15)\n\n ✅Entry: 100.00\n 🔰Stop Loss : 95.00\n\n"
            " 📌TP1 : 105.00\n 📌TP2 : 110.00\n 📌TP3 : 115.00\n 📌TP4 : 120.00\n"
            "  -----------------------------\n 🤑FULL Target : 130.00\n ----- \n"
            " 💰Deposit 1.00%  \n 💠MAX LEV Suggested  =  8 (-17.39%)\n"
            " 💬 : 💡 طبق پلن معاملاتی پیش بروید\n"
            " ⭐️ @example_owner / @example_chart"
        )
        parsed = parse_message(text)
        assert ANALYSIS_PARSER_VERSION >= 2
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.market_type == MarketType.NON_SPOT
        assert parsed.direction == Direction.LONG
        assert parsed.side == Side.BUY
        assert parsed.logical_id == 301
        assert parsed.symbol == "ABCUSDT"
        assert parsed.quote_asset == "USDT"
        assert parsed.symbol_raw == "ABCUSDT.P"
        assert parsed.exchange == "TOOBIT"
        assert parsed.timeframe == 15
        assert parsed.entry_price == Decimal("100.00")
        assert parsed.stop_loss == Decimal("95.00")
        assert [(t.index, t.price) for t in parsed.targets] == [
            (1, Decimal("105.00")),
            (2, Decimal("110.00")),
            (3, Decimal("115.00")),
            (4, Decimal("120.00")),
        ]
        assert parsed.full_target == Decimal("130.00")
        assert parsed.deposit_pct == Decimal("1.00")
        assert parsed.suggested_leverage == Decimal("8")
        assert parsed.leverage_pct == Decimal("-17.39")
        assert parsed.follow_up_event is None
        assert parsed.parse_status == ParseStatus.PARSED
        assert parsed.ambiguities == ()
        assert parsed.actionable is True
        # Jalali date preserved verbatim and converted explicitly.
        assert parsed.source_date_text == "1405/08/01"
        assert parsed.source_time_text == "08:45"
        assert parsed.source_timestamp_iso == "2026-10-23T05:15:00Z"
        # `#301` is a logical signal ID, never a Telegram message reference.
        assert parsed.referenced_message_id is None

    def test_tp_hit_persian_follow_up(self):
        parsed = parse_message(
            "🆔 #302\nTOOBIT:XYZUSDT.P (LONG)\n🎯 تارگت 3 با موفقیت خورد!\n"
            "💰 12.6 % سود"
        )
        assert parsed.classification == SignalClassification.RESULT
        assert parsed.logical_id == 302
        assert parsed.follow_up_event == FollowUpEvent.TP_HIT
        assert parsed.target_index == 3
        assert parsed.profit_pct == Decimal("12.6")
        assert parsed.actionable is False

    def test_tp_hit_english_variant(self):
        parsed = parse_message(
            "🆔 #303\nTOOBIT:XYZUSDT.P LONG\n✅ TP2 hit\n📌 Profit: 8.5 % @ 1.582"
        )
        assert parsed.classification == SignalClassification.RESULT
        assert parsed.follow_up_event == FollowUpEvent.TP_HIT
        assert parsed.target_index == 2
        assert parsed.profit_pct == Decimal("8.5")
        assert parsed.event_price == Decimal("1.582")

    def test_tp1_check_mark_follow_up(self):
        parsed = parse_message(
            "🆔 #304\nTOOBIT:XYZUSDT.P\n🔔 TP1 ✅\n💹 قیمت: 0.1379\n💰 سود: 4.7 %"
        )
        assert parsed.classification == SignalClassification.RESULT
        assert parsed.follow_up_event == FollowUpEvent.TP_HIT
        assert parsed.target_index == 1
        assert parsed.event_price == Decimal("0.1379")
        assert parsed.profit_pct == Decimal("4.7")

    def test_entry_activated_follow_up_fa_with_price(self):
        parsed = parse_message(
            "🆔 #305\nTOOBIT:XYZUSDT.P\n🟢 ورود پوزیشن LONG تریگر شد\n"
            "🎯 Entry: 2.0867"
        )
        assert parsed.classification == SignalClassification.RESULT
        assert parsed.follow_up_event == FollowUpEvent.ENTRY_ACTIVATED
        # A follow-up "Entry:" value is the observed trigger price; it is a
        # historical event price, never a planning value of a new signal.
        assert parsed.entry_price is None
        assert parsed.event_price == Decimal("2.0867")
        assert parsed.actionable is False

    def test_deposit_and_leverage_are_metadata_only(self):
        parsed = parse_message(
            "🆔 #301\n⏰ 1405/08/01\n TIME =08:45\n *LONG*🟢 \n"
            " ⚡️TOOBIT:ABCUSDT.P(15)\n ✅Entry: 100.00\n 🔰Stop Loss : 95.00\n"
            " 📌TP1 : 105.00\n 💰Deposit 1.00%\n"
            " 💠MAX LEV Suggested  =  8 (-17.39%)"
        )
        # Retained for review, but never converted into order sizing or
        # leverage input.
        assert parsed.deposit_pct == Decimal("1.00")
        assert parsed.suggested_leverage == Decimal("8")
        assert parsed.leverage_pct == Decimal("-17.39")
        # The deposit percentage is not mistaken for a profit figure.
        assert parsed.profit_pct is None


class TestDefensiveCases:
    def test_short_signal_is_parsed_but_never_actionable(self):
        parsed = parse_message(
            "🆔 #77\n *SHORT*🔴\n ⚡️TOOBIT:BTCUSDT.P(15)\n"
            " ✅Entry: 65000\n 🔰Stop Loss : 66500\n 📌TP1 : 63000"
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.direction == Direction.SHORT
        assert parsed.side == Side.SELL
        assert parsed.entry_price == Decimal("65000")
        assert parsed.stop_loss == Decimal("66500")
        assert parsed.actionable is False

    def test_both_direction_tokens_are_ambiguous(self):
        parsed = parse_message(
            "🆔 #78\n *LONG*🟢\n ⚡️TOOBIT:BTCUSDT.P(15)\n ✅Entry: 65000\n"
            " 🔰Stop Loss : 64000\n 📌TP1 : 66000\n SHORT"
        )
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert "missing_or_conflicting_direction" in parsed.ambiguities
        assert parsed.actionable is False

    def test_missing_entry_in_signal_like_content(self):
        parsed = parse_message(
            "🆔 #79\n *LONG*🟢\n ⚡️TOOBIT:BTCUSDT.P(15)\n"
            " 🔰Stop Loss : 64000\n 📌TP1 : 66000"
        )
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert parsed.entry_price is None
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert parsed.actionable is False

    def test_duplicate_logical_ids_are_ambiguous(self):
        parsed = parse_message(
            "🆔 #80\n🆔 #81\n *LONG*🟢\n ⚡️TOOBIT:BTCUSDT.P(15)\n"
            " ✅Entry: 65000\n 🔰Stop Loss : 64000\n 📌TP1 : 66000"
        )
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert parsed.logical_id is None
        assert "multiple_logical_ids:80,81" in parsed.ambiguities

    @pytest.mark.parametrize("bad", ["0", "0.0", "-5", "abc"])
    def test_invalid_entry_price_rejected(self, bad):
        parsed = parse_message(
            f"🆔 #82\n *LONG*🟢\n ⚡️TOOBIT:BTCUSDT.P(15)\n"
            f" ✅Entry: {bad}\n 🔰Stop Loss : 64000\n 📌TP1 : 66000"
        )
        assert parsed.entry_price is None
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert "missing_entry_price" in parsed.ambiguities

    def test_unconvertible_date_is_flagged_never_silently_dropped(self):
        parsed = parse_message(
            "🆔 #85\n⏰ 1405/13/01\n TIME =08:45\n *LONG*🟢\n"
            " ⚡️TOOBIT:BTCUSDT.P(15)\n ✅Entry: 65000\n"
            " 🔰Stop Loss : 64000\n 📌TP1 : 66000"
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        # The source text is preserved; the failed conversion is explicit.
        assert parsed.source_date_text == "1405/13/01"
        assert parsed.source_timestamp_iso is None
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert "source_date_not_converted" in parsed.ambiguities

    def test_stop_loss_semantics_long(self):
        parsed = parse_message(
            "🆔 #88\n *LONG*🟢\n ⚡️TOOBIT:BTCUSDT.P(15)\n ✅Entry: 65000\n"
            " 🔰Stop Loss : 66000\n 📌TP1 : 66001"
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert "stop_loss_not_below_entry" in parsed.ambiguities

    def test_target_semantics_short(self):
        parsed = parse_message(
            "🆔 #91\n *SHORT*🔴\n ⚡️TOOBIT:BTCUSDT.P(15)\n ✅Entry: 65000\n"
            " 🔰Stop Loss : 66500\n 📌TP1 : 66000"
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert "target_above_entry:TP1" in parsed.ambiguities

    def test_logical_id_only_chatter_is_not_a_signal(self):
        parsed = parse_message("🆔 #86\nیک بحث عادی درباره بازار")
        assert parsed.classification == SignalClassification.NON_SIGNAL
        assert parsed.logical_id == 86
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW

    def test_generic_format_is_not_claimed_by_v2(self):
        # A generic v1-signal message must still be parsed by the generic
        # heuristic parser, not misidentified as channel-structured content.
        parsed = parse_message("BUY BTCUSDT\nENTRY: 100-102\nTP1: 110\nTP2: 120\nSL: 95")
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.market_type == MarketType.SPOT
        assert parsed.entry_low_price == Decimal("100")
        assert parsed.logical_id is None

    def test_original_text_never_mutated(self):
        text = "🆔 #306\nTOOBIT:XYZUSDT.P (LONG)\n🎯 تارگت 2 با موفقیت خورد!"
        parse_structured(text)
        assert text == "🆔 #306\nTOOBIT:XYZUSDT.P (LONG)\n🎯 تارگت 2 با موفقیت خورد!"


class TestFollowUpLabels:
    @pytest.mark.parametrize(
        ("event", "label"),
        [
            ("entry_activated", "ورود ثبت شد"),
            ("tp_hit", "تارگت خورده"),
            ("stop_loss_hit", "حد ضرر خورده"),
            ("closed", "بسته شد"),
            (None, "—"),
        ],
    )
    def test_labels(self, event, label):
        assert follow_up_event_fa(event) == label
