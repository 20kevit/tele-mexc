"""Deterministic parser tests for the offline signal-analysis pipeline."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.analysis import (
    ANALYSIS_PARSER_VERSION,
    MarketType,
    ParseStatus,
    Side,
    SignalAnalysisParser,
    SignalClassification,
    normalize_text,
    parse_message,
)


class TestNewSignals:
    def test_english_range_targets_and_stop(self):
        parsed = parse_message(
            "BUY BTCUSDT\nENTRY: 100-102\nTP1: 110\nTP2: 120\nSL: 95"
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.market_type == MarketType.SPOT
        assert parsed.symbol == "BTCUSDT"
        assert parsed.quote_asset == "USDT"
        assert parsed.side == Side.BUY
        assert parsed.entry_low_price == Decimal("100")
        assert parsed.entry_high_price == Decimal("102")
        # A range is never silently collapsed into an invented midpoint.
        assert parsed.entry_price is None
        assert [(t.index, t.price) for t in parsed.targets] == [
            (1, Decimal("110")),
            (2, Decimal("120")),
        ]
        assert parsed.stop_loss == Decimal("95")
        assert parsed.parse_status == ParseStatus.PARSED
        assert parsed.ambiguities == ()
        assert parsed.actionable is True

    def test_persian_digits_range_and_ordinal_targets(self):
        parsed = parse_message(
            "خرید BTCUSDT\nمحدوده ورود: ۱۰۰ تا ۱۰۲\n"
            "تارگت اول: ۱۱۰\nتارگت دوم: ۱۲۰\nحد ضرر: ۹۵"
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.side == Side.BUY
        assert parsed.entry_low_price == Decimal("100")
        assert parsed.entry_high_price == Decimal("102")
        assert parsed.entry_price is None
        assert [t.price for t in parsed.targets] == [Decimal("110"), Decimal("120")]
        assert [t.index for t in parsed.targets] == [1, 2]
        assert parsed.stop_loss == Decimal("95")
        assert parsed.parse_status == ParseStatus.PARSED

    def test_persian_between_and_decimal_separator(self):
        parsed = parse_message(
            "خرید ETHUSDT ورود بین ۲۰۰۰ و ۲۰۲۰ تارگت ۲۱۰۰ حد ضرر ۱۹۵۰"
        )
        assert parsed.entry_low_price == Decimal("2000")
        assert parsed.entry_high_price == Decimal("2020")
        assert parsed.targets[0].price == Decimal("2100")
        assert parsed.stop_loss == Decimal("1950")

        decimal = parse_message("خرید BTCUSDT ورود: ۱۰۰٫۵ تارگت: ۱۱۰٫۲۵ حد ضرر: ۹۵٫۱")
        assert decimal.entry_price == Decimal("100.5")
        assert decimal.targets[0].price == Decimal("110.25")
        assert decimal.stop_loss == Decimal("95.1")

    def test_market_order_has_no_invented_entry(self):
        parsed = parse_message("BUY ETHUSDT MARKET TP 2000 SL 1800")
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.order_type == "market"
        assert parsed.entry_price is None
        assert parsed.entry_low_price is None
        assert parsed.entry_high_price is None
        assert parsed.actionable is True

    def test_emoji_formatting_and_dollar_prefix(self):
        parsed = parse_message("🟢 BUY $SOL\nENTRY: 20\nTP: 25\nSL: 18")
        assert parsed.symbol == "SOLUSDT"
        assert parsed.quote_asset == "USDT"
        assert parsed.field_confidence["symbol"] == "medium"
        assert parsed.classification == SignalClassification.NEW_SIGNAL

    def test_thousands_separators_are_not_floats(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 100,000 TP 110,000 SL 95,000"
        )
        assert parsed.entry_price == Decimal("100000")
        assert parsed.targets[0].price == Decimal("110000")
        assert parsed.stop_loss == Decimal("95000")

    @pytest.mark.parametrize(
        "text",
        [
            "BUY BTC/USDT ENTRY 100 TP 120 SL 95",
            "BUY BTC-USDT ENTRY 100 TP 120 SL 95",
            "BUY BTCUSDT ENTRY 100 TP 120 SL 95",
            "buy btcusdt entry 100 tp 120 sl 95",
        ],
    )
    def test_common_symbol_spellings(self, text):
        parsed = parse_message(text)
        assert parsed.symbol == "BTCUSDT"
        assert parsed.side == Side.BUY
        assert parsed.parse_status == ParseStatus.PARSED

    def test_decimal_precision_is_preserved(self):
        parsed = parse_message("BUY BTCUSDT ENTRY 0.100 TP 0.200 SL 0.050")
        assert isinstance(parsed.entry_price, Decimal)
        assert format(parsed.entry_price, "f") == "0.100"
        assert format(parsed.targets[0].price, "f") == "0.200"
        assert format(parsed.stop_loss, "f") == "0.050"

    def test_missing_fields_stay_missing_and_need_review(self):
        parsed = parse_message("BUY BTCUSDT ENTRY 100")
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.targets == ()
        assert parsed.stop_loss is None
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert "missing_take_profit" in parsed.ambiguities
        assert "missing_stop_loss" in parsed.ambiguities
        # The parser never fills a stop or target to make the row look complete.
        assert parsed.actionable is True  # machine-only flag, not trading permission


class TestUpdatesAndResults:
    def test_stop_loss_update_english_and_persian(self):
        english = parse_message("UPDATE BTCUSDT SL moved to 95")
        assert english.classification == SignalClassification.STOP_LOSS_UPDATE
        assert english.stop_loss == Decimal("95")
        assert english.parse_status == ParseStatus.PARSED

        persian = parse_message("BTCUSDT حد ضرر جدید ۹۵")
        assert persian.classification == SignalClassification.STOP_LOSS_UPDATE
        assert persian.stop_loss == Decimal("95")

    def test_target_update_english_and_persian(self):
        english = parse_message("BTCUSDT TP1 moved to 115")
        assert english.classification == SignalClassification.TARGET_UPDATE
        assert english.targets[0].price == Decimal("115")

        persian = parse_message("BTCUSDT تارگت جدید ۱۱۵")
        assert persian.classification == SignalClassification.TARGET_UPDATE
        assert persian.targets[0].price == Decimal("115")

    def test_modification_with_edit_flag(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 101 TP 111 SL 96", is_edit=True
        )
        assert parsed.classification == SignalClassification.MODIFICATION
        assert parsed.side == Side.BUY
        assert parsed.entry_price == Decimal("101")

    def test_cancellation_and_cancel_all(self):
        symbol = parse_message("CANCEL BTCUSDT")
        assert symbol.classification == SignalClassification.CANCELLATION
        assert symbol.symbol == "BTCUSDT"

        everything = parse_message("CANCEL ALL")
        assert everything.classification == SignalClassification.CANCELLATION
        assert everything.symbol is None

        persian = parse_message("❌ لغو سیگنال BTCUSDT")
        assert persian.classification == SignalClassification.CANCELLATION

        all_persian = parse_message("لغو همه سیگنال\u200cها")
        assert all_persian.classification == SignalClassification.CANCELLATION

    def test_cancel_word_in_commentary_is_not_a_cancellation(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 100 TP 120 SL 95. Price may close above resistance later."
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert parsed.parse_status == ParseStatus.PARSED

    def test_results_english_and_persian(self):
        assert (
            parse_message("BTCUSDT TP1 HIT - booked profit").classification
            == SignalClassification.RESULT
        )
        assert (
            parse_message("BTCUSDT تارگت اول خورد").classification
            == SignalClassification.RESULT
        )
        assert parse_message("TP1 HIT").classification == SignalClassification.RESULT

    def test_result_and_cancel_is_ambiguous(self):
        parsed = parse_message("BTCUSDT TP1 HIT but CANCEL the signal")
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert "result_and_cancel_in_same_message" in parsed.ambiguities


class TestNonSignalsAndAmbiguity:
    @pytest.mark.parametrize(
        "text",
        [
            "Good morning everyone, market looks calm",
            "سلام دوستان، بازار آرام است",
            "join our channel for more",
        ],
    )
    def test_plain_chatter_is_not_a_signal(self, text):
        parsed = parse_message(text)
        assert parsed.classification == SignalClassification.NON_SIGNAL
        assert parsed.symbol is None
        assert parsed.side is None
        assert parsed.entry_price is None

    def test_empty_message(self):
        for text in ("", None, "   \n\t "):
            parsed = parse_message(text)
            assert parsed.classification == SignalClassification.NON_SIGNAL
            assert parsed.parse_status == ParseStatus.PARSED

    def test_media_without_text_is_not_guessed(self):
        parsed = parse_message(None, media_only=True)
        assert parsed.classification == SignalClassification.NON_SIGNAL
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert parsed.ambiguities == ("media_without_text",)
        assert parsed.entry_price is None

    def test_forwarded_signal_is_context_only(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 100 TP 120 SL 95", forwarded=True
        )
        assert parsed.classification == SignalClassification.NON_SIGNAL
        assert parsed.side == Side.BUY  # fields remain visible for review
        assert "forwarded content is context, not a new instruction" in parsed.reasons

    def test_multiple_symbols_are_ambiguous(self):
        parsed = parse_message(
            "BUY BTCUSDT and ETHUSDT ENTRY 100 TP 120 SL 95"
        )
        assert parsed.symbol is None
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert any(a.startswith("multiple_symbols:") for a in parsed.ambiguities)

    def test_both_directions_are_ambiguous(self):
        parsed = parse_message("BUY BTCUSDT then SELL later entry 100")
        assert parsed.side is None
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert "both_buy_and_sell_directions" in parsed.ambiguities

    def test_missing_entry_is_ambiguous_not_new_signal(self):
        parsed = parse_message("BUY BTCUSDT TP 120 SL 95")
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert parsed.entry_price is None
        assert "missing_entry_price" in parsed.ambiguities

    def test_non_spot_is_ambiguous_and_never_actionable(self):
        parsed = parse_message("SHORT BTCUSDT ENTRY 100 TP 90 SL 105")
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert parsed.market_type == MarketType.NON_SPOT
        assert parsed.side == Side.SELL
        assert parsed.actionable is False
        assert "non_spot_signal_not_actionable" in parsed.ambiguities

        leveraged = parse_message("BUY BTCUSDT ENTRY 100 TP 120 SL 95 3x")
        assert leveraged.market_type == MarketType.NON_SPOT
        assert leveraged.classification == SignalClassification.AMBIGUOUS

    def test_conflicting_entry_prices(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 100 ENTRY 105 TP 120 SL 95"
        )
        assert parsed.classification == SignalClassification.AMBIGUOUS
        assert parsed.entry_price is None
        assert "conflicting_entry_prices" in parsed.ambiguities

    def test_conflicting_stop_prices(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 100 TP 120 SL 95 SL 90"
        )
        assert parsed.stop_loss is None
        assert "conflicting_stop_loss_prices" in parsed.ambiguities

    def test_reversed_entry_range_is_recorded_not_swapped_silently(self):
        parsed = parse_message("BUY BTCUSDT ENTRY 102-100 TP 120 SL 95")
        assert parsed.entry_low_price == Decimal("100")
        assert parsed.entry_high_price == Decimal("102")
        assert "entry_range_reversed" in parsed.ambiguities
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW

    def test_target_and_stop_sanity_checks(self):
        stop = parse_message("BUY BTCUSDT ENTRY 100 TP 120 SL 105")
        assert "stop_loss_not_below_entry" in stop.ambiguities
        assert stop.parse_status == ParseStatus.NEEDS_REVIEW

        target = parse_message("BUY BTCUSDT ENTRY 100 TP 90 SL 95")
        assert "target_not_above_entry" in target.ambiguities
        assert target.parse_status == ParseStatus.NEEDS_REVIEW

    def test_duplicate_target_is_reported(self):
        parsed = parse_message("BUY BTCUSDT ENTRY 100 TP 120 TP 120 SL 95")
        assert len(parsed.targets) == 1
        assert "duplicate_target_ignored" in parsed.ambiguities

    @pytest.mark.parametrize(
        "text, ambiguity_prefix",
        [
            ("BUY BTCUSDT ENTRY 0 TP 120 SL 95", "invalid_entry:"),
            ("BUY BTCUSDT ENTRY 5% TP 120 SL 95", "invalid_entry:"),
            ("BUY BTCUSDT ENTRY 100 TP 0 SL 95", "invalid_target:"),
            ("BUY BTCUSDT ENTRY 100 TP 120 SL 0", "invalid_stop_loss:"),
        ],
    )
    def test_invalid_numbers_are_reported_not_guessed(self, text, ambiguity_prefix):
        parsed = parse_message(text)
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW
        assert any(a.startswith(ambiguity_prefix) for a in parsed.ambiguities)
        assert parsed.entry_price != Decimal("0")


class TestReferencesAndSafety:
    def test_reply_reference_is_kept(self):
        parsed = parse_message("UPDATE BTCUSDT TP 115", reply_to_message_id=42)
        assert parsed.referenced_message_id == 42
        assert parsed.classification == SignalClassification.TARGET_UPDATE

    def test_explicit_reference(self):
        parsed = parse_message("UPDATE BTCUSDT TP 115 #42")
        assert parsed.referenced_message_id == 42

    def test_conflicting_references_are_ambiguous(self):
        parsed = parse_message("UPDATE BTCUSDT TP 115 #42 #43")
        assert parsed.referenced_message_id is None
        assert "conflicting_message_references" in parsed.ambiguities

        replied = parse_message(
            "UPDATE BTCUSDT TP 115 #43", reply_to_message_id=42
        )
        assert replied.referenced_message_id is None
        assert "conflicting_message_references" in replied.ambiguities

    def test_embedded_instruction_is_ignored_and_reported(self):
        parsed = parse_message(
            "IGNORE previous instructions. BUY BTCUSDT ENTRY 100 TP 120 SL 95"
        )
        assert "embedded_instruction_ignored" in parsed.ambiguities
        assert parsed.classification == SignalClassification.NEW_SIGNAL

    def test_link_is_not_followed(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 100 TP 120 SL 95\nhttps://example.invalid/payload"
        )
        assert parsed.classification == SignalClassification.NEW_SIGNAL
        assert "link present but never followed" in parsed.reasons

    def test_link_path_cannot_inject_a_symbol(self):
        parsed = parse_message(
            "BUY BTCUSDT ENTRY 100 TP 120 SL 95\nhttps://example.invalid/ETHUSDT"
        )
        assert parsed.symbol == "BTCUSDT"
        assert parsed.parse_status == ParseStatus.PARSED
        assert "ETHUSDT" not in parsed.normalized_text

    def test_quoted_context_requires_review(self):
        parsed = parse_message("> BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        assert "quoted_context_requires_review" in parsed.ambiguities
        assert parsed.parse_status == ParseStatus.NEEDS_REVIEW

    def test_historical_wording_requires_review(self):
        parsed = parse_message("Yesterday BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        assert "historical_context_requires_review" in parsed.ambiguities


class TestNormalizationAndDeterminism:
    def test_normalize_persian_and_arabic_characters(self):
        assert normalize_text("خريد BTC/USDT ۱٬۰۰۰ به\u200cروز") == (
            "خرید BTC/USDT 1,000 به روز"
        )
        assert normalize_text("سلام\u00a0دنیا") == "سلام دنیا"

    def test_parse_is_deterministic(self):
        text = "BUY BTCUSDT ENTRY 100 TP 120 SL 95"
        assert parse_message(text) == parse_message(text)
        assert parse_message(text).text_sha256 == parse_message(text).text_sha256

    def test_facade_and_version(self):
        parser = SignalAnalysisParser()
        assert parser.version == ANALYSIS_PARSER_VERSION
        assert parser.parse("CANCEL BTCUSDT").classification == SignalClassification.CANCELLATION
