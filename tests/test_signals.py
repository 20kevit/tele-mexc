"""Signal parser: valid alerts, ranges, ambiguity, cancel/result and fingerprints."""

from __future__ import annotations

from decimal import Decimal

from tg_signal_ingest.trading.parser import SignalParser


def test_parses_plain_spot_alert_with_targets_and_stop():
    parsed = SignalParser().parse("BUY BTCUSDT\nENTRY: 100\nTP1: 110\nTP2: 120\nSL: 95\nSIZE: $25")
    assert parsed.kind == "alert"
    assert parsed.action == "buy"
    assert parsed.symbol == "BTCUSDT"
    assert parsed.entry_price == Decimal("100")
    assert parsed.target_prices == (Decimal("110"), Decimal("120"))
    assert parsed.stop_loss == Decimal("95")
    assert parsed.amount_text == "$25"
    assert parsed.actionable


def test_parses_entry_range_and_slash_symbol():
    parsed = SignalParser().parse("SELL eth/usdt entry zone: 2000 - 2020 TP: 1900 SL: 2050")
    assert parsed.kind == "alert"
    assert parsed.action == "sell"
    assert parsed.symbol == "ETHUSDT"
    assert parsed.entry_price == Decimal("2010")
    assert parsed.entry_low_price == Decimal("2000")
    assert parsed.entry_high_price == Decimal("2020")


def test_edit_is_update_kind_and_same_text_has_same_fingerprint():
    parser = SignalParser()
    first = parser.parse("BUY BTCUSDT ENTRY 100 TP 110 SL 95")
    edited = parser.parse("BUY BTCUSDT ENTRY 100 TP 110 SL 95", is_edit=True)
    assert edited.kind == "update"
    assert first.fingerprint_sha256 == edited.fingerprint_sha256


def test_missing_symbol_is_ambiguous_not_tradeable():
    parsed = SignalParser().parse("BUY now, entry 100")
    assert parsed.kind == "ambiguous"
    assert "no Spot symbol found" in parsed.reasons
    assert not parsed.actionable


def test_unrelated_chat_is_info():
    parsed = SignalParser().parse("Good morning everyone, market looks calm")
    assert parsed.kind == "info"
    assert not parsed.actionable


def test_cancel_and_result_are_not_executable():
    parser = SignalParser()
    cancel = parser.parse("CANCEL BTCUSDT")
    assert cancel.kind == "cancel"
    assert cancel.symbol == "BTCUSDT"
    result = parser.parse("BTCUSDT TP1 HIT - booked profit")
    assert result.kind == "result"
    assert not result.actionable


def test_both_buy_and_sell_words_is_ambiguous():
    parsed = SignalParser().parse("BUY BTCUSDT then SELL later entry 100")
    assert parsed.kind == "ambiguous"
    assert any("direction" in reason for reason in parsed.reasons)


def test_dollar_base_symbol_defaults_to_usdt_quote():
    parsed = SignalParser().parse("BUY $SOL entry 20 TP 25 SL 18")
    assert parsed.symbol == "SOLUSDT"
    assert parsed.kind == "alert"


def test_cancel_word_inside_commentary_does_not_cancel_a_buy():
    parsed = SignalParser().parse("BUY BTCUSDT ENTRY 100 TP 120 SL 95. Price may close above resistance later.")
    assert parsed.kind == "alert"
    assert parsed.action == "buy"


def test_numeric_slash_range_is_not_a_symbol():
    parsed = SignalParser().parse("BUY entry 100/200 TP 120 SL 95")
    assert parsed.kind == "ambiguous"
    assert parsed.symbol is None


def test_direction_mapping_and_futures_terminology_is_ingested():
    parser = SignalParser()
    long_signal = parser.parse("LONG BTCUSDT ENTRY 100 TP 110 SL 95")
    assert long_signal.kind == "alert"
    assert long_signal.action == "buy"
    assert long_signal.actionable

    short_signal = parser.parse("SHORT BTCUSDT ENTRY 100 TP 90 SL 105")
    assert short_signal.kind == "alert"
    assert short_signal.action == "sell"  # parsed, then refused by risk

    for text in (
        "BUY BTCUSDT ENTRY 100 TP 110 SL 95 10x leverage",
        "SELL BTCUSDT FUTURES ENTRY 100 TP 90 SL 105",
    ):
        parsed = parser.parse(text)
        assert parsed.kind == "alert", text
        assert parsed.actionable
        assert not any("Futures" in reason for reason in parsed.reasons)


def test_channel_long_template_maps_to_spot_buy():
    text = (
        "⏰ 1405/08/01\n TIME =08:45\n *LONG*🟢 \n"
        " ⚡️TOOBIT:ABCUSDT.P(15)\n\n ✅Entry: 100.00\n 🔰Stop Loss : 95.00\n\n"
        " 📌TP1 : 105.00\n 📌TP2 : 110.00\n 📌TP3 : 115.00\n 📌TP4 : 120.00\n"
        " 🤑FULL Target : 130.00\n 💰Deposit 1.00%  \n"
        " 💠MAX LEV Suggested  =  8 (-17.39%)\n"
    )
    parsed = SignalParser().parse(text)
    assert parsed.kind == "alert"
    assert parsed.action == "buy"
    assert parsed.symbol == "ABCUSDT"
    assert parsed.entry_price == Decimal("100.00")
    assert parsed.stop_loss == Decimal("95.00")
    assert parsed.amount_text is None  # deposit/leverage never size the order.


def test_channel_follow_up_posts_are_never_actionable():
    parser = SignalParser()
    for text in (
        "🆔 #302\nTOOBIT:XYZUSDT.P\n🟢 ورود پوزیشن LONG تریگر شد\n🎯 Entry: 100.00",
        "🆔 #304\nXYZUSDT\n🚀 TP4 reached @ 1.592\n📊 Profit: 14.8 %",
        "🆔 #305\nABCUSDT\n📌 ورود LONG با موفقیت ثبت شد\nقیمت: 100.00",
        "TOOBIT:XYZUSDT.P (LONG)\n🎯 تارگت 3 با موفقیت خورد!\n💰 11.9 % سود",
    ):
        parsed = parser.parse(text)
        assert parsed.kind == "result", text
        assert not parsed.actionable
