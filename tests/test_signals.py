"""Strict channel-template recognition; all examples here are synthetic."""

import hashlib
from decimal import Decimal

import pytest

from tests.channel_template import channel_signal
from tg_signal_ingest.trading.parser import SignalParser

FOOTER = (
    "\n------------------\n🤑FULL Target : 200\n-----\n💰Deposit 1.00%"
    "\n💠MAX LEV Suggested = 8\n💬 : SHORT?\n⭐️ CHANNEL"
)


@pytest.mark.parametrize(("direction", "action"), [("LONG", "buy"), ("SHORT", "sell")])
@pytest.mark.parametrize("whitespace", ["\n", "\r\n\n", " \t "])
def test_extracts_every_mandatory_field(direction, action, whitespace):
    entry = "0.0000000000000000000000000000000001234567890"
    text = channel_signal(direction=direction, symbol="1INCHUSDT", entry=entry, stop="0", tp1="1.234500")
    text = (text + FOOTER).replace("\n", whitespace).replace("TP2 :", "TP 2=").replace(".P(15)", ".P ( 15 )")
    parsed = SignalParser().parse(text)
    assert parsed.kind == "signal"
    assert (parsed.signal_id, parsed.date, parsed.time) == ("0042", "1405/08/01", "08:45")
    assert (parsed.action, parsed.symbol) == (action, "1INCHUSDT")
    assert isinstance(parsed.entry_price, Decimal)
    assert parsed.entry_price.as_tuple() == Decimal(entry).as_tuple()
    assert parsed.stop_loss == Decimal("0")  # risk is not the parser's decision
    assert parsed.target_prices == tuple(map(Decimal, ("1.234500", "120", "130", "140")))
    assert parsed.raw_text == text
    assert parsed.fingerprint_sha256 == hashlib.sha256(" ".join(text.split()).upper().encode()).hexdigest()


@pytest.mark.parametrize("line", [line for line in channel_signal().splitlines() if line])
def test_missing_required_field(line):
    assert SignalParser().parse(channel_signal().replace(line, "") + FOOTER).kind == "additional"


@pytest.mark.parametrize(
    "text",
    [
        None,
        "",
        "CANCEL ALL",
        "CANCEL BTCUSDT",
        "سیگنال منقضی شد",
        "LONG BTCUSDT 100",
        "BTCUSDT TP1 HIT - booked profit",
        "لغو سیگنال #0042",
        "BUY BTCUSDT ENTRY 100 SL 90",
        "result:\n" + channel_signal(),
        channel_signal() + "\nسیگنال منقضی شد",
        channel_signal().replace("08:45", "25:61"),
        channel_signal().replace("TP2", "TP1"),
        channel_signal().replace("TOOBIT", "OTHER"),
        channel_signal().replace(".P(15)", ".P(1)"),
        channel_signal().replace("Entry: 100", "Entry: 100-101"),
        channel_signal() + ".50abc",
    ],
)
def test_non_signals_return_no_partial_trade_fields(text):
    parsed = SignalParser().parse(text)
    assert parsed.kind == "additional"
    assert (parsed.action, parsed.symbol, parsed.entry_price, parsed.stop_loss, parsed.target_prices) == (
        None,
        None,
        None,
        None,
        (),
    )
