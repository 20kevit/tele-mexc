"""Synthetic channel-format input for offline integration tests (not raw Telegram data)."""


def channel_signal(*, direction="LONG", symbol="BTCUSDT", entry="100", stop="95", tp1="110", tp2="120"):
    return (
        f"🆔 #0042\n⏰ 1405/08/01\nTIME = 08:45\n*{direction}*\n⚡️TOOBIT:{symbol}.P(15)\n\n"
        f"✅Entry: {entry}\n🔰Stop Loss : {stop}\n\n📌TP1 : {tp1}\n📌TP2 : {tp2}\n📌TP3 : 130\n📌TP4 : 140"
    )
