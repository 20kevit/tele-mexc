"""Signal parsing, risk controls and durable MEXC Spot execution state."""

from __future__ import annotations

from .engine import ReconcileStats, TradingEngine
from .monitor import MarketMonitor, PollingPriceFeed
from .parser import PARSER_VERSION, ParsedSignal, SignalParser
from .risk import RiskDecision, RiskManager
from .runtime import apply_persisted_trading_settings, build_mexc_client, build_trading_engine
from .store import (
    OPEN_INTENT_STATES,
    TERMINAL_INTENT_STATES,
    RiskLimitExceeded,
    TradingError,
    TradingStore,
)
from .trade_manager import TradeDecision, TradeManager
from .trade_store import TradeStore

__all__ = [
    "PARSER_VERSION",
    "ParsedSignal",
    "SignalParser",
    "RiskDecision",
    "RiskManager",
    "TradingEngine",
    "TradingStore",
    "TradingError",
    "RiskLimitExceeded",
    "ReconcileStats",
    "OPEN_INTENT_STATES",
    "TERMINAL_INTENT_STATES",
    "build_mexc_client",
    "build_trading_engine",
    "apply_persisted_trading_settings",
    "TradeManager",
    "TradeDecision",
    "TradeStore",
    "MarketMonitor",
    "PollingPriceFeed",
]
