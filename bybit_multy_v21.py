#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEAN REVERSION CORE v21.0 (Lick Hunter Edition - PRODUCTION READY)
Features: 
- Adaptive DCA (non-martingale)
- Dynamic correlation matrix
- Proper risk management
- Execution realism with slippage
- Kelly-optimal position sizing
"""

import os
import sys
import time
import json
import sqlite3
import logging
import traceback
import asyncio
import aiosqlite
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from collections import deque
import numpy as np
import pandas as pd

import ccxt.async_support as ccxt_async

from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.trend import EMAIndicator, MACD

from telegram import Update, Bot
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# ====== CONFIG ======
class Config:
    # Exchange
    BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
    BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
    
    # Telegram
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # Trading Params
    IS_DRY_RUN = True
    BASE_RISK_PER_TRADE = 0.01  # 1% of equity per position (reduced from 1.5%)
    LEVERAGE = 3  # Reduced from 5x to 3x
    MAX_CONCURRENT_TRADES = 3  # Reduced from 5
    
    # ABSOLUTE limits (not percentages)
    MAX_POSITION_VALUE_USDT = 300  # Hard cap per position
    MAX_TOTAL_EXPOSURE_USDT = 1000  # Hard cap total
    
    # TP/SL - Now asymmetric (R > 1)
    TP_PCT = 0.025  # 2.5% take profit
    SL_PCT = 0.015  # 1.5% stop loss (Risk/Reward = 1.67)
    
    # DCA Grid - NON-MARTINGALE
    DCA_ENABLED = True
    MAX_DCA_STEPS = 1  # Only 1 DCA step (total 2 entries max)
    DCA_MULTIPLIER = 1.0  # Equal sizing (not exponential!)
    DCA_TRIGGER_ATR_MULTIPLIER = 1.5  # Trigger at 1.5x ATR, not fixed %
    
    # Risk Limits
    MAX_DAILY_LOSS_PCT = 0.03  # 3% max daily loss (stricter)
    CORRELATION_UPDATE_HOURS = 24  # Update correlations daily
    MIN_VOLUME_MULTIPLIER = 1.2  # Reduced from 1.5
    
    # Technical Analysis
    TIMEFRAME = "5m"
    BB_PERIOD = 20
    BB_STD = 2.5
    RSI_PERIOD = 14
    RSI_OVERSOLD = 20  # More extreme (was 25)
    RSI_OVERBOUGHT = 80  # More extreme (was 75)
    EMA_PERIOD = 50
    ATR_PERIOD = 14
    CANDLE_LIMIT = 100
    
    # Execution
    SLIPPAGE_BASE = 0.0005  # 0.05% base slippage
    SLIPPAGE_VOLATILITY_FACTOR = 2.0  # Multiply by volatility
    
    # System
    SCAN_INTERVAL = 30  # seconds
    BATCH_SIZE = 2
    BATCH_DELAY = 2
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quant_trades.db")
    LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quant_bot.log")
    
    # Symbols (focus on liquid pairs)
    SYMBOLS = [
        "BTC/USDT:USDT",
        "ETH/USDT:USDT", 
        "SOL/USDT:USDT",
        "BNB/USDT:USDT",
    ]

# ====== ENUMS & DATACLASSES ======
class PositionType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class PositionStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    STOPPED = "STOPPED"

@dataclass
class Position:
    id: Optional[int] = None
    symbol: str = ""
    position_type: PositionType = PositionType.LONG
    base_amount: float = 0.0
    avg_price: float = 0.0
    tp_price: float = 0.0
    sl_price: float = 0.0
    dca_count: int = 0
    open_time: str = ""
    close_time: Optional[str] = None
    close_price: Optional[float] = None
    pnl_percent: float = 0.0
    pnl_usdt: float = 0.0
    fee_usdt: float = 0.0
    status: PositionStatus = PositionStatus.OPEN
    leverage: int = 1
    max_risk_usdt: float = 0.0  # Track max risk for this position

@dataclass
class Signal:
    symbol: str
    position_type: PositionType
    price: float
    volume: float
    reason: str
    atr: float  # Store ATR for position sizing

# ====== SAFE LOGGING ======
class SafeLogger:
    MAX_LOG_LENGTH = 500
    
    @staticmethod
    def safe_format(obj: Any) -> str:
        try:
            if isinstance(obj, (dict, list)):
                s = json.dumps(obj, default=str)
            else:
                s = str(obj)
            
            if len(s) > SafeLogger.MAX_LOG_LENGTH:
                return f"{s[:SafeLogger.MAX_LOG_LENGTH]}... [truncated {len(s)} chars]"
            return s
        except:
            return "[Unprintable object]"

class LimitedLengthFormatter(logging.Formatter):
    def format(self, record):
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            if len(record.msg) > 1000:
                record.msg = record.msg[:1000] + f"... [truncated {len(record.msg)} chars]"
        return super().format(record)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

for handler in logging.root.handlers:
    handler.setFormatter(LimitedLengthFormatter('%(asctime)s - %(levelname)s - %(message)s'))

logger = logging.getLogger("LickHunter")

# ====== CORE CLASSES ======
class CircuitBreaker:
    def __init__(self, max_fails: int = 3, max_latency_ms: float = 2000.0, cooldown: int = 300):
        self.max_fails = max_fails
        self.max_latency = max_latency_ms / 1000.0
        self.cooldown = cooldown
        self.failures = 0
        self.paused_until = 0.0
        self.latencies: List[float] = []
        self.last_rate_limit = 0
        self.consecutive_errors = 0

    def record_call(self, latency: float, success: bool, is_rate_limit: bool = False):
        if is_rate_limit:
            self.last_rate_limit = time.time()
            self.failures += 2  # Rate limits count double
            self.consecutive_errors += 1
            logger.warning(f"⚠️ Rate limit hit, failures: {self.failures}")
        elif not success:
            self.failures += 1
            self.consecutive_errors += 1
        else:
            self.failures = max(0, self.failures - 1)
            self.consecutive_errors = max(0, self.consecutive_errors - 1)
            self.latencies.append(latency)
            if len(self.latencies) > 10:
                self.latencies.pop(0)

        avg_lat = sum(self.latencies) / len(self.latencies) if self.latencies else 0.0
        
        if self.failures >= self.max_fails or avg_lat > self.max_latency or self.consecutive_errors >= 5:
            self.paused_until = time.time() + self.cooldown
            self.failures = 0
            self.consecutive_errors = 0
            self.latencies.clear()
            logger.critical(f"🛑 CIRCUIT BREAKER Triggered. Paused for {self.cooldown}s.")

    def is_active(self) -> bool:
        if time.time() < self.paused_until:
            return True
        if self.paused_until > 0:
            self.paused_until = 0.0
            logger.info("🟢 Circuit Breaker Reset.")
        return False
    
    def recent_rate_limit(self) -> bool:
        return time.time() - self.last_rate_limit < 60

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        self.db = await aiosqlite.connect(self.db_path)
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute("PRAGMA foreign_keys=ON;")
        await self._init_tables()
        logger.info("✅ Database initialized")

    async def close(self):
        if self.db:
            await self.db.close()
            logger.info("📁 Database closed")

    async def _init_tables(self):
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                position_type TEXT NOT NULL,
                base_amount REAL NOT NULL,
                avg_price REAL NOT NULL,
                tp_price REAL NOT NULL,
                sl_price REAL NOT NULL,
                dca_count INTEGER DEFAULT 0,
                open_time TEXT NOT NULL,
                close_time TEXT,
                close_price REAL,
                pnl_percent REAL DEFAULT 0,
                pnl_usdt REAL DEFAULT 0,
                fee_usdt REAL DEFAULT 0,
                status TEXT DEFAULT 'OPEN',
                leverage INTEGER DEFAULT 1,
                max_risk_usdt REAL DEFAULT 0
            )
        """)
        
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                starting_equity REAL,
                ending_equity REAL,
                pnl_usdt REAL,
                trades_count INTEGER,
                win_count INTEGER,
                max_drawdown REAL
            )
        """)
        
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS correlations (
                symbol1 TEXT,
                symbol2 TEXT,
                correlation REAL,
                updated TEXT,
                PRIMARY KEY (symbol1, symbol2)
            )
        """)
        
        await self.db.commit()

    async def execute(self, query: str, params: Tuple = ()) -> None:
        await self.db.execute(query, params)
        await self.db.commit()

    async def fetchone(self, query: str, params: Tuple = ()) -> Optional[Tuple]:
        cursor = await self.db.execute(query, params)
        return await cursor.fetchone()

    async def fetchall(self, query: str, params: Tuple = ()) -> List[Tuple]:
        cursor = await self.db.execute(query, params)
        return await cursor.fetchall()

class RiskManager:
    def __init__(self, config: Config, db: DatabaseManager):
        self.config = config
        self.db = db
        self.daily_start_equity: Optional[float] = None
        self.daily_pnl_usdt = 0.0
        self.daily_max_drawdown = 0.0
        self.last_reset_day = datetime.now().date()
        self.last_correlation_update = 0
        self.correlation_matrix: Dict[str, Dict[str, float]] = {}

    async def start_new_day(self, current_equity: float):
        """Reset daily stats"""
        self.daily_start_equity = current_equity
        self.daily_pnl_usdt = 0.0
        self.daily_max_drawdown = 0.0
        self.last_reset_day = datetime.now().date()
        
        await self.db.execute("""
            INSERT OR REPLACE INTO daily_stats 
            (date, starting_equity, ending_equity, pnl_usdt, trades_count, win_count, max_drawdown)
            VALUES (date('now'), ?, ?, ?, ?, ?, ?)
        """, (current_equity, current_equity, 0, 0, 0, 0))
        
        logger.info(f"📅 New trading day started. Starting equity: ${current_equity:.2f}")

    async def update_daily_pnl(self, pnl_usdt: float):
        """Update daily PnL - absolute values, not percentages"""
        if not self.daily_start_equity:
            await self.start_new_day(1000.0)  # Default start
        
        self.daily_pnl_usdt += pnl_usdt
        
        # Track max drawdown
        current_pnl_pct = self.daily_pnl_usdt / self.daily_start_equity
        if current_pnl_pct < self.daily_max_drawdown:
            self.daily_max_drawdown = current_pnl_pct
        
        logger.info(f"📈 Daily PnL: {self.daily_pnl_usdt:+.2f} USDT ({current_pnl_pct*100:+.2f}%)")

    def check_daily_loss(self) -> bool:
        """Check if daily loss limit is reached (absolute, not percentage drift)"""
        today = datetime.now().date()
        
        if today > self.last_reset_day and self.daily_start_equity:
            asyncio.create_task(self.start_new_day(self.daily_start_equity + self.daily_pnl_usdt))
            return True
        
        if not self.daily_start_equity:
            return True
        
        loss_pct = self.daily_pnl_usdt / self.daily_start_equity
        if loss_pct < -self.config.MAX_DAILY_LOSS_PCT:
            logger.critical(f"🚫 Daily loss limit reached: {loss_pct*100:.2f}%")
            return False
        
        return True

    async def update_correlations(self, exchange):
        """Update correlation matrix from real market data"""
        try:
            logger.info("🔄 Updating correlation matrix...")
            data = {}
            
            for symbol in self.config.SYMBOLS:
                # Передаем timeframe и limit как именованные аргументы
                df = await exchange.fetch_ohlcv(symbol, timeframe='1d', limit=30)
                if not df.empty:
                    # Правильно извлекаем колонку 'close' из DataFrame
                    returns = df['close'].pct_change().dropna()
                    data[symbol] = returns
            
            if len(data) > 1:
                df = pd.DataFrame(data)
                corr_matrix = df.corr()
                
                # Store in database
                for sym1 in self.config.SYMBOLS:
                    for sym2 in self.config.SYMBOLS:
                        if sym1 in corr_matrix.index and sym2 in corr_matrix.columns:
                            corr = corr_matrix.loc[sym1, sym2]
                            await self.db.execute("""
                                INSERT OR REPLACE INTO correlations (symbol1, symbol2, correlation, updated)
                                VALUES (?, ?, ?, datetime('now'))
                            """, (sym1, sym2, corr))
                
                self.last_correlation_update = time.time()
                logger.info(f"✅ Correlations updated for {len(data)} symbols")
                
        except Exception as e:
            logger.error(f"Failed to update correlations: {e}")

    async def get_correlation(self, symbol1: str, symbol2: str) -> float:
        """Get current correlation between two symbols"""
        # Update if stale
        if time.time() - self.last_correlation_update > self.config.CORRELATION_UPDATE_HOURS * 3600:
            # Don't await - run in background
            asyncio.create_task(self.update_correlations(None))  # Need exchange instance
        
        # Try to get from DB
        result = await self.db.fetchone("""
            SELECT correlation FROM correlations 
            WHERE symbol1=? AND symbol2=?
        """, (symbol1, symbol2))
        
        if result:
            return result[0]
        
        # Default correlations (fallback)
        defaults = {
            ('BTC/USDT:USDT', 'ETH/USDT:USDT'): 0.8,
            ('BTC/USDT:USDT', 'SOL/USDT:USDT'): 0.7,
            ('ETH/USDT:USDT', 'SOL/USDT:USDT'): 0.75,
        }
        return defaults.get((symbol1, symbol2), 0.5)

    async def check_correlation(self, symbol: str, open_positions: List[Position]) -> bool:
        """Check if new position correlates too much with existing ones"""
        if not open_positions:
            return True
        
        for pos in open_positions:
            corr = await self.get_correlation(symbol, pos.symbol)
            if corr > self.config.CORRELATION_THRESHOLD:
                logger.warning(f"🚫 High correlation {symbol}/{pos.symbol}: {corr:.2f}")
                return False
        
        return True

    def calculate_position_size(self, equity: float, current_exposure: float, atr_pct: float) -> float:
        """Kelly-optimal position sizing based on volatility"""
        # Kelly formula: f = (p * b - q) / b
        # where p = win probability, q = loss probability, b = odds
        
        # Estimate win probability from historical (simplified)
        win_rate = 0.6  # Could be loaded from DB
        
        # Risk/Reward ratio
        rr_ratio = self.config.TP_PCT / self.config.SL_PCT  # 2.5/1.5 = 1.67
        
        # Kelly fraction
        kelly = (win_rate * rr_ratio - (1 - win_rate)) / rr_ratio
        kelly = max(0.1, min(0.25, kelly))  # Cap between 10-25%
        
        # Base size from equity
        base_size = equity * self.config.BASE_RISK_PER_TRADE * self.config.LEVERAGE
        
        # Adjust for volatility (smaller size in high vol)
        vol_adjustment = 1.0 / (1.0 + atr_pct * 10)  # ATR 2% -> adjust 0.83
        
        # Apply Kelly
        kelly_adjusted = base_size * kelly * vol_adjustment
        
        # Apply absolute caps
        max_allowed = min(
            self.config.MAX_POSITION_VALUE_USDT,
            self.config.MAX_TOTAL_EXPOSURE_USDT - current_exposure
        )
        
        final_size = min(kelly_adjusted, max_allowed)
        
        logger.debug(f"💰 Position sizing: equity=${equity:.0f}, exposure=${current_exposure:.0f}, "
                    f"kelly={kelly:.2f}, vol_adj={vol_adjustment:.2f}, size=${final_size:.0f}")
        
        return final_size

class TechnicalAnalyzer:
    def __init__(self, config: Config):
        self.config = config

    async def analyze(self, df: pd.DataFrame) -> Optional[Signal]:
        """Analyze dataframe for trading signals"""
        if df.empty or len(df) < 50:
            return None

        try:
            # Calculate indicators
            bb = BollingerBands(df['close'], window=self.config.BB_PERIOD, window_dev=self.config.BB_STD)
            rsi_ind = RSIIndicator(df['close'], window=self.config.RSI_PERIOD)
            ema = EMAIndicator(df['close'], window=self.config.EMA_PERIOD)
            atr = AverageTrueRange(df['high'], df['low'], df['close'], window=self.config.ATR_PERIOD)
            
            current_price = float(df['close'].iloc[-1])
            current_volume = float(df['volume'].iloc[-1])
            current_atr = float(atr.average_true_range().iloc[-1])
            atr_pct = current_atr / current_price
            
            lower_band = float(bb.bollinger_lband().iloc[-1])
            upper_band = float(bb.bollinger_hband().iloc[-1])
            rsi = float(rsi_ind.rsi().iloc[-1])
            ema_value = float(ema.ema_indicator().iloc[-1])
            
            symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else "unknown"
            
            # Volume check
            avg_volume = float(df['volume'].rolling(20).mean().iloc[-1])
            volume_spike = current_volume > avg_volume * self.config.MIN_VOLUME_MULTIPLIER
            
            # Log indicators
            logger.info(f"📊 {symbol} - Price: ${current_price:.2f}, RSI: {rsi:.1f}, "
                       f"ATR: {atr_pct*100:.2f}%, BB Lower: ${lower_band:.2f}, "
                       f"EMA: ${ema_value:.2f}, Vol: {current_volume:.0f} (avg: {avg_volume:.0f})")

            # LONG signal - extreme oversold
            if current_price < lower_band and rsi < self.config.RSI_OVERSOLD:
                if not volume_spike:
                    logger.debug(f"📉 {symbol} - Oversold but volume too low")
                    return None
                
                # Dynamic EMA deviation based on ATR
                min_deviation = max(0.03, atr_pct * 2)  # At least 3% or 2x ATR
                if current_price < ema_value * (1 - min_deviation):
                    logger.info(f"🔴 {symbol} - LONG SIGNAL! Price=${current_price:.2f}, RSI={rsi:.1f}")
                    return Signal(
                        symbol=symbol,
                        position_type=PositionType.LONG,
                        price=current_price,
                        volume=current_volume,
                        reason=f"Oversold: RSI={rsi:.1f}, BB breach",
                        atr=atr_pct
                    )
            
            # SHORT signal - extreme overbought
            elif current_price > upper_band and rsi > self.config.RSI_OVERBOUGHT:
                if not volume_spike:
                    logger.debug(f"📈 {symbol} - Overbought but volume too low")
                    return None
                
                min_deviation = max(0.03, atr_pct * 2)
                if current_price > ema_value * (1 + min_deviation):
                    logger.info(f"🟢 {symbol} - SHORT SIGNAL! Price=${current_price:.2f}, RSI={rsi:.1f}")
                    return Signal(
                        symbol=symbol,
                        position_type=PositionType.SHORT,
                        price=current_price,
                        volume=current_volume,
                        reason=f"Overbought: RSI={rsi:.1f}, BB breach",
                        atr=atr_pct
                    )
            
            return None
            
        except Exception as e:
            logger.error(f"Error in technical analysis: {e}")
            return None

class ExchangeManager:
    def __init__(self, config: Config):
        self.config = config
        self.exchange = None
        self.circuit_breaker = CircuitBreaker()
        self.last_request_time = 0
        self.min_request_interval = 1.0

    async def init(self):
        try:
            self.exchange = ccxt_async.bybit({
                "apiKey": self.config.BYBIT_API_KEY,
                "secret": self.config.BYBIT_API_SECRET,
                "options": {
                    "defaultType": "swap",
                    "adjustForTimeDifference": True,
                },
                "enableRateLimit": True,
            })
            
            markets = await self.exchange.load_markets()
            logger.info(f"✅ Exchange connected - {len(markets)} markets")
            
        except Exception as e:
            logger.error(f"❌ Exchange init failed: {e}")
            raise

    async def close(self):
        if self.exchange:
            await self.exchange.close()
            logger.info("🔌 Exchange connection closed")

    async def _throttle(self):
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()

    def _calculate_slippage(self, symbol: str, volatility: float) -> float:
        """Calculate dynamic slippage based on volatility"""
        base = self.config.SLIPPAGE_BASE
        vol_factor = self.config.SLIPPAGE_VOLATILITY_FACTOR
        return base * (1 + volatility * vol_factor)

    async def get_volatility(self, symbol: str) -> float:
        """Get current volatility for slippage calculation"""
        try:
            df = await self.fetch_ohlcv(symbol, limit=20)
            if df.empty:
                return 0.0
            
            returns = df['close'].pct_change().dropna()
            return float(returns.std())
        except:
            return 0.0

    async def execute_order(self, symbol: str, side: str, amount: float, 
                           position_type: PositionType) -> Tuple[float, float]:
        """Execute order with realistic slippage"""
        await self._throttle()
        
        try:
            # Get current prices
            bid, ask = await self.fetch_bbo(symbol)
            if bid == 0 or ask == 0:
                return 0.0, 0.0
            
            # Get volatility
            volatility = await self.get_volatility(symbol)
            slippage = self._calculate_slippage(symbol, volatility)
            
            # Calculate execution price
            if position_type == PositionType.LONG:
                exec_price = ask * (1 + slippage) if side == 'buy' else bid * (1 - slippage)
            else:
                exec_price = bid * (1 - slippage) if side == 'buy' else ask * (1 + slippage)
            
            # Calculate fee
            fee = (amount * exec_price) * 0.0005  # 0.05%
            
            logger.debug(f"💹 Order: {side} {amount:.4f} {symbol} @ ${exec_price:.4f} "
                        f"(slippage: {slippage*100:.3f}%)")
            
            return exec_price, fee
            
        except Exception as e:
            logger.error(f"Order execution failed: {e}")
            return 0.0, 0.0

    async def fetch_bbo(self, symbol: str) -> Tuple[float, float]:
        await self._throttle()
        
        start_t = time.time()
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            latency = time.time() - start_t
            self.circuit_breaker.record_call(latency, True)
            
            bid = float(ticker.get('bid', 0))
            ask = float(ticker.get('ask', 0))
            
            if bid == 0 or ask == 0:
                return 0.0, 0.0
            
            return bid, ask
            
        except Exception as e:
            latency = time.time() - start_t
            is_rate_limit = "rate limit" in str(e).lower()
            self.circuit_breaker.record_call(latency, False, is_rate_limit)
            return 0.0, 0.0

    async def fetch_ohlcv(self, symbol: str, timeframe: str = None, limit: int = None) -> pd.DataFrame:
        await self._throttle()
        
        # Если таймфрейм не передан, берем из конфига (5m)
        tf = timeframe or self.config.TIMEFRAME
        lim = limit or self.config.CANDLE_LIMIT
        start_t = time.time()
        
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol, tf, limit=lim
            )
            
            if not ohlcv:
                return pd.DataFrame()
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['symbol'] = symbol
            latency = time.time() - start_t
            
            self.circuit_breaker.record_call(latency, True)
            
            return df
            
        except Exception as e:
            latency = time.time() - start_t
            is_rate_limit = "rate limit" in str(e).lower()
            self.circuit_breaker.record_call(latency, False, is_rate_limit)
            return pd.DataFrame()

    def is_circuit_breaker_active(self) -> bool:
        return self.circuit_breaker.is_active()
    
    def recent_rate_limit(self) -> bool:
        return self.circuit_breaker.recent_rate_limit()

class PositionManager:
    def __init__(self, config: Config, db: DatabaseManager, risk_mgr: RiskManager, exchange_mgr: ExchangeManager):
        self.config = config
        self.db = db
        self.risk_mgr = risk_mgr
        self.exchange_mgr = exchange_mgr

    async def open_position(self, signal: Signal, equity: float) -> Optional[Position]:
        try:
            # Check if position exists
            existing = await self.db.fetchone(
                "SELECT id FROM positions WHERE symbol=? AND status='OPEN'",
                (signal.symbol,)
            )
            if existing:
                return None

            # Get current exposure
            exposure = await self._get_symbol_exposure(signal.symbol)
            
            # Calculate position size (now with ATR)
            usdt_size = self.risk_mgr.calculate_position_size(equity, exposure, signal.atr)
            
            if usdt_size < 10:
                logger.warning(f"⚠️ {signal.symbol} - Position size too small: ${usdt_size:.2f}")
                return None

            # Execute order with slippage
            side = 'buy' if signal.position_type == PositionType.LONG else 'sell'
            exec_price, fee = await self.exchange_mgr.execute_order(
                signal.symbol, side, usdt_size / signal.price, signal.position_type
            )
            
            if exec_price == 0:
                logger.error(f"❌ Failed to execute order for {signal.symbol}")
                return None

            # Calculate TP/SL (asymmetric)
            if signal.position_type == PositionType.LONG:
                tp_price = exec_price * (1 + self.config.TP_PCT)
                sl_price = exec_price * (1 - self.config.SL_PCT)
            else:
                tp_price = exec_price * (1 - self.config.TP_PCT)
                sl_price = exec_price * (1 + self.config.SL_PCT)

            # Create position
            position = Position(
                symbol=signal.symbol,
                position_type=signal.position_type,
                base_amount=usdt_size / exec_price,
                avg_price=exec_price,
                tp_price=tp_price,
                sl_price=sl_price,
                dca_count=0,
                open_time=datetime.now().isoformat(),
                leverage=self.config.LEVERAGE,
                fee_usdt=fee,
                max_risk_usdt=usdt_size * self.config.SL_PCT  # Max possible loss
            )

            # Save to database
            await self.db.execute("""
                INSERT INTO positions 
                (symbol, position_type, base_amount, avg_price, tp_price, sl_price, 
                 dca_count, open_time, leverage, fee_usdt, max_risk_usdt, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
            """, (
                position.symbol, position.position_type.value, position.base_amount,
                position.avg_price, position.tp_price, position.sl_price,
                position.dca_count, position.open_time, position.leverage, 
                position.fee_usdt, position.max_risk_usdt
            ))

            logger.info(f"🔪 NEW POSITION: {position.position_type.value} {position.symbol} "
                       f"@ ${position.avg_price:.4f} | Risk: ${position.max_risk_usdt:.2f}")
            return position
            
        except Exception as e:
            logger.error(f"Error opening position: {e}")
            return None

    async def close_position(self, position: Position, reason: str = "TP") -> None:
        try:
            # Get current price for exit
            bid, ask = await self.exchange_mgr.fetch_bbo(position.symbol)
            if bid == 0 or ask == 0:
                logger.error(f"Cannot close {position.symbol} - no price")
                return
            
            # Execute exit order with slippage
            side = 'sell' if position.position_type == PositionType.LONG else 'buy'
            exit_price, exit_fee = await self.exchange_mgr.execute_order(
                position.symbol, side, position.base_amount, position.position_type
            )
            
            if exit_price == 0:
                logger.error(f"Failed to close {position.symbol}")
                return

            # Calculate PnL
            if position.position_type == PositionType.LONG:
                raw_pnl = position.base_amount * (exit_price - position.avg_price)
            else:
                raw_pnl = position.base_amount * (position.avg_price - exit_price)
            
            total_fee = position.fee_usdt + exit_fee
            net_pnl = raw_pnl - total_fee
            
            margin_used = (position.base_amount * position.avg_price) / position.leverage
            pnl_pct = (net_pnl / margin_used) * 100

            # Update position
            await self.db.execute("""
                UPDATE positions 
                SET status='CLOSED', close_time=datetime('now'), close_price=?,
                    pnl_percent=?, pnl_usdt=?, fee_usdt=?
                WHERE id=?
            """, (exit_price, pnl_pct, net_pnl, total_fee, position.id))

            # Update risk manager
            await self.risk_mgr.update_daily_pnl(net_pnl)

            emoji = "🎯" if reason == "TP" else "🛑"
            logger.info(f"{emoji} CLOSED: {position.symbol} | PnL: {net_pnl:+.2f} USDT ({pnl_pct:+.1f}%)")
            
        except Exception as e:
            logger.error(f"Error closing position: {e}")

    async def check_dca(self, position: Position) -> bool:
        """Check if DCA should be triggered - now using ATR"""
        if not self.config.DCA_ENABLED:
            return False
        
        if position.dca_count >= self.config.MAX_DCA_STEPS:
            return False
        
        try:
            # Get current ATR
            df = await self.exchange_mgr.fetch_ohlcv(position.symbol, limit=20)
            if df.empty:
                return False
            
            atr = AverageTrueRange(df['high'], df['low'], df['close'], window=14)
            current_atr = float(atr.average_true_range().iloc[-1])
            current_price = float(df['close'].iloc[-1])
            atr_pct = current_atr / current_price
            
            # Dynamic DCA trigger based on ATR
            trigger_pct = atr_pct * self.config.DCA_TRIGGER_ATR_MULTIPLIER
            
            # Check if price moved enough
            if position.position_type == PositionType.LONG:
                price_change = (position.avg_price - current_price) / position.avg_price
                should_dca = price_change > trigger_pct
            else:
                price_change = (current_price - position.avg_price) / position.avg_price
                should_dca = price_change > trigger_pct
            
            if should_dca:
                logger.info(f"📉 DCA trigger for {position.symbol}: {price_change*100:.2f}% > {trigger_pct*100:.2f}%")
                
                # Execute DCA order
                side = 'buy' if position.position_type == PositionType.LONG else 'sell'
                dca_size = position.base_amount * self.config.DCA_MULTIPLIER
                
                exec_price, fee = await self.exchange_mgr.execute_order(
                    position.symbol, side, dca_size, position.position_type
                )
                
                if exec_price == 0:
                    return False
                
                # Update position
                new_total = position.base_amount + dca_size
                new_avg = ((position.base_amount * position.avg_price) + 
                          (dca_size * exec_price)) / new_total
                
                await self.db.execute("""
                    UPDATE positions 
                    SET base_amount=?, avg_price=?, dca_count=?, fee_usdt=?
                    WHERE id=?
                """, (new_total, new_avg, position.dca_count + 1, 
                      position.fee_usdt + fee, position.id))
                
                logger.warning(f"📉 DCA STEP {position.dca_count + 1} for {position.symbol}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"DCA check error: {e}")
            return False

    async def _get_symbol_exposure(self, symbol: str) -> float:
        try:
            result = await self.db.fetchone("""
                SELECT SUM(base_amount * avg_price) as exposure 
                FROM positions 
                WHERE symbol=? AND status='OPEN'
            """, (symbol,))
            return result[0] if result and result[0] else 0.0
        except:
            return 0.0

    async def _get_total_exposure(self) -> float:
        try:
            result = await self.db.fetchone("""
                SELECT SUM(base_amount * avg_price) as exposure 
                FROM positions 
                WHERE status='OPEN'
            """)
            return result[0] if result and result[0] else 0.0
        except:
            return 0.0

class LickHunterBot:
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.DB_PATH)
        self.exchange_mgr = ExchangeManager(config)
        self.risk_mgr = RiskManager(config, self.db)
        self.analyzer = TechnicalAnalyzer(config)
        self.position_mgr = PositionManager(config, self.db, self.risk_mgr, self.exchange_mgr)
        
        self.symbols = config.SYMBOLS
        self.is_running = True
        self.tg_bot = Bot(token=config.TELEGRAM_TOKEN) if config.TELEGRAM_TOKEN else None
        self.trading_task = None
        self.app = None
        self.scan_count = 0
        self.last_scan_log = time.time()
        self.consecutive_errors = 0

    async def start(self):
        """Start the bot"""
        await self.db.initialize()
        await self.exchange_mgr.init()
        
        # Initialize daily stats
        await self.risk_mgr.start_new_day(1000.0)
        
        # Update correlations on startup
        asyncio.create_task(self.risk_mgr.update_correlations(self.exchange_mgr))
        
        logger.info("=" * 50)
        logger.info("🤖 LICK HUNTER v21.0 STARTED")
        logger.info("=" * 50)
        logger.info(f"📝 Mode: {'PAPER' if self.config.IS_DRY_RUN else 'LIVE'}")
        logger.info(f"🎯 Monitoring {len(self.symbols)} symbols")
        logger.info(f"💰 Max position: ${self.config.MAX_POSITION_VALUE_USDT}")
        logger.info(f"📊 Daily loss limit: {self.config.MAX_DAILY_LOSS_PCT*100}%")
        
        await self._send_alert("🤖 <b>LICK HUNTER v21.0</b>\nBot started")
        
        self.trading_task = asyncio.create_task(self._trading_loop())

    async def stop(self):
        self.is_running = False
        if self.trading_task:
            self.trading_task.cancel()
            try:
                await self.trading_task
            except asyncio.CancelledError:
                pass
        await self.exchange_mgr.close()
        await self.db.close()
        logger.info("🛑 Bot stopped")

    async def _trading_loop(self):
        logger.info("🔄 Trading loop started")
        
        while self.is_running:
            try:
                cycle_start = time.time()
                await self._run_cycle()
                
                cycle_time = time.time() - cycle_start
                sleep_time = max(1, self.config.SCAN_INTERVAL - cycle_time)
                await asyncio.sleep(sleep_time)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Trading loop error: {e}")
                self.consecutive_errors += 1
                
                if self.consecutive_errors > 5:
                    logger.error("🔴 Too many errors, waiting 60s...")
                    await asyncio.sleep(60)
                    self.consecutive_errors = 0
                else:
                    await asyncio.sleep(self.config.SCAN_INTERVAL)

    async def _run_cycle(self):
        self.consecutive_errors = 0
        self.scan_count += 1
        
        if time.time() - self.last_scan_log > 600:
            logger.info(f"🔄 Scan cycle #{self.scan_count}")
            self.last_scan_log = time.time()
        
        if self.exchange_mgr.is_circuit_breaker_active():
            return

        # Check daily loss limit
        if not self.risk_mgr.check_daily_loss():
            logger.warning("⏸️ Daily loss limit reached, skipping cycle")
            return

        try:
            # Check open positions
            await self._manage_positions()

            # Check for new signals
            if await self._can_open_new_position():
                await self._scan_for_signals()
                
        except Exception as e:
            logger.error(f"Cycle error: {e}")
            self.consecutive_errors += 1

    async def _manage_positions(self):
        positions = await self._get_open_positions()
        
        if positions:
            logger.info(f"📋 Managing {len(positions)} positions")
        
        for pos_data in positions:
            try:
                position = self._dict_to_position(pos_data)
                
                # Check TP/SL
                bid, ask = await self.exchange_mgr.fetch_bbo(position.symbol)
                if bid == 0.0 or ask == 0.0:
                    continue
                
                current = bid if position.position_type == PositionType.LONG else ask
                
                # Check TP
                if (position.position_type == PositionType.LONG and current >= position.tp_price) or \
                   (position.position_type == PositionType.SHORT and current <= position.tp_price):
                    await self.position_mgr.close_position(position, "TP")
                    continue
                
                # Check SL
                if (position.position_type == PositionType.LONG and current <= position.sl_price) or \
                   (position.position_type == PositionType.SHORT and current >= position.sl_price):
                    await self.position_mgr.close_position(position, "SL")
                    continue
                
                # Check DCA
                await self.position_mgr.check_dca(position)
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error managing position: {e}")

    async def _scan_for_signals(self):
        logger.debug("🔍 Scanning for signals...")
        
        batches = [self.symbols[i:i + self.config.BATCH_SIZE] 
                  for i in range(0, len(self.symbols), self.config.BATCH_SIZE)]
        
        for batch_num, batch in enumerate(batches, 1):
            for symbol in batch:
                try:
                    await self._analyze_symbol(symbol)
                except Exception as e:
                    logger.error(f"Error analyzing {symbol}: {e}")
            
            if batch_num < len(batches):
                await asyncio.sleep(self.config.BATCH_DELAY)

    async def _analyze_symbol(self, symbol: str):
        try:
            df = await self.exchange_mgr.fetch_ohlcv(symbol)
            if df.empty:
                return

            signal = await self.analyzer.analyze(df)
            if not signal:
                return

            # Check correlation
            open_positions = await self._get_open_positions()
            if not await self.risk_mgr.check_correlation(symbol, open_positions):
                return

            # Check total exposure
            total_exposure = await self.position_mgr._get_total_exposure()
            if total_exposure > self.config.MAX_TOTAL_EXPOSURE_USDT:
                logger.warning(f"🚫 Max total exposure reached: ${total_exposure:.0f}")
                return

            # Get equity
            equity = await self._get_equity()
            
            position = await self.position_mgr.open_position(signal, equity)
            if position:
                await self._send_alert(
                    f"🔪 <b>NEW {signal.position_type.value}</b>\n"
                    f"Pair: {signal.symbol}\n"
                    f"Price: ${signal.price:.4f}\n"
                    f"Risk: ${position.max_risk_usdt:.2f}"
                )
                
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")

    async def _can_open_new_position(self) -> bool:
        try:
            # Check max concurrent trades
            result = await self.db.fetchone(
                "SELECT COUNT(*) FROM positions WHERE status='OPEN'"
            )
            open_count = result[0] if result else 0
            if open_count >= self.config.MAX_CONCURRENT_TRADES:
                return False
            
            # Check total exposure
            total_exposure = await self.position_mgr._get_total_exposure()
            if total_exposure >= self.config.MAX_TOTAL_EXPOSURE_USDT:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking can open: {e}")
            return False

    async def _get_open_positions(self) -> List[Dict]:
        try:
            rows = await self.db.fetchall("""
                SELECT id, symbol, position_type, base_amount, avg_price, tp_price, sl_price,
                       dca_count, open_time, leverage, fee_usdt, max_risk_usdt
                FROM positions 
                WHERE status='OPEN'
            """)
            
            positions = []
            for row in rows:
                positions.append({
                    'id': row[0],
                    'symbol': row[1],
                    'position_type': PositionType(row[2]),
                    'base_amount': row[3],
                    'avg_price': row[4],
                    'tp_price': row[5],
                    'sl_price': row[6],
                    'dca_count': row[7],
                    'open_time': row[8],
                    'leverage': row[9],
                    'fee_usdt': row[10],
                    'max_risk_usdt': row[11]
                })
            
            return positions
            
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []

    def _dict_to_position(self, data: Dict) -> Position:
        return Position(
            id=data['id'],
            symbol=data['symbol'],
            position_type=data['position_type'],
            base_amount=data['base_amount'],
            avg_price=data['avg_price'],
            tp_price=data['tp_price'],
            sl_price=data['sl_price'],
            dca_count=data['dca_count'],
            open_time=data['open_time'],
            leverage=data['leverage'],
            fee_usdt=data['fee_usdt'],
            max_risk_usdt=data['max_risk_usdt']
        )

    async def _get_equity(self) -> float:
        try:
            result = await self.db.fetchone(
                "SELECT SUM(pnl_usdt) FROM positions WHERE status='CLOSED'"
            )
            closed_pnl = result[0] if result and result[0] else 0.0
            return 1000.0 + closed_pnl
        except:
            return 1000.0

    async def _send_alert(self, text: str):
        if self.tg_bot and self.config.TELEGRAM_CHAT_ID:
            try:
                await self.tg_bot.send_message(
                    chat_id=self.config.TELEGRAM_CHAT_ID,
                    text=text,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Telegram error: {e}")

    # ====== Telegram Commands ======
    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            equity = await self._get_equity()
            open_positions = await self._get_open_positions()
            total_exposure = await self.position_mgr._get_total_exposure()
            
            daily_pnl = self.risk_mgr.daily_pnl_usdt
            daily_pnl_pct = (daily_pnl / self.risk_mgr.daily_start_equity * 100) if self.risk_mgr.daily_start_equity else 0
            
            pos_text = []
            for pos in open_positions:
                pos_text.append(
                    f"• {pos['symbol']} ({pos['position_type'].value}) | "
                    f"Entry: ${pos['avg_price']:.2f} | "
                    f"Risk: ${pos['max_risk_usdt']:.2f}"
                )
            
            positions = "\n".join(pos_text) if pos_text else "No active positions"
            
            msg = (
                f"🔪 <b>LICK HUNTER v21.0</b>\n"
                f"Mode: {'📝 PAPER' if self.config.IS_DRY_RUN else '🔥 LIVE'}\n"
                f"Equity: ${equity:.2f}\n"
                f"Daily PnL: {daily_pnl:+.2f} USDT ({daily_pnl_pct:+.1f}%)\n"
                f"Exposure: ${total_exposure:.0f}/{self.config.MAX_TOTAL_EXPOSURE_USDT}\n"
                f"Active: {len(open_positions)}/{self.config.MAX_CONCURRENT_TRADES}\n\n"
                f"<b>Positions:</b>\n{positions}"
            )
            
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            
        except Exception as e:
            logger.error(f"Status error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            # Get today's stats
            today = await self.db.fetchone("""
                SELECT starting_equity, ending_equity, pnl_usdt, trades_count, win_count, max_drawdown
                FROM daily_stats WHERE date=date('now')
            """)
            
            # Get all-time stats
            all_time = await self.db.fetchall("""
                SELECT pnl_usdt FROM positions WHERE status='CLOSED'
            """)
            
            total_pnl = sum(t[0] for t in all_time) if all_time else 0
            total_trades = len(all_time) if all_time else 0
            
            msg = f"📊 <b>STATISTICS</b>\n"
            msg += f"Total Trades: {total_trades}\n"
            msg += f"Total PnL: {total_pnl:+.2f} USDT\n\n"
            
            if today:
                msg += f"<b>Today:</b>\n"
                msg += f"Start: ${today[0]:.2f}\n"
                msg += f"PnL: {today[2]:+.2f} USDT\n"
                msg += f"Trades: {today[3]}\n"
                msg += f"Winrate: {(today[4]/today[3]*100 if today[3]>0 else 0):.1f}%\n"
                msg += f"Max DD: {today[5]*100:.2f}%"
            
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            
        except Exception as e:
            logger.error(f"Stats error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

# ====== MAIN ======
async def main():
    bot = LickHunterBot(Config)
    
    if Config.TELEGRAM_TOKEN:
        app = Application.builder().token(Config.TELEGRAM_TOKEN).build()
        bot.app = app
        
        app.add_handler(CommandHandler("status", bot.cmd_status))
        app.add_handler(CommandHandler("stats", bot.cmd_stats))
        
        await app.initialize()
        await app.start()
        await bot.start()
        await app.updater.start_polling()
        
        logger.info("✅ Bot operational. Press Ctrl+C to stop.")
        
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            await bot.stop()
    else:
        await bot.start()
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            await bot.stop()

def run_bot():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")
    except Exception as e:
        logger.error(f"Fatal: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_bot()
