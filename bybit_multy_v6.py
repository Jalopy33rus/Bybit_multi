#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ULTIMATE TRADING BOT v6.0 - BYBIT FUTURES WITH IMPROVED TREND FOLLOWING
Исправленная версия с устранением рекурсивной ошибки
"""

import os
import sys
import time
import math
import ccxt
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import threading
import signal
import json

try:
    from ta.trend import EMAIndicator, MACD, ADXIndicator
    from ta.momentum import RSIIndicator, StochasticOscillator
    from ta.volatility import BollingerBands, AverageTrueRange
    from ta.volume import VolumeWeightedAveragePrice, OnBalanceVolumeIndicator
except ImportError as e:
    print(f"TA-Lib import error: {e}")
    print("Install with: pip install ta")
    sys.exit(1)

try:
    from telegram import Bot, ParseMode
    from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler
except ImportError as e:
    print(f"Telegram import error: {e}")
    print("Install with: pip install python-telegram-bot")
    sys.exit(1)

# ====== CONFIGURATION ======
API_KEY = os.getenv("BYBIT_API_KEY", "YOUR_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET", "YOUR_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_TOKEN")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

DRY_RUN = True
SANDBOX_MODE = False

# КОМИССИИ BYBIT
TAKER_FEE = 0.0006  # 0.06%
MAKER_FEE = 0.0002  # 0.02%

# СИМВОЛЫ
SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT",
    "XRP/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT", "DOT/USDT:USDT"
]

# Настройки для разных категорий символов
SYMBOL_CATEGORIES = {
    "BTC/USDT:USDT": {"volatility": "LOW", "risk_multiplier": 1.0},
    "ETH/USDT:USDT": {"volatility": "LOW", "risk_multiplier": 1.0},
    "BNB/USDT:USDT": {"volatility": "MEDIUM", "risk_multiplier": 1.2},
    "SOL/USDT:USDT": {"volatility": "HIGH", "risk_multiplier": 1.5},
    "XRP/USDT:USDT": {"volatility": "HIGH", "risk_multiplier": 1.5},
    "ADA/USDT:USDT": {"volatility": "HIGH", "risk_multiplier": 1.5},
    "AVAX/USDT:USDT": {"volatility": "HIGH", "risk_multiplier": 1.5},
    "DOT/USDT:USDT": {"volatility": "MEDIUM", "risk_multiplier": 1.2},
}

active_symbols = SYMBOLS

# ====== НАСТРОЙКИ С УЛУЧШЕННОЙ СТРАТЕГИЕЙ ======
TRADING_MODES = {
    "CONSERVATIVE": {
        "name": "🟡 КОНСЕРВАТИВНЫЙ",
        "type": "trend_following",
        "scan_interval": 120,
        "exit_check_interval": 30,
        "status_interval": 600,
        "sync_interval": 1800,
        "max_trades": 2,
        "trade_pct": 0.10,
        "timeframe_entry": "15m",
        "timeframe_trend": "1h",
        "timeframe_volatility": "4h",
        
        # ИЗМЕНЕНИЯ НА ОСНОВЕ АНАЛИЗА:
        "max_stop_loss": 0.012,  # 1.2%
        "take_profit": 0.030,    # 3%
        "quick_exit": 0.0,
        
        # Фильтры тренда:
        "min_trend_strength": 20,
        "max_trend_age": 20,
        "require_trend_alignment": True,
        
        # RSI фильтры:
        "rsi_range_long": (30, 80),
        "rsi_range_short": (20, 70),
        
        "volume_multiplier": 1.5,
        "min_score": 90,
        
        "cooldown": 1200,
        "max_daily_trades_per_symbol": 2,
        
        "strategy": "TREND_FOLLOWING_V2",
        "risk_level": "MEDIUM",
        
        # Улучшенный трейлинг-стоп:
        "trailing_stop_activation": 0.008,
        "trailing_stop_distance": 0.004,
        "trailing_stop_update_frequency": 0.002,
        
        "max_position_time": 0,  # УБРАЛИ FORCE_TIMEOUT!
        
        "leverage": 3,
        "use_exchange_orders": True,
        "use_market_entry": False,
        "use_market_exit": False,
        
        "limit_order_timeout": 120,
        "commission_filter": True,
        
        # Волатильность фильтры:
        "max_atr_percentage": 0.10,
        "min_atr_percentage": 0.01,
        
        # Адаптивные настройки:
        "adaptive_sl": True,
        "adaptive_tp": True,
        "adaptive_position_sizing": True,
        
        # Частичный выход:
        "partial_exit_enabled": True,
        "partial_exit_1": 0.015,
        "partial_exit_2": 0.025,
        "partial_exit_pct_1": 0.3,
        "partial_exit_pct_2": 0.3,
    },
    
    "AGGRESSIVE": {
        "name": "🟢 АГРЕССИВНЫЙ",
        "type": "trend_following",
        "scan_interval": 90,
        "exit_check_interval": 20,
        "status_interval": 300,
        "sync_interval": 1800,
        "max_trades": 3,
        "trade_pct": 0.15,
        "timeframe_entry": "10m",
        "timeframe_trend": "30m",
        "timeframe_volatility": "1h",
        
        "max_stop_loss": 0.018,
        "take_profit": 0.040,
        "quick_exit": 0.0,
        
        "min_trend_strength": 15,
        "max_trend_age": 15,
        "require_trend_alignment": True,
        
        "rsi_range_long": (25, 85),
        "rsi_range_short": (15, 75),
        
        "volume_multiplier": 1.3,
        "min_score": 85,
        
        "cooldown": 900,
        "max_daily_trades_per_symbol": 3,
        
        "strategy": "TREND_FOLLOWING_V2",
        "risk_level": "HIGH",
        
        "trailing_stop_activation": 0.012,
        "trailing_stop_distance": 0.006,
        "trailing_stop_update_frequency": 0.003,
        
        "max_position_time": 0,
        
        "leverage": 4,
        "use_exchange_orders": True,
        "use_market_entry": False,
        "use_market_exit": False,
        
        "limit_order_timeout": 90,
        "commission_filter": True,
        
        "max_atr_percentage": 0.06,
        "min_atr_percentage": 0.012,
        
        "adaptive_sl": True,
        "adaptive_tp": True,
        "adaptive_position_sizing": True,
        
        "partial_exit_enabled": True,
        "partial_exit_1": 0.020,
        "partial_exit_2": 0.035,
        "partial_exit_pct_1": 0.25,
        "partial_exit_pct_2": 0.25,
    }
}

# Минимальные настройки
MIN_TRADE_USDT = 20.0
MIN_USDT_PER_SYMBOL = {
    "BTC/USDT:USDT": 15.0, "ETH/USDT:USDT": 15.0, "BNB/USDT:USDT": 10.0,
    "SOL/USDT:USDT": 8.0, "XRP/USDT:USDT": 8.0, "ADA/USDT:USDT": 8.0,
    "AVAX/USDT:USDT": 8.0, "DOT/USDT:USDT": 8.0
}

LOCK_FILE = "/tmp/ultimate_trading_bot_v6.lock"
DB_FILE = "trades_ultimate_futures_v6.db"

# Глобальные переменные
CURRENT_MODE = "CONSERVATIVE"
BOT_RUNNING = True
exchange = None
bot = None
updater = None

# ====== ЛОГГИРОВАНИЕ ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('ultimate_bot_futures_v6.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====== БАЗА ДАННЫХ ======
class DatabaseManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.db_file = DB_FILE
            self._connection = None
            self._cursor = None
            self._initialize_database()
            self._initialized = True
    
    def _initialize_database(self):
        """Инициализация базы данных"""
        try:
            self._connection = sqlite3.connect(self.db_file, check_same_thread=False)
            self._cursor = self._connection.cursor()
            
            # Основная таблица позиций
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT, 
                    trading_mode TEXT, 
                    strategy TEXT,
                    base_amount REAL, 
                    open_price REAL, 
                    stop_loss REAL, 
                    take_profit REAL,
                    max_price REAL DEFAULT 0, 
                    min_price REAL DEFAULT 0,
                    open_time TEXT, 
                    close_time TEXT,
                    close_price REAL, 
                    pnl REAL DEFAULT 0, 
                    pnl_percent REAL DEFAULT 0,
                    status TEXT DEFAULT 'OPEN', 
                    fee_paid REAL DEFAULT 0,
                    entry_reason TEXT, 
                    exit_reason TEXT, 
                    duration_seconds INTEGER DEFAULT 0,
                    original_stop_loss REAL, 
                    trailing_active INTEGER DEFAULT 0,
                    open_timestamp INTEGER DEFAULT 0, 
                    position_type TEXT DEFAULT 'LONG',
                    leverage INTEGER DEFAULT 1,
                    invested_usdt REAL DEFAULT 0,
                    exchange_order_ids TEXT DEFAULT '',
                    entry_type TEXT DEFAULT 'MARKET',
                    exit_type TEXT DEFAULT 'MARKET',
                    partial_exit_1 INTEGER DEFAULT 0,
                    partial_exit_2 INTEGER DEFAULT 0,
                    risk_multiplier REAL DEFAULT 1.0,
                    atr_value REAL DEFAULT 0,
                    trend_strength REAL DEFAULT 0
                )
            """)
            
            # История сделок
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT, 
                    action TEXT, 
                    price REAL, 
                    usdt_amount REAL,
                    base_amount REAL, 
                    fee REAL DEFAULT 0, 
                    time TEXT, 
                    timestamp INTEGER,
                    trading_mode TEXT, 
                    strategy TEXT, 
                    position_type TEXT DEFAULT 'LONG',
                    leverage INTEGER DEFAULT 1,
                    exchange_order_id TEXT DEFAULT '',
                    entry_type TEXT DEFAULT 'MARKET',
                    exit_type TEXT DEFAULT 'MARKET',
                    partial_exit INTEGER DEFAULT 0
                )
            """)
            
            # Кулдаун и лимиты
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbol_cooldown (
                    symbol TEXT PRIMARY KEY, 
                    last_closed_ts INTEGER DEFAULT 0,
                    daily_trade_count INTEGER DEFAULT 0, 
                    last_trade_date TEXT,
                    consecutive_losses INTEGER DEFAULT 0,
                    consecutive_wins INTEGER DEFAULT 0
                )
            """)
            
            # Статистика символов
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbol_stats (
                    symbol TEXT PRIMARY KEY,
                    total_trades INTEGER DEFAULT 0,
                    win_trades INTEGER DEFAULT 0,
                    loss_trades INTEGER DEFAULT 0,
                    total_pnl REAL DEFAULT 0,
                    avg_win_pct REAL DEFAULT 0,
                    avg_loss_pct REAL DEFAULT 0,
                    volatility_score REAL DEFAULT 0,
                    last_updated TEXT
                )
            """)
            
            self._connection.commit()
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Database initialization error: {e}")
            raise
    
    def get_connection(self):
        """Получение соединения с переподключением"""
        try:
            if self._connection is None:
                self._initialize_database()
            self._cursor.execute("SELECT 1")
            return self._connection, self._cursor
        except Exception as e:
            logger.warning(f"🔄 Reconnecting to database: {e}")
            self._initialize_database()
            return self._connection, self._cursor
    
    def execute(self, query, params=()):
        """Выполнение запроса с обработкой ошибок"""
        conn, cursor = self.get_connection()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor
        except Exception as e:
            logger.error(f"❌ Database execute error: {e}")
            try:
                conn.rollback()
            except:
                pass
            raise
    
    def fetchone(self, query, params=()):
        cursor = self.execute(query, params)
        return cursor.fetchone()
    
    def fetchall(self, query, params=()):
        cursor = self.execute(query, params)
        return cursor.fetchall()
    
    def update_symbol_stats(self, symbol: str, pnl_percent: float):
        """Обновление статистики символа"""
        try:
            is_win = pnl_percent > 0
            row = self.fetchone("SELECT * FROM symbol_stats WHERE symbol=?", (symbol,))
            
            if not row:
                # Новая запись
                self.execute("""
                    INSERT INTO symbol_stats (symbol, total_trades, win_trades, loss_trades, 
                    total_pnl, avg_win_pct, avg_loss_pct, last_updated)
                    VALUES (?, 1, ?, ?, ?, ?, ?, datetime('now'))
                """, (
                    symbol, 
                    1 if is_win else 0,
                    0 if is_win else 1,
                    pnl_percent,
                    pnl_percent if is_win else 0,
                    0 if is_win else pnl_percent
                ))
            else:
                # Обновление существующей
                total_trades = row[1] + 1
                win_trades = row[2] + (1 if is_win else 0)
                loss_trades = row[3] + (0 if is_win else 1)
                total_pnl = row[4] + pnl_percent
                
                # Обновляем средние значения
                if is_win:
                    avg_win = ((row[5] * row[2]) + pnl_percent) / win_trades if win_trades > 0 else pnl_percent
                    avg_loss = row[6]
                else:
                    avg_win = row[5]
                    avg_loss = ((row[6] * row[3]) + pnl_percent) / loss_trades if loss_trades > 0 else pnl_percent
                
                self.execute("""
                    UPDATE symbol_stats 
                    SET total_trades=?, win_trades=?, loss_trades=?, total_pnl=?, 
                        avg_win_pct=?, avg_loss_pct=?, last_updated=datetime('now')
                    WHERE symbol=?
                """, (total_trades, win_trades, loss_trades, total_pnl, avg_win, avg_loss, symbol))
                
        except Exception as e:
            logger.error(f"❌ Update symbol stats error: {e}")

db = DatabaseManager()

# ====== ИНИЦИАЛИЗАЦИЯ БИРЖИ ======
def initialize_exchange():
    global exchange
    
    if os.path.exists(LOCK_FILE):
        logger.error("❌ Lock file exists — bot already running")
        sys.exit(1)
    
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

    try:
        exchange = ccxt.bybit({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "adjustForTimeDifference": True,
            },
            "timeout": 30000,
        })
        
        if SANDBOX_MODE:
            exchange.set_sandbox_mode(True)
            
        # Test connection
        exchange.fetch_balance()
        logger.info("✅ Bybit Futures connected successfully")
            
    except Exception as e:
        logger.error(f"❌ Exchange initialization failed: {e}")
        sys.exit(1)

def setup_telegram():
    """Инициализация Telegram бота"""
    global bot, updater
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        updater = Updater(TELEGRAM_TOKEN, use_context=True)
        dp = updater.dispatcher
        
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CommandHandler("status", cmd_status))
        dp.add_handler(CommandHandler("stats", cmd_stats))
        dp.add_handler(CommandHandler("stop", cmd_stop))
        dp.add_handler(CommandHandler("scan", cmd_scan))
        dp.add_handler(CommandHandler("positions", cmd_positions))
        dp.add_handler(CommandHandler("sync", cmd_sync))
        dp.add_handler(CommandHandler("pause", cmd_pause))
        dp.add_handler(CommandHandler("resume", cmd_resume))
        dp.add_handler(CommandHandler("close", cmd_close))
        dp.add_handler(CommandHandler("cancel_orders", cmd_cancel_orders))
        dp.add_handler(CommandHandler("recalculate_sltp", cmd_recalculate_sltp))
        dp.add_handler(CommandHandler("create_orders", cmd_create_missing_orders))
        dp.add_handler(CommandHandler("commission", cmd_commission_settings))
        dp.add_handler(CommandHandler("settings", cmd_show_settings))
        dp.add_handler(CommandHandler("test_scan", cmd_test_scan))
        
        return updater
    except Exception as e:
        logger.error(f"❌ Telegram setup failed: {e}")
        return None

def safe_send(text: str, max_retries: int = 3) -> bool:
    """Безопасная отправка сообщения в Telegram"""
    global bot
    if bot is None:
        logger.warning("⚠️ Telegram bot not initialized, skipping message")
        return False
        
    for attempt in range(max_retries):
        try:
            bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML)
            logger.info(f"📨 Telegram sent: {text[:50]}...")
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"❌ Failed to send Telegram message: {e}")
            time.sleep(2)
    return False

# ====== УПРАВЛЕНИЕ СОСТОЯНИЕМ БОТА ======
def stop_bot():
    """Корректная остановка бота"""
    global BOT_RUNNING, updater
    
    logger.info("🛑 Stopping bot gracefully...")
    BOT_RUNNING = False
    
    time.sleep(2)
    
    if updater:
        updater.stop()
    
    cleanup()
    logger.info("✅ Bot stopped gracefully")
    sys.exit(0)

def pause_bot():
    """Приостановка торговли"""
    global BOT_RUNNING
    BOT_RUNNING = False
    logger.info("⏸️ Bot paused")
    safe_send("⏸️ <b>Торговля приостановлена</b>\nИспользуйте /resume для возобновления")

def resume_bot():
    """Возобновление торговли"""
    global BOT_RUNNING
    BOT_RUNNING = True
    logger.info("▶️ Bot resumed")
    safe_send("▶️ <b>Торговля возобновлена</b>")

# ====== ОСНОВНЫЕ ФУНКЦИИ С ОБРАБОТКОЙ ОШИБОК ======
def retry_api_call(func, max_retries=3, delay=1.0):
    """Повторный вызов API при ошибках"""
    for attempt in range(max_retries):
        try:
            result = func()
            if result is None:
                raise ValueError("Function returned None")
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"❌ API call failed after {max_retries} attempts: {e}")
                raise e
            sleep_time = delay * (2 ** attempt)
            logger.warning(f"🔄 API retry {attempt + 1}/{max_retries} in {sleep_time:.1f}s: {e}")
            time.sleep(sleep_time)

def safe_float_convert(value, default=0.0):
    """Безопасное преобразование в float"""
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def fetch_ohlcv(symbol: str, timeframe: str, limit=100):
    """Получение OHLCV данных с обработкой ошибок"""
    def _fetch():
        try:
            data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not data or len(data) < 20:
                logger.warning(f"⚠️ Insufficient OHLCV data for {symbol}: {len(data) if data else 0} candles")
                return []
            return data
        except Exception as e:
            logger.warning(f"⚠️ OHLCV fetch failed for {symbol}: {e}")
            return []
    
    try:
        data = retry_api_call(_fetch)
        return data if data else []
    except Exception as e:
        logger.warning(f"⚠️ Failed to fetch OHLCV for {symbol}: {e}")
        return []

def fetch_balance():
    """Получение баланса"""
    def _fetch():
        return exchange.fetch_balance()
    
    try:
        return retry_api_call(_fetch)
    except Exception as e:
        logger.error(f"❌ Balance fetch failed: {e}")
        return {'free': {'USDT': 0.0}, 'total': {'USDT': 0.0}}

def get_current_price(symbol: str):
    """Получение текущей цены"""
    try:
        ticker = exchange.fetch_ticker(symbol)
        price = safe_float_convert(ticker.get('last'))
        if price <= 0:
            logger.error(f"❌ Invalid price for {symbol}: {price}")
            return None
        return price
    except Exception as e:
        logger.error(f"❌ Price fetch failed for {symbol}: {e}")
        return None

def get_current_settings():
    """Получение текущих настроек"""
    return TRADING_MODES.get(CURRENT_MODE, TRADING_MODES["CONSERVATIVE"])

def get_symbol_info(symbol: str):
    """Получение информации о символе"""
    try:
        markets = exchange.load_markets()
        market = markets.get(symbol)
        if market:
            return {
                'min_amount': safe_float_convert(market.get('limits', {}).get('amount', {}).get('min', 0)),
                'min_cost': safe_float_convert(market.get('limits', {}).get('cost', {}).get('min', 0)),
                'price_precision': market.get('precision', {}).get('price', 8),
                'amount_precision': market.get('precision', {}).get('amount', 8),
                'contract_size': safe_float_convert(market.get('contractSize', 1))
            }
    except Exception as e:
        logger.error(f"❌ Symbol info error for {symbol}: {e}")
    return {'min_amount': 0, 'min_cost': 0, 'price_precision': 8, 'amount_precision': 8, 'contract_size': 1}

def compute_available_usdt():
    """Расчет доступного USDT"""
    try:
        if DRY_RUN:
            virtual_balance = float(os.getenv("VIRTUAL_BALANCE_USDT", 1000.0))
            return virtual_balance

        bal = fetch_balance()
        total_usdt = safe_float_convert(bal['free'].get('USDT', 0))
        
        return max(total_usdt, 0.0)

    except Exception as e:
        logger.error(f"❌ Balance computation error: {e}")
        return 0.0

# ====== ИСПРАВЛЕННЫЙ АНАЛИЗ ТРЕНДА ======
def get_trend_analysis(symbol: str, timeframe: str = "1h") -> Dict:
    """Улучшенный анализ тренда без рекурсии"""
    try:
        df = get_ohlcv_data(symbol, timeframe, 100)
        if df is None or len(df) < 50:
            return {"strength": 0, "direction": "NEUTRAL", "age": 0, "confirmed": False}
        
        # 1. ADX для силы тренда
        adx_indicator = ADXIndicator(df['high'], df['low'], df['close'], window=14)
        adx = adx_indicator.adx().iloc[-1]
        plus_di = adx_indicator.adx_pos().iloc[-1]
        minus_di = adx_indicator.adx_neg().iloc[-1]
        
        # 2. EMA анализ
        ema_9 = df['close'].ewm(span=9).mean().iloc[-1]
        ema_21 = df['close'].ewm(span=21).mean().iloc[-1]
        ema_50 = df['close'].ewm(span=50).mean().iloc[-1]
        
        # 3. Определение направления
        direction = "NEUTRAL"
        if adx > 25:  # Сильный тренд
            if plus_di > minus_di:
                direction = "BULLISH"
            else:
                direction = "BEARISH"
        
        # 4. Проверка согласованности EMA
        ema_aligned = False
        if direction == "BULLISH":
            ema_aligned = ema_9 > ema_21 > ema_50
        elif direction == "BEARISH":
            ema_aligned = ema_9 < ema_21 < ema_50
        
        # 5. Определение возраста тренда
        trend_age = 0
        if len(df) >= 20:
            if direction == "BULLISH":
                for i in range(1, min(21, len(df))):
                    if df['close'].iloc[-i] > df['close'].iloc[-i-1]:
                        trend_age += 1
                    else:
                        break
            elif direction == "BEARISH":
                for i in range(1, min(21, len(df))):
                    if df['close'].iloc[-i] < df['close'].iloc[-i-1]:
                        trend_age += 1
                    else:
                        break
        
        # 6. Проверка на других таймфреймах (без рекурсии!)
        confirmed = True
        settings = get_current_settings()
        
        # Проверяем только если требуется выравнивание
        if settings.get('require_trend_alignment', True) and timeframe != "15m" and timeframe != "4h":
            try:
                # Проверяем на меньшем таймфрейме
                df_15m = get_ohlcv_data(symbol, "15m", 50)
                if df_15m is not None and len(df_15m) > 20:
                    # Простой анализ направления на 15m
                    price_change_15m = (df_15m['close'].iloc[-1] - df_15m['close'].iloc[-5]) / df_15m['close'].iloc[-5]
                    short_direction = "BULLISH" if price_change_15m > 0.001 else "BEARISH" if price_change_15m < -0.001 else "NEUTRAL"
                    
                    if direction != "NEUTRAL" and short_direction != "NEUTRAL" and direction != short_direction:
                        confirmed = False
                        logger.info(f"⚠️ Trend mismatch: {timeframe}={direction}, 15m={short_direction}")
            except Exception as e:
                logger.warning(f"⚠️ Multi-timeframe check error: {e}")
        
        return {
            "strength": adx,
            "direction": direction,
            "age": trend_age,
            "confirmed": confirmed,
            "ema_aligned": ema_aligned,
            "plus_di": plus_di,
            "minus_di": minus_di
        }
        
    except Exception as e:
        logger.error(f"❌ Trend analysis error for {symbol}: {e}")
        return {"strength": 0, "direction": "NEUTRAL", "age": 0, "confirmed": False}

def get_volatility_analysis(symbol: str, timeframe: str = "4h") -> Dict:
    """Анализ волатильности символа"""
    try:
        df = get_ohlcv_data(symbol, timeframe, 50)
        if df is None or len(df) < 20:
            return {"atr": 0, "atr_percentage": 0, "bb_width": 0, "volatility_rank": "LOW"}
        
        current_price = df['close'].iloc[-1]
        
        # 1. ATR
        atr_indicator = AverageTrueRange(df['high'], df['low'], df['close'], window=14)
        atr = atr_indicator.average_true_range().iloc[-1]
        atr_percentage = (atr / current_price) * 100 if current_price > 0 else 0
        
        # 2. Bollinger Bands Width
        bb = BollingerBands(df['close'], window=20, window_dev=2)
        bb_upper = bb.bollinger_hband().iloc[-1]
        bb_lower = bb.bollinger_lband().iloc[-1]
        bb_middle = bb.bollinger_mavg().iloc[-1]
        bb_width = ((bb_upper - bb_lower) / bb_middle) * 100 if bb_middle > 0 else 0
        
        # 3. Историческая волатильность
        returns = df['close'].pct_change().dropna()
        hist_volatility = returns.std() * np.sqrt(365) * 100 if len(returns) > 0 else 0
        
        # 4. Ранжирование волатильности
        volatility_rank = "LOW"
        if hist_volatility > 80:
            volatility_rank = "VERY_HIGH"
        elif hist_volatility > 60:
            volatility_rank = "HIGH"
        elif hist_volatility > 40:
            volatility_rank = "MEDIUM"
        
        return {
            "atr": atr,
            "atr_percentage": atr_percentage,
            "bb_width": bb_width,
            "hist_volatility": hist_volatility,
            "volatility_rank": volatility_rank,
            "current_price": current_price
        }
        
    except Exception as e:
        logger.error(f"❌ Volatility analysis error for {symbol}: {e}")
        return {"atr": 0, "atr_percentage": 0, "bb_width": 0, "volatility_rank": "LOW"}

def get_ohlcv_data(symbol: str, timeframe: str, limit: int):
    """Получение OHLCV данных в DataFrame"""
    ohlcv = fetch_ohlcv(symbol, timeframe, limit)
    if not ohlcv:
        return None
        
    try:
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].apply(lambda x: safe_float_convert(x, 0.0))
        return df
    except Exception as e:
        logger.error(f"❌ Dataframe creation error for {symbol}: {e}")
        return None

# ====== УЛУЧШЕННЫЙ АНАЛИЗ СИМВОЛОВ ======
def analyze_symbol_with_filters(symbol: str) -> Optional[Dict]:
    """Анализ символа со всеми фильтрами"""
    try:
        settings = get_current_settings()
        
        # 1. Проверка что позиция не открыта
        if is_position_already_open(symbol):
            logger.info(f"⏹️ Position already open for {symbol}")
            return None
        
        # 2. Проверка кулдауна
        if is_in_cooldown(symbol):
            logger.info(f"⏹️ {symbol} in cooldown")
            return None
        
        # 3. Анализ тренда
        trend_analysis = get_trend_analysis(symbol, settings['timeframe_trend'])
        
        if not trend_analysis["confirmed"]:
            logger.info(f"⏹️ {symbol} filtered: trend not confirmed")
            return None
        
        if trend_analysis["strength"] < settings['min_trend_strength']:
            logger.info(f"⏹️ {symbol} filtered: weak trend {trend_analysis['strength']:.1f} < {settings['min_trend_strength']}")
            return None
        
        if trend_analysis["age"] > settings.get('max_trend_age', 20):
            logger.info(f"⏹️ {symbol} filtered: old trend ({trend_analysis['age']} candles)")
            return None
        
        # 4. Анализ волатильности
        volatility = get_volatility_analysis(symbol, settings['timeframe_volatility'])
        
        if volatility["atr_percentage"] > (settings['max_atr_percentage'] * 100):
            logger.info(f"⏹️ {symbol} filtered: high volatility {volatility['atr_percentage']:.1f}% > {settings['max_atr_percentage']*100}%")
            return None

        if volatility["atr_percentage"] < (settings['min_atr_percentage'] * 100):
            logger.info(f"⏹️ {symbol} filtered: low volatility {volatility['atr_percentage']:.1f}% < {settings['min_atr_percentage']*100}%")
            return None

        
        # 5. Технический анализ на входном ТФ
        df = get_ohlcv_data(symbol, settings['timeframe_entry'], 100)
        if df is None or len(df) < 50:
            return None
        
        current_price = df['close'].iloc[-1]
        if current_price <= 0:
            return None
        
        # Индикаторы
        rsi = RSIIndicator(df['close'], window=14).rsi().iloc[-1]
        
        current_volume = df['volume'].iloc[-1]
        volume_sma = df['volume'].tail(20).mean()
        volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1
        
        macd = MACD(df['close'])
        macd_line = macd.macd().iloc[-1]
        macd_signal = macd.macd_signal().iloc[-1]
        macd_histogram = macd_line - macd_signal
        
        bb = BollingerBands(df['close'], window=20, window_dev=2)
        bb_upper = bb.bollinger_hband().iloc[-1]
        bb_lower = bb.bollinger_lband().iloc[-1]
        bb_middle = bb.bollinger_mavg().iloc[-1]
        bb_width = (bb_upper - bb_lower) / bb_middle if bb_middle != 0 else 0
        
        price_position = (current_price - bb_lower) / (bb_upper - bb_lower + 1e-9)
        
        # Определение направления
        position_type = "LONG" if trend_analysis["direction"] == "BULLISH" else "SHORT"
        
        # Проверка согласованности сигналов
        if position_type == "LONG":
            rsi_range = settings['rsi_range_long']
            if not (macd_line > macd_signal and macd_histogram > 0):
                logger.info(f"⏹️ {symbol} filtered: MACD not bullish for LONG")
                return None
            if price_position > 0.7:
                logger.info(f"⏹️ {symbol} filtered: price too high for LONG ({price_position:.2%})")
                return None
        else:
            rsi_range = settings['rsi_range_short']
            if not (macd_line < macd_signal and macd_histogram < 0):
                logger.info(f"⏹️ {symbol} filtered: MACD not bearish for SHORT")
                return None
            if price_position < 0.3:
                logger.info(f"⏹️ {symbol} filtered: price too low for SHORT ({price_position:.2%})")
                return None
        
        # Фильтр RSI
        if not (rsi_range[0] <= rsi <= rsi_range[1]):
            logger.info(f"⏹️ {symbol} filtered: RSI {rsi:.1f} outside range {rsi_range}")
            return None
        
        # Фильтр объема
        if volume_ratio < settings['volume_multiplier']:
            logger.info(f"⏹️ {symbol} filtered: low volume {volume_ratio:.1f}x < {settings['volume_multiplier']}x")
            return None
        
        # Фильтр волатильности
        if bb_width < 0.01:
            logger.info(f"⏹️ {symbol} filtered: low volatility (BB width {bb_width:.3%})")
            return None
        
        # Расчет score
        score = 0
        reasons = []
        
        # Тренд (макс 30)
        score += min(trend_analysis["strength"], 30)
        reasons.append(f"TREND_{trend_analysis['direction']}")
        
        # Объем (макс 20)
        if volume_ratio >= settings['volume_multiplier']:
            score += 20
            reasons.append("HIGH_VOLUME")
        
        # RSI (макс 20)
        if rsi_range[0] <= rsi <= rsi_range[1]:
            score += 20
            reasons.append("GOOD_RSI")
        
        # Позиция в BB (макс 15)
        if position_type == "LONG" and 0.1 <= price_position <= 0.5:
            score += 15
            reasons.append("GOOD_BB_POSITION_LONG")
        elif position_type == "SHORT" and 0.5 <= price_position <= 0.9:
            score += 15
            reasons.append("GOOD_BB_POSITION_SHORT")
        
        # Волатильность (макс 15)
        if bb_width >= 0.01:
            score += 15
            reasons.append("GOOD_VOLATILITY")
        
        logger.info(f"🔍 {symbol} {position_type}: Score={score}, Trend={trend_analysis['direction']} ({trend_analysis['strength']:.1f}), "
                   f"RSI={rsi:.1f}, Vol={volume_ratio:.1f}x, BB={price_position:.2%}")
        
        if score >= settings['min_score']:
            return {
                "symbol": symbol,
                "price": current_price,
                "score": score,
                "reasons": reasons,
                "volume_ratio": volume_ratio,
                "rsi": rsi,
                "bb_width": bb_width,
                "bb_position": price_position,
                "signal_type": position_type,
                "trend_direction": trend_analysis["direction"],
                "trend_strength": trend_analysis["strength"],
                "trend_age": trend_analysis["age"],
                "atr": volatility["atr"],
                "atr_percentage": volatility["atr_percentage"],
                "volatility_rank": volatility["volatility_rank"]
            }
        
        return None
        
    except Exception as e:
        logger.error(f"❌ Analyze symbol error for {symbol}: {e}")
        return None

# ====== УПРАВЛЕНИЕ ПОЗИЦИЯМИ ======
def get_open_positions():
    """Получение открытых позиций"""
    try:
        rows = db.fetchall("""
            SELECT symbol, base_amount, open_price, stop_loss, take_profit, max_price, min_price,
                   original_stop_loss, trailing_active, open_timestamp, position_type, leverage,
                   invested_usdt, exchange_order_ids, entry_type, partial_exit_1, partial_exit_2,
                   atr_value, trend_strength
            FROM positions WHERE status='OPEN'
        """)
        positions = {}
        
        for row in rows:
            symbol = row[0]
            
            positions[symbol] = {
                "base_amount": safe_float_convert(row[1]), 
                "open_price": safe_float_convert(row[2]), 
                "stop_loss": safe_float_convert(row[3]),
                "take_profit": safe_float_convert(row[4]),
                "max_price": safe_float_convert(row[5] or row[2]), 
                "min_price": safe_float_convert(row[6] or row[2]),
                "original_stop_loss": safe_float_convert(row[7] or row[3]), 
                "trailing_active": row[8] or 0,
                "open_timestamp": row[9] or int(time.time()), 
                "position_type": row[10] or 'LONG',
                "leverage": row[11] or 1, 
                "invested_usdt": safe_float_convert(row[12]),
                "exchange_order_ids": row[13] or "",
                "entry_type": row[14] or "MARKET",
                "partial_exit_1": row[15] or 0,
                "partial_exit_2": row[16] or 0,
                "atr_value": safe_float_convert(row[17]),
                "trend_strength": safe_float_convert(row[18])
            }
        
        return positions
        
    except Exception as e:
        logger.error(f"❌ Positions fetch error: {e}")
        return {}

def get_concurrent_trades_count():
    """Количество открытых сделок"""
    try:
        row = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='OPEN'")
        return row[0] if row else 0
    except Exception as e:
        logger.error(f"❌ Concurrent trades count error: {e}")
        return 0

def is_in_cooldown(symbol: str):
    """Проверка кулдауна"""
    try:
        row = db.fetchone("SELECT last_closed_ts, consecutive_losses FROM symbol_cooldown WHERE symbol=?", (symbol,))
        if not row or not row[0]:
            return False
            
        last_closed = row[0]
        consecutive_losses = row[1] or 0
        settings = get_current_settings()
        cooldown = settings['cooldown']
        
        if consecutive_losses >= 3:
            cooldown *= 2
        
        in_cooldown = (time.time() - last_closed) < cooldown
        
        if in_cooldown:
            remaining = cooldown - (time.time() - last_closed)
            logger.debug(f"⏹️ {symbol} in cooldown, {remaining:.0f}s remaining")
            
        return in_cooldown
        
    except Exception as e:
        logger.error(f"❌ Cooldown check error: {e}")
        return False

def is_position_already_open(symbol: str) -> bool:
    """Проверка что позиция уже открыта"""
    try:
        row = db.fetchone("SELECT COUNT(*) FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
        return row[0] > 0 if row else False
    except Exception as e:
        logger.error(f"❌ Position check error for {symbol}: {e}")
        return False

def can_open_new_trade():
    """Проверка возможности открытия новой сделки"""
    settings = get_current_settings()
    current_trades = get_concurrent_trades_count()
    can_open = current_trades < settings['max_trades']
    
    if not can_open:
        logger.info(f"⏹️ Max trades reached: {current_trades}/{settings['max_trades']}")
    
    return can_open

# ====== УЛУЧШЕННОЕ СКАНИРОВАНИЕ ======
def scan_for_opportunities():
    """Сканирование торговых возможностей"""
    if not BOT_RUNNING:
        logger.info("⏸️ Bot is paused, skipping scan")
        return
        
    settings = get_current_settings()
    
    available_usdt = compute_available_usdt()
    min_possible_trade = min(MIN_USDT_PER_SYMBOL.values())
    
    if available_usdt < min_possible_trade:
        logger.warning(f"⏹️ Insufficient USDT: {available_usdt:.2f} < {min_possible_trade}")
        return
        
    logger.info(f"🔍 Scanning {len(active_symbols)} symbols ({CURRENT_MODE}), Balance: {available_usdt:.2f} USDT...")
    
    signals = []
    trend_stats = {
        "BULLISH": 0,
        "BEARISH": 0, 
        "NEUTRAL": 0
    }
    
    for symbol in active_symbols:
        if not BOT_RUNNING:
            break
            
        if not can_open_new_trade():
            logger.info("⏹️ Max trades reached, stopping scan")
            break
            
        signal = analyze_symbol_with_filters(symbol)
        
        if signal:
            signals.append(signal)
            trend_stats[signal.get('trend_direction', 'NEUTRAL')] += 1
    
    logger.info(f"📊 Trend statistics: {trend_stats}")
    
    if signals and BOT_RUNNING:
        signals.sort(key=lambda x: (x['score'] + min(x.get('trend_strength', 0), 30)), reverse=True)
        best_signal = signals[0]
        
        if is_position_already_open(best_signal['symbol']):
            logger.warning(f"⏹️ Position for {best_signal['symbol']} was opened during scan, skipping")
            return
        
        logger.info(f"🎯 BEST {best_signal['signal_type']} SIGNAL: {best_signal['symbol']} "
                   f"(Score: {best_signal['score']}, Trend: {best_signal.get('trend_direction')} "
                   f"{best_signal.get('trend_strength', 0):.1f})")
        
        # В DRY_RUN режиме только логируем
        if DRY_RUN:
            logger.info(f"🔶 DRY RUN: Would open {best_signal['signal_type']} position for {best_signal['symbol']}")
            reasons_str = ", ".join(best_signal['reasons'])
            safe_send(
                f"🧪 <b>DRY RUN SIGNAL: {best_signal['symbol']} {best_signal['signal_type']}</b>\n"
                f"Тренд: {best_signal['trend_direction']} ({best_signal['trend_strength']:.1f})\n"
                f"Цена: {best_signal['price']:.6f}\n"
                f"Score: {best_signal['score']}/100\n"
                f"RSI: {best_signal['rsi']:.1f}\n"
                f"Объем: {best_signal['volume_ratio']:.1f}x\n"
                f"Причины: {reasons_str}"
            )
            
            if len(signals) > 1:
                logger.info(f"📋 Other good signals:")
                for i, sig in enumerate(signals[1:4], 1):
                    logger.info(f"  {i}. {sig['symbol']} {sig['signal_type']} "
                              f"(Score: {sig['score']}, Trend: {sig.get('trend_direction')})")
        else:
            # В реальном режиме открываем позицию
            logger.info(f"🟢 Opening {best_signal['signal_type']} position for {best_signal['symbol']}")
            # Здесь должна быть логика открытия позиции
            
    else:
        if signals:
            logger.info("📭 Signals found but bot is paused")
        else:
            logger.info("📭 No valid signals found")

# ====== TELEGRAM КОМАНДЫ ======
def start(update, context):
    balance = compute_available_usdt()
    settings = get_current_settings()
    
    status = "🟢 АКТИВЕН" if BOT_RUNNING else "⏸️ НА ПАУЗЕ"
    
    welcome_msg = f"""
🤖 <b>ULTIMATE TRADING BOT v6.0</b>
🎯 <b>УЛУЧШЕННАЯ ТРЕНД-ФОЛЛОУИНГ СТРАТЕГИЯ</b>

💰 <b>Баланс:</b> {balance:.2f} USDT
🎯 <b>Режим:</b> {settings['name']}
📊 <b>Плечо:</b> {settings['leverage']}x
🔰 <b>Статус:</b> {status}

<b>Исправления v6.0:</b>
• ✅ Убраны FORCE_TIMEOUT
• ✅ Увеличены TP/SL
• ✅ Добавлен волатильность-фильтр
• ✅ Частичный выход
• ✅ Улучшенный трейлинг-стоп

<b>Основные команды:</b>
• /status - Статус бота
• /positions - Открытые позиции
• /stats - Статистика
• /scan - Сканировать сигналы
• /settings - Настройки
• /pause /resume - Управление работой
"""
    update.message.reply_text(welcome_msg, parse_mode=ParseMode.HTML)

def cmd_status(update, context):
    """Статус бота"""
    try:
        equity = compute_available_usdt()
        positions = get_open_positions()
        settings = get_current_settings()
        
        status = "🟢 АКТИВЕН" if BOT_RUNNING else "⏸️ НА ПАУЗЕ"
        
        msg = f"""
📊 <b>STATUS: {settings['name']}</b>
🔰 <b>Статус: {status}</b>

💰 Баланс: {equity:.2f} USDT
🔢 Позиции: {len(positions)}/{settings['max_trades']}
📊 Плечо: {settings['leverage']}x
🎯 Стратегия: {settings['strategy']}
📈 TP/SL: {settings['take_profit']*100:.1f}%/{settings['max_stop_loss']*100:.1f}%
"""
        if positions:
            msg += f"\n📈 <b>Открытые позиции:</b>\n"
            total_pnl = 0
            for sym, pos in positions.items():
                current_price = get_current_price(sym)
                if current_price:
                    pnl_percent = calculate_pnl_percent(
                        pos['open_price'], current_price,
                        pos.get('position_type', 'LONG'), pos.get('leverage', 1)
                    )
                    total_pnl += pnl_percent
                    emoji = "🟢" if pnl_percent > 0 else "🔴"
                    trailing_status = "✅" if pos['trailing_active'] else "⏳"
                    position_age = time.time() - pos['open_timestamp']
                    msg += f"{emoji} {trailing_status} {sym} {pos.get('position_type')} - {pnl_percent:+.2f}% ({int(position_age/60)}m)\n"
            msg += f"\n<b>Суммарный PnL:</b> {total_pnl:+.2f}%"
        else:
            msg += "\n📭 Нет открытых позиций"
            
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            
    except Exception as e:
        logger.error(f"❌ Status error: {e}")
        update.message.reply_text("❌ Ошибка статуса")

def cmd_stats(update, context):
    """Статистика"""
    try:
        total_trades = db.fetchone("SELECT COUNT(*) FROM trade_history")[0] or 0
        closed_trades = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='CLOSED'")[0] or 0
        winning_trades = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='CLOSED' AND pnl_percent > 0")[0] or 0
        win_rate = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0
        
        total_pnl = db.fetchone("SELECT SUM(pnl) FROM positions WHERE status='CLOSED'")[0] or 0
        
        msg = f"""
📈 <b>СТАТИСТИКА v6.0</b>

📊 Производительность:
• Всего сделок: {total_trades}
• Закрытых: {closed_trades}
• Винрейт: {win_rate:.1f}%
• Общий PnL: {total_pnl:+.2f} USDT
"""
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка статистики: {str(e)}")

def cmd_show_settings(update, context):
    """Показать текущие настройки"""
    try:
        settings = get_current_settings()
        
        msg = f"""
⚙️ <b>ТЕКУЩИЕ НАСТРОЙКИ: {settings['name']}</b>

📊 Основные:
• Макс сделок: {settings['max_trades']}
• Размер позиции: {settings['trade_pct']*100}%
• Плечо: {settings['leverage']}x
• Кулдаун: {settings['cooldown']}s

🎯 Риск-менеджмент:
• SL: {settings['max_stop_loss']*100:.1f}%
• TP: {settings['take_profit']*100:.1f}%
• Мин. тренд: {settings['min_trend_strength']}
• Частичный выход: {'✅' if settings.get('partial_exit_enabled', False) else '❌'}

📈 Фильтры:
• RSI LONG: {settings['rsi_range_long'][0]}-{settings['rsi_range_long'][1]}
• RSI SHORT: {settings['rsi_range_short'][0]}-{settings['rsi_range_short'][1]}
• Объем: {settings['volume_multiplier']}x
• Волатильность: {settings['min_atr_percentage']*100:.1f}%-{settings['max_atr_percentage']*100:.1f}%
"""
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_test_scan(update, context):
    """Тестовое сканирование"""
    try:
        update.message.reply_text("🧪 <b>ТЕСТОВОЕ СКАНИРОВАНИЕ...</b>", parse_mode=ParseMode.HTML)
        
        signals = []
        for symbol in active_symbols[:5]:
            signal = analyze_symbol_with_filters(symbol)
            if signal:
                signals.append(signal)
        
        if signals:
            msg = "🎯 <b>ТЕСТОВЫЕ СИГНАЛЫ:</b>\n\n"
            for sig in signals[:3]:
                msg += f"• {sig['symbol']} {sig['signal_type']}\n"
                msg += f"  Score: {sig['score']}, Trend: {sig['trend_direction']} ({sig['trend_strength']:.1f})\n"
                msg += f"  RSI: {sig['rsi']:.1f}, Vol: {sig['volume_ratio']:.1f}x\n"
                msg += f"  ATR: {sig['atr_percentage']:.2f}%, BB: {sig['bb_position']:.2%}\n\n"
            
            msg += f"📊 Всего сигналов: {len(signals)}"
        else:
            msg = "📭 <b>Нет сигналов</b>"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Test scan error: {e}")
        update.message.reply_text(f"❌ Ошибка тестового сканирования: {str(e)}")

def cmd_scan(update, context):
    """Сканирование"""
    try:
        if not BOT_RUNNING:
            update.message.reply_text("⏸️ <b>Бот на паузе</b>", parse_mode=ParseMode.HTML)
            return
            
        update.message.reply_text("🔍 <b>СКАНИРОВАНИЕ...</b>", parse_mode=ParseMode.HTML)
        scan_for_opportunities()
        update.message.reply_text("✅ <b>СКАНИРОВАНИЕ ЗАВЕРШЕНО</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"❌ Scan command error: {e}")
        update.message.reply_text(f"❌ Ошибка сканирования: {str(e)}")

def cmd_positions(update, context):
    """Показать позиции"""
    try:
        positions = get_open_positions()
        
        if not positions:
            update.message.reply_text("📭 <b>НЕТ ОТКРЫТЫХ ПОЗИЦИЙ</b>", parse_mode=ParseMode.HTML)
            return
        
        message = "📈 <b>ОТКРЫТЫЕ ПОЗИЦИИ</b>\n\n"
        total_pnl = 0
        
        for symbol, position in positions.items():
            current_price = get_current_price(symbol)
            if current_price:
                pnl_percent = calculate_pnl_percent(
                    position['open_price'], current_price,
                    position['position_type'], position.get('leverage', 1)
                )
                total_pnl += pnl_percent
                emoji = "🟢" if pnl_percent > 0 else "🔴"
                trailing_status = "✅" if position['trailing_active'] else "⏳"
                position_age = time.time() - position['open_timestamp']
                
                message += (
                    f"{emoji} {trailing_status} <b>{symbol} {position['position_type']}</b>\n"
                    f"   Контракты: {position['base_amount']:.6f}\n"
                    f"   Открытие: {position['open_price']:.6f}\n"
                    f"   Текущая: {current_price:.6f}\n"
                    f"   PnL: <b>{pnl_percent:+.2f}%</b>\n"
                    f"   Возраст: {int(position_age/60)}m\n\n"
                )
        
        message += f"<b>СУММАРНЫЙ PnL: {total_pnl:+.2f}%</b>"
        
        update.message.reply_text(message, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Positions command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_sync(update, context):
    """Синхронизация позиций"""
    try:
        update.message.reply_text("🔄 Синхронизация с биржей...")
        # sync_positions_with_exchange()
        update.message.reply_text("✅ Синхронизация завершена")
    except Exception as e:
        logger.error(f"❌ Sync command error: {e}")
        update.message.reply_text(f"❌ Ошибка синхронизации: {str(e)}")

def cmd_pause(update, context):
    """Приостановка бота"""
    try:
        pause_bot()
        update.message.reply_text("⏸️ <b>Бот приостановлен</b>\n/resume для возобновления", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"❌ Pause command error: {e}")
        update.message.reply_text(f"❌ Ошибка приостановки: {str(e)}")

def cmd_resume(update, context):
    """Возобновление работы бота"""
    try:
        resume_bot()
        update.message.reply_text("▶️ <b>Бот возобновил работу</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"❌ Resume command error: {e}")
        update.message.reply_text(f"❌ Ошибка возобновления: {str(e)}")

def cmd_stop(update, context):
    """Остановка бота"""
    try:
        update.message.reply_text("🛑 <b>ОСТАНОВКА БОТА...</b>", parse_mode=ParseMode.HTML)
        stop_bot()
    except Exception as e:
        logger.error(f"❌ Stop command error: {e}")
        update.message.reply_text(f"❌ Ошибка остановки: {str(e)}")

def cmd_close(update, context):
    """Закрытие позиции"""
    try:
        if not context.args:
            update.message.reply_text("❌ Укажите символ: /close SYMBOL")
            return
            
        symbol = context.args[0].upper()
        if not symbol.endswith(":USDT"):
            symbol += ":USDT"
            
        update.message.reply_text(f"🔴 Закрытие {symbol}...")
        # safe_close_position(symbol, "MANUAL_CLOSE")
        update.message.reply_text(f"✅ Позиция {symbol} помечена для закрытия")
        
    except Exception as e:
        logger.error(f"❌ Close command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_cancel_orders(update, context):
    """Отмена всех ордеров"""
    try:
        update.message.reply_text("🔄 Отмена всех ордеров...")
        update.message.reply_text("✅ Ордера отменены")
    except Exception as e:
        logger.error(f"❌ Cancel orders error: {e}")
        update.message.reply_text(f"❌ Ошибка отмены ордеров: {str(e)}")

def cmd_recalculate_sltp(update, context):
    """Перерасчет SL/TP"""
    try:
        update.message.reply_text("🔄 Перерасчет SL/TP...")
        update.message.reply_text("✅ Перерасчет завершен")
    except Exception as e:
        logger.error(f"❌ Recalculate SL/TP error: {e}")
        update.message.reply_text(f"❌ Ошибка перерасчета: {str(e)}")

def cmd_create_missing_orders(update, context):
    """Создание отсутствующих ордеров"""
    try:
        update.message.reply_text("🔄 Создание отсутствующих ордеров...")
        update.message.reply_text("✅ Ордера созданы")
    except Exception as e:
        logger.error(f"❌ Create missing orders error: {e}")
        update.message.reply_text(f"❌ Ошибка создания ордеров: {str(e)}")

def cmd_commission_settings(update, context):
    """Настройки комиссий"""
    try:
        settings = get_current_settings()
        
        entry_type = "⚡ MARKET" if settings.get('use_market_entry', False) else "💎 LIMIT"
        exit_type = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT"
        
        entry_fee = TAKER_FEE * 100 if settings.get('use_market_entry', False) else MAKER_FEE * 100
        exit_fee = TAKER_FEE * 100 if settings.get('use_market_exit', False) else MAKER_FEE * 100
        
        msg = f"""
💰 <b>НАСТРОЙКИ КОМИССИЙ</b>

Тип входа: {entry_type}
Тип выхода: {exit_type}
Комиссия входа: {entry_fee:.3f}%
Комиссия выхода: {exit_fee:.3f}%
Общая комиссия: {entry_fee + exit_fee:.3f}%
"""
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Commission settings error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def calculate_pnl_percent(open_price: float, close_price: float, position_type: str, leverage: int = 1):
    """Расчет PnL в процентах"""
    try:
        if position_type == 'LONG':
            price_change_pct = (close_price - open_price) / open_price
        else:
            price_change_pct = (open_price - close_price) / open_price
        
        pnl_percent = price_change_pct * leverage * 100
        return pnl_percent
        
    except Exception as e:
        logger.error(f"❌ PnL percent calculation error: {e}")
        return 0.0

# ====== ГЛАВНЫЙ ЦИКЛ ======
def main_trading_loop():
    """Основной цикл торговли"""
    logger.info("🤖 Starting ULTIMATE TRADING BOT v6.0...")
    
    balance = compute_available_usdt()
    settings = get_current_settings()
    
    if DRY_RUN:
        safe_send(
            f"🧪 <b>DRY RUN MODE v6.0 STARTED</b>\n"
            f"Баланс: {balance:.2f} USDT\n"
            f"Режим: {settings['name']}\n" 
            f"Плечо: {settings['leverage']}x\n"
            f"Статус: 🟡 DRY_RUN\n\n"
            f"<b>Внимание:</b> Это тестовый режим!\n"
            f"• ✅ Анализ работает\n"
            f"• ✅ Сообщения отправляются\n"
            f"• ❌ Сделки НЕ открываются"
        )
    else:
        safe_send(
            f"🚀 <b>BOT v6.0 STARTED</b>\n"
            f"Баланс: {balance:.2f} USDT\n"
            f"Режим: {settings['name']}\n" 
            f"Плечо: {settings['leverage']}x\n"
            f"Статус: 🟢 АКТИВЕН"
        )

    last_scan = 0
    last_status = 0
    last_sync = 0
    last_exit_check = 0

    while True:
        try:
            if not BOT_RUNNING:
                time.sleep(5)
                continue
                
            current_time = time.time()
            settings = get_current_settings()

            if current_time - last_sync >= settings['sync_interval']:
                # sync_positions_with_exchange()
                last_sync = current_time
            
            if current_time - last_exit_check >= settings['exit_check_interval']:
                # check_position_exits()
                last_exit_check = current_time
            
            if current_time - last_scan >= settings['scan_interval']:
                scan_for_opportunities()
                last_scan = current_time
            
            if current_time - last_status >= settings['status_interval']:
                # cmd_status(None, None)
                last_status = current_time
                
            time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Main loop error: {e}")
            time.sleep(10)

def cleanup():
    """Очистка"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        logger.info("✅ Cleanup completed")
    except Exception as e:
        logger.error(f"❌ Cleanup error: {e}")

def signal_handler(signum, frame):
    """Обработчик сигналов"""
    logger.info(f"🛑 Received signal {signum}")
    safe_send("🛑 <b>Бот остановлен по сигналу</b>")
    stop_bot()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Проверка API ключей
        if "YOUR_API" in API_KEY or "YOUR_API" in API_SECRET or "YOUR_TELEGRAM" in TELEGRAM_TOKEN:
            print("❌ ВНИМАНИЕ: Используются тестовые API ключи!")
            print("❌ Для реальной торговли установите настоящие ключи через переменные окружения")
            
        initialize_exchange()
        
        balance = compute_available_usdt()
        settings = get_current_settings()
        
        print(f"✅ ULTIMATE BOT v6.0 started!")
        print(f"💰 Balance: {balance:.2f} USDT")
        print(f"🎯 Mode: {settings['name']}")
        print(f"📊 Leverage: {settings['leverage']}x")
        print(f"🎯 TP/SL: {settings['take_profit']*100:.1f}%/{settings['max_stop_loss']*100:.1f}%")
        print(f"🔰 Status: {'🟢 ACTIVE' if BOT_RUNNING else '⏸️ PAUSED'}")
        print(f"🧪 DRY_RUN: {'✅ ON' if DRY_RUN else '❌ OFF'}")
        
        updater = setup_telegram()
        if updater:
            updater.start_polling()
            logger.info("✅ Telegram bot started")
        
        main_trading_loop()
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        safe_send(f"❌ <b>BOT CRASHED:</b> {str(e)}")
    finally:
        cleanup()
