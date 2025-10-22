#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ULTIMATE TRADING BOT v5.0 - BYBIT FUTURES WITH ERROR HANDLING
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

try:
    from ta.trend import EMAIndicator, MACD, ADXIndicator
    from ta.momentum import RSIIndicator, StochasticOscillator
    from ta.volatility import BollingerBands, AverageTrueRange
    from ta.volume import VolumeWeightedAveragePrice
except ImportError as e:
    print(f"TA-Lib import error: {e}")
    print("Install with: pip install ta")
    sys.exit(1)

try:
    from telegram import Bot, ParseMode, ReplyKeyboardMarkup, KeyboardButton
    from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
except ImportError as e:
    print(f"Telegram import error: {e}")
    print("Install with: pip install python-telegram-bot")
    sys.exit(1)

# ====== CONFIGURATION ======
API_KEY = os.getenv("BYBIT_API_KEY", "BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET", "BYBIT_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "TELEGRAM_TOKEN")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID"))


DRY_RUN = False  # Режим тестирования без реальных ордеров

# Фьючерсные пары на Bybit
SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT",
    "ADA/USDT:USDT", "AVAX/USDT:USDT", "DOT/USDT:USDT", "LINK/USDT:USDT", 
    "DOGE/USDT:USDT", "LTC/USDT:USDT", "ATOM/USDT:USDT", "UNI/USDT:USDT", "XLM/USDT:USDT",
    "ETC/USDT:USDT", "FIL/USDT:USDT", "THETA/USDT:USDT", "AAVE/USDT:USDT"
]

active_symbols = SYMBOLS

# ====== КОМИССИИ BYBIT ======
TAKER_FEE = 0.0006  # 0.06%
MAKER_FEE = 0.0002  # 0.02%

# ====== УНИВЕРСАЛЬНЫЕ НАСТРОЙКИ ======
TRADING_MODES = {
    "AGGRESSIVE": {
        "name": "🟢 АГРЕССИВНЫЙ",
        "type": "momentum", 
        "scan_interval": 60,
        "exit_check_interval": 10,
        "status_interval": 300,
        "sync_interval": 1800,
        "max_trades": 2,
        "trade_pct": 0.15,
        "timeframe_entry": "5m",
        "timeframe_trend": "15m",
        "max_stop_loss": 0.008,
        "take_profit": 0.020,
        "quick_exit": 0.008,
        "rsi_range_long": (30, 75),
        "rsi_range_short": (25, 70),
        "volume_multiplier": 1.3,
        "min_score": 75,
        "cooldown": 300,
        "max_daily_trades_per_symbol": 3,
        "strategy": "MOMENTUM_BREAKOUT",
        "risk_level": "HIGH",
        "trailing_stop_activation": 0.012,
        "trailing_stop_distance": 0.006,
        "max_position_time": 3600,
        "trend_strength_required": 1,
        "leverage": 10,
        "use_exchange_orders": True,
        "use_market_entry": False,
        "use_market_exit": False,
        "limit_order_timeout": 60,
        "commission_filter": True,
    },
    "CONSERVATIVE": {
        "name": "🟡 КОНСЕРВАТИВНЫЙ",
        "type": "swing", 
        "scan_interval": 120,
        "exit_check_interval": 15,
        "status_interval": 600,
        "sync_interval": 1800,
        "max_trades": 1,
        "trade_pct": 0.08,
        "timeframe_entry": "15m", 
        "timeframe_trend": "1h",
        "max_stop_loss": 0.006,
        "take_profit": 0.015,
        "quick_exit": 0.006,
        "rsi_range_long": (35, 70),
        "rsi_range_short": (30, 65),
        "volume_multiplier": 1.4,
        "min_score": 70,
        "cooldown": 600,
        "max_daily_trades_per_symbol": 2,
        "strategy": "TREND_FOLLOWING", 
        "risk_level": "MEDIUM",
        "trailing_stop_activation": 0.010,
        "trailing_stop_distance": 0.005,
        "max_position_time": 7200,
        "trend_strength_required": 1,
        "leverage": 5,
        "use_exchange_orders": True,
        "use_market_entry": False,
        "use_market_exit": False,
        "limit_order_timeout": 90,
        "commission_filter": True,
    },
    "SCALPING": {
        "name": "🔴 СКАЛЬПИНГ",
        "type": "scalping",
        "scan_interval": 30,
        "exit_check_interval": 5,
        "status_interval": 180,
        "sync_interval": 1800,
        "max_trades": 2,
        "trade_pct": 0.08,
        "timeframe_entry": "3m",
        "timeframe_trend": "15m", 
        "max_stop_loss": 0.004,
        "take_profit": 0.010,
        "quick_exit": 0.004,
        "rsi_range_long": (25, 80),
        "rsi_range_short": (20, 75),
        "volume_multiplier": 1.5,
        "min_score": 70,
        "cooldown": 180,
        "max_daily_trades_per_symbol": 4,
        "strategy": "BB_SQUEEZE",
        "risk_level": "HIGH",
        "trailing_stop_activation": 0.006,
        "trailing_stop_distance": 0.003, 
        "max_position_time": 300,
        "timeout_profit_threshold": 0.002,
        "trend_strength_required": 1,
        "leverage": 15,
        "use_exchange_orders": False,
        "use_market_entry": True,
        "use_market_exit": True,
        "limit_order_timeout": 45,
        "commission_filter": False,
    }
}

# Минимальные настройки
MIN_TRADE_USDT = 10.0
MIN_USDT_PER_SYMBOL = {
    "BTC/USDT:USDT": 5.0, "ETH/USDT:USDT": 5.0, "BNB/USDT:USDT": 5.0, "SOL/USDT:USDT": 3.0,
    "XRP/USDT:USDT": 3.0, "ADA/USDT:USDT": 3.0, "AVAX/USDT:USDT": 3.0, "DOT/USDT:USDT": 3.0,
    "LINK/USDT:USDT": 3.0, "DOGE/USDT:USDT": 3.0, "LTC/USDT:USDT": 3.0,
    "ATOM/USDT:USDT": 3.0, "UNI/USDT:USDT": 3.0, "XLM/USDT:USDT": 3.0, "ETC/USDT:USDT": 3.0,
    "FIL/USDT:USDT": 3.0, "THETA/USDT:USDT": 3.0, "AAVE/USDT:USDT": 5.0,
}

LOCK_FILE = "/tmp/ultimate_trading_bot.lock"
DB_FILE = "trades_ultimate_futures_v5.db"

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
        logging.FileHandler('ultimate_bot_futures_v5.log', encoding='utf-8'),
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
        """Инициализация базы данных с созданием таблиц"""
        try:
            self._connection = sqlite3.connect(self.db_file, check_same_thread=False)
            self._cursor = self._connection.cursor()
            
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
                    exit_type TEXT DEFAULT 'MARKET'
                )
            """)
            
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
                    exit_type TEXT DEFAULT 'MARKET'
                )
            """)
            
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbol_cooldown (
                    symbol TEXT PRIMARY KEY, 
                    last_closed_ts INTEGER DEFAULT 0,
                    daily_trade_count INTEGER DEFAULT 0, 
                    last_trade_date TEXT
                )
            """)
            
            self._connection.commit()
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Database initialization error: {e}")
            raise
    
    def get_connection(self):
        """Получение соединения с переподключением при необходимости"""
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
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
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

# Глобальная инициализация базы данных
db = DatabaseManager()

# ====== ИНИЦИАЛИЗАЦИЯ БИРЖИ ======
def initialize_exchange():
    global exchange, bot
    
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
                "defaultType": "future",
                "adjustForTimeDifference": True,
            },
            "timeout": 30000,
        })
        
        # Тестируем подключение
        balance = exchange.fetch_balance()
        logger.info("✅ Bybit Futures connected successfully")
        
        bot = Bot(token=TELEGRAM_TOKEN)
        # Тестируем Telegram
        bot.get_me()
        logger.info("✅ Telegram bot initialized")
            
    except Exception as e:
        logger.error(f"❌ Initialization failed: {e}")
        sys.exit(1)
        
def safe_send(text: str, max_retries: int = 3) -> bool:
    """Безопасная отправка сообщения в Telegram"""
    for attempt in range(max_retries):
        try:
            bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML)
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"❌ Failed to send Telegram message: {e}")
            time.sleep(2)
    return False
    
def setup_telegram():
    """Инициализация Telegram бота"""
    global updater
    try:
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
        dp.add_handler(CommandHandler("maker_entries", cmd_maker_entries))
        dp.add_handler(CommandHandler("market_entries", cmd_market_entries))
        dp.add_handler(CommandHandler("maker_exits", cmd_maker_exits))
        dp.add_handler(CommandHandler("market_exits", cmd_market_exits))
        dp.add_handler(CommandHandler("enable_filter", cmd_enable_filter))
        dp.add_handler(CommandHandler("disable_filter", cmd_disable_filter))
        dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
        dp.add_handler(CallbackQueryHandler(handle_callback))
        
        return updater
    except Exception as e:
        logger.error(f"❌ Telegram setup failed: {e}")
        return None

# ====== УПРАВЛЕНИЕ СОСТОЯНИЕМ БОТА ======
def stop_bot():
    """Корректная остановка бота"""
    global BOT_RUNNING, updater
    
    logger.info("🛑 Stopping bot gracefully...")
    BOT_RUNNING = False
    
    close_all_positions_emergency()
    
    time.sleep(2)
    
    if updater:
        updater.stop()
    
    cleanup()
    logger.info("✅ Bot stopped gracefully")
    sys.exit(0)

def close_all_positions_emergency():
    """Экстренное закрытие всех позиций при остановке"""
    try:
        positions = get_open_positions()
        if not positions:
            return
            
        logger.info(f"🛑 Closing {len(positions)} positions...")
        
        for symbol in positions:
            try:
                safe_close_position(symbol, "EMERGENCY_STOP")
                time.sleep(1)
            except Exception as e:
                logger.error(f"❌ Emergency close failed for {symbol}: {e}")
                
    except Exception as e:
        logger.error(f"❌ Emergency close error: {e}")

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
    """Получение баланса с обработкой ошибок"""
    def _fetch():
        return exchange.fetch_balance()
    
    try:
        return retry_api_call(_fetch)
    except Exception as e:
        logger.error(f"❌ Balance fetch failed: {e}")
        return {'free': {'USDT': 0.0}, 'total': {'USDT': 0.0}}

def get_current_price(symbol: str):
    """Получение текущей цены с обработкой ошибок"""
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

def set_leverage(symbol: str, leverage: int):
    """Установка плеча с обработкой ошибок"""
    try:
        if DRY_RUN:
            logger.info(f"🔶 DRY RUN: Would set leverage {leverage}x for {symbol}")
            return True
            
        markets = exchange.load_markets()
        market = markets.get(symbol)
        
        if not market:
            logger.warning(f"⚠️ Market not found for {symbol}")
            return True
            
        market_type = market.get('type')
        is_linear = market.get('linear', False)
        is_inverse = market.get('inverse', False)
        
        if market_type in ['future', 'swap'] and (is_linear or is_inverse):
            market_symbol = symbol.replace("/", "").replace(":USDT", "")
            response = exchange.set_leverage(leverage, market_symbol)
            logger.info(f"✅ Leverage set to {leverage}x for {symbol}")
        else:
            logger.info(f"ℹ️ Leverage auto-managed for {symbol}")
            
        return True
        
    except Exception as e:
        error_msg = str(e)
        if "only support linear and inverse" in error_msg:
            logger.info(f"ℹ️ Leverage auto-managed for {symbol}")
        elif "leverage not modified" in error_msg:
            logger.info(f"ℹ️ Leverage already set for {symbol}")
        else:
            logger.warning(f"⚠️ Leverage setting issue for {symbol}: {e}")
        return True

def get_symbol_info(symbol: str):
    """Получение информации о символе с обработкой ошибок"""
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

def adjust_amount_to_precision(symbol: str, amount: float):
    """Округление количества до точности биржи"""
    try:
        markets = exchange.load_markets()
        market = markets.get(symbol)
        if market:
            precision = market.get('precision', {}).get('amount')
            if precision is not None:
                if isinstance(precision, int):
                    return float(round(amount, precision))
                else:
                    step = precision
                    return float(math.floor(amount / step) * step)
        return round(amount, 6)
    except Exception as e:
        logger.error(f"❌ Amount adjustment error for {symbol}: {e}")
        return round(amount, 6)

def adjust_price_to_precision(symbol: str, price: float):
    """Округление цены до точности биржи"""
    try:
        markets = exchange.load_markets()
        market = markets.get(symbol)
        if market:
            precision = market.get('precision', {}).get('price')
            if precision is not None:
                if isinstance(precision, int):
                    return float(round(price, precision))
                else:
                    step = precision
                    return float(math.floor(price / step) * step)
        return round(price, 6)
    except Exception as e:
        logger.error(f"❌ Price adjustment error for {symbol}: {e}")
        return round(price, 6)

def calculate_position_size(symbol: str, usdt_amount: float, current_price: float, leverage: int = 1):
    """ПРАВИЛЬНЫЙ расчет размера позиции с проверкой доступности"""
    try:
        markets = exchange.load_markets()
        market = markets.get(symbol)
        
        if not market:
            logger.error(f"❌ Market not found for {symbol}")
            return 0.0
            
        contract_size = safe_float_convert(market.get('contractSize', 1.0))
        
        # Расчет: (USDT * leverage) / (price * contract_size)
        base_amount = (usdt_amount * leverage) / (current_price * contract_size)
        
        # Округление до шага
        precision = market.get('precision', {}).get('amount')
        if precision:
            if isinstance(precision, int):
                base_amount = round(base_amount, precision)
            else:
                step = precision
                base_amount = math.floor(base_amount / step) * step
        
        # Проверка минимального количества
        min_amount = safe_float_convert(market.get('limits', {}).get('amount', {}).get('min', 0))
        if min_amount > 0 and base_amount < min_amount:
            logger.warning(f"⚠️ Amount {base_amount} < min {min_amount}, adjusting to minimum")
            base_amount = min_amount
        
        # Проверка максимального количества
        max_amount = safe_float_convert(market.get('limits', {}).get('amount', {}).get('max', float('inf')))
        if base_amount > max_amount:
            logger.warning(f"⚠️ Amount {base_amount} > max {max_amount}, adjusting to maximum")
            base_amount = max_amount
        
        # Проверка доступного баланса
        required_margin = (base_amount * current_price * contract_size) / leverage
        available_balance = compute_available_usdt()
        
        safety_buffer = 1.1
        total_required = required_margin * safety_buffer
        
        if total_required > available_balance:
            logger.warning(f"⚠️ Required {total_required:.2f} > available {available_balance:.2f}, recalculating...")
            max_usdt_with_buffer = available_balance / safety_buffer
            base_amount = (max_usdt_with_buffer * leverage) / (current_price * contract_size)
            
            if precision:
                if isinstance(precision, int):
                    base_amount = round(base_amount, precision)
                else:
                    step = precision
                    base_amount = math.floor(base_amount / step) * step
            
            if min_amount > 0 and base_amount < min_amount:
                logger.warning(f"⚠️ Recalculated amount {base_amount} still < min {min_amount}")
                return 0.0
            
            logger.info(f"📊 Adjusted position: {base_amount} contracts for {max_usdt_with_buffer:.2f} USDT")
        
        logger.info(f"📊 Position calc: {usdt_amount} USDT * {leverage}x / ({current_price} * {contract_size}) = {base_amount} contracts")
        return base_amount
        
    except Exception as e:
        logger.error(f"❌ Position calculation error for {symbol}: {e}")
        return 0.0

def compute_available_usdt():
    """Расчет доступного USDT"""
    try:
        bal = fetch_balance()
        total_usdt = safe_float_convert(bal['free'].get('USDT', 0))
        logger.info(f"💰 Available USDT: {total_usdt:.2f}")
        return total_usdt
    except Exception as e:
        logger.error(f"❌ Balance computation error: {e}")
        return 0.0

# ====== ОПТИМИЗАЦИЯ КОМИССИЙ ======
def should_skip_low_profit_trade(expected_profit_pct: float) -> bool:
    """ФИЛЬТР МАЛОЙ ПРИБЫЛИ: пропустить сделку если прибыль < комиссий"""
    try:
        settings = get_current_settings()
        
        if not settings.get('commission_filter', True):
            return False
            
        if settings.get('use_market_entry', False):
            entry_fee = TAKER_FEE
        else:
            entry_fee = MAKER_FEE
            
        if settings.get('use_market_exit', False):
            exit_fee = TAKER_FEE
        else:
            exit_fee = MAKER_FEE
            
        total_commission = (entry_fee + exit_fee) * 100
            
        min_profit_buffer = 0.3
        skip_threshold = total_commission + min_profit_buffer
        
        if abs(expected_profit_pct) < skip_threshold:
            logger.info(f"⏹️ Skip low-profit trade — expected {expected_profit_pct:.2f}% < {skip_threshold:.2f}% (commissions + buffer)")
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"❌ Low profit filter error: {e}")
        return False

def calculate_take_profit_with_commission(entry_price: float, position_type: str, settings: Dict) -> float:
    """РАСЧЕТ TP С УЧЕТОМ КОМИССИЙ"""
    try:
        base_take_profit = settings['take_profit']
        
        if settings.get('use_market_entry', False):
            entry_commission = TAKER_FEE
        else:
            entry_commission = MAKER_FEE
            
        if settings.get('use_market_exit', False):
            exit_commission = TAKER_FEE
        else:
            exit_commission = MAKER_FEE
            
        total_commission = entry_commission + exit_commission
        commission_buffer = total_commission * 1.25
        
        if position_type == 'LONG':
            take_profit = entry_price * (1 + base_take_profit + commission_buffer)
        else:
            take_profit = entry_price * (1 - base_take_profit - commission_buffer)
            
        logger.info(f"💰 TP with commission buffer: {base_take_profit*100:.2f}% + {commission_buffer*100:.3f}% = {(base_take_profit+commission_buffer)*100:.2f}%")
        
        return take_profit
        
    except Exception as e:
        logger.error(f"❌ TP with commission calculation error: {e}")
        if position_type == 'LONG':
            return entry_price * (1 + settings['take_profit'])
        else:
            return entry_price * (1 - settings['take_profit'])

def calculate_real_pnl_with_commission(open_price: float, close_price: float, amount: float, 
                                     position_type: str, leverage: int = 1, invested_usdt: float = 0, 
                                     symbol: str = None, entry_type: str = "MARKET", exit_type: str = "MARKET") -> Tuple[float, float, float]:
    """ПРАВИЛЬНЫЙ расчет реального PnL с комиссиями"""
    try:
        if not symbol:
            return calculate_real_pnl_fallback(open_price, close_price, amount, position_type, leverage, invested_usdt)
        
        symbol_info = get_symbol_info(symbol)
        contract_size = symbol_info.get('contract_size', 1)
        
        gross_pnl = calculate_futures_pnl(open_price, close_price, amount, position_type, contract_size)
        
        if entry_type == "MARKET":
            open_fee_rate = TAKER_FEE
        else:
            open_fee_rate = MAKER_FEE
            
        if exit_type == "MARKET":
            close_fee_rate = TAKER_FEE
        else:
            close_fee_rate = MAKER_FEE
            
        open_fee = (amount * open_price * contract_size) * open_fee_rate
        close_fee = (amount * close_price * contract_size) * close_fee_rate
        total_fee = open_fee + close_fee
        
        net_pnl = gross_pnl - total_fee
        
        if invested_usdt > 0:
            net_pnl_percent = (net_pnl / invested_usdt) * 100
        else:
            margin_used = (amount * open_price * contract_size) / leverage
            net_pnl_percent = (net_pnl / margin_used) * 100 if margin_used > 0 else 0
        
        logger.info(f"📊 PnL with commission: Gross={gross_pnl:.4f}, Fees={total_fee:.4f}, Net={net_pnl:.4f} ({net_pnl_percent:.2f}%)")
        
        return net_pnl, net_pnl_percent, total_fee
        
    except Exception as e:
        logger.error(f"❌ Real PnL with commission error for {symbol}: {e}")
        return calculate_real_pnl_fallback(open_price, close_price, amount, position_type, leverage, invested_usdt)

def calculate_futures_pnl(open_price, close_price, amount, position_type, contract_size=1):
    """ПРАВИЛЬНЫЙ расчет PnL для фьючерсов"""
    try:
        if position_type == 'LONG':
            pnl = (close_price - open_price) * amount * contract_size
        else:
            pnl = (open_price - close_price) * amount * contract_size
        return pnl
    except Exception as e:
        logger.error(f"❌ Futures PnL calculation error: {e}")
        return 0

def calculate_real_pnl_fallback(open_price, close_price, amount, position_type, leverage=1, invested_usdt=0):
    """Fallback расчет PnL"""
    try:
        if position_type == 'LONG':
            price_change = close_price - open_price
        else:
            price_change = open_price - close_price
        
        gross_pnl = price_change * amount
        
        turnover = amount * (open_price + close_price) / 2
        total_fee = turnover * 0.0012
        
        net_pnl = gross_pnl - total_fee
        
        if invested_usdt > 0:
            net_pnl_percent = (net_pnl / invested_usdt) * 100
        else:
            margin_used = (amount * open_price) / leverage
            net_pnl_percent = (net_pnl / margin_used) * 100 if margin_used > 0 else 0
        
        logger.info(f"📊 Fallback PnL: Gross={gross_pnl:.4f}, Fees={total_fee:.4f}, Net={net_pnl:.4f} ({net_pnl_percent:.2f}%)")
        
        return net_pnl, net_pnl_percent, total_fee
        
    except Exception as e:
        logger.error(f"❌ Fallback PnL calculation error: {e}")
        return 0, 0, 0

def calculate_pnl_percent(open_price: float, close_price: float, position_type: str, leverage: int = 1):
    """ПРАВИЛЬНЫЙ расчет PnL в процентах"""
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

# ====== УЛУЧШЕННОЕ ОТКРЫТИЕ ПОЗИЦИЙ ======
def wait_for_limit_order_fill(symbol: str, order_id: str, timeout: int = 60) -> bool:
    """Ожидание исполнения лимитного ордера с таймаутом и безопасной проверкой статуса"""
    try:
        logger.info(f"⏳ Waiting for limit order {order_id} to fill (timeout: {timeout}s)")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Сначала пробуем получить ордер среди открытых
                try:
                    order = exchange.fetch_open_order(order_id, symbol)
                except Exception:
                    # Если не найден — возможно, уже исполнен или отменён
                    try:
                        order = exchange.fetch_closed_order(order_id, symbol)
                    except Exception as inner_e:
                        logger.warning(f"⚠️ Error checking order status (open/closed): {inner_e}")
                        order = None

                if not order:
                    logger.debug(f"⚠️ Order {order_id} not found in open/closed sets yet.")
                    time.sleep(5)
                    continue

                status = (order.get('status') or '').lower()

                if status in ('closed', 'filled'):
                    logger.info(f"✅ Limit order {order_id} filled successfully")
                    return True
                elif status in ('canceled', 'rejected', 'expired'):
                    logger.warning(f"❌ Limit order {order_id} was {status}")
                    return False

                # Всё ещё 'open' — ждём
                time.sleep(5)

            except Exception as e:
                msg = str(e)
                if "order not exists" in msg or "can only access an order" in msg:
                    logger.debug(f"⏳ Order {order_id} not yet acknowledged, retrying...")
                else:
                    logger.warning(f"⚠️ Error checking order status: {e}")
                time.sleep(5)

        # ⏰ Таймаут
        logger.warning(f"⏰ Limit order timeout after {timeout}s, cancelling order {order_id}")
        try:
            exchange.cancel_order(order_id, symbol)
            logger.info(f"✅ Cancelled unfilled limit order {order_id}")
        except Exception as cancel_error:
            msg = str(cancel_error)
            if "order not exists" in msg or "too late to cancel" in msg:
                logger.info(f"ℹ️ Order {order_id} already filled or cancelled earlier.")
            else:
                logger.error(f"❌ Failed to cancel order: {cancel_error}")
        time.sleep(2)
        return False

    except Exception as e:
        logger.error(f"❌ Error in wait_for_limit_order_fill: {e}")
        return False


def open_trade_position(signal: Dict):
    """УЛУЧШЕННОЕ ОТКРЫТИЕ ПОЗИЦИИ С ОБРАБОТКОЙ ОШИБОК"""
    try:
        settings = get_current_settings()
        symbol = signal['symbol']
        current_price = signal['price']
        position_type = signal['signal_type']
        leverage = settings['leverage']
        
        # Валидация входных данных
        if not current_price or current_price <= 0:
            logger.error(f"❌ Invalid current price for {symbol}: {current_price}")
            return False
        
        expected_profit_pct = settings['take_profit'] * 100
        if should_skip_low_profit_trade(expected_profit_pct):
            logger.info(f"⏹️ Skipping {symbol} — profit {expected_profit_pct:.2f}% < commissions threshold")
            return False
        
        set_leverage(symbol, leverage)
        
        available_usdt = compute_available_usdt()
        min_usdt_for_symbol = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
        
        safety_buffer = 1.15
        min_required_with_buffer = min_usdt_for_symbol * safety_buffer
        
        if available_usdt < min_required_with_buffer:
            logger.warning(f"⏹️ Low balance: {available_usdt:.2f} < {min_required_with_buffer:.2f}")
            return False
        
        position_size_usdt = min(available_usdt * settings['trade_pct'], available_usdt * 0.4)
        position_size_usdt = max(position_size_usdt, min_usdt_for_symbol)
        position_size_usdt = position_size_usdt / safety_buffer
        
        logger.info(f"💰 Position size: {position_size_usdt:.2f} USDT for {symbol}")
        
        base_amount = calculate_position_size(symbol, position_size_usdt, current_price, leverage)
        
        if base_amount <= 0:
            logger.warning(f"⏹️ Invalid amount: {base_amount}")
            return False
        
        markets = exchange.load_markets()
        market = markets.get(symbol)
        if market:
            min_amount = safe_float_convert(market.get('limits', {}).get('amount', {}).get('min', 0))
            if min_amount > 0 and base_amount < min_amount:
                logger.warning(f"⏹️ Amount too small: {base_amount} < {min_amount}")
                required_usdt = (min_amount * current_price * safe_float_convert(market.get('contractSize', 1))) / leverage
                required_with_buffer = required_usdt * safety_buffer
                
                if required_with_buffer > available_usdt:
                    logger.warning(f"⏹️ Not enough USDT for min position: {required_with_buffer:.2f} > {available_usdt:.2f}")
                    return False
                base_amount = min_amount
                position_size_usdt = required_usdt
        
        logger.info(f"💰 {position_type} {symbol}: {base_amount:.6f} contracts, {position_size_usdt:.2f} USDT")
        
        stop_loss, take_profit = calculate_safe_sl_tp(current_price, position_type, settings)
        take_profit = calculate_take_profit_with_commission(current_price, position_type, settings)
        
        stop_loss = adjust_price_to_precision(symbol, stop_loss)
        take_profit = adjust_price_to_precision(symbol, take_profit)
        
        if position_type == 'LONG':
            if stop_loss >= current_price or take_profit <= current_price:
                logger.error(f"❌ Invalid SL/TP for LONG")
                return False
        else:
            if stop_loss <= current_price or take_profit >= current_price:
                logger.error(f"❌ Invalid SL/TP for SHORT")
                return False
        
        contract_size = safe_float_convert(market.get('contractSize', 1)) if market else 1
        required_margin = (base_amount * current_price * contract_size) / leverage
        margin_with_buffer = required_margin * safety_buffer
        
        if margin_with_buffer > available_usdt:
            logger.error(f"❌ Margin check failed: {margin_with_buffer:.2f} > {available_usdt:.2f}")
            return False
        
        logger.info(f"🟢 Opening {position_type} {symbol} @ {current_price:.6f}, Margin: {required_margin:.2f} USDT")
        
        exchange_order_ids = []
        entry_type = "MARKET"
        exit_type = "MARKET"
        filled_price = current_price
        
        if DRY_RUN:
            logger.info(f"🔶 DRY RUN: Would open {position_type} {symbol}")
            net_pnl, net_pnl_percent, total_fee = calculate_real_pnl_with_commission(
                current_price, take_profit, base_amount, position_type, leverage, 
                position_size_usdt, symbol, entry_type, exit_type
            )
            success = record_open_position(symbol, base_amount, current_price, stop_loss, take_profit, 
                                         position_type, leverage, position_size_usdt, exchange_order_ids, entry_type)
        else:
            try:
                if settings.get('use_market_entry', False):
                    if position_type == 'LONG':
                        order = exchange.create_order(symbol, 'market', 'buy', base_amount)
                    else:
                        order = exchange.create_order(symbol, 'market', 'sell', base_amount)
                    entry_type = "MARKET"
                else:
                    if position_type == 'LONG':
                        order = exchange.create_order(symbol, 'limit', 'buy', base_amount, current_price, 
                                                    {'postOnly': True})
                    else:
                        order = exchange.create_order(symbol, 'limit', 'sell', base_amount, current_price, 
                                                    {'postOnly': True})
                    entry_type = "LIMIT"
                    
                    if order and order.get('id'):
                        order_filled = wait_for_limit_order_fill(symbol, order['id'], settings.get('limit_order_timeout', 60))
                        if not order_filled:
                            logger.warning(f"⏹️ Limit order not filled within timeout, skipping trade")
                            try:
                                exchange.cancel_order(order['id'], symbol)
                            except:
                                pass
                            return False
                
                if order and order.get('id'):
                    # БЕЗОПАСНОЕ получение цены исполнения
                    if order.get('trades'):
                        trades = order['trades']
                        filled_prices = []
                        for trade in trades:
                            price = safe_float_convert(trade.get('price'))
                            if price and price > 0:
                                filled_prices.append(price)
                        
                        if filled_prices:
                            filled_price = sum(filled_prices) / len(filled_prices)
                        else:
                            filled_price = safe_float_convert(order.get('price', current_price))
                    else:
                        filled_price = safe_float_convert(order.get('price', current_price))
                    
                    # Проверяем что получили валидную цену
                    if not filled_price or filled_price <= 0:
                        logger.error(f"❌ Invalid filled price for {symbol}: {filled_price}")
                        filled_price = current_price
                    
                    time.sleep(1)
                    
                    if settings.get('use_exchange_orders', True):
                        sl_success, order_ids = create_exchange_stop_orders(
                            symbol, position_type, stop_loss, take_profit, base_amount
                        )
                        if sl_success:
                            exchange_order_ids = order_ids
                            logger.info(f"✅ Real SL/TP orders created: {order_ids}")
                    
                    success = record_open_position(
                        symbol, base_amount, filled_price, stop_loss, take_profit, 
                        position_type, leverage, position_size_usdt, exchange_order_ids, entry_type
                    )
                else:
                    logger.error(f"❌ Order creation failed for {symbol}")
                    return False
                    
            except Exception as order_error:
                logger.error(f"❌ Order error for {symbol}: {order_error}")
                
                if "postOnly" in str(order_error):
                    logger.info(f"⏹️ Limit order not filled for {symbol}, skipping trade")
                    return False
                    
                if "not enough" in str(order_error).lower():
                    current_balance = compute_available_usdt()
                    logger.error(f"❌ Balance issue: {current_balance:.2f} USDT available")
                return False
        
        if success:
            reasons_str = ", ".join(signal['reasons'])
            risk_amount = position_size_usdt * settings['max_stop_loss']
            
            order_type = "🔰 REAL ORDERS" if settings.get('use_exchange_orders') else "💻 SOFTWARE SL/TP"
            entry_type_emoji = "⚡ MARKET" if entry_type == "MARKET" else "💎 LIMIT"
            exit_type_emoji = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT"
            
            if entry_type == "MARKET":
                entry_fee = TAKER_FEE * 100
            else:
                entry_fee = MAKER_FEE * 100
                
            if settings.get('use_market_exit', False):
                exit_fee = TAKER_FEE * 100
            else:
                exit_fee = MAKER_FEE * 100
            
            safe_send(
                f"🎯 <b>FUTURES ENTRY: {symbol} {position_type}</b>\n"
                f"Режим: {settings['name']}\n"
                f"Вход: {entry_type_emoji} ({entry_fee:.3f}%)\n" 
                f"Выход: {exit_type_emoji} ({exit_fee:.3f}%)\n"
                f"Защита: {order_type}\n"
                f"Контракты: {base_amount:.6f}\n"
                f"Цена: {filled_price:.6f}\n" 
                f"Плечо: {leverage}x\n"
                f"Инвестировано: {position_size_usdt:.2f} USDT\n"
                f"Маржа: {required_margin:.2f} USDT\n"
                f"SL: {stop_loss:.6f}\n"
                f"TP: {take_profit:.6f}\n"
                f"Риск: ${risk_amount:.2f}\n"
                f"Комиссии: {entry_fee + exit_fee:.3f}%\n"
                f"Score: {signal['score']}/100\n"
                f"Причины: {reasons_str}"
            )
            return True
            
    except Exception as e:
        logger.error(f"❌ Open {position_type} error for {signal['symbol']}: {e}")
        safe_send(f"❌ <b>Open failed:</b> {signal['symbol']} {position_type}\n{str(e)}")
        return False
    
    return False

# ====== РЕАЛЬНЫЕ ОРДЕРА НА БИРЖЕ ======
def create_exchange_stop_orders(symbol: str, position_type: str, stop_loss: float, take_profit: float, amount: float):
    """Создание реальных SL/TP ордеров на бирже"""
    try:
        settings = get_current_settings()
        if not settings.get('use_exchange_orders', True) or DRY_RUN:
            logger.info(f"🔶 {'DRY RUN' if DRY_RUN else 'Software'} SL/TP for {symbol}")
            return True, []
            
        order_ids = []
        current_price = get_current_price(symbol)
        
        if not current_price:
            logger.error(f"❌ Cannot get current price for {symbol}")
            return False, []
        
        logger.info(f"📊 Creating orders for {position_type} {symbol}: Current={current_price:.6f}, SL={stop_loss:.6f}, TP={take_profit:.6f}")
        
        if position_type == 'LONG':
            sl_side = 'sell'
            tp_side = 'sell'
        else:
            sl_side = 'buy'
            tp_side = 'buy'
        
        # Тейк-профит
        try:
            tp_order = exchange.create_order(
                symbol=symbol,
                type='limit',
                side=tp_side,
                amount=amount,
                price=take_profit,
                params={
                    'reduceOnly': True,
                    'timeInForce': 'GTC'
                }
            )
            if tp_order and 'id' in tp_order:
                order_ids.append(tp_order['id'])
                logger.info(f"✅ TP order created: {tp_order['id']} @ {take_profit:.6f}")
        except Exception as tp_error:
            logger.error(f"❌ TP order failed: {tp_error}")
        
        # Стоп-лосс
        sl_created = False
        
        try:
            sl_order = exchange.create_order(
                symbol=symbol,
                type='stop',
                side=sl_side,
                amount=amount,
                price=stop_loss,
                params={
                    'stopPrice': stop_loss,
                    'reduceOnly': True,
                    'timeInForce': 'GTC'
                }
            )
            if sl_order and 'id' in sl_order:
                order_ids.append(sl_order['id'])
                logger.info(f"✅ SL stop order created: {sl_order['id']}")
                sl_created = True
        except Exception as sl_error:
            logger.warning(f"⚠️ SL stop order failed: {sl_error}")
        
        if not sl_created:
            try:
                logger.info("🔄 Trying conditional order for SL...")
                sl_conditional = exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=sl_side,
                    amount=amount,
                    price=stop_loss,
                    params={
                        'stopPrice': stop_loss,
                        'reduceOnly': True,
                        'timeInForce': 'GTC',
                        'triggerBy': 'LastPrice'
                    }
                )
                if sl_conditional and 'id' in sl_conditional:
                    order_ids.append(sl_conditional['id'])
                    logger.info(f"✅ SL conditional order created: {sl_conditional['id']}")
                    sl_created = True
            except Exception as conditional_error:
                logger.warning(f"⚠️ SL conditional order failed: {conditional_error}")
        
        if not sl_created:
            try:
                logger.warning("⚠️ Creating SL as market order...")
                sl_market = exchange.create_order(
                    symbol=symbol,
                    type='market',
                    side=sl_side,
                    amount=amount,
                    params={
                        'stopPrice': stop_loss,
                        'reduceOnly': False
                    }
                )
                if sl_market and 'id' in sl_market:
                    order_ids.append(sl_market['id'])
                    logger.warning(f"⚠️ SL market order created: {sl_market['id']}")
                    sl_created = True
                    safe_send(f"⚠️ <b>ВНИМАНИЕ:</b> SL для {symbol} создан как рыночный ордер!")
            except Exception as market_error:
                logger.error(f"❌ SL market order failed: {market_error}")
        
        if len(order_ids) == 2:
            logger.info("✅ Both SL and TP orders created successfully")
        elif len(order_ids) == 1:
            if sl_created:
                logger.warning("⚠️ ONLY SL CREATED - TP ORDER FAILED!")
                safe_send(f"⚠️ <b>ВНИМАНИЕ:</b> Для {symbol} создан только SL ордер!")
            else:
                logger.warning("⚠️ ONLY TP CREATED - SL ORDER FAILED!")
                safe_send(f"⚠️ <b>ВНИМАНИЕ:</b> Для {symbol} создан только TP ордер!")
        else:
            logger.error("❌ NO ORDERS CREATED!")
            safe_send(f"❌ <b>ОШИБКА:</b> Для {symbol} не созданы ордера SL/TP!")
        
        logger.info(f"📊 Orders summary: {len(order_ids)} created")
        return len(order_ids) > 0, order_ids
        
    except Exception as e:
        logger.error(f"❌ Exchange SL/TP creation failed: {e}")
        return False, []

def cmd_create_missing_orders(update, context):
    """Создание отсутствующих ордеров для открытых позиций"""
    try:
        positions = get_open_positions()
        created_count = 0
        
        for symbol, position in positions.items():
            if not position.get('exchange_order_ids'):
                logger.info(f"🔄 Creating missing orders for {symbol}")
                
                settings = get_current_settings()
                if settings.get('use_exchange_orders', True):
                    success, order_ids = create_exchange_stop_orders(
                        symbol, 
                        position['position_type'],
                        position['stop_loss'],
                        position['take_profit'],
                        position['base_amount']
                    )
                    if success:
                        order_ids_str = ','.join(order_ids)
                        db.execute(
                            "UPDATE positions SET exchange_order_ids=? WHERE symbol=? AND status='OPEN'",
                            (order_ids_str, symbol)
                        )
                        created_count += 1
                        logger.info(f"✅ Created orders for {symbol}: {order_ids}")
        
        update.message.reply_text(f"✅ Созданы ордера для {created_count} позиций")
        
    except Exception as e:
        logger.error(f"❌ Create missing orders error: {e}")
        update.message.reply_text(f"❌ Ошибка создания ордеров: {str(e)}")

def cancel_exchange_orders(symbol: str):
    """Отмена всех ордеров для символа"""
    try:
        if DRY_RUN:
            logger.info(f"🔶 DRY RUN: Would cancel orders for {symbol}")
            return True
            
        orders = exchange.fetch_open_orders(symbol)
        cancelled_count = 0
        
        for order in orders:
            try:
                exchange.cancel_order(order['id'], symbol)
                cancelled_count += 1
                logger.info(f"✅ Cancelled order {order['id']} for {symbol}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to cancel order {order['id']}: {e}")
        
        logger.info(f"✅ Cancelled {cancelled_count} orders for {symbol}")
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ Order cancellation failed: {e}")
        return False

def update_exchange_trailing_stop(symbol: str, new_stop_loss: float, amount: float, position_type: str):
    """Обновление трейлинг-стопа на бирже"""
    try:
        cancel_exchange_orders(symbol)
        
        position = get_open_positions().get(symbol)
        if not position:
            return False
            
        take_profit = position['take_profit']
        
        success, order_ids = create_exchange_stop_orders(
            symbol, position_type, new_stop_loss, take_profit, amount
        )
        
        if success:
            order_ids_str = ','.join(order_ids)
            db.execute(
                "UPDATE positions SET exchange_order_ids=? WHERE symbol=? AND status='OPEN'",
                (order_ids_str, symbol)
            )
            logger.info(f"✅ Exchange trailing stop updated: {new_stop_loss:.6f}")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Exchange trailing stop update failed: {e}")
        return False

# ====== СИНХРОНИЗАЦИЯ ПОЗИЦИЙ ======
def sync_positions_with_exchange():
    """Синхронизация позиций с биржей"""
    try:
        logger.info("🔄 Starting position synchronization...")

        exchange_positions = exchange.fetch_positions()
        active_exchange = {
            p['symbol']: p for p in exchange_positions
            if p.get('contracts') and safe_float_convert(p['contracts']) > 0
        }

        local_positions = db.fetchall("SELECT symbol FROM positions WHERE status='OPEN'")
        local_symbols = {row[0] for row in local_positions} if local_positions else set()

        missing_on_exchange = local_symbols - active_exchange.keys()
        missing_in_db = active_exchange.keys() - local_symbols

        for symbol in missing_on_exchange:
            logger.warning(f"⚠️ Position {symbol} not found on exchange — marking as closed")
            db.execute("""
                UPDATE positions 
                SET status='CLOSED', close_time=datetime('now'), exit_reason='SYNC_CLOSE'
                WHERE symbol=? AND status='OPEN'
            """, (symbol,))

        for symbol in missing_in_db:
            p = active_exchange[symbol]
            contracts = safe_float_convert(p.get('contracts', 0))
            side = p.get('side', 'long').upper()
            entry_price = safe_float_convert(p.get('entryPrice', 0))
            leverage = int(safe_float_convert(p.get('leverage', 1)))
            current_price = get_current_price(symbol) or entry_price
            
            logger.warning(f"⚠️ Found position on exchange not in DB: {symbol} {contracts} {side}")
            
            settings = get_current_settings()
            stop_loss, take_profit = calculate_safe_sl_tp(entry_price, side, settings)
            
            db.execute("""
                INSERT INTO positions (
                    symbol, base_amount, open_price, stop_loss, take_profit,
                    max_price, min_price, open_time, status, position_type, leverage,
                    trading_mode, strategy, entry_reason, open_timestamp,
                    original_stop_loss, invested_usdt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, contracts, entry_price, stop_loss, take_profit,
                entry_price, entry_price, side, leverage,
                CURRENT_MODE, get_current_settings()['strategy'], "EXTERNAL_OPEN", int(time.time()),
                stop_loss, contracts * entry_price / leverage
            ))

        db.get_connection()[0].commit()
        logger.info(f"✅ Sync complete: {len(missing_on_exchange)} closed, {len(missing_in_db)} added")
        
    except Exception as e:
        logger.error(f"❌ Position sync error: {e}")

# ====== УПРАВЛЕНИЕ ПОЗИЦИЯМИ ======
def get_open_positions():
    """Получение открытых позиций с защитой от None значений"""
    try:
        rows = db.fetchall("""
            SELECT symbol, base_amount, open_price, stop_loss, take_profit, max_price, min_price,
                   original_stop_loss, trailing_active, open_timestamp, position_type, leverage,
                   invested_usdt, exchange_order_ids, entry_type
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
                "entry_type": row[14] or "MARKET"
            }
        
        logger.info(f"📊 Found {len(positions)} open positions")
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
        row = db.fetchone("SELECT last_closed_ts FROM symbol_cooldown WHERE symbol=?", (symbol,))
        if not row or not row[0]:
            return False
            
        last_closed = row[0]
        cooldown = get_current_settings()['cooldown']
        in_cooldown = (time.time() - last_closed) < cooldown
        
        if in_cooldown:
            remaining = cooldown - (time.time() - last_closed)
            logger.debug(f"⏹️ {symbol} in cooldown, {remaining:.0f}s remaining")
            
        return in_cooldown
        
    except Exception as e:
        logger.error(f"❌ Cooldown check error: {e}")
        return False

# ====== АНАЛИЗ И ТОРГОВЛЯ ======
def get_ohlcv_data(symbol: str, timeframe: str, limit: int):
    """Получение OHLCV данных в DataFrame"""
    ohlcv = fetch_ohlcv(symbol, timeframe, limit)
    if not ohlcv:
        return None
        
    try:
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        # Безопасное преобразование типов
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].apply(lambda x: safe_float_convert(x, 0.0))
        return df
    except Exception as e:
        logger.error(f"❌ Dataframe creation error: {e}")
        return None

def analyze_symbol(symbol: str):
    """Анализ символа для торговли"""
    try:
        settings = get_current_settings()
        
        df = get_ohlcv_data(symbol, settings['timeframe_entry'], 100)
        if df is None or len(df) < 50:
            return None

        current_price = df['close'].iloc[-1]
        if current_price <= 0:
            logger.error(f"❌ Invalid current price for {symbol}: {current_price}")
            return None
        
        # Безопасный расчет индикаторов
        try:
            rsi = RSIIndicator(df['close'], window=14).rsi().iloc[-1]
            current_volume = df['volume'].iloc[-1]
            volume_sma = df['volume'].tail(20).mean()
            volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1

            macd = MACD(df['close'])
            macd_line = macd.macd().iloc[-1]
            macd_signal = macd.macd_signal().iloc[-1]

            bb = BollingerBands(df['close'], window=20, window_dev=2)
            bb_upper = bb.bollinger_hband().iloc[-1]
            bb_lower = bb.bollinger_lband().iloc[-1]
            bb_middle = bb.bollinger_mavg().iloc[-1]
            bb_width = (bb_upper - bb_lower) / bb_middle if bb_middle != 0 else 0

            price_position = (current_price - bb_lower) / (bb_upper - bb_lower + 1e-9)

            macd_bullish = macd_line > macd_signal
            ema_9 = df['close'].ewm(span=9).mean().iloc[-1]
            ema_21 = df['close'].ewm(span=21).mean().iloc[-1]
            trend_bullish = ema_9 > ema_21

            if trend_bullish and macd_bullish:
                position_type = "LONG"
                rsi_range = settings['rsi_range_long']
            else:
                position_type = "SHORT"  
                rsi_range = settings['rsi_range_short']
                
            min_score = settings['min_score']

            score = 0
            reasons = []

            if volume_ratio >= settings['volume_multiplier']:
                score += 25
                reasons.append("HIGH_VOLUME")
            elif volume_ratio >= settings['volume_multiplier'] * 0.8:
                score += 15
                reasons.append("MEDIUM_VOLUME")

            rsi_min, rsi_max = rsi_range
            if rsi_min <= rsi <= rsi_max:
                score += 20
                reasons.append("GOOD_RSI")

            if macd_bullish and position_type == "LONG" or (not macd_bullish and position_type == "SHORT"):
                score += 20
                reasons.append(f"{position_type}_MACD")

            if position_type == "LONG":
                if 0.1 <= price_position <= 0.5:
                    score += 25
                    reasons.append("GOOD_LONG_POSITION")
                elif 0.5 < price_position <= 0.8:
                    score += 15
                    reasons.append("OK_LONG_POSITION")
            else:
                if 0.5 <= price_position <= 0.9:
                    score += 25
                    reasons.append("GOOD_SHORT_POSITION")
                elif 0.2 <= price_position < 0.5:
                    score += 15
                    reasons.append("OK_SHORT_POSITION")

            if bb_width > 0.01:
                score += 10
                reasons.append("GOOD_VOLATILITY")

            logger.info(f"🔍 {symbol} {position_type}: Score={score}, RSI={rsi:.1f}, Vol={volume_ratio:.2f}")

            if score >= min_score:
                logger.info(f"🎯 {position_type} Signal found: {symbol} (Score: {score})")
                return {
                    "symbol": symbol,
                    "price": current_price,
                    "score": score,
                    "reasons": reasons,
                    "volume_ratio": volume_ratio,
                    "rsi": rsi,
                    "bb_width": bb_width,
                    "signal_type": position_type
                }
            else:
                logger.debug(f"⏹️ {symbol} filtered: Score {score} < {min_score}")
                return None

        except Exception as indicator_error:
            logger.error(f"❌ Indicator calculation error for {symbol}: {indicator_error}")
            return None

    except Exception as e:
        logger.error(f"Error in analyze_symbol({symbol}): {e}")
        return None

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
    
    for symbol in active_symbols:
        if not BOT_RUNNING:
            break
            
        if not can_open_new_trade():
            logger.info("⏹️ Max trades reached, stopping scan")
            break
            
        if is_in_cooldown(symbol):
            continue
            
        signal = analyze_symbol(symbol)
        if signal:
            signals.append(signal)
            
    if signals and BOT_RUNNING:
        signals.sort(key=lambda x: x['score'], reverse=True)
        best_signal = signals[0]
        
        logger.info(f"🎯 BEST {best_signal['signal_type']} SIGNAL: {best_signal['symbol']} (Score: {best_signal['score']})")
        
        if open_trade_position(best_signal):
            logger.info(f"✅ {best_signal['signal_type']} Trade opened: {best_signal['symbol']}")
        else:
            logger.warning(f"❌ Failed to open {best_signal['signal_type']} trade: {best_signal['symbol']}")
    else:
        logger.info("📭 No valid signals found")
        
    logger.info(f"✅ Scan complete: {len(signals)} signals found")

def calculate_safe_sl_tp(entry_price: float, position_type: str, settings: Dict):
    """Расчет безопасных SL/TP с защитой от шума"""
    try:
        volatility = get_symbol_volatility(entry_price)
        min_distance = max(volatility * 1.5, entry_price * 0.005)
        
        if position_type == 'LONG':
            stop_loss = entry_price * (1 - settings['max_stop_loss'])
            take_profit = entry_price * (1 + settings['take_profit'])
            
            if take_profit - stop_loss < min_distance:
                take_profit = entry_price + min_distance * 0.7
                stop_loss = entry_price - min_distance * 0.3
                
        else:
            stop_loss = entry_price * (1 + settings['max_stop_loss'])
            take_profit = entry_price * (1 - settings['take_profit'])
            
            if stop_loss - take_profit < min_distance:
                stop_loss = entry_price + min_distance * 0.7
                take_profit = entry_price - min_distance * 0.3
        
        logger.info(f"📊 {position_type} SL/TP: Entry={entry_price:.6f}, SL={stop_loss:.6f}, TP={take_profit:.6f}, Distance={(abs(take_profit-stop_loss)/entry_price*100):.2f}%")
        return stop_loss, take_profit
        
    except Exception as e:
        logger.error(f"❌ SL/TP calculation error: {e}")
        if position_type == 'LONG':
            return entry_price * 0.98, entry_price * 1.02
        else:
            return entry_price * 1.02, entry_price * 0.98

def get_symbol_volatility(current_price: float, period: int = 20):
    """Расчет волатильности символа"""
    try:
        if current_price > 1000:
            return current_price * 0.002
        elif current_price > 100:
            return current_price * 0.003
        elif current_price > 10:
            return current_price * 0.004
        else:
            return current_price * 0.005
    except:
        return current_price * 0.003

def update_trailing_stop(symbol: str, position: Dict, current_price: float):
    """Обновление трейлинг-стопа с реальными ордерами"""
    try:
        settings = get_current_settings()
        position_type = position.get('position_type', 'LONG')
        
        if position_type == 'LONG':
            current_profit_pct = (current_price - position['open_price']) / position['open_price']
            
            if current_price > position['max_price']:
                db.execute("UPDATE positions SET max_price=? WHERE symbol=? AND status='OPEN'", 
                          (current_price, symbol))
                position['max_price'] = current_price
            
            if not position['trailing_active'] and current_profit_pct >= settings['trailing_stop_activation']:
                new_stop_loss = current_price * (1 - settings['trailing_stop_distance'])
                
                if settings.get('use_exchange_orders', True):
                    update_exchange_trailing_stop(symbol, new_stop_loss, position['base_amount'], position_type)
                
                db.execute("UPDATE positions SET trailing_active=1, stop_loss=? WHERE symbol=? AND status='OPEN'", 
                          (new_stop_loss, symbol))
                logger.info(f"🎯 Trailing STOP activated for LONG {symbol}: {new_stop_loss:.6f}")
                return new_stop_loss
                    
            elif position['trailing_active']:
                new_stop_loss = current_price * (1 - settings['trailing_stop_distance'])
                current_stop_loss = position['stop_loss']
                
                if new_stop_loss > current_stop_loss:
                    if settings.get('use_exchange_orders', True):
                        update_exchange_trailing_stop(symbol, new_stop_loss, position['base_amount'], position_type)
                    
                    db.execute("UPDATE positions SET stop_loss=? WHERE symbol=? AND status='OPEN'", 
                              (new_stop_loss, symbol))
                    logger.info(f"📈 Trailing STOP updated for LONG {symbol}: {current_stop_loss:.6f} -> {new_stop_loss:.6f}")
                    return new_stop_loss
                    
        else:
            current_profit_pct = (position['open_price'] - current_price) / position['open_price']
            
            if current_price < position['min_price']:
                db.execute("UPDATE positions SET min_price=? WHERE symbol=? AND status='OPEN'", 
                          (current_price, symbol))
                position['min_price'] = current_price
            
            if not position['trailing_active'] and current_profit_pct >= settings['trailing_stop_activation']:
                new_stop_loss = current_price * (1 + settings['trailing_stop_distance'])
                
                if settings.get('use_exchange_orders', True):
                    update_exchange_trailing_stop(symbol, new_stop_loss, position['base_amount'], position_type)
                
                db.execute("UPDATE positions SET trailing_active=1, stop_loss=? WHERE symbol=? AND status='OPEN'", 
                          (new_stop_loss, symbol))
                logger.info(f"🎯 Trailing STOP activated for SHORT {symbol}: {new_stop_loss:.6f}")
                return new_stop_loss
                    
            elif position['trailing_active']:
                new_stop_loss = current_price * (1 + settings['trailing_stop_distance'])
                current_stop_loss = position['stop_loss']
                
                if new_stop_loss < current_stop_loss:
                    if settings.get('use_exchange_orders', True):
                        update_exchange_trailing_stop(symbol, new_stop_loss, position['base_amount'], position_type)
                    
                    db.execute("UPDATE positions SET stop_loss=? WHERE symbol=? AND status='OPEN'", 
                              (new_stop_loss, symbol))
                    logger.info(f"📈 Trailing STOP updated for SHORT {symbol}: {current_stop_loss:.6f} -> {new_stop_loss:.6f}")
                    return new_stop_loss
                
        return position['stop_loss']
        
    except Exception as e:
        logger.error(f"❌ Trailing stop update error for {symbol}: {e}")
        return position['stop_loss']

def should_close_position(symbol: str, position: Dict, current_price: float):
    """Проверка условий закрытия позиции"""
    try:
        position_type = position.get('position_type', 'LONG')
        stop_loss = position.get('stop_loss', 0)
        take_profit = position.get('take_profit', 0)
        open_timestamp = position.get('open_timestamp', 0)
        
        settings = get_current_settings()
        position_age = time.time() - open_timestamp
        if position_age > settings['max_position_time']:
            current_pnl = calculate_pnl_percent(
                position['open_price'], current_price, position_type, position.get('leverage', 1)
            )
            if current_pnl > 0:
                return "TIMEOUT_PROFIT"
            else:
                return "TIMEOUT_LOSS"
        
        if position_type == 'LONG':
            if current_price <= stop_loss:
                return "STOP_LOSS"
            elif current_price >= take_profit:
                return "TAKE_PROFIT"
        else:
            if current_price >= stop_loss:
                return "STOP_LOSS" 
            elif current_price <= take_profit:
                return "TAKE_PROFIT"
                
        return None
        
    except Exception as e:
        logger.error(f"❌ Should close check error for {symbol}: {e}")
        return None

def check_position_exits():
    """Проверка условий выхода из позиций"""
    if not BOT_RUNNING:
        return
        
    try:
        positions = get_open_positions()
        
        for symbol, position in positions.items():
            try:
                current_price = get_current_price(symbol)
                if not current_price:
                    logger.warning(f"⚠️ Cannot get current price for {symbol}")
                    continue
                
                settings = get_current_settings()
                position_age = time.time() - position['open_timestamp']
                
                if position_age > settings['max_position_time']:
                    logger.warning(f"⏰ FORCE TIMEOUT: {symbol} age {position_age/60:.1f}m > {settings['max_position_time']/60:.1f}m")
                    safe_close_position(symbol, "FORCE_TIMEOUT")
                    continue

                if not position.get('stop_loss') or not position.get('take_profit'):
                    logger.warning(f"⚠️ Missing SL/TP for {symbol}, recalculating...")
                    settings = get_current_settings()
                    new_sl, new_tp = calculate_safe_sl_tp(
                        position['open_price'], 
                        position.get('position_type', 'LONG'), 
                        settings
                    )
                    db.execute(
                        "UPDATE positions SET stop_loss=?, take_profit=?, original_stop_loss=? WHERE symbol=? AND status='OPEN'",
                        (new_sl, new_tp, new_sl, symbol)
                    )
                    position['stop_loss'] = new_sl
                    position['take_profit'] = new_tp
                    logger.info(f"✅ Recalculated SL/TP for {symbol}: SL={new_sl:.6f}, TP={new_tp:.6f}")
                
                update_trailing_stop(symbol, position, current_price)
                
                close_reason = should_close_position(symbol, position, current_price)
                if close_reason:
                    logger.info(f"🔴 {close_reason} triggered for {symbol}")
                    safe_close_position(symbol, close_reason)
                    
            except Exception as e:
                logger.error(f"❌ Exit check error for {symbol}: {e}")
                
    except Exception as e:
        logger.error(f"❌ Global exit check error: {e}")

def safe_close_position(symbol: str, reason: str):
    """Безопасное закрытие позиции"""
    try:
        position_row = db.fetchone("""
            SELECT base_amount, open_price, position_type, leverage, invested_usdt, entry_type
            FROM positions WHERE symbol=? AND status='OPEN'
        """, (symbol,))
        
        if not position_row:
            logger.error(f"❌ No open position found for {symbol}")
            return False
            
        base_amount, open_price, position_type, leverage, invested_usdt, entry_type = position_row
        
        current_price = get_current_price(symbol)
        if not current_price:
            return False
            
        cancel_exchange_orders(symbol)
            
        close_amount = adjust_amount_to_precision(symbol, base_amount)
        
        if close_amount <= 0:
            mark_position_closed(symbol, "ZERO_BALANCE")
            return True
            
        current_pnl_percent = calculate_pnl_percent(open_price, current_price, position_type, leverage)
        
        safe_send(f"🔴 <b>CLOSING: {symbol} {position_type}</b>\nПричина: {reason}\nТекущий PnL: {current_pnl_percent:+.2f}%")
        
        settings = get_current_settings()
        exit_type = "MARKET"
        
        if DRY_RUN:
            logger.info(f"🔶 DRY RUN: Would close {position_type} {symbol}")
            if settings.get('use_market_exit', False):
                exit_type = "MARKET"
                exit_fee_rate = TAKER_FEE
            else:
                exit_type = "LIMIT" 
                exit_fee_rate = MAKER_FEE
                
            if reason == "TAKE_PROFIT":
                close_price = open_price * (1 + settings['take_profit']) if position_type == 'LONG' else open_price * (1 - settings['take_profit'])
            else:
                close_price = current_price
                
            net_pnl, net_pnl_percent, total_fee = calculate_real_pnl_with_commission(
                open_price, close_price, close_amount, position_type, leverage, 
                invested_usdt, symbol, entry_type, exit_type
            )
            
            record_successful_close(symbol, close_amount, close_price, reason, position_type, leverage, invested_usdt, exit_type)
            return True
        else:
            filled_price = current_price
            
            if settings.get('use_market_exit', False):
                if position_type == 'LONG':
                    order = exchange.create_market_sell_order(symbol, close_amount)
                else:
                    order = exchange.create_market_buy_order(symbol, close_amount)
                exit_type = "MARKET"
            else:
                if position_type == 'LONG':
                    order = exchange.create_order(
                        symbol=symbol,
                        type='limit',
                        side='sell',
                        amount=close_amount,
                        price=current_price,
                        params={'reduceOnly': True, 'timeInForce': 'GTC'}
                    )
                else:
                    order = exchange.create_order(
                        symbol=symbol,
                        type='limit', 
                        side='buy',
                        amount=close_amount,
                        price=current_price,
                        params={'reduceOnly': True, 'timeInForce': 'GTC'}
                    )
                exit_type = "LIMIT"
                
                if order and order.get('id'):
                    order_filled = wait_for_limit_order_fill(symbol, order['id'], settings.get('limit_order_timeout', 60))
                    if not order_filled:
                        logger.warning(f"⏹️ Limit exit order not filled within timeout, using market order")
                        try:
                            exchange.cancel_order(order['id'], symbol)
                        except:
                            pass
                        
                        if position_type == 'LONG':
                            order = exchange.create_market_sell_order(symbol, close_amount)
                        else:
                            order = exchange.create_market_buy_order(symbol, close_amount)
                        exit_type = "MARKET"
            
            if order and order.get('id'):
                if order.get('trades'):
                    trades = order['trades']
                    filled_prices = []
                    for trade in trades:
                        price = safe_float_convert(trade.get('price'))
                        if price and price > 0:
                            filled_prices.append(price)
                    
                    if filled_prices:
                        filled_price = sum(filled_prices) / len(filled_prices)
                    else:
                        filled_price = safe_float_convert(order.get('price', current_price))
                else:
                    filled_price = safe_float_convert(order.get('price', current_price))
                
                if not filled_price or filled_price <= 0:
                    logger.error(f"❌ Invalid filled price for {symbol}: {filled_price}")
                    filled_price = current_price
                
                db.execute("INSERT OR REPLACE INTO symbol_cooldown (symbol, last_closed_ts) VALUES (?, ?)", 
                          (symbol, int(time.time())))
                
                record_successful_close(symbol, close_amount, filled_price, reason, position_type, leverage, invested_usdt, exit_type)
                
                net_pnl, net_pnl_percent, total_fee = calculate_real_pnl_with_commission(
                    open_price, filled_price, close_amount, position_type, leverage, invested_usdt, symbol, entry_type, exit_type
                )

                emoji = "🟢" if net_pnl > 0 else "🔴"
                
                if exit_type == "MARKET":
                    exit_fee_pct = TAKER_FEE * 100
                else:
                    exit_fee_pct = MAKER_FEE * 100
                
                safe_send(
                    f"{emoji} <b>CLOSED: {symbol} {position_type}</b>\n"
                    f"Цена: {filled_price:.6f}\n"
                    f"Причина: {reason}\n"
                    f"Тип выхода: {'⚡ MARKET' if exit_type == 'MARKET' else '💎 LIMIT'}\n"
                    f"Комиссия выхода: {exit_fee_pct:.3f}%\n"
                    f"PnL: {net_pnl:+.2f} USDT ({net_pnl_percent:+.2f}%)\n"
                    f"Комиссии: {total_fee:.4f} USDT\n"
                    f"Плечо: {leverage}x"
                )
                return True
            else:
                logger.error(f"❌ Order creation failed for {symbol}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Close {position_type} error for {symbol}: {e}")
        
        if "reduce-only" in str(e).lower():
            logger.error(f"❌ Reduce-only error detected, retrying without reduce-only...")
            try:
                if position_type == 'LONG':
                    order = exchange.create_market_sell_order(symbol, close_amount)
                else:
                    order = exchange.create_market_buy_order(symbol, close_amount)
                
                if order and order.get('id'):
                    logger.info(f"✅ Position closed successfully without reduce-only")
                    return True
            except Exception as retry_error:
                logger.error(f"❌ Retry also failed: {retry_error}")
        
        safe_send(f"❌ <b>Close failed:</b> {symbol} {position_type}\n{str(e)}")
        return False

def check_minimum_balance():
    """Проверка минимального баланса для торговли"""
    available_usdt = compute_available_usdt()
    min_required = 5.0
    
    if available_usdt < min_required:
        logger.warning(f"🚨 CRITICAL: Low balance {available_usdt:.2f} < {min_required} USDT")
        safe_send(f"🚨 <b>КРИТИЧЕСКИЙ БАЛАНС</b>\n{available_usdt:.2f} USDT\nМинимум: {min_required} USDT\nПОПОЛНИТЕ БАЛАНС!")
        return False
    return True

def can_open_new_trade():
    """Проверка возможности открытия новой сделки"""
    if not check_minimum_balance():
        return False
        
    settings = get_current_settings()
    current_trades = get_concurrent_trades_count()
    can_open = current_trades < settings['max_trades']
    
    if not can_open:
        logger.info(f"⏹️ Max trades reached: {current_trades}/{settings['max_trades']}")
    
    return can_open

def mark_position_closed(symbol: str, reason: str):
    """Пометка позиции как закрытой"""
    db.execute("UPDATE positions SET status='CLOSED', close_time=datetime('now'), exit_reason=? WHERE symbol=? AND status='OPEN'", 
               (reason, symbol))

def record_open_position(symbol: str, base_amount: float, open_price: float, stop_loss: float, 
                        take_profit: float, position_type: str = 'LONG', leverage: int = 1, 
                        invested_usdt: float = 0, exchange_order_ids: List[str] = None, 
                        entry_type: str = "MARKET"):
    """Запись открытой позиции"""
    try:
        contract_size = get_symbol_info(symbol).get('contract_size', 1)
        position_value = base_amount * open_price * contract_size
        
        if entry_type == "MARKET":
            fee_rate = TAKER_FEE
        else:
            fee_rate = MAKER_FEE
            
        fee = position_value * fee_rate
        
        order_ids_str = ','.join(exchange_order_ids) if exchange_order_ids else ''
        
        db.execute("""
            INSERT INTO positions (
                symbol, trading_mode, strategy, base_amount, open_price, stop_loss, take_profit,
                max_price, min_price, open_time, fee_paid, original_stop_loss, open_timestamp, 
                position_type, leverage, invested_usdt, exchange_order_ids, entry_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, CURRENT_MODE, get_current_settings()['strategy'], base_amount, open_price, 
            stop_loss, take_profit, open_price, open_price, fee, stop_loss, 
            int(time.time()), position_type, leverage, invested_usdt, order_ids_str, entry_type
        ))
        
        action = 'BUY' if position_type == 'LONG' else 'SELL'
        db.execute("""
            INSERT INTO trade_history (
                symbol, action, price, usdt_amount, base_amount, fee, time, timestamp, 
                trading_mode, strategy, position_type, leverage, entry_type
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?)
        """, (
            symbol, action, open_price, invested_usdt, base_amount, fee, 
            int(time.time()), CURRENT_MODE, get_current_settings()['strategy'], position_type, leverage, entry_type
        ))
        
        logger.info(f"✅ OPEN {position_type} ({entry_type}): {symbol} {base_amount:.6f} @ {open_price:.6f}, Fee: {fee:.4f} USDT")
        return True
    except Exception as e:
        logger.error(f"❌ Position record error: {e}")
        return False

def record_successful_close(symbol: str, amount: float, price: float, reason: str, 
                          position_type: str = 'LONG', leverage: int = 1, invested_usdt: float = 0,
                          exit_type: str = "MARKET"):
    """Запись успешного закрытия позиции"""
    try:
        row = db.fetchone("SELECT open_price, open_timestamp, entry_type FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
        if not row:
            return
            
        open_price, open_timestamp, entry_type = row
        
        net_pnl, net_pnl_percent, total_fee = calculate_real_pnl_with_commission(
            open_price, price, amount, position_type, leverage, invested_usdt, symbol, entry_type, exit_type
        )
        duration = int(time.time()) - open_timestamp
        
        db.execute("""
            UPDATE positions SET status='CLOSED', close_time=datetime('now'), close_price=?, 
            pnl=?, pnl_percent=?, exit_reason=?, duration_seconds=?, exit_type=?
            WHERE symbol=? AND status='OPEN'
        """, (price, net_pnl, net_pnl_percent, reason, duration, exit_type, symbol))
        
        contract_size = get_symbol_info(symbol).get('contract_size', 1)
        usdt_amount = amount * price * contract_size
        
        if exit_type == "MARKET":
            fee_rate = TAKER_FEE
        else:
            fee_rate = MAKER_FEE
        fee = usdt_amount * fee_rate
        
        action = 'SELL' if position_type == 'LONG' else 'BUY'
        db.execute("""
            INSERT INTO trade_history (
                symbol, action, price, usdt_amount, base_amount, fee, time, timestamp, 
                trading_mode, strategy, position_type, leverage, exit_type
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?)
        """, (
            symbol, action, price, usdt_amount, amount, fee, 
            int(time.time()), CURRENT_MODE, get_current_settings()['strategy'], position_type, leverage, exit_type
        ))
        
        logger.info(f"✅ CLOSED {position_type} ({exit_type}): {symbol} {amount:.6f} @ {price:.6f}, PnL: {net_pnl:+.2f} USDT ({net_pnl_percent:+.2f}%), Fees: {total_fee:.4f} USDT")
        
    except Exception as e:
        logger.error(f"❌ Record close error: {e}")

# ====== TELEGRAM ИНТЕРФЕЙС ======
def get_main_keyboard():
    keyboard = [
        [KeyboardButton("🟢 АГРЕССИВНЫЙ"), KeyboardButton("🟡 КОНСЕРВАТИВНЫЙ")],
        [KeyboardButton("🔴 СКАЛЬПИНГ"), KeyboardButton("📊 СТАТУС")],
        [KeyboardButton("📈 ПОЗИЦИИ"), KeyboardButton("🔄 СКАНИРОВАТЬ")],
        [KeyboardButton("🔄 СИНХРОНИЗАЦИЯ"), KeyboardButton("⏸️ ПАУЗА")],
        [KeyboardButton("❌ ОТМЕНА ОРДЕРОВ"), KeyboardButton("🔄 РАСЧЕТ SL/TP")],
        [KeyboardButton("🛡️ ИСПРАВИТЬ ОРДЕРА"), KeyboardButton("📊 СТАТИСТИКА")],
        [KeyboardButton("💰 КОМИССИИ"), KeyboardButton("💎 MAKER ВХОД")],
        [KeyboardButton("⚡ MARKET ВХОД"), KeyboardButton("💎 MAKER ВЫХОД")],
        [KeyboardButton("⚡ MARKET ВЫХОД"), KeyboardButton("🎯 ФИЛЬТР")],
        [KeyboardButton("⏹️ СТОП БОТ")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_positions_keyboard():
    """Клавиатура для управления позициями"""
    positions = get_open_positions()
    keyboard = []
    
    for symbol, position in positions.items():
        current_price = get_current_price(symbol)
        if current_price:
            pnl_percent = calculate_pnl_percent(
                position['open_price'], current_price, 
                position['position_type'], position.get('leverage', 1)
            )
            pnl_text = f"+{pnl_percent:.1f}%" if pnl_percent > 0 else f"{pnl_percent:.1f}%"
        else:
            pnl_text = "N/A"
            
        position_type = position.get('position_type', 'LONG')
        emoji = "📈" if position_type == 'LONG' else "📉"
        
        keyboard.append([
            InlineKeyboardButton(
                f"{emoji} ЗАКРЫТЬ {symbol} {pnl_text}", 
                callback_data=f"close_{symbol}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("🔄 ОБНОВИТЬ", callback_data="refresh_positions")])
    
    return InlineKeyboardMarkup(keyboard)

def start(update, context):
    balance = compute_available_usdt()
    settings = get_current_settings()
    
    status = "🟢 АКТИВЕН" if BOT_RUNNING else "⏸️ НА ПАУЗЕ"
    order_type = "🔰 РЕАЛЬНЫЕ ОРДЕРА" if settings.get('use_exchange_orders') else "💻 ПРОГРАММНЫЕ SL/TP"
    entry_type = "⚡ MARKET" if settings.get('use_market_entry', False) else "💎 LIMIT (MAKER)"
    exit_type = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT (MAKER)"
    
    welcome_msg = f"""
🤖 <b>ULTIMATE TRADING BOT v5.0</b>
🎯 <b>Исправлены ошибки и улучшена стабильность</b>

💰 <b>Баланс:</b> {balance:.2f} USDT
🎯 <b>Режим:</b> {settings['name']}
📊 <b>Плечо:</b> {settings['leverage']}x
🔰 <b>Статус:</b> {status}
💎 <b>Вход:</b> {entry_type}
💎 <b>Выход:</b> {exit_type}
🛡️ <b>Защита:</b> {order_type}

<b>Улучшения v5.0:</b>
• ✅ Исправлены ошибки NoneType в ценах
• ✅ Улучшена обработка ошибок API
• ✅ Безопасное преобразование типов данных
• ✅ Валидация всех входных параметров
• ✅ Улучшенные логи и отладка

<b>Основные команды:</b>
• 📈 ПОЗИЦИИ - Управление с кнопками закрытия
• 💰 КОМИССИИ - Настройки оптимизации комиссий  
• 🔄 СКАНИРОВАТЬ - Поиск сигналов
• ⏸️ ПАУЗА / ▶️ /resume - Управление работой
• ⏹️ СТОП БОТ - Безопасная остановка
"""
    update.message.reply_text(welcome_msg, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

def handle_message(update, context):
    text = update.message.text
    global CURRENT_MODE

    if text == "🟢 АГРЕССИВНЫЙ":
        CURRENT_MODE = "AGGRESSIVE"
        switch_mode(update)
    elif text == "🟡 КОНСЕРВАТИВНЫЙ":
        CURRENT_MODE = "CONSERVATIVE" 
        switch_mode(update)
    elif text == "🔴 СКАЛЬПИНГ":
        CURRENT_MODE = "SCALPING"
        switch_mode(update)
    elif text == "📊 СТАТУС":
        cmd_status(update, context)
    elif text == "📈 ПОЗИЦИИ":
        cmd_positions(update, context)
    elif text == "🔄 СКАНИРОВАТЬ":
        cmd_scan(update, context)
    elif text == "🔄 СИНХРОНИЗАЦИЯ":
        cmd_sync(update, context)
    elif text == "⏸️ ПАУЗА":
        cmd_pause(update, context)
    elif text == "❌ ОТМЕНА ОРДЕРОВ":
        cmd_cancel_orders(update, context)
    elif text == "🔄 РАСЧЕТ SL/TP":
        cmd_recalculate_sltp(update, context)
    elif text == "🛡️ ИСПРАВИТЬ ОРДЕРА":
        cmd_fix_orders(update, context)
    elif text == "📊 СТАТИСТИКА":
        cmd_stats(update, context)
    elif text == "💰 КОМИССИИ":
        cmd_commission_settings(update, context)
    elif text == "💎 MAKER ВХОД":
        cmd_maker_entries(update, context)
    elif text == "⚡ MARKET ВХОД":
        cmd_market_entries(update, context)
    elif text == "💎 MAKER ВЫХОД":
        cmd_maker_exits(update, context)
    elif text == "⚡ MARKET ВЫХОД":
        cmd_market_exits(update, context)
    elif text == "🎯 ФИЛЬТР":
        cmd_enable_filter(update, context)
    elif text == "⏹️ СТОП БОТ":
        cmd_stop(update, context)

def handle_callback(update, context):
    """Обработка inline кнопок"""
    query = update.callback_query
    query.answer()
    
    data = query.data
    
    if data.startswith("close_"):
        symbol = data.replace("close_", "")
        close_position_manual(update, context, symbol)
    elif data == "refresh_positions":
        cmd_positions(update, context)
    elif data.startswith("confirm_close_"):
        symbol = data.replace("confirm_close_", "")
        confirm_close_position(update, context, symbol)
    elif data == "cancel_close":
        query.edit_message_text("❌ Закрытие отменено")

def close_position_manual(update, context, symbol: str):
    """Ручное закрытие позиции"""
    try:
        positions = get_open_positions()
        if symbol not in positions:
            if update.callback_query:
                update.callback_query.edit_message_text(f"❌ Позиция {symbol} не найдена")
            else:
                update.message.reply_text(f"❌ Позиция {symbol} не найдена")
            return
        
        current_price = get_current_price(symbol)
        if not current_price:
            if update.callback_query:
                update.callback_query.edit_message_text(f"❌ Не удалось получить цену для {symbol}")
            else:
                update.message.reply_text(f"❌ Не удалось получить цену для {symbol}")
            return
            
        position = positions[symbol]
        current_pnl_percent = calculate_pnl_percent(
            position['open_price'], current_price, 
            position['position_type'], position.get('leverage', 1)
        )
        
        keyboard = [
            [
                InlineKeyboardButton("✅ ДА, ЗАКРЫТЬ", callback_data=f"confirm_close_{symbol}"),
                InlineKeyboardButton("❌ ОТМЕНА", callback_data="cancel_close")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            update.callback_query.edit_message_text(
                text=f"🔴 <b>ПОДТВЕРЖДЕНИЕ ЗАКРЫТИЯ</b>\n\n"
                     f"Символ: {symbol}\n"
                     f"Тип: {position['position_type']}\n"
                     f"Контракты: {position['base_amount']:.6f}\n"
                     f"Цена открытия: {position['open_price']:.6f}\n"
                     f"Текущая цена: {current_price:.6f}\n"
                     f"Плечо: {position.get('leverage', 1)}x\n"
                     f"Текущий PnL: {current_pnl_percent:+.2f}%\n\n"
                     f"Закрыть позицию?",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
        else:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"🔴 <b>ПОДТВЕРЖДЕНИЕ ЗАКРЫТИЯ</b>\n\n"
                     f"Символ: {symbol}\n"
                     f"Тип: {position['position_type']}\n"
                     f"Контракты: {position['base_amount']:.6f}\n"
                     f"Цена открытия: {position['open_price']:.6f}\n"
                     f"Текущая цена: {current_price:.6f}\n"
                     f"Плечо: {position.get('leverage', 1)}x\n"
                     f"Текущий PnL: {current_pnl_percent:+.2f}%\n\n"
                     f"Закрыть позицию?",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
        
    except Exception as e:
        logger.error(f"❌ Manual close error: {e}")
        error_msg = f"❌ Ошибка закрытия позиции: {str(e)}"
        if update.callback_query:
            update.callback_query.edit_message_text(error_msg)
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text=error_msg)

def confirm_close_position(update, context, symbol: str):
    """Подтверждение закрытия позиции"""
    try:
        query = update.callback_query
        query.answer()
        
        if safe_close_position(symbol, "MANUAL_CLOSE"):
            query.edit_message_text(
                text=f"✅ <b>ПОЗИЦИЯ ЗАКРЫТА</b>\n\n{symbol} - закрыта вручную",
                parse_mode=ParseMode.HTML
            )
        else:
            query.edit_message_text(
                text=f"❌ <b>ОШИБКА ЗАКРЫТИЯ</b>\n\n{symbol} - не удалось закрыть",
                parse_mode=ParseMode.HTML
            )
            
    except Exception as e:
        logger.error(f"❌ Confirm close error: {e}")
        update.callback_query.edit_message_text(f"❌ Ошибка: {str(e)}")

def cmd_close(update, context):
    """Закрытие позиции по команде"""
    try:
        if not context.args:
            update.message.reply_text("❌ Укажите символ: /close SYMBOL")
            return
            
        symbol = context.args[0].upper()
        if not symbol.endswith(":USDT"):
            symbol += ":USDT"
            
        close_position_manual(update, context, symbol)
        
    except Exception as e:
        logger.error(f"❌ Close command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_cancel_orders(update, context):
    """Отмена всех ордеров на бирже"""
    try:
        update.message.reply_text("🔄 Отмена всех ордеров...")
        
        positions = get_open_positions()
        cancelled_total = 0
        
        for symbol in positions:
            if cancel_exchange_orders(symbol):
                cancelled_total += 1
                
        update.message.reply_text(f"✅ Отменены ордера для {cancelled_total} позиций")
        
    except Exception as e:
        logger.error(f"❌ Cancel orders error: {e}")
        update.message.reply_text(f"❌ Ошибка отмены ордеров: {str(e)}")

def cmd_sync(update, context):
    """Синхронизация позиций"""
    try:
        update.message.reply_text("🔄 Синхронизация с биржей...")
        sync_positions_with_exchange()
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

def cmd_positions(update, context):
    """Показать позиции с кнопками закрытия"""
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
                order_status = "🔰" if position.get('exchange_order_ids') else "💻"
                entry_type_emoji = "⚡" if position.get('entry_type') == "MARKET" else "💎"
                
                sl_display = f"{position['stop_loss']:.6f}" if position.get('stop_loss') else "N/A"
                tp_display = f"{position['take_profit']:.6f}" if position.get('take_profit') else "N/A"
                
                message += (
                    f"{emoji} {trailing_status} {order_status} {entry_type_emoji} <b>{symbol} {position['position_type']}</b>\n"
                    f"   Контракты: {position['base_amount']:.6f}\n"
                    f"   Плечо: {position.get('leverage', 1)}x\n"
                    f"   Открытие: {position['open_price']:.6f}\n"
                    f"   Текущая: {current_price:.6f}\n"
                    f"   PnL: <b>{pnl_percent:+.2f}%</b>\n"
                    f"   Возраст: {int(position_age/60)}m\n"
                    f"   SL: {sl_display}\n"
                    f"   TP: {tp_display}\n\n"
                )
        
        message += f"<b>СУММАРНЫЙ PnL: {total_pnl:+.2f}%</b>\n\n"
        message += "🔰 - реальные ордера, 💻 - программные SL/TP\n"
        message += "⚡ - MARKET вход, 💎 - LIMIT (MAKER) вход"
        
        update.message.reply_text(
            message,
            parse_mode=ParseMode.HTML,
            reply_markup=get_positions_keyboard()
        )
        
    except Exception as e:
        logger.error(f"❌ Positions command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_fix_orders(update, context):
    """Проверка и исправление всех ордеров для открытых позиций"""
    try:
        positions = get_open_positions()
        fixed_count = 0
        
        for symbol, position in positions.items():
            try:
                current_price = get_current_price(symbol)
                if not current_price:
                    continue
                
                position_type = position['position_type']
                stop_loss = position['stop_loss']
                take_profit = position['take_profit']
                
                valid_sl_tp = True
                if position_type == 'LONG':
                    if stop_loss >= current_price or take_profit <= current_price:
                        logger.warning(f"⚠️ Invalid SL/TP for LONG {symbol}: SL={stop_loss:.6f}, TP={take_profit:.6f}, Current={current_price:.6f}")
                        valid_sl_tp = False
                else:
                    if stop_loss <= current_price or take_profit >= current_price:
                        logger.warning(f"⚠️ Invalid SL/TP for SHORT {symbol}: SL={stop_loss:.6f}, TP={take_profit:.6f}, Current={current_price:.6f}")
                        valid_sl_tp = False
                
                if not valid_sl_tp:
                    settings = get_current_settings()
                    new_sl, new_tp = calculate_safe_sl_tp(
                        position['open_price'], position_type, settings
                    )
                    db.execute(
                        "UPDATE positions SET stop_loss=?, take_profit=?, original_stop_loss=? WHERE symbol=? AND status='OPEN'",
                        (new_sl, new_tp, new_sl, symbol)
                    )
                    logger.info(f"✅ Fixed SL/TP for {symbol}: SL={new_sl:.6f}, TP={new_tp:.6f}")
                
                if not position.get('exchange_order_ids'):
                    logger.info(f"🔄 Creating orders for {symbol}")
                    settings = get_current_settings()
                    if settings.get('use_exchange_orders', True):
                        success, order_ids = create_exchange_stop_orders(
                            symbol, position_type, stop_loss, take_profit, position['base_amount']
                        )
                        if success:
                            order_ids_str = ','.join(order_ids)
                            db.execute(
                                "UPDATE positions SET exchange_order_ids=? WHERE symbol=? AND status='OPEN'",
                                (order_ids_str, symbol)
                            )
                            fixed_count += 1
                            logger.info(f"✅ Created/fixed orders for {symbol}")
                
            except Exception as e:
                logger.error(f"❌ Error fixing orders for {symbol}: {e}")
        
        if fixed_count > 0:
            update.message.reply_text(f"✅ Исправлены ордера для {fixed_count} позиций")
        else:
            update.message.reply_text("✅ Все ордера в порядке")
        
    except Exception as e:
        logger.error(f"❌ Fix orders error: {e}")
        update.message.reply_text(f"❌ Ошибка исправления ордеров: {str(e)}")

def cmd_recalculate_sltp(update, context):
    """Перерасчет SL/TP для всех открытых позиций"""
    try:
        positions = get_open_positions()
        recalculated = 0
        
        for symbol, position in positions.items():
            if not position.get('stop_loss') or not position.get('take_profit'):
                settings = get_current_settings()
                new_sl, new_tp = calculate_safe_sl_tp(
                    position['open_price'], 
                    position.get('position_type', 'LONG'), 
                    settings
                )
                db.execute(
                    "UPDATE positions SET stop_loss=?, take_profit=?, original_stop_loss=? WHERE symbol=? AND status='OPEN'",
                    (new_sl, new_tp, new_sl, symbol)
                )
                recalculated += 1
                logger.info(f"✅ Recalculated SL/TP for {symbol}")
        
        update.message.reply_text(f"✅ Перерасчет завершен: обновлено {recalculated} позиций")
        
    except Exception as e:
        logger.error(f"❌ Recalculate SL/TP error: {e}")
        update.message.reply_text(f"❌ Ошибка перерасчета: {str(e)}")

def cmd_scan(update, context):
    """Принудительное сканирование"""
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

def switch_mode(update):
    """Смена режима"""
    settings = get_current_settings()
    status = "🟢 АКТИВЕН" if BOT_RUNNING else "⏸️ НА ПАУЗЕ"
    order_type = "🔰 РЕАЛЬНЫЕ ОРДЕРА" if settings.get('use_exchange_orders') else "💻 ПРОГРАММНЫЕ SL/TP"
    entry_type = "⚡ MARKET" if settings.get('use_market_entry', False) else "💎 LIMIT (MAKER)"
    exit_type = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT (MAKER)"
    
    msg = f"""
✅ <b>Режим изменен: {settings['name']}</b>
🔰 <b>Статус: {status}</b>
💎 <b>Вход: {entry_type}</b>
💎 <b>Выход: {exit_type}</b>
🛡️ <b>Защита: {order_type}</b>

📊 <b>Параметры:</b>
• Макс сделок: {settings['max_trades']}
• Размер позиции: {settings['trade_pct']*100}%
• Плечо: {settings['leverage']}x
• SL/TP: {settings['max_stop_loss']*100:.1f}%/{settings['take_profit']*100:.1f}%
• Трейлинг: {settings['trailing_stop_activation']*100:.1f}% активация

🎯 <b>Стратегия:</b> {settings['strategy']}
⚠️ <b>Риск:</b> {settings['risk_level']}
"""
    update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

def cmd_status(update=None, context=None):
    """Статус бота"""
    try:
        equity = compute_available_usdt()
        positions = get_open_positions()
        settings = get_current_settings()
        
        status = "🟢 АКТИВЕН" if BOT_RUNNING else "⏸️ НА ПАУЗЕ"
        order_type = "🔰 REAL ORDERS" if settings.get('use_exchange_orders') else "💻 SOFTWARE SL/TP"
        entry_type = "⚡ MARKET" if settings.get('use_market_entry', False) else "💎 LIMIT (MAKER)"
        exit_type = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT (MAKER)"
        
        msg = f"""
📊 <b>STATUS: {settings['name']}</b>
🔰 <b>Статус: {status}</b>
💎 <b>Вход: {entry_type}</b>
💎 <b>Выход: {exit_type}</b>
🛡️ <b>Защита: {order_type}</b>

💰 Баланс: {equity:.2f} USDT
🔢 Позиции: {len(positions)}/{settings['max_trades']}
📊 Плечо: {settings['leverage']}x
🎯 Стратегия: {settings['strategy']}
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
                    order_status = "🔰" if pos.get('exchange_order_ids') else "💻"
                    entry_type_emoji = "⚡" if pos.get('entry_type') == "MARKET" else "💎"
                    position_age = time.time() - pos['open_timestamp']
                    msg += f"{emoji} {trailing_status} {order_status} {entry_type_emoji} {sym} {pos.get('position_type')} - {pnl_percent:+.2f}% ({int(position_age/60)}m)\n"
            msg += f"\n<b>Суммарный PnL:</b> {total_pnl:+.2f}%"
        else:
            msg += "\n📭 Нет открытых позиций"
            
        if update is None:
            safe_send(msg)
        else:
            update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            
    except Exception as e:
        logger.error(f"❌ Status error: {e}")
        error_msg = "❌ Ошибка статуса"
        if update is None:
            safe_send(error_msg)
        else:
            update.message.reply_text(error_msg)

def cmd_stats(update, context):
    """Статистика"""
    try:
        total_trades = db.fetchone("SELECT COUNT(*) FROM trade_history")[0] or 0
        closed_trades = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='CLOSED'")[0] or 0
        winning_trades = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='CLOSED' AND pnl_percent > 0")[0] or 0
        win_rate = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0
        
        total_pnl = db.fetchone("SELECT SUM(pnl) FROM positions WHERE status='CLOSED'")[0] or 0
        total_fees = db.fetchone("SELECT SUM(fee_paid) FROM positions")[0] or 0
        
        real_orders_count = db.fetchone("SELECT COUNT(*) FROM positions WHERE exchange_order_ids != '' AND status='CLOSED'")[0] or 0
        software_orders_count = closed_trades - real_orders_count
        
        maker_entries = db.fetchone("SELECT COUNT(*) FROM positions WHERE entry_type='LIMIT' AND status='CLOSED'")[0] or 0
        market_entries = closed_trades - maker_entries
        
        maker_exits = db.fetchone("SELECT COUNT(*) FROM positions WHERE exit_type='LIMIT' AND status='CLOSED'")[0] or 0
        market_exits = closed_trades - maker_exits
        
        msg = f"""
📈 <b>СТАТИСТИКА v5.0</b>

📊 Производительность:
• Всего сделок: {total_trades}
• Закрытых: {closed_trades}
• Винрейт: {win_rate:.1f}%
• Общий PnL: {total_pnl:+.2f} USDT
• Комиссии: {total_fees:.2f} USDT

🛡️ Типы ордеров:
• Реальные SL/TP: {real_orders_count}
• Программные SL/TP: {software_orders_count}

💎 Типы входа:
• LIMIT (MAKER): {maker_entries}
• MARKET (TAKER): {market_entries}

💎 Типы выхода:
• LIMIT (MAKER): {maker_exits}  
• MARKET (TAKER): {market_exits}

⚙️ Текущие настройки:
• Режим: {get_current_settings()['name']}
• Статус: {'🟢 АКТИВЕН' if BOT_RUNNING else '⏸️ НА ПАУЗЕ'}
• Плечо: {get_current_settings()['leverage']}x
• Вход: {'⚡ MARKET' if get_current_settings().get('use_market_entry') else '💎 LIMIT'}
• Выход: {'⚡ MARKET' if get_current_settings().get('use_market_exit') else '💎 LIMIT'}
• Фильтр: {'✅ ВКЛ' if get_current_settings().get('commission_filter', True) else '❌ ВЫКЛ'}
"""
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка статистики: {str(e)}")

def cmd_stop(update, context):
    """Остановка бота"""
    try:
        positions = get_open_positions()
        msg = "🛑 <b>ОСТАНОВКА БОТА</b>\n\n"
        
        if positions:
            msg += f"🔴 Закрываю {len(positions)} позиций:\n"
            for symbol in positions:
                safe_close_position(symbol, "EMERGENCY_STOP")
                msg += f"• {symbol}\n"
                time.sleep(1)
        else:
            msg += "📭 Нет открытых позиций\n"
            
        msg += "\n✅ Бот остановлен"
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
        stop_bot()
        
    except Exception as e:
        logger.error(f"❌ Stop command error: {e}")
        update.message.reply_text(f"❌ Ошибка остановки: {str(e)}")

# ====== КОМАНДЫ ДЛЯ УПРАВЛЕНИЯ КОМИССИЯМИ ======
def cmd_commission_settings(update, context):
    """Показать и изменить настройки комиссий"""
    try:
        settings = get_current_settings()
        
        current_entry_type = "⚡ MARKET" if settings.get('use_market_entry', False) else "💎 LIMIT (MAKER)"
        current_exit_type = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT (MAKER)"
        current_filter_status = "✅ ВКЛ" if settings.get('commission_filter', True) else "❌ ВЫКЛ"
        
        if settings.get('use_market_entry', False):
            entry_fee = TAKER_FEE * 100
        else:
            entry_fee = MAKER_FEE * 100
            
        if settings.get('use_market_exit', False):
            exit_fee = TAKER_FEE * 100
        else:
            exit_fee = MAKER_FEE * 100
            
        total_commission = entry_fee + exit_fee
        
        msg = f"""
💰 <b>НАСТРОЙКИ КОМИССИЙ v5.0</b>

Текущий режим: {settings['name']}
Тип входа: {current_entry_type}
Тип выхода: {current_exit_type}
Комиссия входа: {entry_fee:.3f}%
Комиссия выхода: {exit_fee:.3f}%
Общая комиссия: {total_commission:.3f}%
Фильтр малой прибыли: {current_filter_status}
Таймаут лимитных ордеров: {settings.get('limit_order_timeout', 60)}с

<b>Экономия с MAKER-ордерами:</b>
• MARKET вход/выход: {TAKER_FEE*100*2:.3f}% за сделку
• LIMIT вход/выход: {MAKER_FEE*100*2:.3f}% за сделку  
• Смешанный: {TAKER_FEE*100 + MAKER_FEE*100:.3f}% за сделку
• Экономия: {(TAKER_FEE*2 - MAKER_FEE*2)*100:.3f}% за сделку

<b>Команды:</b>
/commission - Это меню
/maker_entries - LIMIT ордера для входа
/market_entries - MARKET ордера для входа
/maker_exits - LIMIT ордера для выхода
/market_exits - MARKET ордера для выхода
/enable_filter - Включить фильтр малой прибыли
/disable_filter - Выключить фильтр малой прибыли

<b>Или используйте кнопки</b>
"""
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Commission settings error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_maker_entries(update, context):
    """Переключиться на использование MAKER ордеров для входа"""
    try:
        settings = get_current_settings()
        settings['use_market_entry'] = False
        update.message.reply_text(
            "💎 <b>Использую LIMIT ордера для входа (MAKER)</b>\n\n"
            "Комиссия входа: 0.02%\n"
            "Общая комиссия за сделку: 0.04%\n\n"
            "✅ Максимальная экономия комиссий!",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ Maker entries error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_market_entries(update, context):
    """Переключиться на использование MARKET ордеров для входа"""
    try:
        settings = get_current_settings()
        settings['use_market_entry'] = True
        update.message.reply_text(
            "⚡ <b>Использую MARKET ордера для входа (TAKER)</b>\n\n"
            "Комиссия входа: 0.06%\n" 
            "Общая комиссия за сделку: 0.08%\n\n"
            "⚠️ Для срочных сделок и скальпинга",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ Market entries error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_maker_exits(update, context):
    """Переключиться на использование MAKER ордеров для выхода"""
    try:
        settings = get_current_settings()
        settings['use_market_exit'] = False
        update.message.reply_text(
            "💎 <b>Использую LIMIT ордера для выхода (MAKER)</b>\n\n"
            "Комиссия выхода: 0.02%\n"
            "Общая комиссия за сделку: 0.04%\n\n"
            "✅ Максимальная экономия комиссий!",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ Maker exits error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_market_exits(update, context):
    """Переключиться на использование MARKET ордеров для выхода"""
    try:
        settings = get_current_settings()
        settings['use_market_exit'] = True
        update.message.reply_text(
            "⚡ <b>Использую MARKET ордера для выхода (TAKER)</b>\n\n"
            "Комиссия выхода: 0.06%\n"
            "Общая комиссия за сделку: 0.08%\n\n"
            "⚠️ Для быстрого выхода и скальпинга",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ Market exits error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_enable_filter(update, context):
    """Включить фильтр малой прибыли"""
    try:
        settings = get_current_settings()
        settings['commission_filter'] = True
        update.message.reply_text(
            "✅ <b>Фильтр малой прибыли ВКЛЮЧЕН</b>\n\n"
            "Бот будет пропускать сделки где:\n"
            "ожидаемая прибыль < комиссии + 0.3%\n\n"
            "Это защищает от убыточных сделок!",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ Enable filter error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_disable_filter(update, context):
    """Выключить фильтр малой прибыли"""
    try:
        settings = get_current_settings()
        settings['commission_filter'] = False
        update.message.reply_text(
            "❌ <b>Фильтр малой прибыли ВЫКЛЮЧЕН</b>\n\n"
            "Бот будет входить во все сигналы\n"
            "⚠️ Внимание: возможны убыточные сделки!",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ Disable filter error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

# ====== ГЛАВНЫЙ ЦИКЛ ======
def main_trading_loop():
    """Основной цикл торговли"""
    logger.info("🤖 Starting ULTIMATE TRADING BOT v5.0...")
    
    balance = compute_available_usdt()
    settings = get_current_settings()
    
    order_type = "🔰 REAL ORDERS" if settings.get('use_exchange_orders') else "💻 SOFTWARE SL/TP"
    entry_type = "⚡ MARKET" if settings.get('use_market_entry', False) else "💎 LIMIT (MAKER)"
    exit_type = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT (MAKER)"
    
    safe_send(
        f"🚀 <b>BOT v5.0 STARTED</b>\n"
        f"Баланс: {balance:.2f} USDT\n"
        f"Режим: {settings['name']}\n" 
        f"Вход: {entry_type}\n"
        f"Выход: {exit_type}\n"
        f"Защита: {order_type}\n"
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
                sync_positions_with_exchange()
                last_sync = current_time
            
            if current_time - last_exit_check >= settings['exit_check_interval']:
                check_position_exits()
                last_exit_check = current_time
            
            if current_time - last_scan >= settings['scan_interval']:
                scan_for_opportunities()
                last_scan = current_time
            
            if current_time - last_status >= settings['status_interval']:
                cmd_status(None, None)
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
        if any("YOUR_" in key for key in [API_KEY, API_SECRET, TELEGRAM_TOKEN]):
            print("❌ CRITICAL: Use real API keys!")
            sys.exit(1)
            
        initialize_exchange()
        
        balance = compute_available_usdt()
        settings = get_current_settings()
        
        print(f"✅ ULTIMATE BOT v5.0 started!")
        print(f"💰 Balance: {balance:.2f} USDT")
        print(f"🎯 Mode: {settings['name']}")
        print(f"📊 Leverage: {settings['leverage']}x")
        print(f"💎 Entry: {'MARKET' if settings.get('use_market_entry') else 'LIMIT'}")
        print(f"💎 Exit: {'MARKET' if settings.get('use_market_exit') else 'LIMIT'}")
        print(f"🛡️ Orders: {'REAL' if settings.get('use_exchange_orders') else 'SOFTWARE'}")
        print(f"🔰 Status: 🟢 ACTIVE")
        
        updater = setup_telegram()
        if updater:
            updater.start_polling()
            logger.info("✅ Telegram bot started with advanced commission controls")
        
        main_trading_loop()
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        safe_send(f"❌ <b>BOT CRASHED:</b> {str(e)}")
    finally:
        cleanup()
