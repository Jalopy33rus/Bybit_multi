#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ultimate_trading_bot.py — универсальный бот с исправленными алгоритмами закрытия позиций
"""
import os, sys, time, math, ccxt, pandas as pd, sqlite3
import logging
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import threading
import signal

from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import VolumeWeightedAveragePrice
from telegram import Bot, ParseMode, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

# ====== CONFIGURATION ======
API_KEY = os.getenv("BYBIT_API_KEY", "API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET", "API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "TELEGRAM_TOKEN")
CHAT_ID = CHAT_ID

# Расширенный список пар
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "AVAX/USDT", "DOT/USDT", "LINK/USDT", 
    "DOGE/USDT", "LTC/USDT", "ATOM/USDT", "UNI/USDT", "XLM/USDT",
    "ETC/USDT", "FIL/USDT", "THETA/USDT", "AAVE/USDT"
]

# ====== TRADING MODES ======
TRADING_MODES = {
    "AGGRESSIVE": {
        "name": "🟢 АГРЕССИВНЫЙ",
        "type": "swing",
        "scan_interval": 30,
        "status_interval": 300,
        "max_trades": 8,
        "trade_pct": 0.15,
        "rsi_min": 35,
        "rsi_max": 75,
        "volume_multiplier": 1.2,
        "adx_min": 12,
        "min_score": 40,
        "cooldown": 2 * 60,
        "max_stop_loss": 0.01,
        "take_profit": 0.02,
        "trailing_start": 0.004,
        "trailing_step": 0.002,
        "max_daily_trades_per_symbol": 3
    },
    "CONSERVATIVE": {
        "name": "🟡 КОНСЕРВАТИВНЫЙ", 
        "type": "swing",
        "scan_interval": 60,
        "status_interval": 600,
        "max_trades": 4,
        "trade_pct": 0.20,
        "rsi_min": 40,
        "rsi_max": 65,
        "volume_multiplier": 1.3,
        "adx_min": 15,
        "min_score": 55,
        "cooldown": 10 * 60,
        "max_stop_loss": 0.015,
        "take_profit": 0.03,
        "trailing_start": 0.015,
        "trailing_step": 0.005,
        "max_daily_trades_per_symbol": 2
    },
    "SCALPING": {
        "name": "🔴 СКАЛЬПИНГ",
        "type": "scalping",
        "scan_interval": 5,
        "status_interval": 180,
        "max_trades": 12,
        "trade_pct": 0.25,
        "active_strategy": "BB_SQUEEZE"
    }
}

# Стратегии скальпинга
SCALPING_STRATEGIES = {
    "BB_SQUEEZE": {
        "name": "Bollinger Squeeze",
        "scan_interval": 3,
        "max_trades": 15,
        "trade_pct": 0.3,
        "timeframe_entry": "5m",
        "timeframe_trend": "15m",
        "max_stop_loss": 0.003,
        "take_profit": 0.008,
        "quick_exit": 0.005,
        "rsi_range": (25, 75),
        "volume_multiplier": 1.3,
        "bb_period": 10,
        "bb_std": 1.2,
        "max_position_age": 900
    },
    "EMA_MOMENTUM": {
        "name": "EMA Momentum",
        "scan_interval": 5,
        "max_trades": 12,
        "trade_pct": 0.15,
        "timeframe_entry": "5m",
        "timeframe_trend": "15m",
        "max_stop_loss": 0.0025,
        "take_profit": 0.005,
        "quick_exit": 0.004,
        "rsi_range": (30, 70),
        "volume_multiplier": 1.2,
        "ema_fast": 5,
        "ema_slow": 12
    },
    "VWAP_BOUNCE": {
        "name": "VWAP Bounce",
        "scan_interval": 4,
        "max_trades": 10,
        "trade_pct": 0.18,
        "timeframe_entry": "5m",
        "timeframe_trend": "15m",
        "max_stop_loss": 0.0015,
        "take_profit": 0.0035,
        "quick_exit": 0.0025,
        "rsi_range": (35, 65),
        "volume_multiplier": 1.8,
        "vwap_period": 20
    },
    "BREAKOUT": {
        "name": "Breakout Scalping",
        "scan_interval": 5,
        "max_trades": 8,
        "trade_pct": 0.20,
        "timeframe_entry": "5m",
        "timeframe_trend": "15m",
        "max_stop_loss": 0.003,
        "take_profit": 0.006,
        "quick_exit": 0.004,
        "rsi_range": (40, 80),
        "volume_multiplier": 2.0,
        "breakout_period": 15
    }
}

# Текущие настройки
CURRENT_MODE = "AGGRESSIVE"
CURRENT_SCALPING_STRATEGY = "BB_SQUEEZE"

# Общие настройки скальпинга
SCALPING_GLOBAL = {
    "cooldown": 15,
    "max_daily_trades_per_symbol": 25,
    "time_in_trade": 180,
    "max_consecutive_losses": 3,
    "profit_target_daily": 0.05,
    "loss_limit_daily": -0.02,
}

# Общие настройки
MIN_TRADE_USDT = 5.0
MIN_USDT_PER_SYMBOL = {
    "BTC/USDT": 5.0, "ETH/USDT": 5.0, "BNB/USDT": 2.0, "SOL/USDT": 2.0,
    "XRP/USDT": 2.0, "ADA/USDT": 2.0, "AVAX/USDT": 3.0, "DOT/USDT": 2.0,
    "LINK/USDT": 2.0, "MATIC/USDT": 2.0, "DOGE/USDT": 2.0, "LTC/USDT": 2.0,
    "ATOM/USDT": 2.0, "UNI/USDT": 2.0, "XLM/USDT": 2.0, "ETC/USDT": 2.0,
    "FIL/USDT": 2.0, "THETA/USDT": 2.0, "EOS/USDT": 2.0, "AAVE/USDT": 3.0,
}

TAKER_FEE = 0.001
ROUNDTRIP_FEE = TAKER_FEE * 2

LOCK_FILE = "/tmp/ultimate_trading_bot.lock"
DB_FILE = "trades_ultimate.db"

DRY_RUN = False

# ====== LOGGING ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultimate_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====== DATABASE MANAGER ======
class DatabaseManager:
    """Менеджер базы данных с обработкой переподключений"""
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
            
            # Таблица позиций
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
                open_time TEXT,
                close_time TEXT,
                close_price REAL,
                pnl REAL DEFAULT 0,
                pnl_percent REAL DEFAULT 0,
                status TEXT DEFAULT 'OPEN',
                fee_paid REAL DEFAULT 0,
                entry_reason TEXT,
                exit_reason TEXT,
                duration_seconds INTEGER DEFAULT 0
            )
            """)
            
            # История торгов
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
                strategy TEXT
            )
            """)
            
            # Кудоуны и лимиты
            self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS symbol_cooldown (
                symbol TEXT PRIMARY KEY,
                last_closed_ts INTEGER DEFAULT 0,
                daily_trade_count INTEGER DEFAULT 0,
                last_trade_date TEXT
            )
            """)
            
            # Статистика
            self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS performance_stats (
                date TEXT PRIMARY KEY,
                total_trades INTEGER DEFAULT 0,
                winning_trades INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                total_volume REAL DEFAULT 0
            )
            """)
            
            # Дневные лимиты для скальпинга
            self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_limits (
                date TEXT PRIMARY KEY,
                daily_pnl REAL DEFAULT 0,
                total_trades INTEGER DEFAULT 0,
                consecutive_losses INTEGER DEFAULT 0
            )
            """)
            
            self._connection.commit()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    def get_connection(self):
        """Получение соединения с проверкой"""
        try:
            if self._connection is None:
                self._initialize_database()
            # Проверяем что соединение живо
            self._cursor.execute("SELECT 1")
            return self._connection, self._cursor
        except sqlite3.ProgrammingError:
            # Пересоздаем соединение если оно мертво
            logger.warning("Database connection dead, recreating...")
            try:
                if self._connection:
                    self._connection.close()
            except:
                pass
            self._initialize_database()
            return self._connection, self._cursor
        except Exception as e:
            logger.error(f"Error getting database connection: {e}")
            raise
    
    def execute(self, query, params=()):
        """Безопасное выполнение запроса"""
        conn, cursor = self.get_connection()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor
        except sqlite3.ProgrammingError:
            # Переподключаемся и повторяем
            logger.warning("Database error, reconnecting...")
            self._initialize_database()
            conn, cursor = self.get_connection()
            cursor.execute(query, params)
            conn.commit()
            return cursor
        except Exception as e:
            logger.error(f"Database execute error: {e}")
            conn.rollback()
            raise
    
    def fetchone(self, query, params=()):
        """Безопасное получение одной строки"""
        cursor = self.execute(query, params)
        return cursor.fetchone()
    
    def fetchall(self, query, params=()):
        """Безопасное получение всех строк"""
        cursor = self.execute(query, params)
        return cursor.fetchall()
    
    def close(self):
        """Закрытие соединения"""
        try:
            if self._connection:
                self._connection.close()
                self._connection = None
                self._cursor = None
        except Exception as e:
            logger.error(f"Error closing database: {e}")

# Глобальный менеджер БД
db = DatabaseManager()

# ====== INITIALIZATION ======
if os.path.exists(LOCK_FILE):
    logger.error("Lock file exists — bot already running. Exit.")
    sys.exit(1)
open(LOCK_FILE, "w").close()

try:
    exchange = ccxt.bybit({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
        "timeout": 30000,
    })
    exchange.fetch_balance()
    logger.info("Exchange connection successful")
except Exception as e:
    logger.error(f"Exchange connection failed: {e}")
    sys.exit(1)

bot = Bot(token=TELEGRAM_TOKEN)

# ====== CACHE FOR SETTINGS ======
_settings_cache = {}
_last_settings_update = 0
CACHE_DURATION = 10  # секунд

# ====== KEYBOARDS ======
def get_main_keyboard():
    """Клавиатура основного меню"""
    keyboard = [
        [KeyboardButton("🟢 АГРЕССИВНЫЙ"), KeyboardButton("🟡 КОНСЕРВАТИВНЫЙ")],
        [KeyboardButton("🔴 СКАЛЬПИНГ"), KeyboardButton("📊 СТАТУС")],
        [KeyboardButton("⚙️ НАСТРОЙКИ"), KeyboardButton("📈 СТАТИСТИКА")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_scalping_keyboard():
    """Клавиатура выбора стратегии скальпинга"""
    keyboard = [
        [KeyboardButton("🎯 BB Squeeze"), KeyboardButton("🚀 EMA Momentum")],
        [KeyboardButton("📊 VWAP Bounce"), KeyboardButton("💥 Breakout")],
        [KeyboardButton("🔙 НАЗАД")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_settings_keyboard():
    """Клавиатура настроек"""
    keyboard = [
        [KeyboardButton("🔄 СИНХРОНИЗАЦИЯ"), KeyboardButton("📥 ИМПОРТ")],
        [KeyboardButton("🔧 ДЕБАГ"), KeyboardButton("🔙 НАЗАД")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ====== CORE FUNCTIONS ======
def safe_send(text: str, max_retries: int = 3) -> bool:
    """Безопасная отправка сообщений"""
    for attempt in range(max_retries):
        try:
            bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML)
            return True
        except Exception as e:
            logger.warning(f"Telegram send attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to send Telegram message: {e}")
            time.sleep(2)
    return False

def retry_api_call(func, max_retries: int = 3, delay: float = 1.0):
    """Повторный вызов API"""
    for attempt in range(max_retries):
        try:
            return func()
        except ccxt.NetworkError as e:
            if attempt == max_retries - 1:
                logger.error(f"Network error after {max_retries} attempts: {e}")
                raise e
            sleep_time = delay * (2 ** attempt) + np.random.uniform(0, 1)
            logger.warning(f"Network error, retrying in {sleep_time:.2f}s: {e}")
            time.sleep(sleep_time)
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error: {e}")
            raise e
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"API error after {max_retries} attempts: {e}")
                raise e
            time.sleep(delay * (2 ** attempt))

def fetch_ohlcv(symbol: str, timeframe: str, limit: int = 100) -> List[List]:
    """Получение OHLCV данных с правильными таймфреймами для Bybit"""
    def _fetch():
        try:
            # Маппинг таймфреймов для Bybit
            timeframe_map = {
                '1m': '1', '3m': '3', '5m': '5', '15m': '15', 
                '30m': '30', '1h': '60', '2h': '120', '4h': '240',
                '6h': '360', '12h': '720', '1d': 'D', '1w': 'W'
            }
            
            bybit_timeframe = timeframe_map.get(timeframe, '15')
            
            data = exchange.fetch_ohlcv(symbol, timeframe=bybit_timeframe, limit=limit)
            if data and len(data) > 0:
                return data
                
            logger.warning(f"No data for {symbol} with timeframe {timeframe}")
            return []
            
        except ccxt.BadSymbol:
            logger.warning(f"Bad symbol {symbol}, removing from active symbols")
            if symbol in active_symbols:
                active_symbols.remove(symbol)
            return []
        except Exception as e:
            logger.warning(f"OHLCV fetch failed for {symbol}: {e}")
            return []
    
    try:
        data = retry_api_call(_fetch, max_retries=2, delay=1.0)
        if not data or len(data) < 20:
            logger.debug(f"Insufficient OHLCV data for {symbol}: {len(data) if data else 0} candles")
            return []
        return data
    except Exception as e:
        logger.warning(f"Failed to fetch OHLCV for {symbol}: {e}")
        return []

def fetch_balance() -> Dict:
    """Получение баланса"""
    def _fetch():
        return exchange.fetch_balance()
    try:
        return retry_api_call(_fetch)
    except Exception as e:
        logger.error(f"Failed to fetch balance: {e}")
        return {'free': {'USDT': 0.0}, 'total': {'USDT': 0.0}, 'used': {'USDT': 0.0}}

def get_current_settings() -> Dict:
    """Получение текущих настроек с кэшированием и исправленными ключами"""
    global _settings_cache, _last_settings_update
    
    current_time = time.time()
    
    # Возвращаем кэшированные настройки если они еще актуальны
    if (current_time - _last_settings_update < CACHE_DURATION and 
        CURRENT_MODE in _settings_cache):
        return _settings_cache[CURRENT_MODE]
    
    try:
        logger.debug(f"🔄 Getting fresh settings for mode: {CURRENT_MODE}")
        
        if CURRENT_MODE == "SCALPING":
            strategy_settings = SCALPING_STRATEGIES.get(CURRENT_SCALPING_STRATEGY, SCALPING_STRATEGIES["BB_SQUEEZE"])
            
            settings = {
                'name': strategy_settings['name'],
                'type': 'scalping',
                'scan_interval': strategy_settings.get('scan_interval', 5),
                'status_interval': TRADING_MODES["SCALPING"]['status_interval'],
                'max_trades': strategy_settings.get('max_trades', 12),
                'trade_pct': strategy_settings.get('trade_pct', 0.15),
                'rsi_min': strategy_settings.get('rsi_range', (25, 75))[0],
                'rsi_max': strategy_settings.get('rsi_range', (25, 75))[1],
                'rsi_range': strategy_settings.get('rsi_range', (25, 75)),
                'volume_multiplier': strategy_settings.get('volume_multiplier', 1.3),
                'adx_min': 0,
                'min_score': 70,
                'max_stop_loss': strategy_settings.get('max_stop_loss', 0.003),
                'take_profit': strategy_settings.get('take_profit', 0.008),
                'quick_exit': strategy_settings.get('quick_exit', 0.005),
                'trailing_start': 0,
                'trailing_step': 0,
                'timeframe_entry': strategy_settings.get('timeframe_entry', '5m'),
                'timeframe_trend': strategy_settings.get('timeframe_trend', '15m'),
                'cooldown': SCALPING_GLOBAL.get('cooldown', 15),
                'max_daily_trades_per_symbol': SCALPING_GLOBAL.get('max_daily_trades_per_symbol', 25),
            }
            
        else:
            base_settings = TRADING_MODES.get(CURRENT_MODE, TRADING_MODES["CONSERVATIVE"])
            
            # ГАРАНТИРУЕМ что все необходимые ключи присутствуют
            settings = {
                'name': base_settings['name'],
                'type': 'swing',
                'scan_interval': base_settings.get('scan_interval', 60),
                'status_interval': base_settings.get('status_interval', 600),
                'max_trades': base_settings.get('max_trades', 5),
                'trade_pct': base_settings.get('trade_pct', 0.1),
                'rsi_min': base_settings.get('rsi_min', 40),
                'rsi_max': base_settings.get('rsi_max', 65),
                'rsi_range': (base_settings.get('rsi_min', 40), base_settings.get('rsi_max', 65)),
                'volume_multiplier': base_settings.get('volume_multiplier', 1.2),
                'adx_min': base_settings.get('adx_min', 15),
                'min_score': base_settings.get('min_score', 50),
                'max_stop_loss': base_settings.get('max_stop_loss', 0.01),
                'take_profit': base_settings.get('take_profit', 0.02),
                'trailing_start': base_settings.get('trailing_start', 0.005),
                'trailing_step': base_settings.get('trailing_step', 0.002),
                'quick_exit': 0,
                'timeframe_entry': '15m',
                'timeframe_trend': '1h',
                'cooldown': base_settings.get('cooldown', 300),
                'max_daily_trades_per_symbol': base_settings.get('max_daily_trades_per_symbol', 5),
            }
        
        # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: гарантируем наличие всех обязательных ключей
        required_keys = ['min_score', 'max_stop_loss', 'take_profit', 'scan_interval', 'max_trades']
        for key in required_keys:
            if key not in settings:
                logger.warning(f"⚠️ Missing key {key} in settings, adding default value")
                if key == 'min_score':
                    settings[key] = 50
                elif key == 'max_stop_loss':
                    settings[key] = 0.01
                elif key == 'take_profit':
                    settings[key] = 0.02
                elif key == 'scan_interval':
                    settings[key] = 60
                elif key == 'max_trades':
                    settings[key] = 5
        
        # Кэшируем настройки
        _settings_cache[CURRENT_MODE] = settings
        _last_settings_update = current_time
        
        logger.debug(f"✅ Settings cached for {CURRENT_MODE}: min_score={settings.get('min_score')}")
        return settings
        
    except Exception as e:
        logger.error(f"❌ Critical error in get_current_settings: {e}")
        # Возвращаем безопасные настройки по умолчанию
        default_settings = {
            'min_score': 50,
            'max_stop_loss': 0.01,
            'take_profit': 0.02,
            'scan_interval': 60,
            'max_trades': 5,
            'trade_pct': 0.1,
            'rsi_min': 40,
            'rsi_max': 65,
            'volume_multiplier': 1.2,
            'adx_min': 15
        }
        return default_settings

def round_amount(symbol: str, amount: float) -> float:
    """Округление количества с гарантией минимального размера"""
    try:
        market = exchange.markets.get(symbol)
        if not market:
            return float(amount)
            
        limits = market.get('limits', {})
        amount_limits = limits.get('amount', {})
        min_amount = amount_limits.get('min', 0.000001)
        step = amount_limits.get('step', min_amount)
        
        if step <= 0:
            return max(float(amount), min_amount)
            
        if step < 1:
            precision = int(round(-math.log10(step)))
            rounded = math.floor(amount / step) * step
            rounded = round(rounded, precision)
        else:
            rounded = math.floor(amount / step) * step
            
        # Гарантируем что не меньше минимального
        result = max(float(rounded), min_amount)
        
        # Для очень дешевых активов увеличиваем минимальное количество
        current_price = get_current_price(symbol)
        if current_price and result * current_price < 1.0:
            # Увеличиваем до минимального размера в 1 USDT
            min_for_1usdt = 1.0 / current_price
            min_for_1usdt_rounded = math.ceil(min_for_1usdt / step) * step
            result = max(result, min_for_1usdt_rounded)
            
        return result
        
    except Exception as e:
        logger.error(f"Error rounding amount for {symbol}: {e}")
        return float(amount)

def get_min_amount(symbol: str) -> float:
    """Получение минимального количества"""
    try:
        market = exchange.markets.get(symbol)
        if market:
            return float(market.get('limits', {}).get('amount', {}).get('min', 0.000001))
    except Exception as e:
        logger.error(f"Error getting min amount for {symbol}: {e}")
    return 0.000001

def check_min_order_value(symbol: str, usdt_amount: float) -> bool:
    """Проверка минимальной суммы ордера"""
    min_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
    if usdt_amount < min_value:
        logger.debug(f"Order value {usdt_amount:.2f} below minimum {min_value:.2f} for {symbol}")
        return False
    return True
    
def compute_available_usdt() -> float:
    """Расчет доступного USDT баланса"""
    bal = fetch_balance()
    return float(bal['free'].get('USDT', 0) or 0)

def can_afford_trade(symbol: str, usdt_amount: float) -> bool:
    """Проверка возможности совершить сделку с исправленным сравнением"""
    min_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
    available_usdt = compute_available_usdt()
    
    # Используем сравнение с допуском для float значений
    if usdt_amount < min_value - 0.01:
        logger.info(f"❌ Order size too small for {symbol}: {usdt_amount:.2f} USDT < {min_value:.2f} USDT min")
        return False
    
    if usdt_amount > available_usdt:
        logger.info(f"❌ Insufficient USDT for {symbol}: need {usdt_amount:.2f}, have {available_usdt:.2f}")
        return False
        
    return True

def get_current_price(symbol: str) -> Optional[float]:
    """Получение текущей цены через тикер"""
    try:
        ticker = exchange.fetch_ticker(symbol)
        return float(ticker['last'])
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        return None

# ====== POSITION MANAGEMENT ======
def get_ohlcv_data(symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    """Получение OHLCV данных с улучшенной обработкой ошибок"""
    # Проверяем доступность символа
    if symbol not in active_symbols:
        logger.debug(f"Symbol {symbol} not in available symbols list")
        return None
        
    ohlcv = fetch_ohlcv(symbol, timeframe, limit)
    if not ohlcv or len(ohlcv) < 20:
        logger.warning(f"Insufficient OHLCV data for {symbol}: {len(ohlcv) if ohlcv else 0} candles")
        return None
        
    try:
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
    except Exception as e:
        logger.error(f"Error processing OHLCV data for {symbol}: {e}")
        return None

def get_open_positions() -> Dict[str, Dict]:
    """Получение открытых позиций"""
    try:
        rows = db.fetchall("SELECT symbol, base_amount, open_price, stop_loss, take_profit, max_price, trading_mode FROM positions WHERE status='OPEN'")
        logger.info(f"Found {len(rows)} open positions in database")
        
        positions = {}
        for row in rows:
            symbol = row[0]
            positions[symbol] = {
                "base_amount": row[1], 
                "open_price": row[2], 
                "stop_loss": row[3],
                "take_profit": row[4],
                "max_price": row[5] or row[2],
                "trading_mode": row[6]
            }
        
        return positions
        
    except Exception as e:
        logger.error(f"Error getting open positions: {e}")
        return {}

def record_open_position(symbol: str, base_amount: float, open_price: float, stop_loss: float, take_profit: float, strategy: str = ""):
    """Запись открытия позиции"""
    try:
        usdt_amount = base_amount * open_price
        fee = usdt_amount * TAKER_FEE
        
        db.execute("""
            INSERT INTO positions (symbol, trading_mode, strategy, base_amount, open_price, stop_loss, take_profit, max_price, open_time, fee_paid) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
        """, (symbol, CURRENT_MODE, strategy, base_amount, open_price, stop_loss, take_profit, open_price, fee))
        
        db.execute("""
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp, trading_mode, strategy) 
            VALUES (?, 'BUY', ?, ?, ?, ?, datetime('now'), ?, ?, ?)
        """, (symbol, open_price, usdt_amount, base_amount, fee, int(time.time()), CURRENT_MODE, strategy))
        
        logger.info(f"Recorded open position for {symbol}: {base_amount:.6f} @ {open_price:.6f}")
    except Exception as e:
        logger.error(f"Error recording open position: {e}")
def calculate_weighted_average_position(symbol: str) -> Optional[Dict]:
    """Расчет средневзвешенной позиции для символа"""
    try:
        positions = get_all_position_info(symbol)
        if not positions:
            return None
            
        total_amount = 0.0
        total_value = 0.0
        
        for pos in positions:
            total_amount += pos['base_amount']
            total_value += pos['base_amount'] * pos['open_price']
        
        if total_amount > 0:
            avg_price = total_value / total_amount
            return {
                'base_amount': total_amount,
                'open_price': avg_price,
                'position_count': len(positions)
            }
        else:
            return None
            
    except Exception as e:
        logger.error(f"Error calculating weighted average for {symbol}: {e}")
        return None

def get_all_position_info(symbol: str) -> List[Dict]:
    """Получение информации о ВСЕХ открытых позициях для символа"""
    try:
        rows = db.fetchall("""
            SELECT base_amount, open_price, trading_mode, open_time 
            FROM positions 
            WHERE symbol=? AND status='OPEN'
            ORDER BY open_time
        """, (symbol,))
        
        positions = []
        for row in rows:
            positions.append({
                'base_amount': row[0], 
                'open_price': row[1], 
                'trading_mode': row[2],
                'open_time': row[3]
            })
        
        return positions
    except Exception as e:
        logger.error(f"Error getting all positions for {symbol}: {e}")
        return []

def get_position_info(symbol: str) -> Optional[Dict]:
    """Получение информации о позиции - ВОЗВРАЩАЕТ ТОЛЬКО ПОСЛЕДНЮЮ ПОЗИЦИЮ"""
    row = db.fetchone("SELECT base_amount, open_price, trading_mode FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
    if row:
        return {'base_amount': row[0], 'open_price': row[1], 'trading_mode': row[2]}
    return None

def mark_position_closed(symbol: str):
    """Закрытие позиции в БД с записью причины"""
    try:
        db.execute("""
            UPDATE positions SET 
                status='CLOSED', 
                close_time=datetime('now'),
                exit_reason='AUTO_CLOSED_NO_BALANCE'
            WHERE symbol=? AND status='OPEN'
        """, (symbol,))
        
        db.execute("REPLACE INTO symbol_cooldown (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
        
        logger.info(f"Position {symbol} marked as closed in DB")
        
    except Exception as e:
        logger.error(f"Error marking position closed: {e}")

def record_successful_close(symbol: str, amount: float, price: float, reason: str):
    """Запись успешного закрытия позиции с правильным расчетом PnL"""
    try:
        usdt_amount = amount * price
        fee = usdt_amount * TAKER_FEE
        
        # Получаем информацию об открытой позиции для расчета PnL
        position_info = get_position_info(symbol)
        if position_info:
            open_value = position_info['base_amount'] * position_info['open_price']
            close_value = amount * price
            pnl = close_value - open_value - fee
            pnl_percent = (pnl / open_value) * 100 if open_value > 0 else 0
            
            db.execute("""
                UPDATE positions SET 
                    status='CLOSED', 
                    close_time=datetime('now'), 
                    close_price=?, 
                    pnl=?, 
                    pnl_percent=?,
                    exit_reason=?,
                    duration_seconds=ROUND((julianday('now') - julianday(open_time)) * 86400)
                WHERE symbol=? AND status='OPEN'
            """, (price, pnl, pnl_percent, reason, symbol))
        else:
            db.execute("""
                UPDATE positions SET 
                    status='CLOSED', 
                    close_time=datetime('now'), 
                    close_price=?,
                    exit_reason=?,
                    duration_seconds=ROUND((julianday('now') - julianday(open_time)) * 86400)
                WHERE symbol=? AND status='OPEN'
            """, (price, reason, symbol))
        
        # Записываем в историю
        db.execute("""
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp, trading_mode) 
            VALUES (?, 'SELL', ?, ?, ?, ?, datetime('now'), ?, ?)
        """, (symbol, price, usdt_amount, amount, fee, int(time.time()), CURRENT_MODE))
        
        db.execute("REPLACE INTO symbol_cooldown (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
        
        logger.info(f"✅ Successfully recorded close for {symbol}: {amount:.6f} @ {price:.6f}, PnL: {pnl:+.2f} USDT ({pnl_percent:+.2f}%)")
        
    except Exception as e:
        logger.error(f"Error recording successful close for {symbol}: {e}")

def calculate_safe_sell_amount(symbol: str, available_balance: float, current_price: float) -> float:
    """Расчет безопасного количества для продажи с улучшенной логикой для Bybit"""
    try:
        # Получаем минимальные лимиты
        min_amount = get_min_amount(symbol)
        min_order_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
        
        # Начинаем с доступного баланса
        amount = available_balance
        
        # Проверяем минимальное количество
        if amount < min_amount:
            logger.info(f"Available balance {amount:.6f} below min amount {min_amount:.6f}")
            return 0
            
        # Проверяем минимальную сумму ордера
        order_value = amount * current_price
        if order_value < min_order_value:
            logger.info(f"Order value {order_value:.2f} below minimum {min_order_value:.2f}")
            return 0
            
        # Округляем до допустимого шага
        amount = round_amount(symbol, amount)
        
        # Дополнительная проверка после округления
        if amount < min_amount:
            logger.info(f"After rounding {amount:.6f} below min amount {min_amount:.6f}")
            return 0
            
        final_value = amount * current_price
        if final_value < min_order_value:
            logger.info(f"Final order value {final_value:.2f} below minimum {min_order_value:.2f}")
            return 0
            
        logger.info(f"✅ Safe sell amount: {amount:.6f} (value: {final_value:.2f} USDT)")
        return amount
        
    except Exception as e:
        logger.error(f"Error calculating safe sell amount for {symbol}: {e}")
        return 0

def close_with_adjusted_amount(symbol: str, available_balance: float, current_price: float, reason: str) -> bool:
    """Закрытие позиции с корректировкой количества для Bybit"""
    try:
        base = symbol.split("/")[0]
        
        # Пробуем несколько раз с уменьшающимся количеством
        for attempt in range(3):
            try:
                # Уменьшаем количество на каждом шаге
                adjustment_factor = 1.0 - (attempt * 0.1)  # 100%, 90%, 80%
                adjusted_amount = available_balance * adjustment_factor
                
                # Округляем до допустимого шага
                adjusted_amount = round_amount(symbol, adjusted_amount)
                
                if adjusted_amount <= 0:
                    continue
                    
                logger.info(f"🔄 Attempt {attempt + 1}: trying amount {adjusted_amount:.6f}")
                
                order = exchange.create_market_sell_order(symbol, adjusted_amount)
                
                if order and order.get('id'):
                    logger.info(f"✅ Successfully closed {symbol} with adjusted amount")
                    
                    # Короткая пауза
                    time.sleep(2)
                    
                    # Проверяем изменение баланса
                    new_bal = fetch_balance()
                    new_base_balance = float(new_bal['free'].get(base, 0) or 0)
                    
                    if new_base_balance < available_balance - 0.000001:
                        filled_amount = available_balance - new_base_balance
                        record_successful_close(symbol, filled_amount, current_price, f"ADJUSTED_{reason}")
                    else:
                        mark_position_closed(symbol)
                        
                    return True
                    
            except ccxt.InsufficientFunds as e:
                logger.warning(f"🔄 Insufficient funds on attempt {attempt + 1}, retrying...")
                continue
                
            except ccxt.InvalidOrder as e:
                logger.warning(f"🔄 Invalid order on attempt {attempt + 1}, retrying...")
                continue
                
            except Exception as e:
                logger.warning(f"🔄 Error on attempt {attempt + 1}: {e}, retrying...")
                continue
        
        # Если все попытки не удались, помечаем как закрытую
        logger.error(f"❌ All attempts failed for {symbol}, marking as closed")
        mark_position_closed(symbol)
        return True
            
    except Exception as e:
        logger.error(f"Error in close_with_adjusted_amount for {symbol}: {e}")
        mark_position_closed(symbol)
        return True

safe_close_position
# ====== EXIT CONDITIONS ======
def check_scalping_exit(symbol: str, pos: Dict):
    """Проверка выхода для скальпинга с исправленной логикой получения цены"""
    try:
        strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
        
        # Получаем текущую цену через тикер (более надежно)
        current_price = get_current_price(symbol)
        if current_price is None:
            logger.warning(f"❌ Cannot get current price for {symbol}")
            return
            
        open_price = pos['open_price']
        stop_loss = pos['stop_loss']
        take_profit = pos['take_profit']
        
        # Детальный логирование цен
        logger.info(f"🔍 {symbol}: Current={current_price:.6f}, Entry={open_price:.6f}")
        logger.info(f"🎯 {symbol}: SL={stop_loss:.6f}, TP={take_profit:.6f}")
        
        # Рассчитываем PnL
        profit_pct = (current_price - open_price) / open_price * 100
        
        exit_reason = ""
        
        # Определяем тип позиции (LONG/SHORT) по соотношению цен
        is_long_position = take_profit > open_price  # Если TP выше цены входа - это LONG
        
        if is_long_position:
            # LONG позиция
            if current_price <= stop_loss:
                exit_reason = f"LONG SL {profit_pct:+.2f}%"
                logger.info(f"🔴 {symbol}: LONG STOP LOSS! Price {current_price:.6f} <= SL {stop_loss:.6f}")
            elif current_price >= take_profit:
                exit_reason = f"LONG TP {profit_pct:+.2f}%"
                logger.info(f"🟢 {symbol}: LONG TAKE PROFIT! Price {current_price:.6f} >= TP {take_profit:.6f}")
            elif profit_pct >= strategy_config['quick_exit'] * 100:  # quick_exit в процентах
                exit_reason = f"QUICK EXIT {profit_pct:+.2f}%"
                logger.info(f"⚡ {symbol}: QUICK EXIT! Profit {profit_pct:+.2f}% >= {strategy_config['quick_exit']*100:.2f}%")
        else:
            # SHORT позиция (обратная логика)
            if current_price >= stop_loss:
                exit_reason = f"SHORT SL {profit_pct:+.2f}%"
                logger.info(f"🔴 {symbol}: SHORT STOP LOSS! Price {current_price:.6f} >= SL {stop_loss:.6f}")
            elif current_price <= take_profit:
                exit_reason = f"SHORT TP {profit_pct:+.2f}%"
                logger.info(f"🟢 {symbol}: SHORT TAKE PROFIT! Price {current_price:.6f} <= TP {take_profit:.6f}")
        
        if exit_reason:
            logger.info(f"🚪 EXECUTING EXIT for {symbol}: {exit_reason}")
            if safe_close_position(symbol, exit_reason):
                logger.info(f"✅ Successfully closed {symbol}")
            else:
                logger.error(f"❌ Failed to close {symbol}")
        else:
            # Логируем текущее состояние
            if is_long_position:
                sl_distance_pct = ((current_price - stop_loss) / current_price) * 100
                tp_distance_pct = ((take_profit - current_price) / current_price) * 100
            else:
                sl_distance_pct = ((stop_loss - current_price) / current_price) * 100
                tp_distance_pct = ((current_price - take_profit) / current_price) * 100
                
            logger.info(f"📊 {symbol}: PnL={profit_pct:+.2f}%, to SL={sl_distance_pct:.2f}%, to TP={tp_distance_pct:.2f}%")
            
    except Exception as e:
        logger.error(f"❌ Scalping exit check error {symbol}: {e}")

def check_swing_exit(symbol: str, pos: Dict):
    """Проверка выхода для свинг-трейдинга с исправленной логикой получения настроек"""
    try:
        # ИСПРАВЛЕНИЕ: используем get_current_settings() вместо TRADING_MODES
        settings = get_current_settings()
        
        ohlcv = fetch_ohlcv(symbol, "15m", limit=20)
        if not ohlcv:
            logger.warning(f"❌ No OHLCV data for {symbol}")
            return
            
        df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume']).astype(float)
        current_price = df['close'].iloc[-1]
        open_price = pos['open_price']
        stop_loss = pos['stop_loss']
        take_profit = pos['take_profit']
        max_price = pos.get('max_price', open_price)
        
        # Детальный логирование
        logger.info(f"🔍 {symbol}: Current={current_price:.6f}, Entry={open_price:.6f}")
        logger.info(f"🎯 {symbol}: SL={stop_loss:.6f}, TP={take_profit:.6f}, Max={max_price:.6f}")
        
        profit_pct = (current_price - open_price) / open_price * 100
        
        exit_reason = ""
        
        # Определяем тип позиции
        is_long_position = take_profit > open_price
        
        if is_long_position:
            # LONG позиция
            if current_price <= stop_loss:
                exit_reason = f"SWING SL {profit_pct:+.2f}%"
                logger.info(f"🔴 {symbol}: SWING STOP LOSS! Price {current_price:.6f} <= SL {stop_loss:.6f}")
            elif current_price >= take_profit:
                exit_reason = f"SWING TP {profit_pct:+.2f}%"
                logger.info(f"🟢 {symbol}: SWING TAKE PROFIT! Price {current_price:.6f} >= TP {take_profit:.6f}")
            elif 'trailing_start' in settings and profit_pct >= settings['trailing_start'] * 100:
                # Безопасный доступ к trailing параметрам
                trailing_start = settings.get('trailing_start', 0.005)
                trailing_step = settings.get('trailing_step', 0.002)
                trail_level = max_price * (1 - trailing_step)
                if current_price <= trail_level:
                    exit_reason = f"TRAILING STOP {profit_pct:+.2f}%"
                    logger.info(f"🔄 {symbol}: TRAILING STOP! Price {current_price:.6f} <= Trail {trail_level:.6f}")
            else:
                # Безопасный доступ к max_stop_loss
                max_stop_loss_pct = settings.get('max_stop_loss', 0.01) * 100
                if profit_pct <= -max_stop_loss_pct * 1.5:
                    exit_reason = f"EMERGENCY EXIT {profit_pct:+.2f}%"
                    logger.info(f"🚨 {symbol}: EMERGENCY EXIT! Loss {profit_pct:+.2f}%")
        else:
            # SHORT позиция
            if current_price >= stop_loss:
                exit_reason = f"SWING SL {profit_pct:+.2f}%"
                logger.info(f"🔴 {symbol}: SWING STOP LOSS! Price {current_price:.6f} >= SL {stop_loss:.6f}")
            elif current_price <= take_profit:
                exit_reason = f"SWING TP {profit_pct:+.2f}%"
                logger.info(f"🟢 {symbol}: SWING TAKE PROFIT! Price {current_price:.6f} <= TP {take_profit:.6f}")
        
        if exit_reason:
            logger.info(f"🚪 EXECUTING EXIT for {symbol}: {exit_reason}")
            if safe_close_position(symbol, exit_reason):
                logger.info(f"✅ Successfully closed {symbol}")
            else:
                logger.error(f"❌ Failed to close {symbol}")
        else:
            # Логируем состояние с безопасным доступом к настройкам
            if is_long_position:
                sl_distance_pct = ((current_price - stop_loss) / current_price) * 100
                tp_distance_pct = ((take_profit - current_price) / current_price) * 100
            else:
                sl_distance_pct = ((stop_loss - current_price) / current_price) * 100
                tp_distance_pct = ((current_price - take_profit) / current_price) * 100
                
            logger.info(f"📊 {symbol}: PnL={profit_pct:+.2f}%, to SL={sl_distance_pct:.2f}%, to TP={tp_distance_pct:.2f}%")
            
    except Exception as e:
        logger.error(f"❌ Swing exit check error {symbol}: {e}")

def close_unprofitable_positions():
    """Принудительное закрытие убыточных позиций для освобождения средств"""
    try:
        positions = get_open_positions()
        if not positions:
            return
            
        logger.info(f"🔍 Checking {len(positions)} positions for forced closing")
        
        closed_count = 0
        for symbol, pos in positions.items():
            try:
                current_price = get_current_price(symbol)
                if current_price is None:
                    continue
                    
                # Рассчитываем PnL
                pnl_percent = (current_price - pos['open_price']) / pos['open_price'] * 100
                
                # Закрываем позиции с убытком более 2%
                if pnl_percent < -2.0:
                    logger.info(f"🔴 Closing unprofitable position {symbol}: PnL {pnl_percent:.2f}%")
                    if safe_close_position(symbol, f"UNPROFITABLE {pnl_percent:.2f}%"):
                        closed_count += 1
                        time.sleep(1)  # Пауза между закрытиями
                        
            except Exception as e:
                logger.error(f"Error checking position {symbol}: {e}")
                continue
                
        if closed_count > 0:
            logger.info(f"✅ Closed {closed_count} unprofitable positions")
            return True
        else:
            logger.info("✅ No unprofitable positions to close")
            return False
            
    except Exception as e:
        logger.error(f"Error in close_unprofitable_positions: {e}")
        return False

def check_position_exits():
    """Проверка условий выхода с принудительным закрытием при нехватке средств"""
    positions = get_open_positions()
    logger.info(f"🔍 Checking exits for {len(positions)} positions: {list(positions.keys())}")
    
    # Если позиций слишком много и мало USDT, закрываем некоторые
    available_usdt = compute_available_usdt()
    if len(positions) >= 5 and available_usdt < 10.0:
        logger.warning(f"⚠️ Too many positions ({len(positions)}) with low USDT ({available_usdt:.2f}), closing some...")
        close_unprofitable_positions()
        return
    
    # ТЕСТИРУЕМ настройки перед проверкой
    try:
        settings = get_current_settings()
        logger.info(f"✅ Exit check settings: max_stop_loss={settings.get('max_stop_loss')}, take_profit={settings.get('take_profit')}")
    except Exception as e:
        logger.error(f"❌ Failed to get settings for exit check: {e}")
        return
    
    for symbol, pos in positions.items():
        try:
            logger.info(f"📊 Analyzing {symbol}: mode={pos.get('trading_mode')}, entry={pos['open_price']}")
            
            if pos.get('trading_mode') == 'SCALPING':
                check_scalping_exit(symbol, pos)
            else:
                check_swing_exit(symbol, pos)
                
        except Exception as e:
            logger.error(f"❌ Exit check error {symbol}: {e}")

def update_max_price_db(symbol: str, price: float):
    """Обновление максимальной цены"""
    db.execute("UPDATE positions SET max_price=? WHERE symbol=? AND status='OPEN'", (price, symbol))

# ====== COOLDOWN AND LIMITS ======
def is_in_cooldown(symbol: str) -> bool:
    """Проверка кудоуна с полным исправлением"""
    try:
        # Получаем настройки для текущего режима
        if CURRENT_MODE == "SCALPING":
            cooldown_period = SCALPING_GLOBAL.get('cooldown', 15)
        else:
            settings = TRADING_MODES.get(CURRENT_MODE, TRADING_MODES["CONSERVATIVE"])
            cooldown_period = settings.get('cooldown', 300)
        
        # Получаем время последнего закрытия
        row = db.fetchone("SELECT last_closed_ts FROM symbol_cooldown WHERE symbol=?", (symbol,))
        
        if not row or not row[0] or row[0] == 0:
            return False  # Нет записи о кудоуне
            
        last_closed = int(row[0])
        current_time = int(time.time())
        
        # Проверяем, истек ли кудоун
        time_since_last = current_time - last_closed
        is_in_cooldown = time_since_last < cooldown_period
        
        if is_in_cooldown:
            remaining = cooldown_period - time_since_last
            logger.debug(f"Symbol {symbol} in cooldown, {remaining}s remaining")
        else:
            logger.debug(f"Symbol {symbol} cooldown finished")
            
        return is_in_cooldown
        
    except Exception as e:
        logger.error(f"Error checking cooldown for {symbol}: {e}")
        # В случае ошибки разрешаем торговлю
        return False

def check_daily_trade_limit(symbol: str) -> bool:
    """Проверка дневного лимита trades с исправлениями"""
    try:
        # Получаем настройки лимитов
        if CURRENT_MODE == "SCALPING":
            max_daily_trades = SCALPING_GLOBAL.get('max_daily_trades_per_symbol', 25)
        else:
            settings = TRADING_MODES.get(CURRENT_MODE, TRADING_MODES["CONSERVATIVE"])
            max_daily_trades = settings.get('max_daily_trades_per_symbol', 5)
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        row = db.fetchone("SELECT daily_trade_count, last_trade_date FROM symbol_cooldown WHERE symbol=?", (symbol,))
        
        if not row:
            return True  # Нет записей - лимит не превышен
            
        daily_count, last_date = row
        
        # Если последняя дата не сегодня, сбрасываем счетчик
        if last_date != today:
            db.execute("UPDATE symbol_cooldown SET daily_trade_count=0, last_trade_date=? WHERE symbol=?", (today, symbol))
            return True
        
        # Проверяем не превышен ли лимит
        return daily_count < max_daily_trades
        
    except Exception as e:
        logger.error(f"Error checking daily trade limit for {symbol}: {e}")
        return True  # В случае ошибки разрешаем торговлю

def update_daily_trade_count(symbol: str):
    """Обновление счетчика дневных trades"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    row = db.fetchone("SELECT daily_trade_count, last_trade_date FROM symbol_cooldown WHERE symbol=?", (symbol,))
    
    if not row:
        db.execute("INSERT INTO symbol_cooldown (symbol, daily_trade_count, last_trade_date) VALUES (?, 1, ?)", (symbol, today))
    else:
        daily_count, last_date = row
        if last_date == today:
            db.execute("UPDATE symbol_cooldown SET daily_trade_count=daily_trade_count+1 WHERE symbol=?", (symbol,))
        else:
            db.execute("UPDATE symbol_cooldown SET daily_trade_count=1, last_trade_date=? WHERE symbol=?", (today, symbol))

def compute_equity() -> float:
    """Расчет общего капитала"""
    bal = fetch_balance()
    if not bal:
        return 0.0
        
    usdt_free = float(bal['free'].get('USDT', 0) or 0)
    total = usdt_free
    
    positions = get_open_positions()
    for symbol, pos in positions.items():
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
            total += pos['base_amount'] * current_price
        except Exception as e:
            logger.error(f"Error calculating equity for {symbol}: {e}")
            continue
            
    return total

def get_concurrent_trades_count() -> int:
    """Получение количества открытых trades"""
    row = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='OPEN'")
    return row[0] if row else 0

def can_open_new_trade() -> bool:
    """Проверка возможности открытия нового trade с безопасным доступом к настройкам"""
    try:
        settings = get_current_settings()
        max_trades = settings.get('max_trades', 5)
        current_trades = get_concurrent_trades_count()
        
        can_open = current_trades < max_trades
        
        if not can_open:
            logger.debug(f"Cannot open new trade: {current_trades}/{max_trades} positions open")
            
        return can_open
        
    except Exception as e:
        logger.error(f"Error in can_open_new_trade: {e}")
        return False  # В случае ошибки запрещаем открытие новых сделок

# ====== SCALPING FUNCTIONS ======
def check_scalping_daily_limits() -> bool:
    """Проверка дневных лимитов для скальпинга"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    row = db.fetchone("SELECT daily_pnl, total_trades, consecutive_losses FROM daily_limits WHERE date=?", (today,))
    
    if not row:
        db.execute("INSERT INTO daily_limits (date, daily_pnl, total_trades, consecutive_losses) VALUES (?, 0, 0, 0)", (today,))
        return True
    
    daily_pnl, total_trades, consecutive_losses = row
    
    if daily_pnl >= SCALPING_GLOBAL['profit_target_daily']:
        logger.info(f"Daily profit target reached: {daily_pnl:.2%}")
        return False
    
    if daily_pnl <= SCALPING_GLOBAL['loss_limit_daily']:
        logger.info(f"Daily loss limit reached: {daily_pnl:.2%}")
        return False
    
    if consecutive_losses >= SCALPING_GLOBAL['max_consecutive_losses']:
        logger.info(f"Max consecutive losses reached: {consecutive_losses}")
        return False
    
    return True

def update_scalping_daily_pnl(pnl_percent: float):
    """Обновление дневного PnL для скальпинга"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    row = db.fetchone("SELECT daily_pnl, consecutive_losses FROM daily_limits WHERE date=?", (today,))
    
    if row:
        current_pnl, current_losses = row
        new_pnl = current_pnl + pnl_percent
        
        if pnl_percent > 0:
            db.execute("UPDATE daily_limits SET daily_pnl=?, consecutive_losses=0 WHERE date=?", (new_pnl, today))
        else:
            new_losses = current_losses + 1
            db.execute("UPDATE daily_limits SET daily_pnl=?, consecutive_losses=? WHERE date=?", (new_pnl, new_losses, today))

# ====== POSITION CLEANUP ======
def cleanup_duplicate_positions():
    """Очистка дублирующих позиций"""
    try:
        # Находим символы с несколькими открытыми позициями
        rows = db.fetchall("""
            SELECT symbol, COUNT(*) as cnt 
            FROM positions 
            WHERE status='OPEN' 
            GROUP BY symbol 
            HAVING cnt > 1
        """)
        
        cleaned_count = 0
        for row in rows:
            symbol, count = row
            logger.warning(f"Found {count} open positions for {symbol}, cleaning...")
            
            # Оставляем только самую новую позицию
            db.execute("""
                UPDATE positions SET status='CLOSED', close_time=datetime('now'), exit_reason='DUPLICATE_CLEANUP'
                WHERE symbol=? AND status='OPEN' AND id NOT IN (
                    SELECT id FROM positions 
                    WHERE symbol=? AND status='OPEN' 
                    ORDER BY open_time DESC 
                    LIMIT 1
                )
            """, (symbol, symbol))
            
            cleaned_count += (count - 1)
        
        if cleaned_count > 0:
            logger.info(f"Cleaned {cleaned_count} duplicate positions")
        
        return cleaned_count
        
    except Exception as e:
        logger.error(f"Error cleaning duplicate positions: {e}")
        return 0

def import_existing_positions():
    """Импорт существующих позиций с проверкой дубликатов"""
    logger.info("Importing existing positions from exchange...")
    
    try:
        balance = fetch_balance()
        imported_count = 0
        
        current_positions = get_open_positions()
        
        for symbol in active_symbols:
            base_currency = symbol.split('/')[0]
            base_balance = float(balance.get('total', {}).get(base_currency, 0) or 0)
            
            # Пропускаем нулевые балансы
            if base_balance <= 0:
                continue
                
            # Если позиция уже есть в БД, пропускаем
            if symbol in current_positions:
                logger.debug(f"Position {symbol} already in database, skipping import")
                continue
            
            try:
                ticker = exchange.fetch_ticker(symbol)
                current_price = float(ticker['last'])
                
                # Рассчитываем примерную цену входа (текущая цена как приближение)
                open_price = current_price
                
                # Используем настройки скальпинга для новых позиций
                strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
                stop_loss = open_price * (1 - strategy_config['max_stop_loss'])
                take_profit = open_price * (1 + strategy_config['take_profit'])
                
                # Проверяем минимальный размер ордера
                order_value = base_balance * current_price
                min_order_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
                
                if order_value < min_order_value:
                    logger.info(f"Skipping {symbol}: order value {order_value:.2f} < min {min_order_value:.2f}")
                    continue
                
                record_open_position(symbol, base_balance, open_price, stop_loss, take_profit, CURRENT_SCALPING_STRATEGY)
                imported_count += 1
                logger.info(f"Imported position: {symbol} - {base_balance:.6f} @ {open_price:.6f}")
                
            except Exception as e:
                logger.error(f"Error importing position for {symbol}: {e}")
                continue
        
        logger.info(f"Import completed: {imported_count} new positions imported")
        return imported_count
        
    except Exception as e:
        logger.error(f"Error importing existing positions: {e}")
        return 0

def sync_balance_before_close(symbol: str) -> bool:
    """Синхронизация баланса перед закрытием позиции"""
    try:
        # Получаем реальный баланс с биржи
        bal = fetch_balance()
        base = symbol.split("/")[0]
        real_balance = float(bal['free'].get(base, 0) or 0)
        
        # Получаем баланс из базы данных
        position = get_position_info(symbol)
        if not position:
            return False
            
        db_balance = position['base_amount']
        
        logger.info(f"🔄 Balance sync for {symbol}: DB={db_balance:.6f}, Real={real_balance:.6f}")
        
        # Если балансы сильно отличаются, обновляем БД
        if abs(real_balance - db_balance) > 0.000001:
            logger.warning(f"📊 Balance mismatch for {symbol}: updating DB {db_balance:.6f} -> {real_balance:.6f}")
            db.execute("UPDATE positions SET base_amount=? WHERE symbol=? AND status='OPEN'", 
                      (real_balance, symbol))
            return True
            
        return real_balance > 0
        
    except Exception as e:
        logger.error(f"Error syncing balance for {symbol}: {e}")
        return False

def auto_sync_positions():
    """Автоматическая синхронизация позиций при запуске"""
    try:
        positions = get_open_positions()
        if not positions:
            return
            
        logger.info(f"🔄 Auto-syncing {len(positions)} positions on startup")
        
        for symbol in positions.keys():
            sync_balance_before_close(symbol)
            
        logger.info("✅ Auto-sync completed")
        
    except Exception as e:
        logger.error(f"Error in auto-sync: {e}")

# ====== TELEGRAM COMMANDS ======
def start(update, context):
    """Команда старта"""
    welcome_msg = """
🤖 <b>UNIVERSAL TRADING BOT</b>

<b>Доступные режимы:</b>
🟢 <b>АГРЕССИВНЫЙ</b> - Максимальная активность, больше сделок
🟡 <b>КОНСЕРВАТИВНЫЙ</b> - Меньше сделок, выше качество
🔴 <b>СКАЛЬПИНГ</b> - Быстрые сделки с разными стратегиями

<b>Основные команды:</b>
/status - Текущий статус
/stats - Детальная статистика
/close SYMBOL - Закрыть позицию
/close_all - Закрыть все позиции
/close_unprofitable - Закрыть убыточные позиции
/mode - Сменить режим

Используйте кнопки ниже для управления ботом!
    """
    update.message.reply_text(welcome_msg, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

def handle_message(update, context):
    """Обработка текстовых сообщений"""
    text = update.message.text
    
    if text == "🟢 АГРЕССИВНЫЙ":
        switch_mode("AGGRESSIVE", update)
    elif text == "🟡 КОНСЕРВАТИВНЫЙ":
        switch_mode("CONSERVATIVE", update)
    elif text == "🔴 СКАЛЬПИНГ":
        show_scalping_menu(update)
    elif text == "📊 СТАТУС":
        cmd_status(update, context)
    elif text == "📈 СТАТИСТИКА":
        cmd_stats(update, context)
    elif text == "⚙️ НАСТРОЙКИ":
        show_settings_menu(update)
    elif text in ["🎯 BB Squeeze", "🚀 EMA Momentum", "📊 VWAP Bounce", "💥 Breakout"]:
        handle_scalping_strategy(text, update)
    elif text == "🔙 НАЗАД":
        update.message.reply_text("Главное меню:", reply_markup=get_main_keyboard())
    elif text == "🔄 СИНХРОНИЗАЦИЯ":
        cmd_force_sync(update, context)
    elif text == "📥 ИМПОРТ":
        cmd_import(update, context)
    elif text == "🔧 ДЕБАГ":
        cmd_debug_settings(update, context)

def switch_mode(mode: str, update):
    """Смена режима торговли с сбросом кэша настроек"""
    global CURRENT_MODE, _settings_cache
    
    # СБРАСЫВАЕМ кэш настроек при смене режима
    _settings_cache = {}
    
    CURRENT_MODE = mode
    
    mode_info = TRADING_MODES[mode]
    msg = f"✅ Режим изменен: <b>{mode_info['name']}</b>\n\n"
    
    # Принудительно получаем свежие настройки
    settings = get_current_settings()
    
    if mode == "SCALPING":
        msg += f"📊 Активная стратегия: <b>{SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]['name']}</b>\n"
        msg += f"⏱ Интервал сканирования: {settings['scan_interval']}с\n"
        msg += f"🔢 Макс сделок: {settings['max_trades']}\n"
        msg += f"💰 Размер позиции: {settings['trade_pct']*100}%"
        update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_scalping_keyboard())
    else:
        msg += f"⏱ Интервал сканирования: {settings['scan_interval']}с\n"
        msg += f"🔢 Макс сделок: {settings['max_trades']}\n"
        msg += f"💰 Размер позиции: {settings['trade_pct']*100}%\n"
        msg += f"🎯 TP/SL: +{settings['take_profit']*100:.1f}%/ -{settings['max_stop_loss']*100:.1f}%\n"
        msg += f"📊 RSI диапазон: {settings['rsi_min']}-{settings['rsi_max']}"
        update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

def show_scalping_menu(update):
    """Показать меню скальпинга"""
    global CURRENT_MODE
    CURRENT_MODE = "SCALPING"
    
    current_strategy = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
    
    msg = f"🔴 <b>РЕЖИМ СКАЛЬПИНГА</b>\n\n"
    msg += f"📊 Активная стратегия: <b>{current_strategy['name']}</b>\n"
    msg += f"⏱ Таймфрейм: {current_strategy['timeframe_entry']}\n"
    msg += f"🎯 TP/SL: +{current_strategy['take_profit']*100:.1f}%/ -{current_strategy['max_stop_loss']*100:.1f}%\n"
    msg += f"🔢 Макс сделок: {current_strategy['max_trades']}\n\n"
    msg += "Выберите стратегию скальпинга:"
    
    update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_scalping_keyboard())

def handle_scalping_strategy(text: str, update):
    """Обработка выбора стратегии скальпинга"""
    global CURRENT_SCALPING_STRATEGY
    
    strategy_map = {
        "🎯 BB Squeeze": "BB_SQUEEZE",
        "🚀 EMA Momentum": "EMA_MOMENTUM", 
        "📊 VWAP Bounce": "VWAP_BOUNCE",
        "💥 Breakout": "BREAKOUT"
    }
    
    if text in strategy_map:
        CURRENT_SCALPING_STRATEGY = strategy_map[text]
        strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
        
        msg = f"✅ Стратегия скальпинга изменена: <b>{strategy_config['name']}</b>\n\n"
        msg += f"⏱ Таймфрейм: {strategy_config['timeframe_entry']}\n"
        msg += f"🎯 TP: +{strategy_config['take_profit']*100:.1f}% | SL: -{strategy_config['max_stop_loss']*100:.1f}%\n"
        msg += f"⚡ Быстрый выход: +{strategy_config['quick_exit']*100:.1f}%\n"
        msg += f"📊 RSI диапазон: {strategy_config['rsi_range'][0]}-{strategy_config['rsi_range'][1]}\n"
        msg += f"🔢 Макс сделок: {strategy_config['max_trades']}\n"
        msg += f"💰 Размер позиции: {strategy_config['trade_pct']*100}%"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_scalping_keyboard())

def show_settings_menu(update):
    """Показать меню настроек"""
    msg = "⚙️ <b>НАСТРОЙКИ БОТА</b>\n\n"
    msg += "🔄 <b>СИНХРОНИЗАЦИЯ</b> - Синхронизация баланса с БД\n"
    msg += "📥 <b>ИМПОРТ</b> - Импорт существующих позиций\n"
    msg += "🔧 <b>ДЕБАГ</b> - Отладочная информация\n\n"
    msg += "Выберите действие:"
    
    update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_settings_keyboard())
def unrealized_pnl_total() -> float:
    """Общий нереализованный PnL с исправленным расчетом"""
    total = 0.0
    try:
        positions = get_open_positions()
        for sym, pos in positions.items():
            current_price = get_current_price(sym)
            if current_price and pos['open_price'] > 0:
                current_value = current_price * pos['base_amount']
                open_value = pos['open_price'] * pos['base_amount']
                total += (current_value - open_value)
    except Exception as e:
        logger.error(f"Unrealized PnL error: {e}")
    return total

def cmd_status(update, context):
    """Команда статуса с учетом множественных позиций"""
    try:
        equity = compute_equity()
        realized = realized_pnl_total()
        unrealized = unrealized_pnl_total()
        positions = get_open_positions()
        stats = get_trading_stats()
        settings = get_current_settings()
        
        total_pnl = realized + unrealized
        pnl_color = "🟢" if total_pnl >= 0 else "🔴"
        
        current_mode_info = TRADING_MODES[CURRENT_MODE]
        
        msg = f"📊 <b>UNIVERSAL TRADING BOT</b>\n\n"
        msg += f"🎯 <b>Режим:</b> {current_mode_info['name']}\n"
        
        if CURRENT_MODE == "SCALPING":
            msg += f"📈 <b>Стратегия:</b> {SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]['name']}\n"
        
        msg += f"💰 <b>Капитал:</b> {equity:.2f} USDT\n"
        msg += f"📈 <b>P&L:</b> {pnl_color} {total_pnl:+.2f} USDT "
        msg += f"(Реализ: {realized:+.2f} | Нереализ: {unrealized:+.2f})\n"
        msg += f"🔢 <b>Позиции:</b> {len(positions)}/{settings['max_trades']}\n\n"
        
        bal = fetch_balance()
        usdt_free = float(bal.get('free', {}).get('USDT', 0) or 0)
        
        if positions:
            msg += f"📈 <b>Открытые позиции ({len(positions)}):</b>\n"
            for sym in positions.keys():
                try:
                    # Используем средневзвешенную цену для символа
                    avg_position = calculate_weighted_average_position(sym)
                    if not avg_position:
                        continue
                        
                    current_price = get_current_price(sym)
                    if not current_price:
                        continue
                    
                    total_amount = avg_position['base_amount']
                    avg_open_price = avg_position['open_price']
                    position_count = avg_position.get('position_count', 1)
                    
                    profit = (current_price - avg_open_price) / avg_open_price * 100
                    profit_net = profit - ROUNDTRIP_FEE * 100
                    
                    position_value = current_price * total_amount
                    
                    emoji = "🟢" if profit_net > 0 else "🔴"
                    base_currency = sym.split('/')[0]
                    
                    msg += f"{emoji} <b>{sym}</b> [{CURRENT_MODE}]"
                    if position_count > 1:
                        msg += f" (x{position_count})"
                    msg += f"\n"
                    msg += f"   Кол-во: {total_amount:.4f} {base_currency}\n"
                    msg += f"   Ср.вход: {avg_open_price:.6f} | Текущ: {current_price:.6f}\n"
                    msg += f"   P&L: {profit_net:+.2f}% | Стоимость: {position_value:.2f} USDT\n"
                    
                except Exception as e:
                    logger.error(f"Error processing position {sym}: {e}")
                    msg += f"❌ <b>{sym}</b> - Ошибка получения данных\n"
        else:
            msg += "📭 <b>Нет открытых позиций</b>\n"
        
        msg += f"\n💸 <b>Доступно:</b> {usdt_free:.2f} USDT"
        
        if update and hasattr(update, 'message'):
            update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        else:
            safe_send(msg)
        
    except Exception as e:
        logger.error(f"Error in cmd_status: {e}")
        error_msg = f"❌ Ошибка генерации статуса: {str(e)}"
        if update and hasattr(update, 'message'):
            update.message.reply_text(error_msg)
        else:
            safe_send(error_msg)

def realized_pnl_total() -> float:
    """Общий реализованный PnL"""
    try:
        row = db.fetchone("SELECT SUM(pnl) FROM positions WHERE status='CLOSED' AND pnl IS NOT NULL")
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        logger.error(f"Realized PnL error: {e}")
        return 0.0

def realized_pnl_total() -> float:
    """Общий реализованный PnL с исправленным расчетом"""
    try:
        row = db.fetchone("""
            SELECT SUM(pnl) 
            FROM positions 
            WHERE status='CLOSED' AND pnl IS NOT NULL
        """)
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        logger.error(f"Realized PnL error: {e}")
        return 0.0


def get_trading_stats() -> Dict[str, Any]:
    """Статистика trading с исправленными расчетами"""
    try:
        # Общая статистика по всем сделкам
        stats_row = db.fetchone("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(usdt_amount) as total_volume,
                SUM(fee) as total_fees
            FROM trade_history
        """)
        
        # Статистика по закрытым позициям
        trades_row = db.fetchone("""
            SELECT 
                COUNT(*) as closed_trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                AVG(CASE WHEN pnl > 0 THEN pnl_percent ELSE NULL END) as avg_win_pct,
                AVG(CASE WHEN pnl <= 0 THEN pnl_percent ELSE NULL END) as avg_loss_pct,
                SUM(pnl) as total_pnl,
                SUM(base_amount * open_price) as total_invested
            FROM positions 
            WHERE status='CLOSED' AND pnl IS NOT NULL
        """)
        
        total_pnl = float(trades_row[4]) if trades_row and trades_row[4] else 0.0
        total_invested = float(trades_row[5]) if trades_row and trades_row[5] else 0.0
        
        # Правильный расчет общей доходности
        total_return = (total_pnl / total_invested * 100) if total_invested > 0 else 0
        
        stats = {
            'total_trades': stats_row[0] if stats_row else 0,
            'total_volume': float(stats_row[1]) if stats_row and stats_row[1] else 0,
            'total_fees': float(stats_row[2]) if stats_row and stats_row[2] else 0,
            'closed_trades': trades_row[0] if trades_row else 0,
            'winning_trades': trades_row[1] if trades_row else 0,
            'avg_win_pct': float(trades_row[2]) if trades_row and trades_row[2] else 0,
            'avg_loss_pct': float(trades_row[3]) if trades_row and trades_row[3] else 0,
            'total_pnl': total_pnl,
            'total_return': total_return,
            'total_invested': total_invested
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return {
            'total_trades': 0,
            'total_volume': 0,
            'total_fees': 0,
            'closed_trades': 0,
            'winning_trades': 0,
            'avg_win_pct': 0,
            'avg_loss_pct': 0,
            'total_pnl': 0,
            'total_return': 0,
            'total_invested': 0
        }

def cmd_stats(update, context):
    """Детальная статистика с исправленными расчетами"""
    stats = get_trading_stats()
    equity = compute_equity()
    realized = realized_pnl_total()
    unrealized = unrealized_pnl_total()
    
    msg = f"📈 <b>Детальная статистика</b>\n\n"
    
    closed_trades = stats.get('closed_trades', 0)
    if closed_trades > 0:
        winning_trades = stats.get('winning_trades', 0)
        win_rate = (winning_trades / closed_trades) * 100
        
        msg += f"📊 <b>Производительность:</b>\n"
        msg += f"• Винрейт: {win_rate:.1f}% ({winning_trades}/{closed_trades})\n"
        msg += f"• Средняя прибыль: {stats.get('avg_win_pct', 0):.2f}%\n"
        msg += f"• Средний убыток: {stats.get('avg_loss_pct', 0):.2f}%\n"
        msg += f"• Общая доходность: {stats.get('total_return', 0):.2f}%\n"
        msg += f"• Реализованный P&L: {stats.get('total_pnl', 0):.2f} USDT\n"
        msg += f"• Нереализованный P&L: {unrealized:+.2f} USDT\n"
        msg += f"• Общий P&L: {realized + unrealized:+.2f} USDT\n"
        msg += f"• Всего сделок: {stats['total_trades']}\n"
        msg += f"• Объем торгов: {stats['total_volume']:.0f} USDT\n"
        msg += f"• Комиссии: {stats.get('total_fees', 0):.2f} USDT\n\n"
    else:
        msg += f"📊 <b>Производительность:</b>\n"
        msg += f"• Нет закрытых сделок для статистики\n\n"
    
    # Текущие настройки
    settings = get_current_settings()
    current_mode_info = TRADING_MODES[CURRENT_MODE]
    
    msg += f"⚙️ <b>Текущие настройки:</b>\n"
    msg += f"• Режим: {current_mode_info['name']}\n"
    
    if CURRENT_MODE == "SCALPING":
        strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
        msg += f"• Стратегия: {strategy_config['name']}\n"
        msg += f"• TP/SL: +{strategy_config['take_profit']*100:.1f}%/ -{strategy_config['max_stop_loss']*100:.1f}%\n"
        msg += f"• Размер позиции: {strategy_config['trade_pct']*100}%\n"
    else:
        take_profit = settings.get('take_profit', 0.02)
        max_stop_loss = settings.get('max_stop_loss', 0.01)
        trade_pct = settings.get('trade_pct', 0.1)
        
        msg += f"• TP/SL: +{take_profit*100:.1f}%/ -{max_stop_loss*100:.1f}%\n"
        msg += f"• Размер позиции: {trade_pct*100}%\n"
        msg += f"• RSI диапазон: {settings.get('rsi_min', 40)}-{settings.get('rsi_max', 65)}\n"
    
    update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

def cmd_close(update, context):
    """Закрытие позиции"""
    try:
        if not context.args:
            update.message.reply_text("Использование: /close SYMBOL")
            return
            
        symbol = context.args[0].upper() + "/USDT" if "/" not in context.args[0].upper() else context.args[0].upper()
        if safe_close_position(symbol, "Manual"):
            update.message.reply_text(f"✅ Закрыта {symbol}")
        else:
            update.message.reply_text(f"❌ Ошибка закрытия {symbol}")
    except Exception as e:
        update.message.reply_text(f"Ошибка: {str(e)}")

def cmd_close_all(update, context):
    """Немедленное закрытие всех позиций"""
    try:
        positions = get_open_positions()
        if not positions:
            update.message.reply_text("❌ Нет открытых позиций")
            return
            
        msg = f"🚪 Закрываю {len(positions)} позиций:\n"
        for symbol in positions.keys():
            msg += f"• {symbol}\n"
        
        update.message.reply_text(msg)
        
        closed_count = 0
        failed_count = 0
        
        for symbol in list(positions.keys()):
            if safe_close_position(symbol, "FORCED CLOSE ALL"):
                closed_count += 1
                time.sleep(1)  # Пауза между закрытиями
            else:
                failed_count += 1
        
        result_msg = f"✅ Результат закрытия:\n"
        result_msg += f"• Успешно: {closed_count}\n"
        result_msg += f"• Не удалось: {failed_count}\n"
        result_msg += f"• Всего: {len(positions)}"
        
        update.message.reply_text(result_msg)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_close_unprofitable(update, context):
    """Закрытие убыточных позиций"""
    try:
        if close_unprofitable_positions():
            update.message.reply_text("✅ Убыточные позиции закрыты")
        else:
            update.message.reply_text("✅ Нет убыточных позиций для закрытия")
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_force_sync(update, context):
    """Принудительная синхронизация всех позиций"""
    try:
        positions = get_open_positions()
        if not positions:
            update.message.reply_text("❌ Нет открытых позиций")
            return
            
        synced_count = 0
        closed_count = 0
        
        for symbol in positions.keys():
            if sync_balance_before_close(symbol):
                synced_count += 1
            else:
                # Если синхронизация не удалась, закрываем позицию
                mark_position_closed(symbol)
                closed_count += 1
                
        msg = f"🔄 Синхронизация завершена:\n"
        msg += f"• Синхронизировано: {synced_count} позиций\n"
        msg += f"• Закрыто (не найдено): {closed_count} позиций\n"
        msg += f"• Всего обработано: {len(positions)} позиций"
        
        update.message.reply_text(msg)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка синхронизации: {str(e)}")

def cmd_import(update, context):
    """Импорт позиций"""
    imported_count = import_existing_positions()
    if imported_count > 0:
        update.message.reply_text(f"📥 Импортировано {imported_count} позиций", reply_markup=get_main_keyboard())
    else:
        update.message.reply_text("✅ Нет новых позиций для импорта", reply_markup=get_main_keyboard())

def cmd_debug_settings(update, context):
    """Отладка текущих настроек"""
    try:
        settings = get_current_settings()
        
        msg = f"⚙️ <b>ТЕКУЩИЕ НАСТРОЙКИ</b>\n\n"
        msg += f"🎯 <b>Режим:</b> {CURRENT_MODE}\n"
        
        if CURRENT_MODE == "SCALPING":
            msg += f"📊 <b>Стратегия:</b> {settings['name']}\n"
        
        msg += f"\n<b>Основные параметры:</b>\n"
        msg += f"• Интервал сканирования: {settings['scan_interval']}с\n"
        msg += f"• Макс сделок: {settings['max_trades']}\n"
        msg += f"• Размер позиции: {settings['trade_pct']*100}%\n"
        
        msg += f"\n<b>Параметры выхода:</b>\n"
        msg += f"• Stop Loss: {settings.get('max_stop_loss', 'N/A')*100:.1f}%\n"
        msg += f"• Take Profit: {settings.get('take_profit', 'N/A')*100:.1f}%\n"
        msg += f"• Trailing Start: {settings.get('trailing_start', 'N/A')*100:.1f}%\n"
        msg += f"• Trailing Step: {settings.get('trailing_step', 'N/A')*100:.1f}%\n"
        
        msg += f"\n<b>Параметры входа:</b>\n"
        msg += f"• RSI диапазон: {settings.get('rsi_min', 'N/A')}-{settings.get('rsi_max', 'N/A')}\n"
        msg += f"• Минимальный score: {settings.get('min_score', 'N/A')}\n"
        msg += f"• Множитель объема: {settings.get('volume_multiplier', 'N/A')}x\n"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка отладки настроек: {str(e)}")

def cmd_cleanup_duplicates(update, context):
    """Очистка дублирующих позиций"""
    try:
        cleaned_count = cleanup_duplicate_positions()
        if cleaned_count > 0:
            update.message.reply_text(f"🧹 Очищено {cleaned_count} дублирующих позиций")
        else:
            update.message.reply_text("✅ Дублирующих позиций не найдено")
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка очистки: {str(e)}")

# ====== SWING TRADING LOGIC ======
def get_trend_direction(df: pd.DataFrame) -> str:
    """Определение направления тренда"""
    if df is None or len(df) < 50:
        return "SIDEWAYS"
    
    try:
        ema_fast = EMAIndicator(df['close'], window=9).ema_indicator()
        ema_slow = EMAIndicator(df['close'], window=21).ema_indicator()
        ema_trend = EMAIndicator(df['close'], window=50).ema_indicator()
        
        if len(ema_fast) < 5 or len(ema_slow) < 5 or len(ema_trend) < 5:
            return "SIDEWAYS"
            
        price = df['close'].iloc[-1]
        fast_ema = ema_fast.iloc[-1]
        slow_ema = ema_slow.iloc[-1]
        trend_ema = ema_trend.iloc[-1]
        
        ema_alignment = (price > fast_ema > slow_ema > trend_ema) or (price < fast_ema < slow_ema < trend_ema)
        ema_slope = (ema_fast.iloc[-1] - ema_fast.iloc[-5]) / ema_fast.iloc[-5] if ema_fast.iloc[-5] != 0 else 0
        
        if ema_alignment and abs(ema_slope) > 0.001:
            if price > fast_ema and ema_slope > 0:
                return "BULLISH"
            elif price < fast_ema and ema_slope < 0:
                return "BEARISH"
        
        return "SIDEWAYS"
        
    except Exception as e:
        logger.error(f"Error calculating trend direction: {e}")
        return "SIDEWAYS"

def calculate_swing_signals(df: pd.DataFrame) -> Dict[str, Any]:
    """Расчет сигналов для свинг-трейдинга с улучшенной обработкой ошибок"""
    settings = get_current_settings()
    
    if df is None or len(df) < 30:
        return {'score': 0, 'error': 'Insufficient data', 'trend': 'UNKNOWN'}
    
    try:
        current_price = df['close'].iloc[-1]
        current_volume = df['volume'].iloc[-1]
        volume_sma = df['volume'].tail(20).mean()
        
        # RSI
        rsi = RSIIndicator(df['close'], window=14).rsi().iloc[-1]
        rsi_prev = RSIIndicator(df['close'], window=14).rsi().iloc[-2] if len(df) > 1 else rsi
        
        # MACD
        macd_line = MACD(df['close']).macd().iloc[-1]
        macd_signal = MACD(df['close']).macd_signal().iloc[-1]
        macd_bullish = macd_line > macd_signal
        
        # ADX
        adx = 0
        adx_bullish = False
        adx_strong = False
        if len(df) >= 15:
            try:
                adx_indicator = ADXIndicator(df['high'], df['low'], df['close'], window=14)
                adx = adx_indicator.adx().iloc[-1]
                plus_di = adx_indicator.adx_pos().iloc[-1]
                minus_di = adx_indicator.adx_neg().iloc[-1]
                adx_bullish = plus_di > minus_di
                adx_strong = adx >= settings.get('adx_min', 15)  # Безопасный доступ
            except Exception as e:
                logger.warning(f"ADX calculation error: {e}")
        
        # Bollinger Bands
        bb = BollingerBands(df['close'], window=20, window_dev=2)
        bb_upper = bb.bollinger_hband().iloc[-1]
        bb_lower = bb.bollinger_lband().iloc[-1]
        bb_position = (current_price - bb_lower) / (bb_upper - bb_lower) if bb_upper != bb_lower else 0.5
        
        # Stochastic
        stoch = StochasticOscillator(df['high'], df['low'], df['close'], window=14, smooth_window=3).stoch().iloc[-1]
        
        # Volume analysis
        volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1
        volume_ok = volume_ratio > settings.get('volume_multiplier', 1.2)
        
        # RSI проверка с безопасным доступом
        rsi_min = settings.get('rsi_min', 40)
        rsi_max = settings.get('rsi_max', 65)
        rsi_ok = rsi_min <= rsi <= rsi_max
        
        signals = {
            'price': current_price,
            'volume_ok': volume_ok,
            'volume_ratio': volume_ratio,
            'rsi_ok': rsi_ok,
            'rsi_value': rsi,
            'rsi_trend': 'BULLISH' if rsi > rsi_prev else 'BEARISH',
            'macd_bullish': macd_bullish,
            'adx_strong': adx_strong,
            'adx_value': adx,
            'adx_bullish': adx_bullish,
            'bb_position': bb_position,
            'bb_signal': 0.2 <= bb_position <= 0.8,
            'stoch_ok': 20 <= stoch <= 80,
            'stoch_value': stoch,
            'score': 0,
            'trend': 'UNKNOWN'
        }
        
        # Scoring system
        score = 0
        
        # RSI scoring (30%)
        if signals['rsi_ok']:
            score += 30
            if signals['rsi_trend'] == 'BULLISH':
                score += 5
        elif rsi < 30:
            score += 15  # Oversold but outside range
        elif rsi > 70:
            score += 10  # Overbought but outside range
            
        # Volume scoring (20%)
        if volume_ok:
            score += 20
        elif volume_ratio > 0.8:
            score += 10
            
        # MACD scoring (15%)
        if macd_bullish:
            score += 15
            
        # ADX scoring (15%)
        if signals['adx_strong']:
            score += 15
            if signals['adx_bullish']:
                score += 5
                
        # Bollinger Bands scoring (10%)
        if signals['bb_signal']:
            score += 10
        elif bb_position < 0.2:
            score += 5  # Near lower band
            
        # Stochastic scoring (10%)
        if signals['stoch_ok']:
            score += 10
            
        signals['score'] = score
        
        # Определение тренда с обработкой ошибок
        try:
            signals['trend'] = get_trend_direction(df)
        except Exception as e:
            logger.warning(f"Error calculating trend for signal: {e}")
            signals['trend'] = 'UNKNOWN'
        
        return signals
        
    except Exception as e:
        logger.error(f"Error calculating swing signals: {e}")
        return {'score': 0, 'error': str(e), 'trend': 'UNKNOWN'}

def should_enter_swing_position(symbol: str) -> Tuple[bool, Dict]:
    """Определение входа в свинг-позицию с улучшенной обработкой ошибок"""
    try:
        if not can_open_new_trade():
            return False, {'reason': 'Max trades reached'}
            
        if is_in_cooldown(symbol):
            return False, {'reason': 'Cooldown active'}
            
        if not check_daily_trade_limit(symbol):
            return False, {'reason': 'Daily limit reached'}
            
        df = get_ohlcv_data(symbol, "15m", 100)
        if df is None:
            return False, {'reason': 'No data'}
            
        signals = calculate_swing_signals(df)
        
        # БЕЗОПАСНАЯ проверка min_score
        settings = get_current_settings()
        min_score = settings.get('min_score', 50)  # Значение по умолчанию если ключ отсутствует
        
        if signals.get('score', 0) < min_score:
            return False, {**signals, 'reason': f'Score too low: {signals.get("score", 0)} < {min_score}'}
            
        # Проверяем тренд
        if signals.get('trend') == "BEARISH":
            return False, {**signals, 'reason': 'Bearish trend'}
            
        # Дополнительные проверки для консервативного режима
        if CURRENT_MODE == "CONSERVATIVE":
            if not signals.get('adx_strong', False):
                return False, {**signals, 'reason': 'Weak trend in conservative mode'}
                
        return True, signals
        
    except Exception as e:
        logger.error(f"Error checking swing entry for {symbol}: {e}")
        return False, {'error': str(e), 'reason': 'Check failed'}

def ensure_min_order_size(symbol: str, base_amount: float, current_price: float, available_usdt: float) -> Tuple[float, float]:
    """Гарантирует что размер ордера соответствует минимальным требованиям"""
    min_order_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
    actual_usdt_amount = base_amount * current_price
    
    # Если сумма меньше минимальной, увеличиваем количество
    if actual_usdt_amount < min_order_value:
        logger.warning(f"🔄 Order size too small: {actual_usdt_amount:.2f} < {min_order_value}, increasing...")
        
        # Рассчитываем минимальное необходимое количество
        min_base_amount = min_order_value / current_price
        base_amount = round_amount(symbol, min_base_amount)
        actual_usdt_amount = base_amount * current_price
        
        # Проверяем что не превышаем доступный баланс
        if actual_usdt_amount > available_usdt:
            logger.error(f"❌ Cannot meet min order size: {actual_usdt_amount:.2f} > {available_usdt:.2f}")
            return 0, 0
    
    return base_amount, actual_usdt_amount

def open_swing_position(symbol: str, signals: Dict):
    """Открытие свинг позиции с улучшенным расчетом размера"""
    try:
        settings = get_current_settings()
        current_price = signals['price']
        
        # Расчет размера позиции на основе ДОСТУПНОГО USDT
        available_usdt = compute_available_usdt()
        usdt_amount = available_usdt * settings['trade_pct']
        
        # Гарантируем минимальный размер ордера
        min_order_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
        
        # Если расчетная сумма меньше минимальной, используем минимальную
        if usdt_amount < min_order_value:
            usdt_amount = min_order_value
            
        # Но не превышаем доступный баланс
        if usdt_amount > available_usdt:
            usdt_amount = available_usdt * 0.95  # Оставляем запас
            
        logger.info(f"💰 Available USDT: {available_usdt:.2f}, Order size: {usdt_amount:.2f}")
        
        # Расчет количества с учетом минимальных лимитов
        base_amount = usdt_amount / current_price
        
        # Округляем количество
        base_amount = round_amount(symbol, base_amount)
        
        if base_amount <= 0:
            logger.error(f"Invalid amount for {symbol}: {base_amount}")
            return False
            
        # Пересчитываем фактическую сумму после округления
        actual_usdt_amount = base_amount * current_price
        
        # Если после округления сумма стала меньше минимальной, УВЕЛИЧИВАЕМ количество
        if actual_usdt_amount < min_order_value:
            logger.warning(f"🔄 Amount too small after rounding: {actual_usdt_amount:.2f}, increasing...")
            
            # Рассчитываем минимальное необходимое количество
            min_base_amount = min_order_value / current_price
            base_amount = round_amount(symbol, min_base_amount)
            
            # Пересчитываем сумму
            actual_usdt_amount = base_amount * current_price
            
            # Проверяем что не превышаем доступный баланс
            if actual_usdt_amount > available_usdt:
                logger.error(f"❌ Even min amount too expensive: {actual_usdt_amount:.2f} > {available_usdt:.2f}")
                return False
        
        logger.info(f"📊 Final calculation: {base_amount:.6f} {symbol} = {actual_usdt_amount:.2f} USDT")
        
        # Проверяем лимиты с небольшим допуском
        if actual_usdt_amount < min_order_value - 0.01:
            logger.error(f"❌ Final order value too small: {actual_usdt_amount:.2f} USDT < {min_order_value} USDT")
            return False
            
        # Дальнейшая логика открытия позиции остается без изменений...
        # Расчет TP/SL (только LONG для свинга)
        stop_loss = current_price * (1 - settings['max_stop_loss'])
        take_profit = current_price * (1 + settings['take_profit'])
        
        if DRY_RUN:
            logger.info(f"DRY RUN: Would open {symbol} - {base_amount:.6f} @ {current_price:.6f}")
            record_open_position(symbol, base_amount, current_price, stop_loss, take_profit, "")
            return True
            
        # Проверка баланса
        bal = fetch_balance()
        usdt_free = float(bal['free'].get('USDT', 0) or 0)
        
        if usdt_free < actual_usdt_amount:
            logger.info(f"Insufficient USDT for {symbol}: {usdt_free:.2f} < {actual_usdt_amount:.2f}")
            return False
        
        # Упрощенная проверка минимального количества
        market = exchange.market(symbol)
        min_amount = float(market['limits']['amount']['min'])
        
        if base_amount < min_amount:
            logger.error(f"❌ Amount too small: {base_amount:.6f} < {min_amount}")
            # Пробуем увеличить до минимального количества
            base_amount = min_amount
            actual_usdt_amount = base_amount * current_price
            
            # Проверяем что новая сумма не превышает доступный баланс
            if actual_usdt_amount > available_usdt:
                logger.error(f"❌ Even min amount too expensive: {actual_usdt_amount:.2f} > {available_usdt:.2f}")
                return False
        
        # Создание ордера для Bybit
        logger.info(f"🟢 Opening SWING position: {symbol} {base_amount:.6f} @ {current_price:.6f}")
        
        try:
            order = exchange.create_market_order(symbol, 'buy', base_amount)
            
            logger.info(f"📦 Order response: {order}")
            
            if order and order.get('id'):
                order_id = order['id']
                logger.info(f"📋 Order ID: {order_id}")
                
                time.sleep(3)
                
                # Проверяем исполнение через изменение баланса
                new_bal = fetch_balance()
                base_currency = symbol.split('/')[0]
                new_base_balance = float(new_bal['free'].get(base_currency, 0) or 0)
                old_base_balance = float(bal['free'].get(base_currency, 0) or 0)
                
                logger.info(f"💰 Balance check - Before: {old_base_balance:.6f}, After: {new_base_balance:.6f}")
                
                if new_base_balance > old_base_balance + 0.000001:
                    filled_amount = new_base_balance - old_base_balance
                    
                    try:
                        order_status = exchange.fetch_order(order_id, symbol, params={'acknowledged': True})
                        average_price = float(order_status.get('average', current_price))
                        logger.info(f"✅ Order executed - Amount: {filled_amount:.6f}, Avg Price: {average_price:.6f}")
                    except:
                        average_price = current_price
                        logger.info(f"✅ Order executed (fallback) - Amount: {filled_amount:.6f}, Price: {average_price:.6f}")
                    
                    record_open_position(symbol, filled_amount, average_price, stop_loss, take_profit, "")
                    update_daily_trade_count(symbol)
                    
                    safe_send(
                        f"📈 <b>SWING ENTRY: {symbol}</b>\n"
                        f"Режим: {TRADING_MODES[CURRENT_MODE]['name']}\n"
                        f"Количество: {filled_amount:.6f}\n"
                        f"Цена входа: {average_price:.6f}\n"
                        f"Сумма: {filled_amount * average_price:.2f} USDT\n"
                        f"Stop Loss: {stop_loss:.6f}\n"
                        f"Take Profit: {take_profit:.6f}\n"
                        f"Score: {signals.get('score', 0)}"
                    )
                    
                    logger.info(f"✅ Swing position opened: {symbol} {filled_amount:.6f} @ {average_price:.6f}")
                    return True
                else:
                    logger.error(f"❌ Order not executed - balance unchanged")
                    try:
                        exchange.cancel_order(order_id, symbol)
                        logger.info(f"📝 Order {order_id} cancelled")
                    except:
                        pass
                    return False
            else:
                logger.error(f"❌ Order creation failed: {order}")
                return False
                
        except ccxt.InsufficientFunds as e:
            logger.error(f"Insufficient funds for {symbol}: {e}")
            return False
        except ccxt.InvalidOrder as e:
            logger.error(f"Invalid order for {symbol}: {e}")
            return False
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error for {symbol}: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error opening swing position for {symbol}: {e}")
        safe_send(f"❌ Ошибка открытия свинг позиции {symbol}: {str(e)}")
        return False

# ====== SCALPING LOGIC ======
def get_scalping_signals(symbol: str) -> Dict[str, Any]:
    """Расчет сигналов для скальпинга"""
    strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
    
    try:
        # Данные для входа
        df_entry = get_ohlcv_data(symbol, strategy_config['timeframe_entry'], 50)
        if df_entry is None:
            return {'score': 0, 'error': 'No entry data'}
            
        # Данные для тренда
        df_trend = get_ohlcv_data(symbol, strategy_config['timeframe_trend'], 30)
        if df_trend is None:
            return {'score': 0, 'error': 'No trend data'}
            
        current_price = df_entry['close'].iloc[-1]
        current_volume = df_entry['volume'].iloc[-1]
        volume_sma = df_entry['volume'].tail(20).mean()
        volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1
        volume_ok = volume_ratio >= strategy_config['volume_multiplier']
        
        # RSI
        rsi = RSIIndicator(df_entry['close'], window=14).rsi().iloc[-1]
        rsi_range = strategy_config['rsi_range']
        rsi_ok = rsi_range[0] <= rsi <= rsi_range[1]
        
        signals = {
            'price': current_price,
            'volume_ratio': volume_ratio,
            'volume_ok': volume_ok,
            'rsi_value': rsi,
            'rsi_ok': rsi_ok,
            'score': 0,
            'strategy': CURRENT_SCALPING_STRATEGY
        }

        
        # Стратегия Bollinger Squeeze
        if CURRENT_SCALPING_STRATEGY == "BB_SQUEEZE":
            bb_period = strategy_config['bb_period']
            bb_std = strategy_config['bb_std']
            
            bb = BollingerBands(df_entry['close'], window=bb_period, window_dev=bb_std)
            bb_upper = bb.bollinger_hband().iloc[-1]
            bb_lower = bb.bollinger_lband().iloc[-1]
            bb_middle = bb.bollinger_mavg().iloc[-1]
            
            bb_width = (bb_upper - bb_lower) / bb_middle
            typical_width = 0.02  # 2% типичная ширина
            
            squeeze_ended = bb_width > typical_width and bb_width > (bb_upper - bb_lower) / bb_middle
            
            signals.update({
                'bb_upper': bb_upper,
                'bb_lower': bb_lower,
                'bb_middle': bb_middle,
                'bb_width': bb_width,
                'squeeze_ended': squeeze_ended,
                'price_above_middle': current_price > bb_middle,
                'price_below_middle': current_price < bb_middle
            })
            
            # Scoring для BB Squeeze
            score = 0
            if squeeze_ended and volume_ok:
                score += 40
            if rsi_ok:
                score += 30
            if volume_ok:
                score += 30
                
            signals['score'] = score
            
        # Стратегия EMA Momentum
        elif CURRENT_SCALPING_STRATEGY == "EMA_MOMENTUM":
            ema_fast = EMAIndicator(df_entry['close'], window=strategy_config['ema_fast']).ema_indicator()
            ema_slow = EMAIndicator(df_entry['close'], window=strategy_config['ema_slow']).ema_indicator()
            
            ema_fast_current = ema_fast.iloc[-1]
            ema_slow_current = ema_slow.iloc[-1]
            ema_fast_prev = ema_fast.iloc[-2] if len(ema_fast) > 1 else ema_fast_current
            ema_slow_prev = ema_slow.iloc[-2] if len(ema_slow) > 1 else ema_slow_current
            
            ema_bullish = ema_fast_current > ema_slow_current and ema_fast_current > ema_fast_prev
            ema_bearish = ema_fast_current < ema_slow_current and ema_fast_current < ema_fast_prev
            
            signals.update({
                'ema_fast': ema_fast_current,
                'ema_slow': ema_slow_current,
                'ema_bullish': ema_bullish,
                'ema_bearish': ema_bearish,
                'ema_cross': (ema_fast_prev <= ema_slow_prev and ema_fast_current > ema_slow_current) or 
                            (ema_fast_prev >= ema_slow_prev and ema_fast_current < ema_slow_current)
            })
            
            # Scoring для EMA Momentum
            score = 0
            if ema_bullish and volume_ok:
                score += 50
            elif ema_bearish and volume_ok:
                score += 40
            if rsi_ok:
                score += 30
            if signals['ema_cross']:
                score += 20
                
            signals['score'] = score
            
        # Стратегия VWAP Bounce
        elif CURRENT_SCALPING_STRATEGY == "VWAP_BOUNCE":
            vwap_period = strategy_config['vwap_period']
            vwap = VolumeWeightedAveragePrice(df_entry['high'], df_entry['low'], df_entry['close'], df_entry['volume'], window=vwap_period).volume_weighted_average_price()
            vwap_current = vwap.iloc[-1]
            
            price_vwap_ratio = (current_price - vwap_current) / vwap_current
            bounce_threshold = 0.001  # 0.1%
            
            signals.update({
                'vwap': vwap_current,
                'price_above_vwap': current_price > vwap_current,
                'price_below_vwap': current_price < vwap_current,
                'vwap_distance': abs(price_vwap_ratio),
                'near_vwap': abs(price_vwap_ratio) <= bounce_threshold
            })
            
            # Scoring для VWAP Bounce
            score = 0
            if signals['near_vwap'] and volume_ok:
                score += 60
            if rsi_ok:
                score += 30
            if volume_ok:
                score += 20
                
            signals['score'] = score
            
        # Стратегия Breakout
        elif CURRENT_SCALPING_STRATEGY == "BREAKOUT":
            breakout_period = strategy_config['breakout_period']
            high_breakout = df_entry['high'].tail(breakout_period).max()
            low_breakout = df_entry['low'].tail(breakout_period).min()
            range_size = (high_breakout - low_breakout) / low_breakout
            
            breaking_high = current_price >= high_breakout
            breaking_low = current_price <= low_breakout
            
            signals.update({
                'breakout_high': high_breakout,
                'breakout_low': low_breakout,
                'range_size': range_size,
                'breaking_high': breaking_high,
                'breaking_low': breaking_low,
                'consolidation': range_size < 0.01  # Консолидация менее 1%
            })
            
            # Scoring для Breakout
            score = 0
            if (breaking_high or breaking_low) and volume_ok:
                score += 70
            if signals['consolidation']:
                score += 20
            if rsi_ok:
                score += 20
                
            signals['score'] = score
            
        return signals
        
    except Exception as e:
        logger.error(f"Error calculating scalping signals for {symbol}: {e}")
        return {'score': 0, 'error': str(e)}

def get_scalping_signal(symbol: str) -> Tuple[bool, Dict]:
    """Определение сигнала для скальпинга"""
    try:
        if not can_open_new_trade():
            return False, {'reason': 'Max trades reached'}
            
        if not check_scalping_daily_limits():
            return False, {'reason': 'Daily limits reached'}
            
        if is_in_cooldown(symbol):
            return False, {'reason': 'Cooldown active'}
            
        if not check_daily_trade_limit(symbol):
            return False, {'reason': 'Daily trade limit reached'}
            
        signals = get_scalping_signals(symbol)
        
        if signals['score'] < 70:  # Минимальный score для скальпинга
            return False, signals
            
        return True, signals
        
    except Exception as e:
        logger.error(f"Error checking scalping signal for {symbol}: {e}")
        return False, {'error': str(e)}

def open_scalping_position(symbol: str, signals: Dict):
    """Открытие скальпинг позиции с исправлениями для Bybit"""
    if CURRENT_MODE != "SCALPING":
        logger.error(f"❌ Trying to open scalping position in {CURRENT_MODE} mode")
        return False

    try:
        strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
        current_price = signals['price']
        
        # Расчет размера позиции на основе ДОСТУПНОГО USDT
        available_usdt = compute_available_usdt()
        usdt_amount = available_usdt * strategy_config['trade_pct']
        
        # Гарантируем минимальный размер ордера
        min_order_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
        
        # Если расчетная сумма меньше минимальной, используем минимальную
        if usdt_amount < min_order_value:
            usdt_amount = min_order_value
            
        # Но не превышаем доступный баланс
        if usdt_amount > available_usdt:
            usdt_amount = available_usdt * 0.95  # Оставляем запас
            
        logger.info(f"💰 Available USDT: {available_usdt:.2f}, Order size: {usdt_amount:.2f}")
        
        # Расчет количества с учетом минимальных лимитов
        base_amount = usdt_amount / current_price
        
        # Округляем количество
        base_amount = round_amount(symbol, base_amount)
        
        if base_amount <= 0:
            logger.error(f"Invalid amount for {symbol}: {base_amount}")
            return False
            
        # Пересчитываем фактическую сумму после округления
        actual_usdt_amount = base_amount * current_price
        
        # Если после округления сумма стала меньше минимальной, УВЕЛИЧИВАЕМ количество
        if actual_usdt_amount < min_order_value:
            logger.warning(f"🔄 Amount too small after rounding: {actual_usdt_amount:.2f}, increasing...")
            
            # Рассчитываем минимальное необходимое количество
            min_base_amount = min_order_value / current_price
            base_amount = round_amount(symbol, min_base_amount)
            
            # Пересчитываем сумму
            actual_usdt_amount = base_amount * current_price
            
            # Проверяем что не превышаем доступный баланс
            if actual_usdt_amount > available_usdt:
                logger.error(f"❌ Even min amount too expensive: {actual_usdt_amount:.2f} > {available_usdt:.2f}")
                return False
        
        logger.info(f"📊 Final calculation: {base_amount:.6f} {symbol} = {actual_usdt_amount:.2f} USDT")
        
        # Проверяем лимиты с небольшим допуском
        if actual_usdt_amount < min_order_value - 0.01:
            logger.error(f"❌ Final order value too small: {actual_usdt_amount:.2f} USDT < {min_order_value} USDT")
            return False
            
        # Расчет TP/SL
        if CURRENT_SCALPING_STRATEGY in ["BB_SQUEEZE", "EMA_MOMENTUM", "BREAKOUT"]:
            # LONG позиции для этих стратегий
            stop_loss = current_price * (1 - strategy_config['max_stop_loss'])
            take_profit = current_price * (1 + strategy_config['take_profit'])
        else:
            # VWAP Bounce может быть в обе стороны
            if signals.get('price_below_vwap', False):
                # LONG если цена ниже VWAP
                stop_loss = current_price * (1 - strategy_config['max_stop_loss'])
                take_profit = current_price * (1 + strategy_config['take_profit'])
            else:
                # SHORT если цена выше VWAP
                stop_loss = current_price * (1 + strategy_config['max_stop_loss'])
                take_profit = current_price * (1 - strategy_config['take_profit'])
        
        if DRY_RUN:
            logger.info(f"DRY RUN: Would open {symbol} - {base_amount:.6f} @ {current_price:.6f}")
            record_open_position(symbol, base_amount, current_price, stop_loss, take_profit, CURRENT_SCALPING_STRATEGY)
            return True
            
        # Проверка баланса
        bal = fetch_balance()
        usdt_free = float(bal['free'].get('USDT', 0) or 0)
        
        if usdt_free < actual_usdt_amount:
            logger.info(f"Insufficient USDT for {symbol}: {usdt_free:.2f} < {actual_usdt_amount:.2f}")
            return False
        
        # Упрощенная проверка минимального количества
        market = exchange.market(symbol)
        min_amount = float(market['limits']['amount']['min'])
        
        if base_amount < min_amount:
            logger.error(f"❌ Amount too small: {base_amount:.6f} < {min_amount}")
            # Пробуем увеличить до минимального количества
            base_amount = min_amount
            actual_usdt_amount = base_amount * current_price
            
            # Проверяем что новая сумма не превышает доступный баланс
            if actual_usdt_amount > available_usdt:
                logger.error(f"❌ Even min amount too expensive: {actual_usdt_amount:.2f} > {available_usdt:.2f}")
                return False
        
        # Создание ордера для Bybit
        logger.info(f"🟢 Opening SCALPING position: {symbol} {base_amount:.6f} @ {current_price:.6f}")
        
        try:
            order = exchange.create_market_order(symbol, 'buy', base_amount)
            
            logger.info(f"📦 Order response: {order}")
            
            if order and order.get('id'):
                order_id = order['id']
                logger.info(f"📋 Order ID: {order_id}")
                
                time.sleep(3)
                
                # Проверяем исполнение через изменение баланса
                new_bal = fetch_balance()
                base_currency = symbol.split('/')[0]
                new_base_balance = float(new_bal['free'].get(base_currency, 0) or 0)
                old_base_balance = float(bal['free'].get(base_currency, 0) or 0)
                
                logger.info(f"💰 Balance check - Before: {old_base_balance:.6f}, After: {new_base_balance:.6f}")
                
                if new_base_balance > old_base_balance + 0.000001:
                    filled_amount = new_base_balance - old_base_balance
                    
                    try:
                        order_status = exchange.fetch_order(order_id, symbol, params={'acknowledged': True})
                        average_price = float(order_status.get('average', current_price))
                        logger.info(f"✅ Order executed - Amount: {filled_amount:.6f}, Avg Price: {average_price:.6f}")
                    except:
                        average_price = current_price
                        logger.info(f"✅ Order executed (fallback) - Amount: {filled_amount:.6f}, Price: {average_price:.6f}")
                    
                    record_open_position(symbol, filled_amount, average_price, stop_loss, take_profit, CURRENT_SCALPING_STRATEGY)
                    update_daily_trade_count(symbol)
                    
                    safe_send(
                        f"🎯 <b>SCALPING ENTRY: {symbol}</b>\n"
                        f"Стратегия: {strategy_config['name']}\n"
                        f"Количество: {filled_amount:.6f}\n"
                        f"Цена входа: {average_price:.6f}\n"
                        f"Сумма: {filled_amount * average_price:.2f} USDT\n"
                        f"Stop Loss: {stop_loss:.6f}\n"
                        f"Take Profit: {take_profit:.6f}\n"
                        f"Score: {signals.get('score', 0)}"
                    )
                    
                    logger.info(f"✅ Scalping position opened: {symbol} {filled_amount:.6f} @ {average_price:.6f}")
                    return True
                else:
                    logger.error(f"❌ Order not executed - balance unchanged")
                    try:
                        exchange.cancel_order(order_id, symbol)
                        logger.info(f"📝 Order {order_id} cancelled")
                    except:
                        pass
                    return False
            else:
                logger.error(f"❌ Order creation failed: {order}")
                return False
                
        except ccxt.InsufficientFunds as e:
            logger.error(f"Insufficient funds for {symbol}: {e}")
            return False
        except ccxt.InvalidOrder as e:
            logger.error(f"Invalid order for {symbol}: {e}")
            return False
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error for {symbol}: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error opening scalping position for {symbol}: {e}")
        safe_send(f"❌ Ошибка открытия скальпинг позиции {symbol}: {str(e)}")
        return False

def check_market_limits(symbol: str, amount: float, cost: float) -> bool:
    """Проверка минимальных лимитов ордера с исправлениями для Bybit"""
    try:
        market = exchange.market(symbol)
        
        # Для Bybit spot используем правильные лимиты
        min_amount = float(market['limits']['amount']['min'])
        
        # Bybit имеет разные минимальные стоимости для разных пар
        # Используем наши настройки MIN_USDT_PER_SYMBOL как основной источник
        min_cost = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
        
        # Дополнительно проверяем лимиты биржи, но не полагаемся на них полностью
        exchange_min_cost = float(market['limits']['cost'].get('min', 0))
        if exchange_min_cost > 0 and exchange_min_cost > min_cost:
            min_cost = exchange_min_cost
        
        logger.info(f"📏 Market limits for {symbol}: min_amount={min_amount}, our_min_cost={min_cost}")
        logger.info(f"📊 Order details: amount={amount:.6f}, cost={cost:.2f} USDT")
        
        if amount < min_amount:
            logger.error(f"❌ Amount too small: {amount:.6f} < {min_amount}")
            return False
            
        if cost < min_cost:
            logger.error(f"❌ Cost too small: {cost:.2f} USDT < {min_cost} USDT")
            return False
            
        logger.info(f"✅ Market limits check passed for {symbol}")
        return True
        
    except Exception as e:
        logger.error(f"Error checking market limits for {symbol}: {e}")
        # В случае ошибки используем наши минимальные настройки
        min_cost = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
        if cost < min_cost:
            logger.error(f"❌ Cost too small (fallback): {cost:.2f} USDT < {min_cost} USDT")
            return False
        return True

# ====== SYMBOL MANAGEMENT ======
def get_available_symbols() -> List[str]:
    """Получение только доступных символов на бирже"""
    try:
        markets = exchange.load_markets()
        available_symbols = []
        
        for symbol in SYMBOLS:
            if symbol in markets:
                market = markets[symbol]
                if market.get('active', False) and market.get('spot', False):
                    available_symbols.append(symbol)
            else:
                logger.warning(f"Symbol {symbol} not available on exchange")
        
        logger.info(f"Available symbols: {available_symbols}")
        return available_symbols
        
    except Exception as e:
        logger.error(f"Error loading available symbols: {e}")
        return SYMBOLS  # Возвращаем оригинальный список в случае ошибки

# Инициализация активных символов
active_symbols = get_available_symbols()

# ====== MAIN TRADING LOOP ======
def scan_for_opportunities():
    """Сканирование торговых возможностей с проверкой доступных средств"""
    logger.info(f"🔍 Scanning opportunities in {CURRENT_MODE} mode...")
    
    # Проверяем доступные средства
    available_usdt = compute_available_usdt()
    if available_usdt < 5.0:  # Минимум 5 USDT для новых позиций
        logger.warning(f"⚠️ Skipping opportunities - insufficient USDT: {available_usdt:.2f}")
        return
        
    opportunities = []
    
    logger.info(f"💰 Available USDT: {available_usdt:.2f}")
    
    for symbol in active_symbols:
        try:
            # Пропускаем символы с ошибками
            if is_in_cooldown(symbol):
                logger.debug(f"Skipping {symbol} - in cooldown")
                continue
                
            if CURRENT_MODE == "SCALPING":
                should_enter, signals = get_scalping_signal(symbol)
            else:
                should_enter, signals = should_enter_swing_position(symbol)
                
            if should_enter and isinstance(signals, dict) and signals.get('score', 0) > 0:
                opportunities.append((symbol, signals))
                logger.info(f"🎯 Found opportunity: {symbol} (Score: {signals.get('score', 0)})")
                
        except Exception as e:
            logger.error(f"Error scanning {symbol}: {e}")
            continue
            
    # Сортируем по score и открываем лучшие (максимум 1 за сканирование)
    opportunities.sort(key=lambda x: x[1].get('score', 0), reverse=True)
    
    logger.info(f"📊 Found {len(opportunities)} opportunities, attempting to open top 1")
    
    for symbol, signals in opportunities[:1]:  # Только 1 позиция за сканирование
        try:
            if CURRENT_MODE == "SCALPING":
                success = open_scalping_position(symbol, signals)
            else:
                success = open_swing_position(symbol, signals)
                
            if success:
                logger.info(f"✅ Successfully opened position for {symbol}")
                break  # Только одну позицию за сканирование
            else:
                logger.warning(f"❌ Failed to open position for {symbol}")
                
        except Exception as e:
            logger.error(f"Error opening position for {symbol}: {e}")
            continue

def main_trading_loop():
    """Главный торговый цикл с улучшенным управлением позициями"""
    logger.info("🤖 Starting Universal Trading Bot...")
    safe_send(f"🚀 <b>UNIVERSAL TRADING BOT STARTED</b>\nРежим: {TRADING_MODES[CURRENT_MODE]['name']}")
    
    last_status_time = 0
    last_cleanup_time = 0
    settings = get_current_settings()
    
    while True:
        try:
            current_time = time.time()
            
            # Проверка выхода из позиций
            check_position_exits()
            
            # Периодическая очистка убыточных позиций (каждые 30 минут)
            if current_time - last_cleanup_time >= 1800:  # 30 минут
                logger.info("🔄 Periodic cleanup check...")
                close_unprofitable_positions()
                last_cleanup_time = current_time
            
            # Сканирование новых возможностей (только если есть доступные средства)
            available_usdt = compute_available_usdt()
            if available_usdt >= 10.0:  # Только если есть минимум 10 USDT
                scan_for_opportunities()
            else:
                logger.warning(f"⚠️ Skipping scan - insufficient USDT: {available_usdt:.2f}")
            
            # Автоматический статус
            if current_time - last_status_time >= settings.get('status_interval', 600):
                cmd_status(None, None)
                last_status_time = current_time
                
            # Пауза между итерациями
            time.sleep(settings.get('scan_interval', 60))
            
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(30)

# ====== TELEGRAM SETUP ======
def setup_telegram():
    """Настройка Telegram бота"""
    try:
        updater = Updater(TELEGRAM_TOKEN, use_context=True)
        dp = updater.dispatcher
        
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CommandHandler("status", cmd_status))
        dp.add_handler(CommandHandler("stats", cmd_stats))
        dp.add_handler(CommandHandler("close", cmd_close))        
        dp.add_handler(CommandHandler("close_all", cmd_close_all))
        dp.add_handler(CommandHandler("close_unprofitable", cmd_close_unprofitable))
        dp.add_handler(CommandHandler("sync", cmd_force_sync))
        dp.add_handler(CommandHandler("import", cmd_import))
        dp.add_handler(CommandHandler("debug", cmd_debug_settings))
        dp.add_handler(CommandHandler("cleanup", cmd_cleanup_duplicates))
        
        dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
        
        return updater
    except Exception as e:
        logger.error(f"Telegram setup failed: {e}")
        return None

# ====== CLEANUP ======
def cleanup():
    """Очистка ресурсов"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        db.close()
        logger.info("Cleanup completed")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

def signal_handler(signum, frame):
    """Обработчик сигналов"""
    logger.info(f"Received signal {signum}, shutting down...")
    cleanup()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Очистка дубликатов при запуске
        cleanup_duplicate_positions()
        
        # Автоматическая синхронизация при запуске
        auto_sync_positions()
        
        # Получаем доступные символы
        active_symbols = get_available_symbols()
        logger.info(f"Trading with {len(active_symbols)} available symbols")
        
        # Импорт существующих позиций при запуске (с проверкой дубликатов)
        imported = import_existing_positions()
        if imported > 0:
            logger.info(f"Imported {imported} existing positions")
        
        # Запуск Telegram бота в отдельном потоке
        updater = setup_telegram()
        if updater:
            updater.start_polling()
            logger.info("Telegram bot started")
        
        # Запуск основного торгового цикла
        main_trading_loop()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        safe_send(f"❌ <b>BOT CRASHED:</b> {str(e)}")
    finally:
        cleanup()
