#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ultimate_trading_bot.py — универсальный бот с выбором режимов: агрессивный, консервативный, скальпинг
"""
import os, sys, time, math, ccxt, pandas as pd, sqlite3
import logging
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Optional, Tuple, Any

from ta.trend import EMAIndicator, MACD, ADXIndicator, IchimokuIndicator
from ta.momentum import RSIIndicator, StochasticOscillator, WilliamsRIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import VolumeWeightedAveragePrice, OnBalanceVolumeIndicator
from telegram import Bot, ParseMode, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

# ====== CONFIGURATION ======
API_KEY = os.getenv("BYBIT_API_KEY", "BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET", "BYBIT_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "TELEGRAM_TOKEN")
CHAT_ID = CHAT_ID

# Расширенный список пар
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "AVAX/USDT", "DOT/USDT", "LINK/USDT", "MATIC/USDT",
    "DOGE/USDT", "LTC/USDT", "ATOM/USDT", "UNI/USDT", "XLM/USDT",
    "ETC/USDT", "FIL/USDT", "THETA/USDT", "EOS/USDT", "AAVE/USDT"
]

# ====== TRADING MODES ======
TRADING_MODES = {
    "AGGRESSIVE": {
        "name": "🟢 АГРЕССИВНЫЙ",
        "type": "swing",
        "scan_interval": 30,
        "status_interval": 300,
        "max_trades": 8,
        "trade_pct": 0.08,
        "rsi_min": 35,
        "rsi_max": 75,
        "volume_multiplier": 1.2,
        "adx_min": 12,
        "min_score": 40,
        "cooldown": 2 * 60,
        "max_stop_loss": 0.008,
        "take_profit": 0.012,
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
        "trade_pct": 0.10,
        "rsi_min": 40,
        "rsi_max": 65,
        "volume_multiplier": 1.3,
        "adx_min": 15,
        "min_score": 55,
        "cooldown": 10 * 60,
        "max_stop_loss": 0.015,
        "take_profit": 0.030,
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
        "trade_pct": 0.15,
        "active_strategy": "BB_SQUEEZE"
    }
}

# Стратегии скальпинга
SCALPING_STRATEGIES = {
    "BB_SQUEEZE": {
        "name": "Bollinger Squeeze",
        "scan_interval": 3,
        "max_trades": 15,
        "trade_pct": 0.12,
        "timeframe_entry": "1m",
        "timeframe_trend": "5m",
        "max_stop_loss": 0.002,
        "take_profit": 0.004,
        "quick_exit": 0.003,
        "rsi_range": (25, 75),
        "volume_multiplier": 1.3,
        "bb_period": 10,
        "bb_std": 1.2,
        # Добавлены недостающие ключи для совместимости
        "trailing_start": 0.002,
        "trailing_step": 0.001,
        "status_interval": 180
    },
    "EMA_MOMENTUM": {
        "name": "EMA Momentum",
        "scan_interval": 5,
        "max_trades": 12,
        "trade_pct": 0.15,
        "timeframe_entry": "2m",
        "timeframe_trend": "10m",
        "max_stop_loss": 0.0025,
        "take_profit": 0.005,
        "quick_exit": 0.004,
        "rsi_range": (30, 70),
        "volume_multiplier": 1.2,
        "ema_fast": 5,
        "ema_slow": 12,
        "trailing_start": 0.002,
        "trailing_step": 0.001,
        "status_interval": 180
    },
    "VWAP_BOUNCE": {
        "name": "VWAP Bounce",
        "scan_interval": 4,
        "max_trades": 10,
        "trade_pct": 0.18,
        "timeframe_entry": "3m",
        "timeframe_trend": "15m",
        "max_stop_loss": 0.0015,
        "take_profit": 0.0035,
        "quick_exit": 0.0025,
        "rsi_range": (35, 65),
        "volume_multiplier": 1.8,
        "vwap_period": 20,
        "trailing_start": 0.002,
        "trailing_step": 0.001,
        "status_interval": 180
    },
    "BREAKOUT": {
        "name": "Breakout Scalping",
        "scan_interval": 5,
        "max_trades": 8,
        "trade_pct": 0.20,
        "timeframe_entry": "2m",
        "timeframe_trend": "10m",
        "max_stop_loss": 0.003,
        "take_profit": 0.006,
        "quick_exit": 0.004,
        "rsi_range": (40, 80),
        "volume_multiplier": 2.0,
        "breakout_period": 15,
        "trailing_start": 0.002,
        "trailing_step": 0.001,
        "status_interval": 180
    }
}

# Текущие настройки
CURRENT_MODE = "CONSERVATIVE"
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
MIN_TRADE_USDT = 10.0
MIN_USDT_PER_SYMBOL = {
    "BTC/USDT": 5.0, "ETH/USDT": 5.0, "BNB/USDT": 3.0, "SOL/USDT": 2.0,
    "XRP/USDT": 2.0, "ADA/USDT": 2.0, "AVAX/USDT": 2.0, "DOT/USDT": 2.0,
    "LINK/USDT": 2.0, "MATIC/USDT": 2.0, "DOGE/USDT": 2.0, "LTC/USDT": 2.0,
    "ATOM/USDT": 2.0, "UNI/USDT": 2.0, "XLM/USDT": 2.0, "ETC/USDT": 2.0,
    "FIL/USDT": 2.0, "THETA/USDT": 2.0, "EOS/USDT": 2.0, "AAVE/USDT": 5.0,
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

# ====== INITIALIZATION ======
def initialize_database():
    """Инициализация базы данных"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    
    # Таблица позиций
    cursor.execute("""
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
    cursor.execute("""
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
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS symbol_cooldown (
        symbol TEXT PRIMARY KEY,
        last_closed_ts INTEGER DEFAULT 0,
        daily_trade_count INTEGER DEFAULT 0,
        last_trade_date TEXT
    )
    """)
    
    # Статистика
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS performance_stats (
        date TEXT PRIMARY KEY,
        total_trades INTEGER DEFAULT 0,
        winning_trades INTEGER DEFAULT 0,
        total_pnl REAL DEFAULT 0,
        total_volume REAL DEFAULT 0
    )
    """)
    
    # Дневные лимиты для скальпинга
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_limits (
        date TEXT PRIMARY KEY,
        daily_pnl REAL DEFAULT 0,
        total_trades INTEGER DEFAULT 0,
        consecutive_losses INTEGER DEFAULT 0
    )
    """)
    
    conn.commit()
    return conn, cursor

# Инициализация
conn, cursor = initialize_database()

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
    """Получение OHLCV данных"""
    def _fetch():
        return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    
    try:
        data = retry_api_call(_fetch, max_retries=2)
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
    """Получение текущих настроек"""
    if CURRENT_MODE == "SCALPING":
        return SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
    else:
        return TRADING_MODES[CURRENT_MODE]

def round_amount(symbol: str, amount: float) -> float:
    """Округление количества"""
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
            
        return max(float(rounded), min_amount)
        
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
    """Расчет сигналов для свинг-трейдинга"""
    settings = get_current_settings()
    
    if df is None or len(df) < 30:
        return {'score': 0, 'error': 'Insufficient data'}
    
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
        if len(df) >= 15:
            try:
                adx_indicator = ADXIndicator(df['high'], df['low'], df['close'], window=14)
                adx = adx_indicator.adx().iloc[-1]
                plus_di = adx_indicator.adx_pos().iloc[-1]
                minus_di = adx_indicator.adx_neg().iloc[-1]
                adx_bullish = plus_di > minus_di
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
        volume_ok = volume_ratio > settings['volume_multiplier']
        
        signals = {
            'price': current_price,
            'volume_ok': volume_ok,
            'volume_ratio': volume_ratio,
            'rsi_ok': settings['rsi_min'] <= rsi <= settings['rsi_max'],
            'rsi_value': rsi,
            'rsi_trend': 'BULLISH' if rsi > rsi_prev else 'BEARISH',
            'macd_bullish': macd_bullish,
            'adx_strong': adx >= settings['adx_min'],
            'adx_value': adx,
            'adx_bullish': adx_bullish,
            'bb_position': bb_position,
            'bb_signal': 0.2 <= bb_position <= 0.8,
            'stoch_ok': 20 <= stoch <= 80,
            'stoch_value': stoch,
            'trend': get_trend_direction(df)
        }
        
        # Score calculation
        score = 0
        if signals['volume_ok']: score += 20
        if signals['rsi_ok']: score += 15
        if signals['rsi_trend'] == 'BULLISH': score += 5
        if signals['macd_bullish']: score += 15
        if signals['adx_strong']: score += 10
        if signals['adx_bullish']: score += 5
        if signals['bb_signal']: score += 15
        if signals['stoch_ok']: score += 10
        if signals['trend'] == "BULLISH": score += 5
        
        signals['score'] = min(score, 100)
        return signals
        
    except Exception as e:
        logger.error(f"Error calculating swing signals: {e}")
        return {'score': 0, 'error': str(e)}

def should_enter_swing_position(symbol: str) -> Tuple[bool, Any]:
    """Логика входа для свинг-трейдинга"""
    try:
        # Анализ тренда
        df_trend = get_ohlcv_data(symbol, "1h", 100)
        if df_trend is None or len(df_trend) < 50:
            return False, "No trend data"
            
        trend = get_trend_direction(df_trend)
        if trend != "BULLISH":
            return False, f"Trend not bullish: {trend}"
        
        # Анализ входа
        df_entry = get_ohlcv_data(symbol, "15m", 100)
        if df_entry is None or len(df_entry) < 30:
            return False, "No entry data"
            
        signals = calculate_swing_signals(df_entry)
        settings = get_current_settings()
        
        if 'error' in signals:
            return False, f"Signal error: {signals['error']}"
        
        # Условия входа
        entry_conditions = (
            signals['score'] >= settings['min_score'],
            signals['volume_ok'],
            signals['trend'] == "BULLISH",
            signals.get('adx_strong', False),
            not is_in_cooldown(symbol),
            check_daily_trade_limit(symbol)
        )
        
        if all(entry_conditions):
            sl_price = signals['price'] * (1 - settings['max_stop_loss'])
            tp_price = signals['price'] * (1 + settings['take_profit'])
            
            entry_info = {
                'price': signals['price'],
                'stop_loss': sl_price,
                'take_profit': tp_price,
                'score': signals['score'],
                'rsi': signals['rsi_value'],
                'adx': signals['adx_value'],
                'volume_ratio': signals.get('volume_ratio', 1)
            }
            
            return True, entry_info
        else:
            reason = f"Score: {signals['score']}, Volume: {signals['volume_ok']}, Trend: {signals['trend']}, ADX: {signals.get('adx_strong', False)}"
            return False, reason
            
    except Exception as e:
        logger.error(f"Swing entry check error {symbol}: {e}")
        return False, f"Error: {str(e)}"

# ====== SCALPING STRATEGIES ======
def bollinger_squeeze_strategy(df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """Стратегия Bollinger Band Squeeze"""
    strategy_config = SCALPING_STRATEGIES["BB_SQUEEZE"]
    
    if len(df) < 20:
        return {'signal': 'NO_SIGNAL', 'score': 0}
    
    try:
        bb = BollingerBands(df['close'], window=strategy_config['bb_period'], 
                           window_dev=strategy_config['bb_std'])
        
        bb_upper = bb.bollinger_hband().iloc[-1]
        bb_lower = bb.bollinger_lband().iloc[-1]
        bb_middle = bb.bollinger_mavg().iloc[-1]
        
        bb_width = (bb_upper - bb_lower) / bb_middle
        rsi = RSIIndicator(df['close'], window=7).rsi().iloc[-1]
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].tail(10).mean()
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        price_position = (df['close'].iloc[-1] - bb_lower) / (bb_upper - bb_lower)
        
        long_conditions = (
            bb_width < 0.01,
            rsi < strategy_config['rsi_range'][1],
            volume_ratio > strategy_config['volume_multiplier'],
            price_position < 0.7,
            df['close'].iloc[-1] > bb_middle
        )
        
        short_conditions = (
            bb_width < 0.01,
            rsi > strategy_config['rsi_range'][0],
            volume_ratio > strategy_config['volume_multiplier'],
            price_position > 0.3,
            df['close'].iloc[-1] < bb_middle
        )
        
        if all(long_conditions):
            score = min(70 + (volume_ratio * 10) + ((0.01 - bb_width) * 1000), 95)
            return {
                'signal': 'LONG',
                'score': score,
                'price': df['close'].iloc[-1],
                'stop_loss': df['close'].iloc[-1] * (1 - strategy_config['max_stop_loss']),
                'take_profit': df['close'].iloc[-1] * (1 + strategy_config['take_profit']),
                'rsi': rsi,
                'volume_ratio': volume_ratio,
                'bb_width': bb_width
            }
        elif all(short_conditions):
            score = min(70 + (volume_ratio * 10) + ((0.01 - bb_width) * 1000), 95)
            return {
                'signal': 'SHORT', 
                'score': score,
                'price': df['close'].iloc[-1],
                'stop_loss': df['close'].iloc[-1] * (1 + strategy_config['max_stop_loss']),
                'take_profit': df['close'].iloc[-1] * (1 - strategy_config['take_profit']),
                'rsi': rsi,
                'volume_ratio': volume_ratio,
                'bb_width': bb_width
            }
        
        return {'signal': 'NO_SIGNAL', 'score': 0}
        
    except Exception as e:
        logger.error(f"BB Squeeze error for {symbol}: {e}")
        return {'signal': 'NO_SIGNAL', 'score': 0}

def ema_momentum_strategy(df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """Стратегия EMA Momentum"""
    strategy_config = SCALPING_STRATEGIES["EMA_MOMENTUM"]
    
    if len(df) < 15:
        return {'signal': 'NO_SIGNAL', 'score': 0}
    
    try:
        ema_fast = EMAIndicator(df['close'], window=strategy_config['ema_fast']).ema_indicator()
        ema_slow = EMAIndicator(df['close'], window=strategy_config['ema_slow']).ema_indicator()
        
        rsi = RSIIndicator(df['close'], window=7).rsi().iloc[-1]
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].tail(10).mean()
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        macd_line = MACD(df['close']).macd().iloc[-1]
        macd_signal = MACD(df['close']).macd_signal().iloc[-1]
        
        long_conditions = (
            ema_fast.iloc[-1] > ema_slow.iloc[-1],
            ema_fast.iloc[-2] <= ema_slow.iloc[-2],
            rsi < strategy_config['rsi_range'][1],
            volume_ratio > strategy_config['volume_multiplier'],
            macd_line > macd_signal,
            df['close'].iloc[-1] > ema_fast.iloc[-1]
        )
        
        short_conditions = (
            ema_fast.iloc[-1] < ema_slow.iloc[-1],
            ema_fast.iloc[-2] >= ema_slow.iloc[-2],
            rsi > strategy_config['rsi_range'][0],
            volume_ratio > strategy_config['volume_multiplier'], 
            macd_line < macd_signal,
            df['close'].iloc[-1] < ema_fast.iloc[-1]
        )
        
        if all(long_conditions):
            score = min(75 + (volume_ratio * 8) + (max(0, macd_line - macd_signal) * 100), 92)
            return {
                'signal': 'LONG',
                'score': score,
                'price': df['close'].iloc[-1],
                'stop_loss': df['close'].iloc[-1] * (1 - strategy_config['max_stop_loss']),
                'take_profit': df['close'].iloc[-1] * (1 + strategy_config['take_profit']),
                'rsi': rsi,
                'volume_ratio': volume_ratio
            }
        elif all(short_conditions):
            score = min(75 + (volume_ratio * 8) + (max(0, macd_signal - macd_line) * 100), 92)
            return {
                'signal': 'SHORT',
                'score': score,
                'price': df['close'].iloc[-1],
                'stop_loss': df['close'].iloc[-1] * (1 + strategy_config['max_stop_loss']),
                'take_profit': df['close'].iloc[-1] * (1 - strategy_config['take_profit']),
                'rsi': rsi, 
                'volume_ratio': volume_ratio
            }
        
        return {'signal': 'NO_SIGNAL', 'score': 0}
        
    except Exception as e:
        logger.error(f"EMA Momentum error for {symbol}: {e}")
        return {'signal': 'NO_SIGNAL', 'score': 0}

def vwap_bounce_strategy(df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """Стратегия VWAP Bounce"""
    strategy_config = SCALPING_STRATEGIES["VWAP_BOUNCE"]
    
    if len(df) < strategy_config['vwap_period'] + 5:
        return {'signal': 'NO_SIGNAL', 'score': 0}
    
    try:
        vwap = VolumeWeightedAveragePrice(
            high=df['high'],
            low=df['low'], 
            close=df['close'],
            volume=df['volume'],
            window=strategy_config['vwap_period']
        ).volume_weighted_average_price().iloc[-1]
        
        current_price = df['close'].iloc[-1]
        price_distance = abs(current_price - vwap) / vwap
        
        rsi = RSIIndicator(df['close'], window=7).rsi().iloc[-1]
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].tail(10).mean()
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        long_conditions = (
            current_price > vwap,
            df['close'].iloc[-2] <= vwap,
            price_distance < 0.005,
            rsi < strategy_config['rsi_range'][1],
            volume_ratio > strategy_config['volume_multiplier'],
            df['volume'].iloc[-1] > df['volume'].iloc[-2]
        )
        
        short_conditions = (
            current_price < vwap,
            df['close'].iloc[-2] >= vwap,
            price_distance < 0.005,
            rsi > strategy_config['rsi_range'][0],
            volume_ratio > strategy_config['volume_multiplier'],
            df['volume'].iloc[-1] > df['volume'].iloc[-2]
        )
        
        if all(long_conditions):
            score = min(80 + (volume_ratio * 6) + ((0.005 - price_distance) * 1000), 94)
            return {
                'signal': 'LONG',
                'score': score,
                'price': current_price,
                'stop_loss': vwap * (1 - strategy_config['max_stop_loss'] * 1.5),
                'take_profit': current_price * (1 + strategy_config['take_profit']),
                'rsi': rsi,
                'volume_ratio': volume_ratio,
                'vwap_distance': price_distance
            }
        elif all(short_conditions):
            score = min(80 + (volume_ratio * 6) + ((0.005 - price_distance) * 1000), 94)
            return {
                'signal': 'SHORT',
                'score': score,
                'price': current_price,
                'stop_loss': vwap * (1 + strategy_config['max_stop_loss'] * 1.5),
                'take_profit': current_price * (1 - strategy_config['take_profit']),
                'rsi': rsi,
                'volume_ratio': volume_ratio,
                'vwap_distance': price_distance
            }
        
        return {'signal': 'NO_SIGNAL', 'score': 0}
        
    except Exception as e:
        logger.error(f"VWAP Bounce error for {symbol}: {e}")
        return {'signal': 'NO_SIGNAL', 'score': 0}

def breakout_strategy(df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """Стратегия Breakout"""
    strategy_config = SCALPING_STRATEGIES["BREAKOUT"]
    
    period = strategy_config['breakout_period']
    if len(df) < period + 5:
        return {'signal': 'NO_SIGNAL', 'score': 0}
    
    try:
        resistance = df['high'].tail(period).max()
        support = df['low'].tail(period).min()
        
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]
        current_close = df['close'].iloc[-1]
        
        rsi = RSIIndicator(df['close'], window=7).rsi().iloc[-1]
        
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].tail(10).mean()
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
        
        long_conditions = (
            current_high > resistance,
            current_close > resistance,
            volume_ratio > strategy_config['volume_multiplier'],
            rsi < strategy_config['rsi_range'][1],
            df['volume'].iloc[-1] > df['volume'].iloc[-2]
        )
        
        short_conditions = (
            current_low < support,
            current_close < support,
            volume_ratio > strategy_config['volume_multiplier'],
            rsi > strategy_config['rsi_range'][0],
            df['volume'].iloc[-1] > df['volume'].iloc[-2]
        )
        
        if all(long_conditions):
            score = min(85 + (volume_ratio * 5), 96)
            return {
                'signal': 'LONG',
                'score': score,
                'price': current_close,
                'stop_loss': resistance * (1 - strategy_config['max_stop_loss']),
                'take_profit': current_close * (1 + strategy_config['take_profit']),
                'rsi': rsi,
                'volume_ratio': volume_ratio
            }
        elif all(short_conditions):
            score = min(85 + (volume_ratio * 5), 96)
            return {
                'signal': 'SHORT', 
                'score': score,
                'price': current_close,
                'stop_loss': support * (1 + strategy_config['max_stop_loss']),
                'take_profit': current_close * (1 - strategy_config['take_profit']),
                'rsi': rsi,
                'volume_ratio': volume_ratio
            }
        
        return {'signal': 'NO_SIGNAL', 'score': 0}
        
    except Exception as e:
        logger.error(f"Breakout error for {symbol}: {e}")
        return {'signal': 'NO_SIGNAL', 'score': 0}

def get_scalping_signal(symbol: str) -> Tuple[bool, Any]:
    """Получение сигнала скальпинга"""
    try:
        strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
        
        df_entry = get_ohlcv_data(symbol, strategy_config['timeframe_entry'], 50)
        if df_entry is None or len(df_entry) < 20:
            return False, "No entry data"
        
        # Выбор стратегии
        if CURRENT_SCALPING_STRATEGY == "BB_SQUEEZE":
            result = bollinger_squeeze_strategy(df_entry, symbol)
        elif CURRENT_SCALPING_STRATEGY == "EMA_MOMENTUM":
            result = ema_momentum_strategy(df_entry, symbol)
        elif CURRENT_SCALPING_STRATEGY == "VWAP_BOUNCE":
            result = vwap_bounce_strategy(df_entry, symbol)
        elif CURRENT_SCALPING_STRATEGY == "BREAKOUT":
            result = breakout_strategy(df_entry, symbol)
        else:
            return False, "Unknown strategy"
        
        if result['signal'] != 'NO_SIGNAL' and result['score'] >= 70:
            if not check_scalping_daily_limits():
                return False, "Daily limits reached"
            
            return True, {
                'signal': result['signal'],
                'price': result['price'],
                'stop_loss': result['stop_loss'],
                'take_profit': result['take_profit'],
                'score': result['score'],
                'strategy': CURRENT_SCALPING_STRATEGY,
                'rsi': result.get('rsi', 0),
                'volume_ratio': result.get('volume_ratio', 1)
            }
        
        return False, f"No signal: {result.get('signal', 'UNKNOWN')}, Score: {result.get('score', 0)}"
        
    except Exception as e:
        logger.error(f"Scalping signal error for {symbol}: {e}")
        return False, f"Error: {str(e)}"

def check_scalping_daily_limits() -> bool:
    """Проверка дневных лимитов для скальпинга"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute("SELECT daily_pnl, total_trades, consecutive_losses FROM daily_limits WHERE date=?", (today,))
    row = cursor.fetchone()
    
    if not row:
        cursor.execute("INSERT INTO daily_limits (date, daily_pnl, total_trades, consecutive_losses) VALUES (?, 0, 0, 0)", (today,))
        conn.commit()
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
    
    cursor.execute("SELECT daily_pnl, consecutive_losses FROM daily_limits WHERE date=?", (today,))
    row = cursor.fetchone()
    
    if row:
        current_pnl, current_losses = row
        new_pnl = current_pnl + pnl_percent
        
        if pnl_percent > 0:
            cursor.execute("UPDATE daily_limits SET daily_pnl=?, consecutive_losses=0 WHERE date=?", (new_pnl, today))
        else:
            new_losses = current_losses + 1
            cursor.execute("UPDATE daily_limits SET daily_pnl=?, consecutive_losses=? WHERE date=?", (new_pnl, new_losses, today))
        
        conn.commit()

# ====== POSITION MANAGEMENT ======
def get_ohlcv_data(symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    """Получение OHLCV данных"""
    ohlcv = fetch_ohlcv(symbol, timeframe, limit)
    if not ohlcv or len(ohlcv) < 20:
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
        cursor.execute("SELECT symbol, base_amount, open_price, stop_loss, take_profit, max_price, trading_mode FROM positions WHERE status='OPEN'")
        rows = cursor.fetchall()
        
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
        
        cursor.execute("""
            INSERT INTO positions (symbol, trading_mode, strategy, base_amount, open_price, stop_loss, take_profit, max_price, open_time, fee_paid) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
        """, (symbol, CURRENT_MODE, strategy, base_amount, open_price, stop_loss, take_profit, open_price, fee))
        
        cursor.execute("""
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp, trading_mode, strategy) 
            VALUES (?, 'BUY', ?, ?, ?, ?, datetime('now'), ?, ?, ?)
        """, (symbol, open_price, usdt_amount, base_amount, fee, int(time.time()), CURRENT_MODE, strategy))
        
        conn.commit()
        logger.info(f"Recorded open position for {symbol}: {base_amount:.6f} @ {open_price:.6f}")
    except Exception as e:
        logger.error(f"Error recording open position: {e}")
        conn.rollback()

def safe_close_position(symbol: str, reason: str = "") -> bool:
    """Безопасное закрытие позиции"""
    try:
        if DRY_RUN:
            logger.info(f"DRY RUN: Would close {symbol} - {reason}")
            mark_position_closed(symbol)
            return True

        bal = fetch_balance()
        base = symbol.split("/")[0]
        real_balance = float(bal['free'].get(base, 0) or 0)
        
        if real_balance <= 0:
            logger.info(f"No balance for {symbol}, marking as closed")
            mark_position_closed(symbol)
            return True

        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
        except Exception as e:
            logger.error(f"Failed to get price for {symbol}: {e}")
            return False

        amount_to_sell = round_amount(symbol, real_balance)
        min_amount = get_min_amount(symbol)
        
        if amount_to_sell < min_amount:
            logger.info(f"Amount too small for {symbol}: {amount_to_sell} < {min_amount}")
            mark_position_closed(symbol)
            return True

        order_value = amount_to_sell * current_price
        min_order_value = 2.0
        
        if order_value < min_order_value:
            logger.info(f"Order value too small for {symbol}: {order_value:.2f} USDT")
            mark_position_closed(symbol)
            return True

        if not check_min_order_value(symbol, order_value):
            logger.warning(f"Below exchange limit for {symbol}: {order_value:.2f} USDT")
            mark_position_closed(symbol)
            return True

        # Execute sell order
        try:
            order = exchange.create_market_order(symbol, 'sell', amount_to_sell)
            
            actual_amount = amount_to_sell
            actual_price = current_price
            
            if order and 'filled' in order and order['filled'] is not None:
                actual_amount = float(order['filled'])
            if order and 'price' in order and order['price'] is not None:
                actual_price = float(order['price'])
            
            # Calculate PnL
            position = get_position_info(symbol)
            if position:
                open_value = position['base_amount'] * position['open_price']
                close_value = actual_amount * actual_price
                fee = close_value * TAKER_FEE
                pnl = close_value - open_value - fee
                pnl_percent = (pnl / open_value) * 100 if open_value > 0 else 0
                
                record_close_with_pnl(symbol, actual_price, close_value, actual_amount, pnl, pnl_percent, fee)
                
                # Update scalping PnL if needed
                if position.get('trading_mode') == 'SCALPING':
                    update_scalping_daily_pnl(pnl_percent / 100)
                
                pnl_emoji = "🟢" if pnl >= 0 else "🔴"
                safe_send(
                    f"✅ {pnl_emoji} Closed {symbol}\n"
                    f"Amount: {actual_amount:.4f} @ {actual_price:.6f}\n"
                    f"P&L: {pnl:+.4f} USDT ({pnl_percent:+.2f}%)\n"
                    f"Reason: {reason}"
                )
            else:
                record_close_position(symbol, actual_price, close_value, actual_amount)
                safe_send(f"✅ Closed {symbol}: {actual_amount:.4f} @ {actual_price:.6f} ({reason})")
                
        except Exception as e:
            logger.error(f"Error executing sell order for {symbol}: {e}")
            return False
            
        update_daily_trade_count(symbol)
        return True
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error closing position {symbol}: {error_msg}")
        
        if any(err in error_msg for err in ["Insufficient balance", "lower limit", "too small"]):
            mark_position_closed(symbol)
            return True
        else:
            safe_send(f"❌ Error closing {symbol}: {error_msg}")
            return False

def get_position_info(symbol: str) -> Optional[Dict]:
    """Получение информации о позиции"""
    cursor.execute("SELECT base_amount, open_price, trading_mode FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
    row = cursor.fetchone()
    if row:
        return {'base_amount': row[0], 'open_price': row[1], 'trading_mode': row[2]}
    return None

def mark_position_closed(symbol: str):
    """Закрытие позиции в БД"""
    try:
        cursor.execute("UPDATE positions SET status='CLOSED', close_time=datetime('now') WHERE symbol=? AND status='OPEN'", (symbol,))
        cursor.execute("REPLACE INTO symbol_cooldown (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
        conn.commit()
        logger.info(f"Position {symbol} marked as closed")
    except Exception as e:
        logger.error(f"Error marking position closed: {e}")

def record_close_position(symbol: str, price: float, usdt_amount: float, base_amount: float):
    """Запись закрытия позиции"""
    try:
        fee = usdt_amount * TAKER_FEE
        cursor.execute("UPDATE positions SET status='CLOSED', close_time=datetime('now'), close_price=? WHERE symbol=? AND status='OPEN'", (price, symbol))
        cursor.execute("""
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp, trading_mode) 
            VALUES (?, 'SELL', ?, ?, ?, ?, datetime('now'), ?, ?)
        """, (symbol, price, usdt_amount, base_amount, fee, int(time.time()), CURRENT_MODE))
        cursor.execute("REPLACE INTO symbol_cooldown (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
        conn.commit()
    except Exception as e:
        logger.error(f"Error recording close position: {e}")

def record_close_with_pnl(symbol: str, price: float, usdt_amount: float, base_amount: float, pnl: float, pnl_percent: float, fee: float):
    """Запись закрытия позиции с PnL"""
    try:
        cursor.execute("""
            UPDATE positions SET status='CLOSED', close_time=datetime('now'), close_price=?, pnl=?, pnl_percent=?, fee_paid=fee_paid+? 
            WHERE symbol=? AND status='OPEN'
        """, (price, pnl, pnl_percent, fee, symbol))
        
        cursor.execute("""
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp, trading_mode) 
            VALUES (?, 'SELL', ?, ?, ?, ?, datetime('now'), ?, ?)
        """, (symbol, price, usdt_amount, base_amount, fee, int(time.time()), CURRENT_MODE))
        
        cursor.execute("REPLACE INTO symbol_cooldown (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
        conn.commit()
        
        logger.info(f"Recorded close position for {symbol} with PnL: {pnl:.4f} USDT ({pnl_percent:.2f}%)")
    except Exception as e:
        logger.error(f"Error recording close position with PnL: {e}")

def is_in_cooldown(symbol: str) -> bool:
    """Проверка кудоуна"""
    settings = get_current_settings()
    cursor.execute("SELECT last_closed_ts FROM symbol_cooldown WHERE symbol=?", (symbol,))
    row = cursor.fetchone()
    if not row:
        return False
    return (time.time() - int(row[0])) < settings['cooldown']

def check_daily_trade_limit(symbol: str) -> bool:
    """Проверка дневного лимита trades"""
    settings = get_current_settings()
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute("SELECT daily_trade_count, last_trade_date FROM symbol_cooldown WHERE symbol=?", (symbol,))
    row = cursor.fetchone()
    
    if not row:
        return True
        
    daily_count, last_date = row
    if last_date != today:
        cursor.execute("UPDATE symbol_cooldown SET daily_trade_count=0, last_trade_date=? WHERE symbol=?", (today, symbol))
        conn.commit()
        return True
        
    return daily_count < settings.get('max_daily_trades_per_symbol', 5)

def update_daily_trade_count(symbol: str):
    """Обновление счетчика дневных trades"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute("SELECT daily_trade_count, last_trade_date FROM symbol_cooldown WHERE symbol=?", (symbol,))
    row = cursor.fetchone()
    
    if not row:
        cursor.execute("INSERT INTO symbol_cooldown (symbol, daily_trade_count, last_trade_date) VALUES (?, 1, ?)", (symbol, today))
    else:
        daily_count, last_date = row
        if last_date == today:
            cursor.execute("UPDATE symbol_cooldown SET daily_trade_count=daily_trade_count+1 WHERE symbol=?", (symbol,))
        else:
            cursor.execute("UPDATE symbol_cooldown SET daily_trade_count=1, last_trade_date=? WHERE symbol=?", (today, symbol))
    
    conn.commit()

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
    cursor.execute("SELECT COUNT(*) FROM positions WHERE status='OPEN'")
    return cursor.fetchone()[0]

def can_open_new_trade() -> bool:
    """Проверка возможности открытия нового trade"""
    settings = get_current_settings()
    return get_concurrent_trades_count() < settings['max_trades']

# ====== EXIT CONDITIONS ======
def check_position_exits():
    """Проверка условий выхода"""
    positions = get_open_positions()
    
    for symbol, pos in positions.items():
        try:
            # Определяем настройки в зависимости от режима позиции
            if pos['trading_mode'] == 'SCALPING':
                check_scalping_exit(symbol, pos)
            else:
                check_swing_exit(symbol, pos)
                
        except Exception as e:
            logger.error(f"Exit check error {symbol}: {e}")

def check_swing_exit(symbol: str, pos: Dict):
    """Проверка выхода для свинг-трейдинга"""
    # Используем настройки текущего режима или консервативные по умолчанию
    if CURRENT_MODE in TRADING_MODES and TRADING_MODES[CURRENT_MODE]["type"] == "swing":
        settings = TRADING_MODES[CURRENT_MODE]
    else:
        # Используем консервативные настройки по умолчанию для импортированных позиций
        settings = TRADING_MODES["CONSERVATIVE"]
    
    ohlcv = fetch_ohlcv(symbol, "15m", limit=20)
    if not ohlcv:
        return
        
    df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume']).astype(float)
    current_price = df['close'].iloc[-1]
    open_price = pos['open_price']
    stop_loss = pos['stop_loss']
    take_profit = pos['take_profit']
    max_price = pos.get('max_price', open_price)
    
    if current_price > max_price:
        update_max_price_db(symbol, current_price)
        max_price = current_price
    
    profit_pct = (current_price - open_price) / open_price
    
    exit_reason = ""
    
    # Базовые условия выхода (стоп-лосс и тейк-профит)
    if current_price <= stop_loss:
        exit_reason = f"SL {profit_pct*100:+.2f}%"
    elif current_price >= take_profit:
        exit_reason = f"TP {profit_pct*100:+.2f}%"
    # Трейлинг-стоп (только если есть настройки)
    elif 'trailing_start' in settings and profit_pct >= settings['trailing_start']:
        trail_level = max_price * (1 - settings['trailing_step'])
        if current_price <= trail_level:
            exit_reason = f"TRAIL {profit_pct*100:+.2f}%"
    # Экстренный выход при большой просадке
    elif 'max_stop_loss' in settings and profit_pct <= -settings['max_stop_loss'] * 1.5:
        exit_reason = f"EMERGENCY {profit_pct*100:+.2f}%"
    
    if exit_reason:
        logger.info(f"Exit condition triggered for {symbol}: {exit_reason}")
        safe_close_position(symbol, exit_reason)

def check_scalping_exit(symbol: str, pos: Dict):
    """Проверка выхода для скальпинга"""
    # Используем настройки текущей стратегии скальпинга
    strategy_config = SCALPING_STRATEGIES.get(CURRENT_SCALPING_STRATEGY, SCALPING_STRATEGIES["BB_SQUEEZE"])
    
    ohlcv = fetch_ohlcv(symbol, strategy_config['timeframe_entry'], limit=10)
    if not ohlcv:
        return
        
    df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume']).astype(float)
    current_price = df['close'].iloc[-1]
    open_price = pos['open_price']
    stop_loss = pos['stop_loss']
    take_profit = pos['take_profit']
    
    profit_pct = (current_price - open_price) / open_price
    
    exit_reason = ""
    
    # Базовые условия выхода для скальпинга
    if current_price <= stop_loss:
        exit_reason = f"SCALPING SL {profit_pct*100:+.2f}%"
    elif current_price >= take_profit:
        exit_reason = f"SCALPING TP {profit_pct*100:+.2f}%"
    # Быстрый выход для скальпинга
    elif 'quick_exit' in strategy_config and profit_pct >= strategy_config['quick_exit']:
        exit_reason = f"QUICK EXIT {profit_pct*100:+.2f}%"
    
    if exit_reason:
        logger.info(f"Scalping exit condition triggered for {symbol}: {exit_reason}")
        safe_close_position(symbol, exit_reason)
        
def update_max_price_db(symbol: str, price: float):
    """Обновление максимальной цены"""
    cursor.execute("UPDATE positions SET max_price=? WHERE symbol=? AND status='OPEN'", (price, symbol))
    conn.commit()

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
        cmd_sync(update, context)
    elif text == "📥 ИМПОРТ":
        cmd_import(update, context)
    elif text == "🔧 ДЕБАГ":
        cmd_debug(update, context)

def switch_mode(mode: str, update):
    """Смена режима торговли"""
    global CURRENT_MODE
    CURRENT_MODE = mode
    
    mode_info = TRADING_MODES[mode]
    msg = f"✅ Режим изменен: <b>{mode_info['name']}</b>\n\n"
    
    if mode == "SCALPING":
        msg += f"📊 Активная стратегия: <b>{SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]['name']}</b>\n"
        msg += f"⏱ Интервал сканирования: {mode_info['scan_interval']}с\n"
        msg += f"🔢 Макс сделок: {mode_info['max_trades']}\n"
        msg += f"💰 Размер позиции: {mode_info['trade_pct']*100}%"
        update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=get_scalping_keyboard())
    else:
        msg += f"⏱ Интервал сканирования: {mode_info['scan_interval']}с\n"
        msg += f"🔢 Макс сделок: {mode_info['max_trades']}\n"
        msg += f"💰 Размер позиции: {mode_info['trade_pct']*100}%\n"
        msg += f"🎯 TP/SL: +{mode_info['take_profit']*100:.1f}%/ -{mode_info['max_stop_loss']*100:.1f}%\n"
        msg += f"📊 RSI диапазон: {mode_info['rsi_min']}-{mode_info['rsi_max']}"
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

def cmd_status(update, context):
    """Команда статуса"""
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
            for sym, pos in positions.items():
                try:
                    ticker = exchange.fetch_ticker(sym)
                    current_price = float(ticker['last'])
                    
                    profit = (current_price - pos['open_price']) / pos['open_price'] * 100
                    profit_net = profit - ROUNDTRIP_FEE * 100
                    
                    position_value = current_price * pos['base_amount']
                    
                    emoji = "🟢" if profit_net > 0 else "🔴"
                    base_currency = sym.split('/')[0]
                    
                    msg += f"{emoji} <b>{sym}</b> [{pos['trading_mode']}]\n"
                    msg += f"   Кол-во: {pos['base_amount']:.4f} {base_currency}\n"
                    msg += f"   Вход: {pos['open_price']:.6f} | Текущ: {current_price:.6f}\n"
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

def send_auto_status():
    """Автоматическая отправка статуса"""
    try:
        equity = compute_equity()
        realized = realized_pnl_total()
        unrealized = unrealized_pnl_total()
        positions = get_open_positions()
        settings = get_current_settings()
        
        total_pnl = realized + unrealized
        pnl_color = "🟢" if total_pnl >= 0 else "🔴"
        
        current_mode_info = TRADING_MODES[CURRENT_MODE]
        
        msg = f"📊 <b>АВТО-СТАТУС</b>\n\n"
        msg += f"🎯 <b>Режим:</b> {current_mode_info['name']}\n"
        
        if CURRENT_MODE == "SCALPING":
            msg += f"📈 <b>Стратегия:</b> {SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]['name']}\n"
        
        msg += f"💰 <b>Капитал:</b> {equity:.2f} USDT\n"
        msg += f"📈 <b>P&L:</b> {pnl_color} {total_pnl:+.2f} USDT\n"
        msg += f"🔢 <b>Позиции:</b> {len(positions)}/{settings['max_trades']}\n"
        
        if positions:
            total_value = 0
            for sym, pos in positions.items():
                try:
                    ticker = exchange.fetch_ticker(sym)
                    current_price = float(ticker['last'])
                    total_value += pos['base_amount'] * current_price
                except:
                    continue
            msg += f"💎 <b>Стоимость позиций:</b> {total_value:.2f} USDT"
        
        safe_send(msg)
        
    except Exception as e:
        logger.error(f"Error in auto status: {e}")

def realized_pnl_total() -> float:
    """Общий реализованный PnL"""
    try:
        cursor.execute("SELECT SUM(pnl) FROM positions WHERE status='CLOSED' AND pnl IS NOT NULL")
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        logger.error(f"Realized PnL error: {e}")
        return 0.0

def unrealized_pnl_total() -> float:
    """Общий нереализованный PnL"""
    total = 0.0
    try:
        positions = get_open_positions()
        for sym, pos in positions.items():
            ohlcv = fetch_ohlcv(sym, "15m", limit=1)
            if ohlcv:
                price = float(ohlcv[-1][4])
                current_value = price * pos['base_amount']
                open_value = pos['open_price'] * pos['base_amount']
                total += (current_value - open_value)
    except Exception as e:
        logger.error(f"Unrealized PnL error: {e}")
    return total

def get_trading_stats() -> Dict[str, Any]:
    """Статистика trading"""
    try:
        cursor.execute("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(usdt_amount) as total_volume,
                SUM(fee) as total_fees
            FROM trade_history
        """)
        stats_row = cursor.fetchone()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as closed_trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                AVG(CASE WHEN pnl > 0 THEN pnl_percent ELSE NULL END) as avg_win_pct,
                AVG(CASE WHEN pnl <= 0 THEN pnl_percent ELSE NULL END) as avg_loss_pct,
                SUM(pnl) as total_pnl
            FROM positions 
            WHERE status='CLOSED' AND pnl IS NOT NULL
        """)
        trades_row = cursor.fetchone()
        
        stats = {
            'total_trades': stats_row[0] if stats_row else 0,
            'total_volume': float(stats_row[1]) if stats_row and stats_row[1] else 0,
            'total_fees': float(stats_row[2]) if stats_row and stats_row[2] else 0,
            'closed_trades': trades_row[0] if trades_row else 0,
            'winning_trades': trades_row[1] if trades_row else 0,
            'avg_win_pct': float(trades_row[2]) if trades_row and trades_row[2] else 0,
            'avg_loss_pct': float(trades_row[3]) if trades_row and trades_row[3] else 0,
            'total_pnl': float(trades_row[4]) if trades_row and trades_row[4] else 0,
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
        }

def cmd_stats(update, context):
    """Детальная статистика"""
    stats = get_trading_stats()
    equity = compute_equity()
    realized = realized_pnl_total()
    
    msg = f"📈 <b>Детальная статистика</b>\n\n"
    
    closed_trades = stats.get('closed_trades', 0)
    if closed_trades > 0:
        winning_trades = stats.get('winning_trades', 0)
        win_rate = (winning_trades / closed_trades) * 100
        total_return = (realized / equity) * 100 if equity > 0 else 0
        
        msg += f"📊 <b>Производительность:</b>\n"
        msg += f"• Винрейт: {win_rate:.1f}% ({winning_trades}/{closed_trades})\n"
        msg += f"• Средняя прибыль: {stats.get('avg_win_pct', 0):.2f}%\n"
        msg += f"• Средний убыток: {stats.get('avg_loss_pct', 0):.2f}%\n"
        msg += f"• Общая доходность: {total_return:+.2f}%\n"
        msg += f"• Общий P&L: {stats.get('total_pnl', 0):.2f} USDT\n"
        msg += f"• Всего сделок: {stats['total_trades']}\n"
        msg += f"• Объем торгов: {stats['total_volume']:.0f} USDT\n"
        msg += f"• Комиссии: {stats.get('total_fees', 0):.2f} USDT\n\n"
    
    current_mode_info = TRADING_MODES[CURRENT_MODE]
    msg += f"⚙️ <b>Текущие настройки:</b>\n"
    msg += f"• Режим: {current_mode_info['name']}\n"
    
    if CURRENT_MODE == "SCALPING":
        strategy_config = SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]
        msg += f"• Стратегия: {strategy_config['name']}\n"
        msg += f"• TP/SL: +{strategy_config['take_profit']*100:.1f}%/ -{strategy_config['max_stop_loss']*100:.1f}%\n"
        msg += f"• Размер позиции: {strategy_config['trade_pct']*100}%\n"
    else:
        msg += f"• TP/SL: +{current_mode_info['take_profit']*100:.1f}%/ -{current_mode_info['max_stop_loss']*100:.1f}%\n"
        msg += f"• Размер позиции: {current_mode_info['trade_pct']*100}%\n"
        msg += f"• RSI диапазон: {current_mode_info['rsi_min']}-{current_mode_info['rsi_max']}\n"
    
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

def cmd_sync(update, context):
    """Синхронизация баланса"""
    sync_balance_with_db()
    update.message.reply_text("✅ Синхронизация завершена", reply_markup=get_main_keyboard())

def cmd_import(update, context):
    """Импорт позиций"""
    imported_count = import_existing_positions()
    if imported_count > 0:
        update.message.reply_text(f"📥 Импортировано {imported_count} позиций", reply_markup=get_main_keyboard())
    else:
        update.message.reply_text("✅ Нет новых позиций для импорта", reply_markup=get_main_keyboard())

def cmd_debug(update, context):
    """Отладочная информация"""
    debug_info = []
    checked = 0
    
    for symbol in active_symbols[:5]:
        if checked >= 3:
            break
            
        if CURRENT_MODE == "SCALPING":
            should_enter, entry_info = get_scalping_signal(symbol)
        else:
            should_enter, entry_info = should_enter_swing_position(symbol)
            
        if isinstance(entry_info, dict):
            usdt_amount = compute_equity() * get_current_settings()['trade_pct']
            usdt_free = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
            
            debug_info.append(f"🎯 {symbol}: Score {entry_info['score']} | RSI {entry_info.get('rsi', 0):.1f}")
            debug_info.append(f"   Volume: {entry_info.get('volume_ratio', 1):.1f}x")
            debug_info.append(f"   Need: {usdt_amount:.2f} USDT | Have: {usdt_free:.2f} USDT")
            checked += 1
    
    if debug_info:
        update.message.reply_text("🔍 ТОП СИГНАЛЫ:\n" + "\n".join(debug_info), reply_markup=get_main_keyboard())
    else:
        update.message.reply_text("🔍 Сильных сигналов не найдено", reply_markup=get_main_keyboard())

def sync_balance_with_db():
    """Синхронизация баланса с БД"""
    safe_send("🔄 Синхронизация баланса...")
    
    bal = fetch_balance()
    synced_count = 0
    created_count = 0
    
    current_positions = get_open_positions()
    
    for symbol in active_symbols:
        try:
            base = symbol.split("/")[0]
            real_balance = float(bal['total'].get(base, 0) or 0)
            
            if symbol in current_positions:
                db_balance = current_positions[symbol]['base_amount']
                
                if real_balance <= 0:
                    cursor.execute("UPDATE positions SET status='CLOSED', close_time=datetime('now') WHERE symbol=? AND status='OPEN'", (symbol,))
                    logger.info(f"Closed position {symbol} - zero balance")
                    
                elif abs(real_balance - db_balance) / max(real_balance, db_balance) > 0.01:
                    cursor.execute("UPDATE positions SET base_amount=? WHERE symbol=? AND status='OPEN'", (real_balance, symbol))
                    synced_count += 1
                    logger.info(f"Synced position {symbol}: {db_balance} -> {real_balance}")
                        
            else:
                if real_balance > 0:
                    try:
                        ticker = exchange.fetch_ticker(symbol)
                        current_price = float(ticker['last'])
                        open_price = current_price
                        
                        settings = get_current_settings()
                        sl_price = open_price * (1 - settings['max_stop_loss'])
                        tp_price = open_price * (1 + settings['take_profit'])
                        
                        record_open_position(symbol, real_balance, open_price, sl_price, tp_price, CURRENT_SCALPING_STRATEGY if CURRENT_MODE == "SCALPING" else "")
                        created_count += 1
                        logger.info(f"Created position {symbol}: {real_balance} @ {open_price:.6f}")
                        
                    except Exception as e:
                        logger.error(f"Error creating position for {symbol}: {e}")
                        
        except Exception as e:
            logger.error(f"Sync error for {symbol}: {e}")
    
    conn.commit()
    safe_send(f"✅ Синхронизация завершена: {synced_count} обновлено, {created_count} создано")

def import_existing_positions():
    """Импорт существующих позиций"""
    logger.info("Importing existing positions from exchange...")
    
    try:
        balance = fetch_balance()
        imported_count = 0
        
        current_positions = get_open_positions()
        
        for symbol in active_symbols:
            base_currency = symbol.split('/')[0]
            base_balance = float(balance.get('total', {}).get(base_currency, 0) or 0)
            
            if base_balance > 0 and symbol not in current_positions:
                try:
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = float(ticker['last'])
                    
                    # Для импортированных позиций используем консервативные настройки
                    settings = TRADING_MODES["CONSERVATIVE"]
                    stop_loss = current_price * (1 - settings['max_stop_loss'])
                    take_profit = current_price * (1 + settings['take_profit'])
                    
                    # Импортированные позиции помечаем как консервативные
                    record_open_position(symbol, base_balance, current_price, stop_loss, take_profit, "IMPORTED")
                    imported_count += 1
                    logger.info(f"Imported position: {symbol} - {base_balance:.6f} @ {current_price:.6f}")
                    
                except Exception as e:
                    logger.error(f"Error importing position for {symbol}: {e}")
                    continue
        
        conn.commit()
        return imported_count
        
    except Exception as e:
        logger.error(f"Error importing existing positions: {e}")
        return 0
def fix_imported_positions():
    """Исправление уже импортированных позиций"""
    try:
        cursor.execute("SELECT symbol FROM positions WHERE status='OPEN' AND trading_mode='SCALPING'")
        rows = cursor.fetchall()
        
        fixed_count = 0
        for row in rows:
            symbol = row[0]
            # Обновляем режим на консервативный для импортированных позиций
            cursor.execute("UPDATE positions SET trading_mode='CONSERVATIVE' WHERE symbol=? AND status='OPEN'", (symbol,))
            fixed_count += 1
            logger.info(f"Fixed position mode for {symbol}")
        
        conn.commit()
        return fixed_count
        
    except Exception as e:
        logger.error(f"Error fixing imported positions: {e}")
        return 0
def cmd_fix_positions(update, context):
    """Исправление импортированных позиций"""
    fixed_count = fix_imported_positions()
    if fixed_count > 0:
        update.message.reply_text(f"✅ Исправлено {fixed_count} позиций", reply_markup=get_main_keyboard())
    else:
        update.message.reply_text("✅ Нет позиций для исправления", reply_markup=get_main_keyboard())

# ====== MAIN EXECUTION ======
def main():
    """Основная функция"""
    try:
        global active_symbols
        markets = exchange.load_markets()
        active_symbols = [s for s in SYMBOLS if s in markets]
        
        logger.info(f"Loaded {len(active_symbols)} active symbols")
        
        # Инициализация Telegram
        updater = Updater(TELEGRAM_TOKEN, use_context=True)
        dp = updater.dispatcher
        
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CommandHandler("status", cmd_status))
        dp.add_handler(CommandHandler("stats", cmd_stats))
        dp.add_handler(CommandHandler("close", cmd_close))
        dp.add_handler(CommandHandler("sync", cmd_sync))
        dp.add_handler(CommandHandler("import", cmd_import))
        dp.add_handler(CommandHandler("debug", cmd_debug))
        dp.add_handler(CommandHandler("fix", cmd_fix_positions))
        dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
        
        updater.start_polling()
        
        safe_send(f"🚀 UNIVERSAL TRADING BOT STARTED!")
        safe_send(f"📈 Monitoring {len(active_symbols)} symbols")
        safe_send(f"🎯 Current mode: {TRADING_MODES[CURRENT_MODE]['name']}")
        
        if CURRENT_MODE == "SCALPING":
            safe_send(f"📊 Scalping strategy: {SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]['name']}")
        
        # Авто-импорт и синхронизация
        time.sleep(2)
        import_existing_positions()
        sync_balance_with_db()
        
        last_scan = 0
        last_auto_status = 0
        error_count = 0
        max_errors = 10
        
        logger.info("Main loop started")
        
        while True:
            try:
                current_time = time.time()
                settings = get_current_settings()
                
                # Проверка выходов
                if current_time - last_scan >= settings['scan_interval']:
                    check_position_exits()
                    
                    # Поиск новых входов
                    if can_open_new_trade():
                        usdt_balance = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
                        
                        for symbol in active_symbols:
                            if (symbol not in get_open_positions() and 
                                not is_in_cooldown(symbol) and 
                                usdt_balance > MIN_TRADE_USDT):
                                
                                if CURRENT_MODE == "SCALPING":
                                    should_enter, entry_info = get_scalping_signal(symbol)
                                else:
                                    should_enter, entry_info = should_enter_swing_position(symbol)
                                
                                if should_enter and isinstance(entry_info, dict):
                                    usdt_amount = compute_equity() * settings['trade_pct']
                                    
                                    current_usdt_balance = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
                                    if current_usdt_balance < usdt_amount:
                                        continue
                                    
                                    if check_min_order_value(symbol, usdt_amount) and usdt_amount <= current_usdt_balance:
                                        base_amount = round_amount(symbol, usdt_amount / entry_info['price'])
                                        
                                        if not DRY_RUN:
                                            try:
                                                order = exchange.create_market_order(symbol, 'buy', base_amount)
                                                logger.info(f"Buy order executed for {symbol}")
                                                
                                                actual_amount = base_amount
                                                actual_price = entry_info['price']
                                                
                                                if 'filled' in order and order['filled'] is not None:
                                                    actual_amount = float(order['filled'])
                                                if 'price' in order and order['price'] is not None:
                                                    actual_price = float(order['price'])
                                                
                                                actual_usdt_amount = actual_amount * actual_price
                                                
                                                strategy_name = CURRENT_SCALPING_STRATEGY if CURRENT_MODE == "SCALPING" else ""
                                                record_open_position(symbol, actual_amount, actual_price, entry_info['stop_loss'], entry_info['take_profit'], strategy_name)
                                                update_daily_trade_count(symbol)
                                                
                                                mode_name = TRADING_MODES[CURRENT_MODE]['name']
                                                if CURRENT_MODE == "SCALPING":
                                                    strategy_text = f" | {SCALPING_STRATEGIES[CURRENT_SCALPING_STRATEGY]['name']}"
                                                else:
                                                    strategy_text = ""
                                                
                                                safe_send(
                                                    f"🎯 ENTER {symbol} | {mode_name}{strategy_text}\n"
                                                    f"Price: {actual_price:.6f}\n"
                                                    f"Amount: {actual_amount:.4f} ({actual_usdt_amount:.2f} USDT)\n"
                                                    f"Score: {entry_info['score']}/100\n"
                                                    f"TP: +{settings['take_profit']*100:.1f}% | SL: -{settings['max_stop_loss']*100:.1f}%"
                                                )
                                            except Exception as e:
                                                logger.error(f"Error executing buy order for {symbol}: {e}")
                                                continue

                
                    last_scan = current_time
                    error_count = 0
                
                # Автоматический статус
                if current_time - last_auto_status >= settings['status_interval']:
                    send_auto_status()
                    last_auto_status = current_time
                
                time.sleep(5)
                
            except Exception as e:
                error_count += 1
                logger.error(f"Main loop error #{error_count}: {e}")
                
                if error_count >= max_errors:
                    safe_send(f"🆘 CRITICAL: Too many errors ({error_count}), restarting...")
                    raise e
                
                time.sleep(30)
                
    except KeyboardInterrupt:
        safe_send("⏹ Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error in main: {e}")
        safe_send(f"💥 FATAL ERROR: {e}")
    finally:
        try:
            conn.close()
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
            logger.info("Bot shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

if __name__ == "__main__":
    main()
