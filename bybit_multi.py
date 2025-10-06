#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bybit_multy_v8_ultimate.py — ультимативная версия с улучшенными настройками
"""
import os, sys, time, math, ccxt, pandas as pd, sqlite3
import logging
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, List, Optional, Tuple, Any

from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands, AverageTrueRange
from telegram import Bot, ParseMode
from telegram.ext import Updater, CommandHandler

# ====== ULTIMATE CONFIG ======
API_KEY = os.getenv("BYBIT_API_KEY", "BAD0EojgsWuAi8pWj1")
API_SECRET = os.getenv("BYBIT_API_SECRET", "xsZqrt7UvC9SJHEMdeEiavO3HGxvVKjEwKdL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "6021208398:AAEKxMvsNvpt-f1afnZ2TJwzcODCEQy6XZQ")
CHAT_ID = 279609886

# Расширенный список пар
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "AVAX/USDT", "DOT/USDT", "LINK/USDT", "MATIC/USDT",
    "DOGE/USDT", "LTC/USDT", "ATOM/USDT", "UNI/USDT", "XLM/USDT",
    "ETC/USDT", "FIL/USDT", "THETA/USDT", "EOS/USDT", "AAVE/USDT"
]

# Оптимизированные режимы работы
TRADING_MODES = {
    "AGGRESSIVE": {
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
    }
}

CURRENT_MODE = "CONSERVATIVE"

TIMEFRAME_ENTRY = "15m"
TIMEFRAME_TREND = "1h"
LIMIT = 100

# Минимальные суммы
MIN_TRADE_USDT = 10.0

MIN_USDT_PER_SYMBOL = {
    "BTC/USDT": 5.0, "ETH/USDT": 5.0, "BNB/USDT": 3.0, "SOL/USDT": 2.0,
    "XRP/USDT": 2.0, "ADA/USDT": 2.0, "AVAX/USDT": 2.0, "DOT/USDT": 2.0,
    "LINK/USDT": 2.0, "MATIC/USDT":2.0, "DOGE/USDT": 2.0, "LTC/USDT": 2.0,
    "ATOM/USDT": 2.0, "UNI/USDT": 2.0, "XLM/USDT": 2.0, "ETC/USDT": 2.0,
    "FIL/USDT": 2.0, "THETA/USDT": 2.0, "EOS/USDT": 2.0, "AAVE/USDT": 5.0,
}

TAKER_FEE = 0.001
ROUNDTRIP_FEE = TAKER_FEE * 2

LOCK_FILE = "/tmp/bybit_multy_v8_ultimate.lock"
DB_FILE = "trades_multi_v8_ultimate.db"

DRY_RUN = False  # Выключил dry run для реальной работы

# ====== ENHANCED LOGGING ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_v8_ultimate.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====== INIT ======
def initialize_database():
    """Инициализация базы данных с проверкой существующих таблиц"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    
    # Проверяем существование таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='positions'")
    table_exists = cursor.fetchone()
    
    if not table_exists:
        # Создаем новые таблицы с правильной схемой
        cursor.execute("""
        CREATE TABLE positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
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
            fee_paid REAL DEFAULT 0
        )
        """)
        
        cursor.execute("""
        CREATE TABLE trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            price REAL,
            usdt_amount REAL,
            base_amount REAL,
            fee REAL DEFAULT 0,
            time TEXT,
            timestamp INTEGER
        )
        """)
        
        cursor.execute("""
        CREATE TABLE symbol_cooldown (
            symbol TEXT PRIMARY KEY,
            last_closed_ts INTEGER DEFAULT 0,
            daily_trade_count INTEGER DEFAULT 0,
            last_trade_date TEXT
        )
        """)
        
        cursor.execute("""
        CREATE TABLE performance_stats (
            date TEXT PRIMARY KEY,
            total_trades INTEGER DEFAULT 0,
            winning_trades INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            total_volume REAL DEFAULT 0
        )
        """)
        
        logger.info("Database initialized with new schema")
    else:
        # Проверяем и добавляем недостающие колонки
        try:
            cursor.execute("ALTER TABLE positions ADD COLUMN pnl REAL DEFAULT 0")
            logger.info("Added pnl column to positions")
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
            
        try:
            cursor.execute("ALTER TABLE positions ADD COLUMN pnl_percent REAL DEFAULT 0")
            logger.info("Added pnl_percent column to positions")
        except sqlite3.OperationalError:
            pass
            
        try:
            cursor.execute("ALTER TABLE positions ADD COLUMN fee_paid REAL DEFAULT 0")
            logger.info("Added fee_paid column to positions")
        except sqlite3.OperationalError:
            pass
    
    conn.commit()
    return conn, cursor

# Инициализируем БД
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
    
    # Test connection
    exchange.fetch_balance()
    logger.info("Exchange connection successful")
except Exception as e:
    logger.error(f"Exchange connection failed: {e}")
    sys.exit(1)

bot = Bot(token=TELEGRAM_TOKEN)

# ====== IMPORT EXISTING POSITIONS ======
def import_existing_positions():
    """Импорт существующих позиций с биржи в БД с улучшенной логикой"""
    logger.info("Importing existing positions from exchange...")
    
    try:
        balance = fetch_balance()
        imported_count = 0
        updated_count = 0
        
        # Сначала получаем текущие открытые позиции из БД
        current_positions = get_open_positions()
        
        for symbol in active_symbols:
            base_currency = symbol.split('/')[0]
            base_balance = float(balance.get('total', {}).get(base_currency, 0) or 0)
            
            if base_balance > 0:
                # Получаем текущую цену
                try:
                    ticker = exchange.fetch_ticker(symbol)
                    current_price = float(ticker['last'])
                except Exception as e:
                    logger.error(f"Error getting price for {symbol}: {e}")
                    continue
                
                if symbol in current_positions:
                    # Позиция уже есть в БД - проверяем актуальность
                    db_balance = current_positions[symbol]['base_amount']
                    
                    if abs(base_balance - db_balance) / max(base_balance, db_balance) > 0.01:
                        # Обновляем количество
                        cursor.execute("UPDATE positions SET base_amount=? WHERE symbol=? AND status='OPEN'", (base_balance, symbol))
                        updated_count += 1
                        logger.info(f"Updated position {symbol}: {db_balance} -> {base_balance}")
                        
                else:
                    # Новая позиция - добавляем в БД
                    mode_settings = get_current_mode_settings()
                    stop_loss = current_price * (1 - mode_settings['max_stop_loss'])
                    take_profit = current_price * (1 + mode_settings['take_profit'])
                    
                    # Записываем позицию в БД
                    cursor.execute("""
                        INSERT INTO positions 
                        (symbol, base_amount, open_price, stop_loss, take_profit, max_price, open_time, status) 
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'), 'OPEN')
                    """, (symbol, base_balance, current_price, stop_loss, take_profit, current_price))
                    
                    # Записываем в историю как покупку
                    cursor.execute("""
                        INSERT INTO trade_history 
                        (symbol, action, price, usdt_amount, base_amount, time, timestamp) 
                        VALUES (?, 'BUY', ?, ?, ?, datetime('now'), ?)
                    """, (symbol, current_price, base_balance * current_price, base_balance, int(time.time())))
                    
                    imported_count += 1
                    logger.info(f"Imported position: {symbol} - {base_balance:.6f} @ {current_price:.6f}")
        
        conn.commit()
        
        if imported_count > 0 or updated_count > 0:
            safe_send(f"📥 Import results: {imported_count} new, {updated_count} updated")
        else:
            logger.info("No positions to import or update")
            
        return imported_count + updated_count
        
    except Exception as e:
        logger.error(f"Error importing existing positions: {e}")
        return 0
        
def cmd_force_sync(update, context):
    """Принудительная синхронизация всех позиций"""
    safe_send("🔧 Starting forced synchronization...")
    
    # Сначала импортируем все существующие позиции
    imported = import_existing_positions()
    
    # Затем синхронизируем баланс
    synced = sync_balance_with_db()
    
    # Показываем итоговый статус
    positions = get_open_positions()
    total_value = 0
    
    for symbol, pos in positions.items():
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
            position_value = pos['base_amount'] * current_price
            total_value += position_value
        except:
            continue
    
    bal = fetch_balance()
    usdt_balance = float(bal['free'].get('USDT', 0) or 0)
    total_equity = compute_equity()
    
    msg = f"✅ Force sync completed:\n"
    msg += f"• Imported/Updated: {imported} positions\n"
    msg += f"• Synced: {synced} positions\n"
    msg += f"• Total positions: {len(positions)}\n"
    msg += f"• Positions value: {total_value:.2f} USDT\n"
    msg += f"• USDT balance: {usdt_balance:.2f} USDT\n"
    msg += f"• Total equity: {total_equity:.2f} USDT\n"
    msg += f"• Check: {total_value + usdt_balance:.2f} vs {total_equity:.2f}"
    
    safe_send(msg)



# ====== IMPROVED CORE FUNCTIONS ======
def safe_send(text: str, max_retries: int = 3) -> bool:
    """Улучшенная отправка сообщений с обработкой ошибок"""
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
    """Улучшенный повторный вызов API с экспоненциальной задержкой"""
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
    """Безопасное получение OHLCV данных с улучшенной обработкой"""
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
    """Получение баланса с обработкой ошибок"""
    def _fetch():
        return exchange.fetch_balance()
    try:
        return retry_api_call(_fetch)
    except Exception as e:
        logger.error(f"Failed to fetch balance: {e}")
        return {'free': {'USDT': 0.0}, 'total': {'USDT': 0.0}, 'used': {'USDT': 0.0}}

def get_current_mode_settings() -> Dict:
    """Получение текущих настроек режима"""
    return TRADING_MODES[CURRENT_MODE]

def check_min_order_value(symbol: str, usdt_amount: float) -> bool:
    """Проверка минимальной суммы ордера"""
    min_value = MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)
    if usdt_amount < min_value:
        logger.debug(f"Order value {usdt_amount:.2f} below minimum {min_value:.2f} for {symbol}")
        return False
    return True

def round_amount(symbol: str, amount: float) -> float:
    """Точное округление количества с учетом лимитов биржи"""
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
            
        # Округление до шага
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
    """Получение минимального количества для торговли"""
    try:
        market = exchange.markets.get(symbol)
        if market:
            return float(market.get('limits', {}).get('amount', {}).get('min', 0.000001))
    except Exception as e:
        logger.error(f"Error getting min amount for {symbol}: {e}")
    return 0.000001

# ====== ENHANCED TRADING LOGIC ======
def get_trend_direction(df: pd.DataFrame) -> str:
    """Улучшенное определение направления тренда"""
    if df is None or len(df) < 50:
        return "SIDEWAYS"
    
    try:
        # Мульти-таймфрейм анализ тренда
        ema_fast = EMAIndicator(df['close'], window=9).ema_indicator()
        ema_slow = EMAIndicator(df['close'], window=21).ema_indicator()
        ema_trend = EMAIndicator(df['close'], window=50).ema_indicator()
        
        if len(ema_fast) < 5 or len(ema_slow) < 5 or len(ema_trend) < 5:
            return "SIDEWAYS"
            
        price = df['close'].iloc[-1]
        fast_ema = ema_fast.iloc[-1]
        slow_ema = ema_slow.iloc[-1]
        trend_ema = ema_trend.iloc[-1]
        
        # Проверка выстроенности EMA
        ema_alignment = (price > fast_ema > slow_ema > trend_ema) or (price < fast_ema < slow_ema < trend_ema)
        
        # Дополнительная проверка наклона EMA
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

def calculate_entry_signals(df: pd.DataFrame) -> Dict[str, Any]:
    """Улучшенный расчет сигналов для входа"""
    mode_settings = get_current_mode_settings()
    
    if df is None or len(df) < 30:
        return {'score': 0, 'error': 'Insufficient data'}
    
    try:
        current_price = df['close'].iloc[-1]
        current_volume = df['volume'].iloc[-1]
        volume_sma = df['volume'].tail(20).mean()
        
        # RSI с проверкой дивергенции
        rsi = RSIIndicator(df['close'], window=14).rsi().iloc[-1]
        rsi_prev = RSIIndicator(df['close'], window=14).rsi().iloc[-2] if len(df) > 1 else rsi
        
        # MACD
        macd_line, macd_signal, macd_hist = calc_macd(df['close'])
        macd_bullish = len(macd_hist) > 1 and macd_hist.iloc[-1] > 0 and macd_hist.iloc[-1] > macd_hist.iloc[-2]
        
        # ADX с силой тренда
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
        volume_ok = volume_ratio > mode_settings['volume_multiplier']
        
        # Price position relative to BB
        bb_signal = 0.2 <= bb_position <= 0.8
        
        signals = {
            'price': current_price,
            'volume_ok': volume_ok,
            'volume_ratio': volume_ratio,
            'rsi_ok': mode_settings['rsi_min'] <= rsi <= mode_settings['rsi_max'],
            'rsi_value': rsi,
            'rsi_trend': 'BULLISH' if rsi > rsi_prev else 'BEARISH',
            'macd_bullish': macd_bullish,
            'adx_strong': adx >= mode_settings['adx_min'],
            'adx_value': adx,
            'adx_bullish': adx_bullish,
            'bb_position': bb_position,
            'bb_signal': bb_signal,
            'stoch_ok': 20 <= stoch <= 80,
            'stoch_value': stoch,
            'trend': get_trend_direction(df)
        }
        
        # Enhanced score calculation
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
        
        signals['score'] = min(score, 100)  # Cap at 100
        
        return signals
        
    except Exception as e:
        logger.error(f"Error calculating entry signals: {e}")
        return {'score': 0, 'error': str(e)}

def should_enter_position(symbol: str) -> Tuple[bool, Any]:
    """Улучшенная логика входа в позицию"""
    try:
        # Проверка тренда на старшем таймфрейме
        df_trend = get_ohlcv_data(symbol, TIMEFRAME_TREND, 100)
        if df_trend is None or len(df_trend) < 50:
            return False, "No trend data"
            
        trend = get_trend_direction(df_trend)
        if trend != "BULLISH":
            return False, f"Trend not bullish: {trend}"
        
        # Анализ на таймфрейме входа
        df_entry = get_ohlcv_data(symbol, TIMEFRAME_ENTRY, 100)
        if df_entry is None or len(df_entry) < 30:
            return False, "No entry data"
            
        signals = calculate_entry_signals(df_entry)
        mode_settings = get_current_mode_settings()
        
        if 'error' in signals:
            return False, f"Signal error: {signals['error']}"
        
        # Условия входа
        entry_conditions = (
            signals['score'] >= mode_settings['min_score'],
            signals['volume_ok'],
            signals['trend'] == "BULLISH",
            signals.get('adx_strong', False),
            not is_in_cooldown(symbol),
            check_daily_trade_limit(symbol)
        )
        
        if all(entry_conditions):
            # Расчет TP/SL
            sl_price = signals['price'] * (1 - mode_settings['max_stop_loss'])
            tp_price = signals['price'] * (1 + mode_settings['take_profit'])
            
            entry_info = {
                'price': signals['price'],
                'stop_loss': sl_price,
                'take_profit': tp_price,
                'score': signals['score'],
                'rsi': signals['rsi_value'],
                'adx': signals['adx_value'],
                'volume_ratio': signals.get('volume_ratio', 1),
                'signals': signals
            }
            
            return True, entry_info
        else:
            reason = f"Score: {signals['score']}, Volume: {signals['volume_ok']}, Trend: {signals['trend']}, ADX: {signals.get('adx_strong', False)}"
            return False, reason
            
    except Exception as e:
        logger.error(f"Entry check error {symbol}: {e}")
        return False, f"Error: {str(e)}"

def get_ohlcv_data(symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    """Получение и подготовка OHLCV данных"""
    ohlcv = fetch_ohlcv(symbol, timeframe, limit)
    if not ohlcv or len(ohlcv) < 20:
        return None
        
    try:
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
    except Exception as e:
        logger.error(f"Error processing OHLCV data for {symbol}: {e}")
        return None

def calc_macd(series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Расчет MACD"""
    try:
        macd_obj = MACD(series, window_slow=26, window_fast=12, window_sign=9)
        return macd_obj.macd(), macd_obj.macd_signal(), macd_obj.macd_diff()
    except Exception as e:
        logger.error(f"Error calculating MACD: {e}")
        return pd.Series(), pd.Series(), pd.Series()

# ====== ENHANCED POSITION MANAGEMENT ======
def safe_close_position(symbol: str, reason: str = "") -> bool:
    """Безопасное закрытие позиции с улучшенной логикой"""
    try:
        if DRY_RUN:
            logger.info(f"DRY RUN: Would close {symbol} - {reason}")
            mark_position_closed(symbol)
            return True

        bal = fetch_balance()
        base = symbol.split("/")[0]
        real_balance = float(bal['free'].get(base, 0) or 0)
        
        logger.info(f"Closing {symbol}: balance = {real_balance}")
        
        if real_balance <= 0:
            logger.info(f"No balance for {symbol}, marking as closed")
            mark_position_closed(symbol)
            return True

        # Получаем текущую цену через тикер (более надежно)
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
            logger.info(f"Got price via ticker for {symbol}: {current_price}")
        except Exception as e:
            logger.error(f"Failed to get price for {symbol}: {e}")
            return False

        amount_to_sell = round_amount(symbol, real_balance)
        min_amount = get_min_amount(symbol)
        
        if amount_to_sell < min_amount:
            logger.info(f"Amount too small for {symbol}: {amount_to_sell} < {min_amount}")
            mark_position_closed(symbol)
            return True

        # Проверка минимальной стоимости ордера
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
            logger.info(f"Executing market sell for {symbol}: {amount_to_sell:.6f} @ {current_price:.6f}")
            order = exchange.create_market_order(symbol, 'sell', amount_to_sell)
            logger.info(f"Sell order executed for {symbol}")
            
            # Получаем фактическое количество из ордера если доступно
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
                
                pnl_emoji = "🟢" if pnl >= 0 else "🔴"
                safe_send(
                    f"✅ {pnl_emoji} Closed {symbol}\n"
                    f"Amount: {actual_amount:.4f} @ {actual_price:.6f}\n"
                    f"P&L: {pnl:+.4f} USDT ({pnl_percent:+.2f}%)\n"
                    f"Reason: {reason}"
                )
            else:
                record_close(symbol, actual_price, close_value, actual_amount)
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
    cursor.execute("SELECT base_amount, open_price FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
    row = cursor.fetchone()
    if row:
        return {'base_amount': row[0], 'open_price': row[1]}
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

def check_position_exits():
    """Улучшенная проверка условий выхода"""
    positions = get_open_positions()
    mode_settings = get_current_mode_settings()
    
    for symbol, pos in positions.items():
        try:
            ohlcv = fetch_ohlcv(symbol, TIMEFRAME_ENTRY, limit=20)
            if not ohlcv:
                continue
                
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume']).astype(float)
            current_price = df['close'].iloc[-1]
            open_price = pos['open_price']
            stop_loss = pos['stop_loss']
            take_profit = pos['take_profit']
            max_price = pos.get('max_price', open_price)
            
            # Обновление максимальной цены
            if current_price > max_price:
                update_max_price_db(symbol, current_price)
                max_price = current_price
            
            profit_pct = (current_price - open_price) / open_price
            
            exit_reason = ""
            
            # Стоп-лосс
            if current_price <= stop_loss:
                exit_reason = f"SL {profit_pct*100:+.2f}%"
            
            # Тейк-профит
            elif current_price >= take_profit:
                exit_reason = f"TP {profit_pct*100:+.2f}%"
            
            # Трейлинг-стоп
            elif profit_pct >= mode_settings['trailing_start']:
                trail_level = max_price * (1 - mode_settings['trailing_step'])
                if current_price <= trail_level:
                    exit_reason = f"TRAIL {profit_pct*100:+.2f}%"
            
            # Экстренный выход при большой просадке
            elif profit_pct <= -mode_settings['max_stop_loss'] * 1.5:  # 1.5x от обычного SL
                exit_reason = f"EMERGENCY {profit_pct*100:+.2f}%"
            
            if exit_reason:
                logger.info(f"Exit condition triggered for {symbol}: {exit_reason}")
                safe_close_position(symbol, exit_reason)
                
        except Exception as e:
            logger.error(f"Exit check error {symbol}: {e}")

# ====== ENHANCED DATABASE FUNCTIONS ======
def get_open_positions() -> Dict[str, Dict]:
    """Получение открытых позиций"""
    try:
        cursor.execute("SELECT symbol, base_amount, open_price, stop_loss, take_profit, max_price FROM positions WHERE status='OPEN'")
        rows = cursor.fetchall()
        logger.info(f"Found {len(rows)} open positions in database")
        
        positions = {}
        for row in rows:
            symbol = row[0]
            positions[symbol] = {
                "base_amount": row[1], 
                "open_price": row[2], 
                "stop_loss": row[3],
                "take_profit": row[4],
                "max_price": row[5] or row[2]
            }
            logger.info(f"Position: {symbol} - Amount: {row[1]}, Price: {row[2]}")
        
        return positions
        
    except Exception as e:
        logger.error(f"Error getting open positions: {e}")
        return {}


def record_open(symbol: str, base_amount: float, open_price: float, stop_loss: float, take_profit: float):
    """Запись открытия позиции с улучшенной логикой"""
    try:
        # Рассчитываем USDT сумму с учетом фактического исполнения
        usdt_amount = base_amount * open_price
        fee = usdt_amount * TAKER_FEE
        
        cursor.execute("""
            INSERT INTO positions (symbol, base_amount, open_price, stop_loss, take_profit, max_price, open_time, fee_paid) 
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)
        """, (symbol, base_amount, open_price, stop_loss, take_profit, open_price, fee))
        
        cursor.execute("""
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp) 
            VALUES (?, 'BUY', ?, ?, ?, ?, datetime('now'), ?)
        """, (symbol, open_price, usdt_amount, base_amount, fee, int(time.time())))
        
        conn.commit()
        logger.info(f"Recorded open position for {symbol}: {base_amount:.6f} @ {open_price:.6f} = {usdt_amount:.2f} USDT")
    except Exception as e:
        logger.error(f"Error recording open position: {e}")
        conn.rollback()


def record_close(symbol: str, price: float, usdt_amount: float, base_amount: float):
    """Запись закрытия позиции (без PnL)"""
    try:
        fee = usdt_amount * TAKER_FEE
        cursor.execute("UPDATE positions SET status='CLOSED', close_time=datetime('now'), close_price=? WHERE symbol=? AND status='OPEN'", (price, symbol))
        cursor.execute("""
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp) 
            VALUES (?, 'SELL', ?, ?, ?, ?, datetime('now'), ?)
        """, (symbol, price, usdt_amount, base_amount, fee, int(time.time())))
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
            INSERT INTO trade_history (symbol, action, price, usdt_amount, base_amount, fee, time, timestamp) 
            VALUES (?, 'SELL', ?, ?, ?, ?, datetime('now'), ?)
        """, (symbol, price, usdt_amount, base_amount, fee, int(time.time())))
        
        cursor.execute("REPLACE INTO symbol_cooldown (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
        conn.commit()
        
        logger.info(f"Recorded close position for {symbol} with PnL: {pnl:.4f} USDT ({pnl_percent:.2f}%)")
    except Exception as e:
        logger.error(f"Error recording close position with PnL: {e}")

def update_max_price_db(symbol: str, price: float):
    """Обновление максимальной цены"""
    cursor.execute("UPDATE positions SET max_price=? WHERE symbol=? AND status='OPEN'", (price, symbol))
    conn.commit()

def is_in_cooldown(symbol: str) -> bool:
    """Проверка кудоуна для символа"""
    mode_settings = get_current_mode_settings()
    cursor.execute("SELECT last_closed_ts FROM symbol_cooldown WHERE symbol=?", (symbol,))
    row = cursor.fetchone()
    if not row:
        return False
    return (time.time() - int(row[0])) < mode_settings['cooldown']

def check_daily_trade_limit(symbol: str) -> bool:
    """Проверка дневного лимита trades для символа"""
    mode_settings = get_current_mode_settings()
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute("SELECT daily_trade_count, last_trade_date FROM symbol_cooldown WHERE symbol=?", (symbol,))
    row = cursor.fetchone()
    
    if not row:
        return True
        
    daily_count, last_date = row
    if last_date != today:
        # Сброс счетчика на новый день
        cursor.execute("UPDATE symbol_cooldown SET daily_trade_count=0, last_trade_date=? WHERE symbol=?", (today, symbol))
        conn.commit()
        return True
        
    return daily_count < mode_settings['max_daily_trades_per_symbol']

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
    
    # Учет открытых позиций
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
    mode_settings = get_current_mode_settings()
    return get_concurrent_trades_count() < mode_settings['max_trades']

def realized_pnl_total() -> float:
    """Расчет общего реализованного PnL"""
    try:
        cursor.execute("SELECT SUM(pnl) FROM positions WHERE status='CLOSED' AND pnl IS NOT NULL")
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        logger.error(f"Realized PnL error: {e}")
        return 0.0

def unrealized_pnl_total() -> float:
    """Расчет общего нереализованного PnL"""
    total = 0.0
    try:
        positions = get_open_positions()
        for sym, pos in positions.items():
            ohlcv = fetch_ohlcv(sym, TIMEFRAME_ENTRY, limit=1)
            if ohlcv:
                price = float(ohlcv[-1][4])
                current_value = price * pos['base_amount']
                open_value = pos['open_price'] * pos['base_amount']
                total += (current_value - open_value)
    except Exception as e:
        logger.error(f"Unrealized PnL error: {e}")
    return total

def get_trading_stats() -> Dict[str, Any]:
    """Получение статистики trading с защитой от ошибок"""
    try:
        # Базовая статистика
        cursor.execute("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(usdt_amount) as total_volume,
                SUM(fee) as total_fees
            FROM trade_history
        """)
        stats_row = cursor.fetchone()
        
        # Статистика по закрытым сделкам
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
        # Возвращаем безопасные значения по умолчанию
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

# ====== TELEGRAM COMMANDS ======
def cmd_status(update, context):
    """Команда статуса с защитой от ошибок"""
    try:
        equity = compute_equity()
        realized = realized_pnl_total()
        unrealized = unrealized_pnl_total()
        positions = get_open_positions()
        stats = get_trading_stats()
        mode_settings = get_current_mode_settings()
        
        total_pnl = realized + unrealized
        pnl_color = "🟢" if total_pnl >= 0 else "🔴"
        
        msg = f"📊 <b>ULTIMATE BOT v8.1 - {CURRENT_MODE} MODE</b>\n\n"
        
        msg += f"💰 <b>Equity:</b> {equity:.2f} USDT\n"
        msg += f"📈 <b>PnL:</b> {pnl_color} {total_pnl:+.2f} USDT "
        msg += f"(Realized: {realized:+.2f} | Unrealized: {unrealized:+.2f})\n"
        msg += f"🔢 <b>Positions:</b> {len(positions)}/{mode_settings['max_trades']}\n"
        msg += f"💸 <b>Total Fees:</b> {stats.get('total_fees', 0):.2f} USDT\n\n"
        
        # Безопасная проверка closed_trades
        closed_trades = stats.get('closed_trades', 0)
        if closed_trades > 0:
            winning_trades = stats.get('winning_trades', 0)
            win_rate = (winning_trades / closed_trades) * 100
            msg += f"📊 <b>Statistics:</b>\n"
            msg += f"• Win Rate: {win_rate:.1f}% ({winning_trades}/{closed_trades})\n"
            msg += f"• Avg Win: {stats.get('avg_win_pct', 0):.2f}%\n"
            msg += f"• Avg Loss: {stats.get('avg_loss_pct', 0):.2f}%\n"
            msg += f"• Total PnL: {stats.get('total_pnl', 0):.2f} USDT\n\n"
        
        bal = fetch_balance()
        usdt_free = float(bal.get('free', {}).get('USDT', 0) or 0)
        
        if positions:
            msg += f"📈 <b>Open Positions ({len(positions)}):</b>\n"
            for sym, pos in positions.items():
                try:
                    # Получаем текущую цену
                    ticker = exchange.fetch_ticker(sym)
                    current_price = float(ticker['last'])
                    
                    # Рассчитываем P&L
                    profit = (current_price - pos['open_price']) / pos['open_price'] * 100
                    profit_net = profit - ROUNDTRIP_FEE * 100
                    
                    position_value = current_price * pos['base_amount']
                    sl_distance = ((current_price - pos['stop_loss']) / current_price) * 100
                    tp_distance = ((pos['take_profit'] - current_price) / current_price) * 100
                    
                    emoji = "🟢" if profit_net > 0 else "🔴"
                    base_currency = sym.split('/')[0]
                    
                    msg += f"{emoji} <b>{sym}</b>\n"
                    msg += f"   Amount: {pos['base_amount']:.4f} {base_currency}\n"
                    msg += f"   Entry: {pos['open_price']:.6f} | Current: {current_price:.6f}\n"
                    msg += f"   P&L: {profit_net:+.2f}% | Value: {position_value:.2f} USDT\n"
                    msg += f"   SL: -{sl_distance:.1f}% | TP: +{tp_distance:.1f}%\n"
                    
                except Exception as e:
                    logger.error(f"Error processing position {sym}: {e}")
                    msg += f"❌ <b>{sym}</b> - Error getting data\n"
        else:
            msg += "📭 <b>No open positions</b>\n"
        
        msg += f"\n⚙️ <b>Mode Settings:</b>\n"
        msg += f"• Scan: {mode_settings['scan_interval']}s | "
        msg += f"Cooldown: {mode_settings['cooldown']//60}min\n"
        msg += f"• RSI: {mode_settings['rsi_min']}-{mode_settings['rsi_max']} | "
        msg += f"Min Score: {mode_settings['min_score']}/100\n"
        msg += f"• TP/SL: +{mode_settings['take_profit']*100:.1f}%/ -{mode_settings['max_stop_loss']*100:.1f}%\n"
        msg += f"• Available: {usdt_free:.2f} USDT"
        
        safe_send(msg)
        
    except Exception as e:
        logger.error(f"Error in cmd_status: {e}")
        safe_send(f"❌ Error generating status: {str(e)}")


def cmd_close(update, context):
    """Команда закрытия позиции"""
    try:
        if not context.args:
            update.message.reply_text("Usage: /close SYMBOL")
            return
            
        symbol = context.args[0].upper() + "/USDT" if "/" not in context.args[0].upper() else context.args[0].upper()
        if safe_close_position(symbol, "Manual"):
            update.message.reply_text(f"✅ Closed {symbol}")
        else:
            update.message.reply_text(f"❌ Failed to close {symbol}")
    except Exception as e:
        update.message.reply_text(f"Error: {str(e)}")

def cmd_restart(update, context):
    """Команда перезапуска"""
    safe_send("♻️ Restarting bot...")
    conn.close()
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
    python = sys.executable
    os.execl(python, python, *sys.argv)

def cmd_mode(update, context):
    """Команда смены режима"""
    global CURRENT_MODE
    
    if CURRENT_MODE == "AGGRESSIVE":
        CURRENT_MODE = "CONSERVATIVE"
        mode_msg = "🟢 CONSERVATIVE MODE"
    else:
        CURRENT_MODE = "AGGRESSIVE" 
        mode_msg = "🔴 AGGRESSIVE MODE"
    
    mode_settings = get_current_mode_settings()
    msg = f"{mode_msg}\n"
    msg += f"• Max trades: {mode_settings['max_trades']}\n"
    msg += f"• Trade size: {mode_settings['trade_pct']*100}%\n"
    msg += f"• TP/SL: +{mode_settings['take_profit']*100:.1f}%/ -{mode_settings['max_stop_loss']*100:.1f}%\n"
    msg += f"• RSI: {mode_settings['rsi_min']}-{mode_settings['rsi_max']}\n"
    msg += f"• Scan: {mode_settings['scan_interval']}s\n"
    msg += f"• Cooldown: {mode_settings['cooldown']//60}min\n"
    msg += f"• Daily trades per symbol: {mode_settings['max_daily_trades_per_symbol']}"
    
    safe_send(msg)

def cmd_debug(update, context):
    """Команда отладки"""
    debug_info = []
    checked = 0
    
    for symbol in active_symbols[:5]:
        if checked >= 3:
            break
            
        should_enter, entry_info = should_enter_position(symbol)
        if isinstance(entry_info, dict):
            usdt_amount = compute_equity() * get_current_mode_settings()['trade_pct']
            usdt_free = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
            
            debug_info.append(f"🎯 {symbol}: Score {entry_info['score']} | RSI {entry_info['rsi']:.1f}")
            debug_info.append(f"   Volume: {entry_info.get('volume_ratio', 1):.1f}x | ADX: {entry_info['adx']:.1f}")
            debug_info.append(f"   Need: {usdt_amount:.2f} USDT | Have: {usdt_free:.2f} USDT")
            checked += 1
    
    if debug_info:
        safe_send("🔍 TOP SIGNALS:\n" + "\n".join(debug_info))
    else:
        safe_send("🔍 No strong signals found")

def cmd_sync(update, context):
    """Синхронизация баланса с БД"""
    changes_count = sync_balance_with_db()
    if changes_count == 0:
        safe_send("✅ Balance already synchronized with DB")
    else:
        safe_send(f"✅ Synchronized {changes_count} positions")

def cmd_import(update, context):
    """Команда импорта существующих позиций"""
    imported_count = import_existing_positions()
    if imported_count > 0:
        safe_send(f"📥 Imported {imported_count} positions from exchange")
    else:
        safe_send("✅ No new positions to import")

def cmd_stats(update, context):
    """Детальная статистика"""
    stats = get_trading_stats()
    equity = compute_equity()
    realized = realized_pnl_total()
    
    msg = f"📈 <b>Detailed Statistics</b>\n\n"
    
    closed_trades = stats.get('closed_trades', 0)
    if closed_trades > 0:
        winning_trades = stats.get('winning_trades', 0)
        win_rate = (winning_trades / closed_trades) * 100
        total_return = (realized / equity) * 100 if equity > 0 else 0
        
        msg += f"📊 <b>Performance:</b>\n"
        msg += f"• Win Rate: {win_rate:.1f}% ({winning_trades}/{closed_trades})\n"
        msg += f"• Avg Win: {stats.get('avg_win_pct', 0):.2f}%\n"
        msg += f"• Avg Loss: {stats.get('avg_loss_pct', 0):.2f}%\n"
        msg += f"• Total Return: {total_return:+.2f}%\n"
        msg += f"• Total PnL: {stats.get('total_pnl', 0):.2f} USDT\n"
        msg += f"• Total Trades: {stats['total_trades']}\n"
        msg += f"• Trade Volume: {stats['total_volume']:.0f} USDT\n"
        msg += f"• Total Fees: {stats.get('total_fees', 0):.2f} USDT\n\n"
    
    mode_settings = get_current_mode_settings()
    msg += f"⚙️ <b>Current Settings:</b>\n"
    msg += f"• Mode: {CURRENT_MODE}\n"
    msg += f"• TP/SL: +{mode_settings['take_profit']*100:.1f}%/ -{mode_settings['max_stop_loss']*100:.1f}%\n"
    msg += f"• Position Size: {mode_settings['trade_pct']*100}%\n"
    msg += f"• RSI Range: {mode_settings['rsi_min']}-{mode_settings['rsi_max']}\n"
    msg += f"• Min Score: {mode_settings['min_score']}/100\n"
    msg += f"• Daily Trades/Symbol: {mode_settings['max_daily_trades_per_symbol']}"
    
    safe_send(msg)

def sync_balance_with_db() -> int:
    """Синхронизация баланса с БД с улучшенной логикой"""
    safe_send("🔄 Starting balance synchronization...")
    
    bal = fetch_balance()
    synced_count = 0
    created_count = 0
    closed_count = 0
    
    # Получаем текущие открытые позиции из БД
    current_positions = get_open_positions()
    
    for symbol in active_symbols:
        try:
            base = symbol.split("/")[0]
            real_balance = float(bal['total'].get(base, 0) or 0)
            
            if symbol in current_positions:
                # Позиция есть в БД - проверяем актуальность
                db_balance = current_positions[symbol]['base_amount']
                
                if real_balance <= 0:
                    # Баланса нет - закрываем позицию
                    cursor.execute("UPDATE positions SET status='CLOSED', close_time=datetime('now') WHERE symbol=? AND status='OPEN'", (symbol,))
                    closed_count += 1
                    logger.info(f"Closed position {symbol} - zero balance")
                    
                elif abs(real_balance - db_balance) / max(real_balance, db_balance) > 0.01:  # Разница более 1%
                    # Синхронизируем количество с реальным балансом
                    cursor.execute("UPDATE positions SET base_amount=? WHERE symbol=? AND status='OPEN'", (real_balance, symbol))
                    synced_count += 1
                    logger.info(f"Synced position {symbol}: {db_balance} -> {real_balance}")
                        
            else:
                # Позиции нет в БД, но есть в балансе - создаем
                if real_balance > 0:
                    try:
                        # Получаем текущую цену
                        ticker = exchange.fetch_ticker(symbol)
                        current_price = float(ticker['last'])
                        
                        # Используем текущую цену как приблизительную цену открытия
                        open_price = current_price
                        
                        mode_settings = get_current_mode_settings()
                        sl_price = open_price * (1 - mode_settings['max_stop_loss'])
                        tp_price = open_price * (1 + mode_settings['take_profit'])
                        
                        record_open(symbol, real_balance, open_price, sl_price, tp_price)
                        created_count += 1
                        logger.info(f"Created position {symbol}: {real_balance} @ {open_price:.6f}")
                        
                    except Exception as e:
                        logger.error(f"Error creating position for {symbol}: {e}")
                        
        except Exception as e:
            logger.error(f"Sync error for {symbol}: {e}")
    
    conn.commit()
    
    # Проверяем расхождения
    positions_after_sync = get_open_positions()
    
    # Рассчитываем общую стоимость позиций в БД
    total_db_value = 0
    for symbol, pos in positions_after_sync.items():
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
            total_db_value += pos['base_amount'] * current_price
        except:
            continue
    
    # Рассчитываем общую стоимость из баланса (исключая USDT)
    total_balance_value = 0
    for symbol in active_symbols:
        base = symbol.split("/")[0]
        amount = float(bal['total'].get(base, 0) or 0)
        if amount > 0:
            try:
                ticker = exchange.fetch_ticker(symbol)
                current_price = float(ticker['last'])
                total_balance_value += amount * current_price
            except:
                continue
    
    usdt_balance = float(bal['free'].get('USDT', 0) or 0)
    total_equity = compute_equity()
    
    msg = f"✅ Sync completed:\n"
    msg += f"• Synced: {synced_count} positions\n"
    msg += f"• Created: {created_count} positions\n" 
    msg += f"• Closed: {closed_count} positions\n"
    msg += f"• Total open: {len(positions_after_sync)} positions\n\n"
    
    msg += f"💰 Balance check:\n"
    msg += f"• DB positions value: {total_db_value:.2f} USDT\n"
    msg += f"• Balance positions value: {total_balance_value:.2f} USDT\n"
    msg += f"• USDT balance: {usdt_balance:.2f} USDT\n"
    msg += f"• Total equity: {total_equity:.2f} USDT\n"
    
    # Проверяем расхождение
    discrepancy = abs((total_db_value + usdt_balance) - total_equity)
    if discrepancy > 1.0:  # Если расхождение больше 1 USDT
        msg += f"⚠️ Discrepancy: {discrepancy:.2f} USDT\n"
    else:
        msg += f"✅ Balance match: OK"
    
    safe_send(msg)
    return synced_count + created_count + closed_count



# ====== MAIN EXECUTION ======
def main():
    """Основная функция запуска бота"""
    try:
        # Загрузка маркетов
        global active_symbols
        markets = exchange.load_markets()
        active_symbols = [s for s in SYMBOLS if s in markets]
        
        logger.info(f"Loaded {len(active_symbols)} active symbols")
        
        # Инициализация Telegram
        updater = Updater(TELEGRAM_TOKEN, use_context=True)
        updater.dispatcher.add_handler(CommandHandler("status", cmd_status))
        updater.dispatcher.add_handler(CommandHandler("close", cmd_close))
        updater.dispatcher.add_handler(CommandHandler("restart", cmd_restart))
        updater.dispatcher.add_handler(CommandHandler("mode", cmd_mode))
        updater.dispatcher.add_handler(CommandHandler("debug", cmd_debug))
        updater.dispatcher.add_handler(CommandHandler("sync", cmd_sync))
        updater.dispatcher.add_handler(CommandHandler("import", cmd_import))
        updater.dispatcher.add_handler(CommandHandler("stats", cmd_stats))
        updater.dispatcher.add_handler(CommandHandler("force_sync", cmd_force_sync))

        updater.start_polling()
        
        safe_send(f"🚀 ULTIMATE BOT v8.1 STARTED - {CURRENT_MODE} MODE")
        safe_send(f"📈 Monitoring {len(active_symbols)} symbols")
        safe_send(f"🔧 DRY RUN: {DRY_RUN}")
        
        # Авто-импорт существующих позиций
        time.sleep(2)
        imported_count = import_existing_positions()
        if imported_count > 0:
            safe_send(f"📥 Auto-imported {imported_count} existing positions")
        
        # Авто-синхронизация
        sync_balance_with_db()
        
        last_scan = 0
        last_auto_status = 0
        error_count = 0
        max_errors = 10
        
        logger.info("Main loop started")
        
        while True:
            try:
                current_time = time.time()
                mode_settings = get_current_mode_settings()
                
                # Проверка выходов
                if current_time - last_scan >= mode_settings['scan_interval']:
                    check_position_exits()
                    
                    # Поиск новых входов
                    # В основном цикле, в секции открытия позиций, добавьте:
                    if can_open_new_trade():
                        usdt_balance = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
                        
                        for symbol in active_symbols:
                            if (symbol not in get_open_positions() and 
                                not is_in_cooldown(symbol) and 
                                usdt_balance > MIN_TRADE_USDT):
                                
                                should_enter, entry_info = should_enter_position(symbol)
                                
                                if should_enter and isinstance(entry_info, dict):
                                    usdt_amount = compute_equity() * mode_settings['trade_pct']
                                    
                                    # Обновляем баланс перед проверкой
                                    current_usdt_balance = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
                                    if current_usdt_balance < usdt_amount:
                                        logger.warning(f"Insufficient balance for {symbol}: {current_usdt_balance:.2f} < {usdt_amount:.2f}")
                                        continue
                                    
                                    if check_min_order_value(symbol, usdt_amount) and usdt_amount <= current_usdt_balance:
                                        base_amount = round_amount(symbol, usdt_amount / entry_info['price'])
                                        
                                        if not DRY_RUN:
                                            try:
                                                order = exchange.create_market_order(symbol, 'buy', base_amount)
                                                logger.info(f"Buy order executed for {symbol}")
                                                
                                                # Используем фактические данные из ордера
                                                actual_amount = base_amount
                                                actual_price = entry_info['price']
                                                
                                                if 'filled' in order and order['filled'] is not None:
                                                    actual_amount = float(order['filled'])
                                                if 'price' in order and order['price'] is not None:
                                                    actual_price = float(order['price'])
                                                
                                                # Пересчитываем фактическую сумму USDT
                                                actual_usdt_amount = actual_amount * actual_price
                                                
                                                record_open(symbol, actual_amount, actual_price, entry_info['stop_loss'], entry_info['take_profit'])
                                                update_daily_trade_count(symbol)
                                                
                                                safe_send(
                                                    f"🎯 ENTER {symbol} | {CURRENT_MODE}\n"
                                                    f"Price: {actual_price:.6f}\n"
                                                    f"Amount: {actual_amount:.4f} ({actual_usdt_amount:.2f} USDT)\n"
                                                    f"Score: {entry_info['score']}/100\n"
                                                    f"TP: +{mode_settings['take_profit']*100:.1f}% | SL: -{mode_settings['max_stop_loss']*100:.1f}%"
                                                )
                                            except Exception as e:
                                                logger.error(f"Error executing buy order for {symbol}: {e}")
                                                continue

                
                    last_scan = current_time
                    error_count = 0  # Reset error count on successful cycle
                
                # Автоматический статус
                if current_time - last_auto_status >= mode_settings['status_interval']:
                    cmd_status(None, None)
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
