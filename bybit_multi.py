#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bybit_multy_v8_ultimate.py — ультимативная версия с улучшенными настройками
"""
import os, sys, time, math, ccxt, pandas as pd, sqlite3
import logging
from datetime import datetime
import numpy as np

from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands, AverageTrueRange
from telegram import Bot, ParseMode
from telegram.ext import Updater, CommandHandler

# ====== ULTIMATE CONFIG ======
API_KEY = os.getenv("BYBIT_API_KEY", "BB_A{I")
API_SECRET = os.getenv("BYBIT_API_SECRET", "BB_S")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "TG_TK")
CHAT_ID = CHAT_ID

# Расширенный список пар
MIN_USDT_PER_SYMBOL = {
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "AVAX/USDT", "DOT/USDT", "LINK/USDT", "MATIC/USDT",
    "DOGE/USDT", "LTC/USDT", "ATOM/USDT", "UNI/USDT", "XLM/USDT",
    "ETC/USDT", "FIL/USDT", "THETA/USDT", "EOS/USDT", "AAVE/USDT"
}

# Оптимизированные режимы работы
TRADING_MODES = {
    "AGGRESSIVE": {
        "scan_interval": 10,        # 10 секунд
        "status_interval": 300,     # 5 минут
        "max_trades": 12,
        "trade_pct": 0.08,          # 8%
        "rsi_min": 35,
        "rsi_max": 75,
        "volume_multiplier": 1.2,
        "adx_min": 15,
        "min_score": 60,
        "cooldown": 2 * 60,         # 2 минуты
        # АГРЕССИВНЫЕ TP/SL - быстрые тейки
        "max_stop_loss": 0.008,     # 0.8%
        "take_profit": 0.012,       # 1.2%
        "trailing_start": 0.004,    # 0.4%
        "trailing_step": 0.002      # 0.2%
    },
    "CONSERVATIVE": {
        "scan_interval": 30,        # 30 секунд
        "status_interval": 600,     # 10 минут
        "max_trades": 6,
        "trade_pct": 0.10,          # 10%
        "rsi_min": 45,
        "rsi_max": 65,
        "volume_multiplier": 1.5,
        "adx_min": 20,
        "min_score": 75,
        "cooldown": 10 * 60,        # 10 минут
        # КОНСЕРВАТИВНЫЕ TP/SL - большие тейки
        "max_stop_loss": 0.015,     # 1.5%
        "take_profit": 0.030,       # 3.0%
        "trailing_start": 0.015,    # 1.5%
        "trailing_step": 0.005      # 0.5%
    }
}

CURRENT_MODE = "AGGRESSIVE"

TIMEFRAME_ENTRY = "15m"
TIMEFRAME_TREND = "1h"
LIMIT = 100

# Увеличенные минимальные суммы
MIN_TRADE_USDT = 2.0  # 10 USDT минимум

MIN_USDT_PER_SYMBOL = {
    "BTC/USDT": 5.0, "ETH/USDT": 5.0, "BNB/USDT": 3.0, "SOL/USDT": 1.0, "XRP/USDT": 1.0, "ADA/USDT": 1.0, "AVAX/USDT": 2.0, "DOT/USDT": 1.0, "LINK/USDT": 2.0, "MATIC/USDT": 1.0, "DOGE/USDT": 1.0, "LTC/USDT": 2.0, "ATOM/USDT": 1.0, "UNI/>
}

TAKER_FEE = 0.001
ROUNDTRIP_FEE = TAKER_FEE * 2

LOCK_FILE = "/tmp/bybit_multy_v8_ultimate.lock"
DB_FILE = "trades_multi_v8_ultimate.db"

DRY_RUN = False

# ====== LOGGING ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('bot_v8_ultimate.log'), logging.StreamHandler()]
)

# ====== INIT ======
if os.path.exists(LOCK_FILE):
    logging.error("Lock file exists — bot already running. Exit.")
    sys.exit(1)
open(LOCK_FILE, "w").close()

exchange = ccxt.bybit({
    "apiKey": API_KEY,
    "secret": API_SECRET,
    "enableRateLimit": True,
    "options": {"defaultType": "spot"},
})

bot = Bot(token=TELEGRAM_TOKEN)

# DB
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    base_amount REAL,
    open_price REAL,
    stop_loss REAL,
    take_profit REAL,
    max_price REAL DEFAULT 0,
    open_time TEXT,
    status TEXT DEFAULT 'OPEN'
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    action TEXT,
    price REAL,
    usdt_amount REAL,
    base_amount REAL,
    time TEXT
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS closed_log (
    symbol TEXT PRIMARY KEY,
    last_closed_ts INTEGER
)
""")
conn.commit()

# ====== IMPROVED CORE FUNCTIONS ======
def safe_send(text, max_retries=3):
    for attempt in range(max_retries):
        try:
            bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML)
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Telegram send error: {e}")
            time.sleep(1)
    return False

def retry_api_call(func, max_retries=3, delay=1):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay * (2 ** attempt))

def fetch_ohlcv(symbol, timeframe, limit=100):
    def _fetch():
        return exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    try:
        return retry_api_call(_fetch)
    except Exception as e:
        logging.error(f"fetch_ohlcv error {symbol}: {e}")
        return []

def fetch_balance():
    def _fetch():
        return exchange.fetch_balance()
    try:
        return retry_api_call(_fetch)
    except Exception as e:
        logging.error(f"fetch_balance error: {e}")
        return {'free': {'USDT': 0.0}, 'total': {'USDT': 0.0}}

def get_current_mode_settings():
    return TRADING_MODES[CURRENT_MODE]

def check_min_order_value(symbol, usdt_amount):
    """Проверка минимальной суммы с запасом"""
    return usdt_amount >= MIN_USDT_PER_SYMBOL.get(symbol, MIN_TRADE_USDT)

def round_amount(symbol, amount):
    """Безопасное округление с запасом"""
    try:
        market = exchange.markets.get(symbol)
        if market:
            limits = market.get('limits', {})
            amount_limits = limits.get('amount', {})
            min_amount = amount_limits.get('min', 0.000001)
            step = amount_limits.get('step', min_amount)
            
            if step <= 0:
                return 0.0
                
            precision = int(-math.log10(step)) if step < 1 else 0
            if precision > 0:
                amount = round(amount - step/2, precision)
            else:
                amount = math.floor(amount / step) * step
                
            return max(float(amount), min_amount)
    except Exception:
        pass
    return float(amount)

def get_min_amount(symbol):
    try:
        market = exchange.markets.get(symbol)
        if market:
            return float(market.get('limits', {}).get('amount', {}).get('min', 0.000001))
    except Exception:
        pass
    return 0.000001

# ====== ENHANCED TRADING LOGIC ======
def get_trend_direction(df):
    """Определение направления тренда"""
    ema_fast = EMAIndicator(df['c'], window=9).ema_indicator()
    ema_slow = EMAIndicator(df['c'], window=21).ema_indicator()
    ema_trend = EMAIndicator(df['c'], window=50).ema_indicator()
    
    price = df['c'].iloc[-1]
    fast_ema = ema_fast.iloc[-1]
    slow_ema = ema_slow.iloc[-1]
    trend_ema = ema_trend.iloc[-1]
    
    bullish = (price > fast_ema and fast_ema > slow_ema and slow_ema > trend_ema)
    bearish = (price < fast_ema and fast_ema < slow_ema and slow_ema < trend_ema)
    
    if bullish:
        return "BULLISH"
    elif bearish:
        return "BEARISH"
    else:
        return "SIDEWAYS"

def calculate_entry_signals(df):
    """Расчет сигналов для входа"""
    mode_settings = get_current_mode_settings()
    signals = {}
    
    current_price = df['c'].iloc[-1]
    current_volume = df['v'].iloc[-1]
    volume_sma = df['v'].tail(20).mean()
    
    # RSI
    rsi = RSIIndicator(df['c'], window=14).rsi().iloc[-1]
    
    # MACD
    macd_line, macd_signal, macd_hist = calc_macd(df['c'])
    macd_bullish = macd_hist.iloc[-1] > 0 and macd_hist.iloc[-1] > macd_hist.iloc[-2]
    
    # ADX
    adx = ADXIndicator(df['h'], df['l'], df['c']).adx().iloc[-1] if len(df) >= 15 else 0
    
    # Bollinger Bands
    bb = BollingerBands(df['c'], window=20, window_dev=2)
    bb_upper = bb.bollinger_hband().iloc[-1]
    bb_lower = bb.bollinger_lband().iloc[-1]
    bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
    
    # Stochastic
    stoch = StochasticOscillator(df['h'], df['l'], df['c']).stoch().iloc[-1]
    
    signals['price'] = current_price
    signals['volume_ok'] = current_volume > volume_sma * mode_settings['volume_multiplier']
    signals['rsi_ok'] = mode_settings['rsi_min'] <= rsi <= mode_settings['rsi_max']
    signals['macd_bullish'] = macd_bullish
    signals['adx_strong'] = adx >= mode_settings['adx_min']
    signals['bb_position'] = bb_position
    signals['stoch_ok'] = 20 <= stoch <= 80
    signals['trend'] = get_trend_direction(df)
    
    # Score calculation
    score = 0
    if signals['volume_ok']: score += 20
    if signals['rsi_ok']: score += 20
    if signals['macd_bullish']: score += 15
    if signals['adx_strong']: score += 15
    if 0.2 <= signals['bb_position'] <= 0.8: score += 15
    if signals['stoch_ok']: score += 10
    if signals['trend'] == "BULLISH": score += 5
    
    signals['score'] = score
    signals['rsi_value'] = rsi
    signals['adx_value'] = adx
    
    return signals

def should_enter_position(symbol):
    """Логика входа в позицию"""
    try:
        # Тренд на старшем таймфрейме
        df_trend = get_ohlcv_data(symbol, TIMEFRAME_TREND, 50)
        if df_trend is None or len(df_trend) < 30:
            return False, "No trend data"
            
        trend = get_trend_direction(df_trend)
        if trend != "BULLISH":
            return False, f"Trend not bullish: {trend}"
        
        # Анализ на таймфрейме входа
        df_entry = get_ohlcv_data(symbol, TIMEFRAME_ENTRY, 50)
        if df_entry is None or len(df_entry) < 30:
            return False, "No entry data"
            
        signals = calculate_entry_signals(df_entry)
        mode_settings = get_current_mode_settings()
        
        if (signals['score'] >= mode_settings['min_score'] and 
            signals['volume_ok'] and 
            signals['trend'] == "BULLISH" and
            signals['adx_strong']):
            
            # Используем фиксированные TP/SL из настроек режима
            sl_price = signals['price'] * (1 - mode_settings['max_stop_loss'])
            tp_price = signals['price'] * (1 + mode_settings['take_profit'])
            
            entry_info = {
                'price': signals['price'],
                'stop_loss': sl_price,
                'take_profit': tp_price,
                'score': signals['score'],
                'rsi': signals['rsi_value'],
                'adx': signals['adx_value']
            }
            
            return True, entry_info
        else:
            return False, f"Score too low: {signals['score']}"
            
    except Exception as e:
        logging.error(f"Entry check error {symbol}: {e}")
        return False, f"Error: {str(e)}"

def get_ohlcv_data(symbol, timeframe, limit):
    ohlcv = fetch_ohlcv(symbol, timeframe, limit)
    if not ohlcv or len(ohlcv) < 20:
        return None
        
    df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v'])
    return df.astype({'o': float, 'h': float, 'l': float, 'c': float, 'v': float})

def calc_macd(series):
    try:
        macd_obj = MACD(series, window_slow=26, window_fast=12, window_sign=9)
        return macd_obj.macd(), macd_obj.macd_signal(), macd_obj.macd_diff()
    except Exception:
        return pd.Series(), pd.Series(), pd.Series()

# ====== ENHANCED POSITION MANAGEMENT ======
def safe_close_position(symbol, reason=""):
    """Безопасное закрытие с проверкой минимальной стоимости"""
    try:
        bal = fetch_balance()
        base = symbol.split("/")[0]
        real_balance = float(bal['free'].get(base, 0) or 0)
        
        if real_balance <= 0:
            mark_position_closed(symbol)
            return True

        ohlcv = fetch_ohlcv(symbol, TIMEFRAME_ENTRY, limit=1)
        if not ohlcv:
            return False
            
        price = float(ohlcv[-1][4])
        amount_to_sell = round_amount(symbol, real_balance * 0.995)
        min_amount = get_min_amount(symbol)
        
        if amount_to_sell < min_amount:
            mark_position_closed(symbol)
            return True

        # ⭐ ВАЖНО: Проверка минимальной стоимости ордера
        order_value = amount_to_sell * price
        min_order_value = 0.5  # Минимум 0/5 USDT
        
        if order_value < min_order_value:
            safe_send(f"⚠️ {symbol}: Order too small ({order_value:.2f} USDT), marking closed")
            mark_position_closed(symbol)
            return True

        if not check_min_order_value(symbol, order_value):
            safe_send(f"⚠️ {symbol}: Below exchange limit ({order_value:.2f} USDT)")
            mark_position_closed(symbol)
            return True

        if not DRY_RUN:
            exchange.create_market_order(symbol, 'sell', amount_to_sell)
            
        record_close(symbol, price, order_value, amount_to_sell)
        safe_send(f"✅ Closed {symbol}: {amount_to_sell:.4f} @ {price:.6f} ({reason})")
        return True
        
    except Exception as e:
        error_msg = str(e)
        if any(err in error_msg for err in ["Insufficient balance", "lower limit"]):
            mark_position_closed(symbol)
            return True
        else:
            safe_send(f"❌ Error closing {symbol}: {error_msg}")
            return False

def mark_position_closed(symbol):
    mode_settings = get_current_mode_settings()
    cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=? AND status='OPEN'", (symbol,))
    cursor.execute("REPLACE INTO closed_log (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
    conn.commit()

def check_position_exits():
    """Проверка условий выхода"""
    positions = get_open_positions()
    mode_settings = get_current_mode_settings()
    
    for symbol, pos in positions.items():
        try:
            ohlcv = fetch_ohlcv(symbol, TIMEFRAME_ENTRY, limit=20)
            if not ohlcv:
                continue
                
            df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v']).astype(float)
            current_price = df['c'].iloc[-1]
            open_price = pos['open_price']
            stop_loss = pos['stop_loss']
            take_profit = pos['take_profit']
            max_price = pos.get('max_price', open_price)
            
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
            
            if exit_reason:
                safe_close_position(symbol, exit_reason)
                
        except Exception as e:
            logging.error(f"Exit check error {symbol}: {e}")

# ====== DATABASE FUNCTIONS ======
def get_open_positions():
    cursor.execute("SELECT symbol, base_amount, open_price, stop_loss, take_profit, max_price FROM positions WHERE status='OPEN'")
    rows = cursor.fetchall()
    return {r[0]: {
        "base_amount": r[1], 
        "open_price": r[2], 
        "stop_loss": r[3],
        "take_profit": r[4],
        "max_price": r[5] or r[2]
    } for r in rows}

def record_open(symbol, base_amount, open_price, stop_loss, take_profit):
    cursor.execute("""
        INSERT INTO positions (symbol, base_amount, open_price, stop_loss, take_profit, max_price, open_time) 
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
    """, (symbol, base_amount, open_price, stop_loss, take_profit, open_price))
    cursor.execute("INSERT INTO history (symbol, action, price, usdt_amount, base_amount, time) VALUES (?, 'BUY', ?, ?, ?, datetime('now'))",
                   (symbol, open_price, round(base_amount*open_price, 8), base_amount))
    conn.commit()

def record_close(symbol, price, usdt_amount, base_amount):
    cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=? AND status='OPEN'", (symbol,))
    cursor.execute("INSERT INTO history (symbol, action, price, usdt_amount, base_amount, time) VALUES (?, 'SELL', ?, ?, ?, datetime('now'))",
                   (symbol, price, usdt_amount, base_amount))
    cursor.execute("REPLACE INTO closed_log (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
    conn.commit()

def update_max_price_db(symbol, price):
    cursor.execute("UPDATE positions SET max_price=? WHERE symbol=? AND status='OPEN'", (price, symbol))
    conn.commit()

def is_in_cooldown(symbol):
    mode_settings = get_current_mode_settings()
    cursor.execute("SELECT last_closed_ts FROM closed_log WHERE symbol=?", (symbol,))
    row = cursor.fetchone()
    if not row:
        return False
    return (time.time() - int(row[0])) < mode_settings['cooldown']

def compute_equity():
    bal = fetch_balance()
    if not bal:
        return 0.0
    usdt_free = float(bal['free'].get('USDT', 0) or 0)
    total = usdt_free
    
    for sym in SYMBOLS:
        base = sym.split("/")[0]
        base_amt = float(bal['total'].get(base, 0) or 0)
        if base_amt > 0:
            try:
                tick = exchange.fetch_ticker(sym)
                total += base_amt * float(tick['last'])
            except Exception:
                continue
    return total

def get_concurrent_trades_count():
    cursor.execute("SELECT COUNT(*) FROM positions WHERE status='OPEN'")
    return cursor.fetchone()[0]

def can_open_new_trade():
    mode_settings = get_current_mode_settings()
    return get_concurrent_trades_count() < mode_settings['max_trades']

def realized_pnl_total():
    try:
        cursor.execute("""
            SELECT SUM(CASE WHEN action='SELL' THEN usdt_amount ELSE -usdt_amount END) 
            FROM history
        """)
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        logging.error(f"PnL error: {e}")
        return 0.0

def unrealized_pnl_total():
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
        logging.error(f"Unrealized PnL error: {e}")
    return total

def get_trading_stats():
    try:
        cursor.execute("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN action='SELL' THEN usdt_amount ELSE 0 END) as total_sell,
                SUM(CASE WHEN action='BUY' THEN usdt_amount ELSE 0 END) as total_buy
            FROM history
        """)
        stats_row = cursor.fetchone()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as closed_trades,
                SUM(CASE WHEN sell_price > buy_price THEN 1 ELSE 0 END) as winning_trades,
                AVG(CASE WHEN sell_price > buy_price THEN (sell_price - buy_price)/buy_price ELSE 0 END) as avg_win_pct
            FROM (
                SELECT 
                    MAX(CASE WHEN action='BUY' THEN price END) as buy_price,
                    MAX(CASE WHEN action='SELL' THEN price END) as sell_price
                FROM history 
                GROUP BY symbol, date(time)
                HAVING COUNT(*) = 2
            ) trades
        """)
        trades_row = cursor.fetchone()
        
        return {
            'total_trades': stats_row[0] if stats_row else 0,
            'total_volume': float(stats_row[1]) if stats_row and stats_row[1] else 0,
            'closed_trades': trades_row[0] if trades_row else 0,
            'winning_trades': trades_row[1] if trades_row else 0,
            'avg_win_pct': float(trades_row[2]) * 100 if trades_row and trades_row[2] else 0
        }
    except Exception as e:
        logging.error(f"Stats error: {e}")
        return {}

def sync_balance_with_db():
    safe_send("🔄 Starting balance synchronization...")
    
    bal = fetch_balance()
    synced_count = 0
    created_count = 0
    closed_count = 0
    
    for symbol in active_symbols:
        try:
            base = symbol.split("/")[0]
            real_balance = float(bal['total'].get(base, 0) or 0)
            
            cursor.execute("SELECT base_amount FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
            row = cursor.fetchone()
            
            if row:
                db_balance = float(row[0])
                if real_balance <= 0:
                    cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=? AND status='OPEN'", (symbol,))
                    closed_count += 1
                elif abs(real_balance - db_balance) / db_balance > 0.01:
                    cursor.execute("UPDATE positions SET base_amount=? WHERE symbol=? AND status='OPEN'", (real_balance, symbol))
                    synced_count += 1
            else:
                if real_balance > 0:
                    ohlcv = fetch_ohlcv(symbol, TIMEFRAME_ENTRY, limit=1)
                    if ohlcv:
                        price = float(ohlcv[-1][4])
                        base_amount_rounded = round_amount(symbol, real_balance)
                        mode_settings = get_current_mode_settings()
                        sl_price = price * (1 - mode_settings['max_stop_loss'])
                        tp_price = price * (1 + mode_settings['take_profit'])
                        record_open(symbol, base_amount_rounded, price, sl_price, tp_price)
                        created_count += 1
                        
        except Exception as e:
            logging.error(f"Sync error for {symbol}: {e}")
    
    conn.commit()
    
    msg = f"✅ Sync completed:\n"
    msg += f"• Synced: {synced_count} positions\n"
    msg += f"• Created: {created_count} positions\n" 
    msg += f"• Closed: {closed_count} positions\n"
    msg += f"• Total open: {get_concurrent_trades_count()} positions"
    
    safe_send(msg)
    return synced_count + created_count + closed_count

# ====== TELEGRAM COMMANDS ======
def cmd_status(update, context):
    equity = compute_equity()
    realized = realized_pnl_total()
    unrealized = unrealized_pnl_total()
    positions = get_open_positions()
    stats = get_trading_stats()
    mode_settings = get_current_mode_settings()
    
    total_pnl = realized + unrealized
    pnl_color = "🟢" if total_pnl >= 0 else "🔴"
    
    msg = f"📊 <b>ULTIMATE BOT v8 - {CURRENT_MODE} MODE</b>\n\n"
    
    msg += f"💰 <b>Equity:</b> {equity:.2f} USDT\n"
    msg += f"📈 <b>PnL:</b> {pnl_color} {total_pnl:+.2f} USDT "
    msg += f"(Realized: {realized:+.2f} | Unrealized: {unrealized:+.2f})\n"
    msg += f"🔢 <b>Positions:</b> {len(positions)}/{mode_settings['max_trades']}\n\n"
    
    if stats['closed_trades'] > 0:
        win_rate = (stats['winning_trades'] / stats['closed_trades']) * 100
        msg += f"📊 <b>Statistics:</b>\n"
        msg += f"• Win Rate: {win_rate:.1f}% ({stats['winning_trades']}/{stats['closed_trades']})\n"
        msg += f"• Avg Win: {stats['avg_win_pct']:.2f}%\n"
        msg += f"• Total Trades: {stats['total_trades']}\n\n"
    
    if positions:
        msg += f"📈 <b>Open Positions:</b>\n"
        for sym, pos in positions.items():
            ohlcv = fetch_ohlcv(sym, TIMEFRAME_ENTRY, limit=1)
            if ohlcv:
                price = float(ohlcv[-1][4])
                profit = (price - pos['open_price']) / pos['open_price'] * 100
                profit_net = profit - ROUNDTRIP_FEE * 100
                position_value = price * pos['base_amount']
                
                emoji = "🟢" if profit_net > 0 else "🔴"
                sl_distance = ((price - pos['stop_loss']) / price) * 100
                tp_distance = ((pos['take_profit'] - price) / price) * 100
                
                msg += f"{emoji} <b>{sym}</b>\n"
                msg += f"   P&L: {profit_net:+.2f}% | Value: {position_value:.2f} USDT\n"
                msg += f"   SL: -{sl_distance:.1f}% | TP: +{tp_distance:.1f}%\n"
    else:
        msg += "📭 <b>No open positions</b>\n"
    
    msg += f"\n⚙️ <b>Mode Settings:</b>\n"
    msg += f"• Scan: {mode_settings['scan_interval']}s | "
    msg += f"Cooldown: {mode_settings['cooldown']//60}min\n"
    msg += f"• RSI: {mode_settings['rsi_min']}-{mode_settings['rsi_max']} | "
    msg += f"Min Score: {mode_settings['min_score']}/100\n"
    msg += f"• TP/SL: +{mode_settings['take_profit']*100:.1f}%/ -{mode_settings['max_stop_loss']*100:.1f}%"
    
    safe_send(msg)

def cmd_close(update, context):
    try:
        symbol = context.args[0].upper()
        if safe_close_position(symbol, "Manual"):
            update.message.reply_text(f"✅ Closed {symbol}")
    except:
        update.message.reply_text("Usage: /close SYMBOL")

def cmd_restart(update, context):
    safe_send("♻️ Restarting...")
    conn.close()
    os.remove(LOCK_FILE)
    python = sys.executable
    os.execv(python, [python] + sys.argv)

def cmd_mode(update, context):
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
    msg += f"• Cooldown: {mode_settings['cooldown']//60}min"
    
    safe_send(msg)

def cmd_debug(update, context):
    debug_info = []
    checked = 0
    
    for symbol in active_symbols[:8]:
        if checked >= 4:
            break
            
        should_enter, entry_info = should_enter_position(symbol)
        if should_enter:
            usdt_amount = compute_equity() * get_current_mode_settings()['trade_pct']
            usdt_free = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
            
            debug_info.append(f"🎯 {symbol}: Score {entry_info['score']} | RSI {entry_info['rsi']:.1f}")
            debug_info.append(f"   Need: {usdt_amount:.2f} USDT | Have: {usdt_free:.2f} USDT")
            checked += 1
    
    if debug_info:
        safe_send("🔍 TOP SIGNALS:\n" + "\n".join(debug_info))
    else:
        safe_send("🔍 No strong signals found")

def cmd_sync(update, context):
    changes_count = sync_balance_with_db()
    if changes_count == 0:
        safe_send("✅ Balance already synchronized with DB")

def cmd_stats(update, context):
    stats = get_trading_stats()
    equity = compute_equity()
    realized = realized_pnl_total()
    
    msg = f"📈 <b>Detailed Statistics</b>\n\n"
    
    if stats['closed_trades'] > 0:
        win_rate = (stats['winning_trades'] / stats['closed_trades']) * 100
        total_return = (realized / equity) * 100 if equity > 0 else 0
        
        msg += f"📊 <b>Performance:</b>\n"
        msg += f"• Win Rate: {win_rate:.1f}% ({stats['winning_trades']}/{stats['closed_trades']})\n"
        msg += f"• Avg Win: {stats['avg_win_pct']:.2f}%\n"
        msg += f"• Total Return: {total_return:+.2f}%\n"
        msg += f"• Total Trades: {stats['total_trades']}\n"
        msg += f"• Trade Volume: {stats['total_volume']:.0f} USDT\n\n"
    
    mode_settings = get_current_mode_settings()
    msg += f"⚙️ <b>Current Settings:</b>\n"
    msg += f"• Mode: {CURRENT_MODE}\n"
    msg += f"• TP/SL: +{mode_settings['take_profit']*100:.1f}%/ -{mode_settings['max_stop_loss']*100:.1f}%\n"
    msg += f"• Position Size: {mode_settings['trade_pct']*100}%\n"
    msg += f"• RSI Range: {mode_settings['rsi_min']}-{mode_settings['rsi_max']}\n"
    msg += f"• Min Score: {mode_settings['min_score']}/100"
    
    safe_send(msg)

# ====== INIT & MAIN LOOP ======
updater = Updater(TELEGRAM_TOKEN, use_context=True)
updater.dispatcher.add_handler(CommandHandler("status", cmd_status))
updater.dispatcher.add_handler(CommandHandler("close", cmd_close))
updater.dispatcher.add_handler(CommandHandler("restart", cmd_restart))
updater.dispatcher.add_handler(CommandHandler("mode", cmd_mode))
updater.dispatcher.add_handler(CommandHandler("debug", cmd_debug))
updater.dispatcher.add_handler(CommandHandler("sync", cmd_sync))
updater.dispatcher.add_handler(CommandHandler("stats", cmd_stats))
updater.start_polling()

# Загрузка маркетов
markets = exchange.load_markets()
active_symbols = [s for s in SYMBOLS if s in markets]

safe_send(f"🚀 ULTIMATE BOT v8 STARTED - {CURRENT_MODE} MODE")
safe_send(f"📈 Monitoring {len(active_symbols)} symbols")

# Авто-синхронизация
time.sleep(2)
sync_balance_with_db()

last_scan = 0
last_auto_status = 0

try:
    while True:
        current_time = time.time()
        mode_settings = get_current_mode_settings()
        
        # Проверка выходов
        if current_time - last_scan >= mode_settings['scan_interval']:
            check_position_exits()
            
            # Поиск новых входов
            if can_open_new_trade():
                usdt_balance = float(fetch_balance().get('free', {}).get('USDT', 0) or 0)
                
                for symbol in active_symbols:
                    if (symbol not in get_open_positions() and 
                        not is_in_cooldown(symbol) and 
                        usdt_balance > MIN_TRADE_USDT):
                        
                        should_enter, entry_info = should_enter_position(symbol)
                        
                        if should_enter:
                            usdt_amount = compute_equity() * mode_settings['trade_pct']
                            
                            if check_min_order_value(symbol, usdt_amount) and usdt_amount <= usdt_balance:
                                base_amount = round_amount(symbol, usdt_amount / entry_info['price'])
                                
                                if not DRY_RUN:
                                    exchange.create_market_order(symbol, 'buy', base_amount)
                                
                                record_open(
                                    symbol, base_amount, entry_info['price'],
                                    entry_info['stop_loss'], entry_info['take_profit']
                                )
                                
                                safe_send(
                                    f"🎯 ENTER {symbol} | {CURRENT_MODE}\n"
                                    f"Price: {entry_info['price']:.6f}\n"
                                    f"Score: {entry_info['score']}/100\n"
                                    f"TP: +{mode_settings['take_profit']*100:.1f}% | SL: -{mode_settings['max_stop_loss']*100:.1f}%"
                                )
            
            last_scan = current_time
        
        # Автоматический статус
        if current_time - last_auto_status >= mode_settings['status_interval']:
            cmd_status(None, None)
            last_auto_status = current_time
        
        time.sleep(5)
        
except KeyboardInterrupt:
    safe_send("⏹ Bot stopped")
finally:
    conn.close()
    os.remove(LOCK_FILE)
