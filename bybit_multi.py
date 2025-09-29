#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bybit_multy_v6_fixed.py — исправленная версия с синхронизацией баланса
"""
import os, sys, time, math, ccxt, pandas as pd, sqlite3
import logging
from datetime import datetime
import numpy as np

from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
from telegram import Bot, ParseMode
from telegram.ext import Updater, CommandHandler

# ====== FIXED CONFIG ======
API_KEY = os.getenv("BYBIT_API_KEY", "BB_api")
API_SECRET = os.getenv("BYBIT_API_SECRET", "API")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "TG-token")
CHAT_ID = 279609886

SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "ADA/USDT", "DOGE/USDT", 
    "SOL/USDT", "XRP/USDT", "LTC/USDT", "AVAX/USDT", "DOT/USDT",
    "MATIC/USDT", "LINK/USDT", "ATOM/USDT", "UNI/USDT", "XLM/USDT"
]

TIMEFRAME_PRIMARY = "5m"
TIMEFRAME_CONFIRMATION = "15m"
LIMIT = 50

# УВЕЛИЧЕННЫЕ МИНИМАЛЬНЫЕ СУММЫ
MIN_TRADE_USDT = 3.0  # было 1.0
TRADE_PCT = 0.08  # 8% вместо 5%

MIN_USDT_PER_SYMBOL = {
    "BTC/USDT": 5.0, "ETH/USDT": 4.0, "BNB/USDT": 5.0, "ADA/USDT": 3.0,
    "DOGE/USDT": 3.0, "SOL/USDT": 3.0, "XRP/USDT": 3.0, "LTC/USDT": 4.0,
    "AVAX/USDT": 4.0, "DOT/USDT": 3.0, "MATIC/USDT": 3.0, "LINK/USDT": 3.0,
    "ATOM/USDT": 3.0, "UNI/USDT": 3.0, "XLM/USDT": 3.0
}

# ИСПРАВЛЕННЫЕ TP/SL
TP_ATR_MULT = 2.0
SL_ATR_MULT = 1.5
STOP_LOSS_RAW = 0.015
MIN_TP_PCT = 0.003  # минимальный TP 0.3%

TRAIL_START_NET = 0.005
TRAIL_GAP = 0.005

RSI_MIN = 30
ADX_MIN = 8
VOLUME_MIN_MULT = 0.05

SCAN_INTERVAL = 5
STATUS_INTERVAL = 300
COOLDOWN_SECONDS = 5 * 60  # 5 минут

MAX_CONCURRENT_TRADES = 8

USE_MULTI_TIMEFRAME = True
USE_PRICE_ACTION = False

TAKER_FEE = 0.001
ROUNDTRIP_FEE = TAKER_FEE * 2

LOCK_FILE = "/tmp/bybit_multy_v6_fixed.lock"
DB_FILE = "trades_multi_v6_fixed.db"

DRY_RUN = False

# ====== LOGGING ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('bot_v6_fixed.log'), logging.StreamHandler()]
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

# ====== CORE FUNCTIONS ======
def safe_send(text):
    try:
        bot.send_message(chat_id=CHAT_ID, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Telegram send error: {e}")

def retry_api_call(func, max_retries=3, delay=1):
    for attempt in range(max_retries):
        try:
            return func()
        except ccxt.NetworkError as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay * (2 ** attempt))
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)

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
        bal = retry_api_call(_fetch)
        if not bal or 'free' not in bal:
            raise Exception("Invalid balance response")
        return bal
    except Exception as e:
        logging.error(f"fetch_balance error: {e}")
        return {'free': {'USDT': 0.0}, 'total': {'USDT': 0.0}}

def check_min_order_value(symbol, usdt_amount):
    """Проверка минимальной суммы ордера для Bybit"""
    min_limits = {
        "BTC/USDT": 1.0, "ETH/USDT": 1.0, "BNB/USDT": 1.0, "ADA/USDT": 1.0,
        "DOGE/USDT": 2.0, "SOL/USDT": 1.0, "XRP/USDT": 1.0, "LTC/USDT": 1.0,
        "AVAX/USDT": 1.0, "DOT/USDT": 1.0, "MATIC/USDT": 1.0, "LINK/USDT": 1.0,
        "ATOM/USDT": 1.0, "UNI/USDT": 1.0, "XLM/USDT": 1.0
    }
    
    min_required = min_limits.get(symbol, 3.0)
    return usdt_amount >= min_required

def sync_balance_with_reality(symbol):
    """УЛУЧШЕННАЯ синхронизация баланса с принудительной коррекцией"""
    try:
        bal = fetch_balance()
        base = symbol.split("/")[0]
        real_balance = float(bal['free'].get(base, 0) or 0)
        
        cursor.execute("SELECT base_amount FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
        row = cursor.fetchone()
        
        if not row:
            # Нет позиции в БД, но есть баланс - создаем запись
            if real_balance > 0:
                ohlcv = fetch_ohlcv(symbol, TIMEFRAME_PRIMARY, limit=1)
                price = float(ohlcv[-1][4]) if ohlcv else 0.0
                base_amount_rounded = round_amount(symbol, real_balance)
                record_open(symbol, base_amount_rounded, price)
                safe_send(f"🔧 AUTO-FIX: Created position {symbol}: {base_amount_rounded:.4f}")
            return real_balance
            
        db_balance = float(row[0])
        
        # ПРИНУДИТЕЛЬНАЯ СИНХРОНИЗАЦИЯ - если расхождение > 0.1%
        if db_balance > 0 and abs(real_balance - db_balance) / db_balance > 0.001:
            logging.warning(f"Balance sync needed for {symbol}: DB={db_balance}, Real={real_balance}")
            
            if real_balance <= 0:
                # Реального баланса нет - закрываем позицию
                cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=?", (symbol,))
                conn.commit()
                safe_send(f"🔧 AUTO-FIX: Closed {symbol} (no balance)")
                return 0.0
            else:
                # Обновляем БД реальным балансом
                cursor.execute("UPDATE positions SET base_amount=? WHERE symbol=? AND status='OPEN'", 
                             (real_balance, symbol))
                conn.commit()
                safe_send(f"🔧 AUTO-FIX: Updated {symbol}: {db_balance:.4f} → {real_balance:.4f}")
                return real_balance
                
        return real_balance
        
    except Exception as e:
        logging.error(f"Sync balance error for {symbol}: {e}")
        return 0.0

def emergency_balance_sync(symbol, reason=""):
    """Аварийная синхронизация при ошибке баланса"""
    try:
        bal = fetch_balance()
        base = symbol.split("/")[0]
        available_balance = float(bal['free'].get(base, 0) or 0)
        
        if available_balance <= 0:
            # Вообще нет баланса - закрываем в БД
            cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=?", (symbol,))
            conn.commit()
            safe_send(f"🔧 EMERGENCY: {symbol} marked closed (zero balance)")
            return True
            
        # Пробуем продать доступный баланс
        amount_to_sell = round_amount(symbol, available_balance * 0.999)
        min_amount = get_min_amount(symbol)
        
        if amount_to_sell < min_amount:
            cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=?", (symbol,))
            conn.commit()
            safe_send(f"🔧 EMERGENCY: {symbol} marked closed (tiny balance)")
            return True
            
        ohlcv = fetch_ohlcv(symbol, TIMEFRAME_PRIMARY, limit=1)
        price = float(ohlcv[-1][4]) if ohlcv else 0.0
        
        if not DRY_RUN:
            exchange.create_market_order(symbol, 'sell', amount_to_sell)
            
        record_close(symbol, price, amount_to_sell * price, amount_to_sell)
        safe_send(f"🔧 EMERGENCY CLOSE {symbol}: {amount_to_sell:.4f} @ {price:.6f}")
        return True
        
    except Exception as e:
        # ФИНАЛЬНОЕ ЗАКРЫТИЕ В БД
        cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=?", (symbol,))
        conn.commit()
        safe_send(f"🔧 FINAL: {symbol} forced closed in DB")
        return True

def safe_close_position(symbol, reason=""):
    """УЛУЧШЕННОЕ закрытие с принудительной синхронизацией"""
    try:
        # ПРИНУДИТЕЛЬНАЯ СИНХРОНИЗАЦИЯ ПЕРЕД ЗАКРЫТИЕМ
        real_balance = sync_balance_with_reality(symbol)
        
        if real_balance <= 0:
            cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=? AND status='OPEN'", (symbol,))
            conn.commit()
            safe_send(f"ℹ️ {symbol}: Already closed ({reason})")
            return True

        # Получаем актуальную цену
        ohlcv = fetch_ohlcv(symbol, TIMEFRAME_PRIMARY, limit=1)
        if not ohlcv:
            safe_send(f"⚠️ {symbol}: Cannot fetch price")
            return False
            
        price = float(ohlcv[-1][4])
        
        # Округляем объем с запасом
        amount_to_sell = round_amount(symbol, real_balance * 0.999)  # 0.1% запас
        min_amount = get_min_amount(symbol)
        
        if amount_to_sell < min_amount:
            # Слишком маленький объем - закрываем в БД
            cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=?", (symbol,))
            conn.commit()
            safe_send(f"ℹ️ {symbol}: Amount too small, marked closed")
            return True

        # ДВОЙНАЯ ПРОВЕРКА БАЛАНСА
        bal = fetch_balance()
        base = symbol.split("/")[0]
        available_balance = float(bal['free'].get(base, 0) or 0)
        
        # Берем МЕНЬШЕЕ значение для безопасности
        safe_amount = min(amount_to_sell, round_amount(symbol, available_balance * 0.999))
        
        if safe_amount < min_amount:
            cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=?", (symbol,))
            conn.commit()
            safe_send(f"ℹ️ {symbol}: Safe amount too small, marked closed")
            return True

        # ЗАКРЫТИЕ
        if not DRY_RUN:
            exchange.create_market_order(symbol, 'sell', safe_amount)
            
        record_close(symbol, price, safe_amount * price, safe_amount)
        safe_send(f"✅ Closed {symbol}: {safe_amount:.4f} @ {price:.6f} ({reason})")
        return True
        
    except Exception as e:
        error_msg = str(e)
        if "Insufficient balance" in error_msg:
            # КРИТИЧЕСКАЯ СИНХРОНИЗАЦИЯ ПРИ ОШИБКЕ
            safe_send(f"🔄 {symbol}: Critical balance sync needed")
            return emergency_balance_sync(symbol, reason)
        else:
            safe_send(f"❌ Error closing {symbol}: {error_msg}")
            return False

def round_amount(symbol, amount):
    if "DOGE" in symbol:
        return math.floor(amount)
    
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
                amount = round(amount, precision)
            else:
                amount = math.floor(amount / step) * step
                
            return float(amount)
    except Exception as e:
        logging.error(f"Round amount error for {symbol}: {e}")
    
    return float(amount)

def get_min_amount(symbol):
    try:
        market = exchange.markets.get(symbol)
        if market:
            return float(market.get('limits', {}).get('amount', {}).get('min', 0.000001))
    except Exception:
        pass
    return 0.000001

def calc_ema_rsi(series, ema_short=10, ema_long=50, rsi_len=14):
    ema10 = EMAIndicator(series, window=ema_short).ema_indicator()
    ema50 = EMAIndicator(series, window=ema_long).ema_indicator()
    rsi = RSIIndicator(series, window=rsi_len).rsi()
    return ema10, ema50, rsi

def calc_macd(series):
    try:
        macd_obj = MACD(series, window_slow=26, window_fast=12, window_sign=9)
        return macd_obj.macd(), macd_obj.macd_signal(), macd_obj.macd_diff()
    except Exception:
        return None, None, None

def calc_adx(high_s, low_s, close_s, window=14):
    try:
        return ADXIndicator(high_s, low_s, close_s, window=window).adx()
    except Exception:
        return None

def calc_atr(high_s, low_s, close_s, window=14):
    try:
        return AverageTrueRange(high_s, low_s, close_s, window=window).average_true_range()
    except Exception:
        return None

def get_open_positions():
    cursor.execute("SELECT symbol, base_amount, open_price, max_price FROM positions WHERE status='OPEN'")
    rows = cursor.fetchall()
    return {r[0]: {"base_amount": r[1], "open_price": r[2], "max_price": r[3] or r[2]} for r in rows}

def record_open(symbol, base_amount, open_price):
    cursor.execute("INSERT INTO positions (symbol, base_amount, open_price, max_price, open_time) VALUES (?, ?, ?, ?, datetime('now'))",
                   (symbol, base_amount, open_price, open_price))
    cursor.execute("INSERT INTO history (symbol, action, price, usdt_amount, base_amount, time) VALUES (?, 'BUY', ?, ?, ?, datetime('now'))",
                   (symbol, open_price, round(base_amount*open_price, 8), base_amount))
    conn.commit()

def record_close(symbol, price, usdt_amount, base_amount):
    cursor.execute("UPDATE positions SET status='CLOSED' WHERE symbol=? AND status='OPEN'", (symbol,))
    cursor.execute("INSERT INTO history (symbol, action, price, usdt_amount, base_amount, time) VALUES (?, 'SELL', ?, ?, ?, datetime('now'))",
                   (symbol, price, usdt_amount, base_amount))
    cursor.execute("REPLACE INTO closed_log (symbol, last_closed_ts) VALUES (?, ?)", (symbol, int(time.time())))
    conn.commit()

def is_in_cooldown(symbol):
    cursor.execute("SELECT last_closed_ts FROM closed_log WHERE symbol=?", (symbol,))
    row = cursor.fetchone()
    if not row:
        return False
    last = int(row[0])
    return (time.time() - last) < COOLDOWN_SECONDS

def update_max_price_db(symbol, price):
    cursor.execute("SELECT max_price FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
    row = cursor.fetchone()
    if not row:
        return
    try:
        cur_max = float(row[0]) if row[0] else 0.0
    except:
        cur_max = 0.0
    if price > cur_max:
        cursor.execute("UPDATE positions SET max_price=? WHERE symbol=? AND status='OPEN'", (price, symbol))
        conn.commit()

def compute_equity():
    bal = fetch_balance()
    if not bal:
        return 0.0
    usdt_free = float(bal['free'].get('USDT', 0) or 0)
    total = usdt_free
    
    for sym in SYMBOLS:
        base = sym.split("/")[0]
        base_amt = float(bal['total'].get(base, 0) or 0)
        if base_amt <= 0:
            continue
        try:
            tick = exchange.fetch_ticker(sym)
            price = float(tick['last'])
            total += base_amt * price
        except Exception:
            continue
            
    return float(total)

def get_concurrent_trades_count():
    cursor.execute("SELECT COUNT(*) FROM positions WHERE status='OPEN'")
    return cursor.fetchone()[0]

def can_open_new_trade():
    current_trades = get_concurrent_trades_count()
    return current_trades < MAX_CONCURRENT_TRADES

def realized_pnl_total():
    """КОРРЕКТНЫЙ расчет реализованного PnL - только закрытые сделки"""
    try:
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN action = 'SELL' THEN usdt_amount ELSE 0 END) as total_sell,
                SUM(CASE WHEN action = 'BUY' THEN usdt_amount ELSE 0 END) as total_buy
            FROM history
            WHERE symbol IN (
                SELECT DISTINCT symbol FROM history 
                WHERE action = 'SELL'
            )
        """)
        row = cursor.fetchone()
        if row and row[0] is not None and row[1] is not None:
            total_sell = float(row[0])
            total_buy = float(row[1])
            return total_sell - total_buy
        return 0.0
    except Exception as e:
        logging.error(f"realized_pnl_total err: {e}")
        return 0.0

def unrealized_pnl_total():
    total = 0.0
    try:
        positions = get_open_positions()
        for sym, pos in positions.items():
            ohlcv = fetch_ohlcv(sym, TIMEFRAME_PRIMARY, limit=1)
            if not ohlcv:
                continue
            price = float(ohlcv[-1][4])
            cur_val = price * pos['base_amount']
            open_cost = pos['open_price'] * pos['base_amount']
            total += (cur_val - open_cost)
    except Exception as e:
        logging.error(f"unrealized_pnl_total err: {e}")
    return total

# ====== SIMPLIFIED TRADING LOGIC ======
def simple_multi_timeframe_signal(symbol):
    if not USE_MULTI_TIMEFRAME:
        return True
        
    try:
        ohlcv = fetch_ohlcv(symbol, "15m", limit=15)
        if not ohlcv:
            return True
            
        df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v']).astype(float)
        if len(df) < 10:
            return True
            
        ema20 = EMAIndicator(df['c'], window=20).ema_indicator().iloc[-1]
        current_price = df['c'].iloc[-1]
        
        return current_price > ema20 * 0.98
        
    except Exception as e:
        logging.error(f"Simple MTF error {symbol}: {e}")
        return True

def simple_rsi_strategy(symbol):
    try:
        ohlcv = fetch_ohlcv(symbol, TIMEFRAME_PRIMARY, limit=20)
        if not ohlcv:
            return False
            
        df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v']).astype(float)
        rsi = RSIIndicator(df['c']).rsi().iloc[-1]
        
        return 25 <= rsi <= 75
        
    except Exception as e:
        logging.error(f"Simple RSI strategy error {symbol}: {e}")
        return False

def debug_all_signals():
    debug_messages = []
    
    for symbol in active_symbols:
        try:
            cursor.execute("SELECT COUNT(*) FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
            if cursor.fetchone()[0] > 0:
                continue
                
            if is_in_cooldown(symbol):
                debug_messages.append(f"⏳ {symbol}: IN COOLDOWN")
                continue

            ohlcv = fetch_ohlcv(symbol, TIMEFRAME_PRIMARY, limit=25)
            if not ohlcv or len(ohlcv) < 20:
                debug_messages.append(f"❌ {symbol}: NO DATA")
                continue
                
            df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v']).astype(float)
            
            ema10, ema50, rsi = calc_ema_rsi(df['c'])
            last_close = df['c'].iloc[-1]
            last_ema10 = ema10.iloc[-1] if not ema10.empty else 0
            last_ema50 = ema50.iloc[-1] if not ema50.empty else 0
            last_rsi = rsi.iloc[-1] if not rsi.empty else 0
            
            cond_ema = last_ema10 > last_ema50
            cond_rsi = last_rsi > RSI_MIN
            volume_avg = df['v'].tail(20).mean()
            cond_volume = df['v'].iloc[-1] > volume_avg * VOLUME_MIN_MULT if volume_avg > 0 else True
            cond_multi_tf = simple_multi_timeframe_signal(symbol)
            cond_simple_rsi = simple_rsi_strategy(symbol)
            
            conditions = [
                f"EMA:{int(cond_ema)}", 
                f"RSI:{last_rsi:.1f}(>{RSI_MIN})",
                f"VOL:{int(cond_volume)}",
                f"MTF:{int(cond_multi_tf)}",
                f"SIMPLE:{int(cond_simple_rsi)}"
            ]
            
            signal = (cond_ema and cond_rsi and cond_volume and cond_multi_tf) or cond_simple_rsi
            
            if signal:
                debug_messages.append(f"🎯 {symbol}: ✅ BUY SIGNAL | {' | '.join(conditions)}")
            else:
                debug_messages.append(f"❌ {symbol}: NO SIGNAL | {' | '.join(conditions)}")
                
        except Exception as e:
            debug_messages.append(f"⚠️ {symbol}: ERROR - {str(e)}")
    
    return debug_messages

# ====== STATUS & REPORTING ======
def status_report():
    equity = compute_equity()
    realized = realized_pnl_total()
    unrealized = unrealized_pnl_total()
    bal = fetch_balance()
    usdt_free = float(bal['free'].get('USDT', 0) or 0) if bal else 0.0
    
    current_trades = get_concurrent_trades_count()
    
    msg = f"⚡ FIXED BOT v6 | Trades: {current_trades}/{MAX_CONCURRENT_TRADES}\n"
    msg += f"Equity: {equity:.4f} USDT (free {usdt_free:.4f})\n"
    
    positions = get_open_positions()
    total_value = 0.0
    
    for sym, pos in positions.items():
        ohlcv = fetch_ohlcv(sym, TIMEFRAME_PRIMARY, limit=1)
        if not ohlcv:
            continue
        price = float(ohlcv[-1][4])
        profit_raw = (price - pos['open_price']) / pos['open_price']
        profit_net = profit_raw - ROUNDTRIP_FEE
        pos_value = price * pos['base_amount']
        total_value += pos_value
        emoji = "🟢" if profit_net > 0 else ("🔴" if profit_net < 0 else "🟡")
        msg += f"{emoji} {sym}: {profit_net*100:+.2f}% | {pos_value:.2f} USDT\n"
    
    msg += f"💰 Total exposure: {total_value:.2f} USDT\n"
    msg += f"📊 Realized: {realized:+.2f} | Unrealized: {unrealized:+.2f} USDT"
    safe_send(msg)

# ====== TELEGRAM COMMANDS ======
def cmd_restart(update, context):
    safe_send("♻️ Fixed bot v6 restarting...")
    try:
        conn.close()
    except:
        pass
    try:
        os.remove(LOCK_FILE)
    except:
        pass
    python = sys.executable
    os.execv(python, [python] + sys.argv)

def cmd_close(update, context):
    try:
        symbol = context.args[0].upper()
    except Exception:
        update.message.reply_text("Usage: /close SYMBOL")
        return
        
    if safe_close_position(symbol, "Manual close"):
        update.message.reply_text(f"✅ Closed {symbol}")
    else:
        update.message.reply_text(f"❌ Error closing {symbol}")

def cmd_status(update, context):
    status_report()

def cmd_debug(update, context):
    debug_info = debug_all_signals()
    if debug_info:
        message = "🔍 FORCED DEBUG:\n" + "\n".join(debug_info[:15])
        safe_send(message)
    else:
        safe_send("🔍 No debug information available")

def cmd_sync(update, context):
    """Принудительная синхронизация всех позиций"""
    safe_send("🔄 FORCED SYNC STARTED...")
    
    positions = get_open_positions()
    synced_count = 0
    
    for symbol in positions.keys():
        real_balance = sync_balance_with_reality(symbol)
        if real_balance > 0:
            synced_count += 1
    
    safe_send(f"✅ SYNC COMPLETE: {synced_count}/{len(positions)} positions synced")

def cmd_mode(update, context):
    """Переключение режима агрессивности"""
    global USE_MULTI_TIMEFRAME, USE_PRICE_ACTION, RSI_MIN
    
    if USE_MULTI_TIMEFRAME:
        # Переключаем в агрессивный режим
        USE_MULTI_TIMEFRAME = False
        USE_PRICE_ACTION = False
        RSI_MIN = 25
        safe_send("🔴 AGGRESSIVE MODE: MTF disabled, RSI_MIN=25")
    else:
        # Переключаем в консервативный режим
        USE_MULTI_TIMEFRAME = True
        USE_PRICE_ACTION = False
        RSI_MIN = 30
        safe_send("🟢 CONSERVATIVE MODE: MTF enabled, RSI_MIN=30")

def cmd_check(update, context):
    """Проверка расчетов PnL"""
    cursor.execute("SELECT action, SUM(usdt_amount) FROM history GROUP BY action")
    rows = cursor.fetchall()
    
    msg = "📊 RAW HISTORY:\n"
    for action, total in rows:
        msg += f"{action}: {float(total) if total else 0:.2f} USDT\n"
    
    # Текущий расчет
    realized = realized_pnl_total()
    msg += f"\n📈 CALCULATED PnL: {realized:.2f} USDT"
    
    safe_send(msg)

# ====== INIT TELEGRAM ======
updater = Updater(TELEGRAM_TOKEN, use_context=True)
updater.dispatcher.add_handler(CommandHandler("close", cmd_close))
updater.dispatcher.add_handler(CommandHandler("restart", cmd_restart))
updater.dispatcher.add_handler(CommandHandler("status", cmd_status))
updater.dispatcher.add_handler(CommandHandler("debug", cmd_debug))
updater.dispatcher.add_handler(CommandHandler("sync", cmd_sync))
updater.dispatcher.add_handler(CommandHandler("mode", cmd_mode))
updater.dispatcher.add_handler(CommandHandler("check", cmd_check))
updater.start_polling()

# ====== STARTUP ======
safe_send("🚀 FIXED BOT v6 STARTING - Balance sync & minimum limits fixed!")

try:
    markets = exchange.load_markets()
except Exception as e:
    safe_send(f"⚠️ load_markets error: {e}")
    try: os.remove(LOCK_FILE)
    except: pass
    sys.exit(1)

active_symbols = [s for s in SYMBOLS if s in exchange.markets]
safe_send(f"📊 Monitoring {len(active_symbols)} symbols")

# Автоматическая синхронизация при старте
safe_send("🔄 Auto-syncing existing positions...")
bal = fetch_balance()
synced_count = 0
for sym in active_symbols:
    base = sym.split("/")[0]
    base_amount_on_balance = float(bal['total'].get(base, 0) or 0)
    min_amount = get_min_amount(sym)
    
    if base_amount_on_balance >= min_amount:
        cursor.execute("SELECT id FROM positions WHERE symbol=? AND status='OPEN'", (sym,))
        if not cursor.fetchone():
            ohlcv = fetch_ohlcv(sym, TIMEFRAME_PRIMARY, limit=3)
            open_price = float(ohlcv[-1][4]) if ohlcv else 0.0
            base_amount_rounded = round_amount(sym, base_amount_on_balance)
            record_open(sym, base_amount_rounded, open_price)
            synced_count += 1

safe_send(f"✅ Startup complete! {synced_count} positions synced. Scanning every 5 seconds!")

# ====== FIXED MAIN LOOP ======
last_status = 0
last_debug = 0

try:
    while True:
        cycle_start = time.time()
        
        bal = fetch_balance()
        if not bal:
            time.sleep(SCAN_INTERVAL)
            continue
            
        usdt_free = float(bal['free'].get('USDT', 0) or 0)
        equity = compute_equity()
        open_positions = get_open_positions()

        # 1) ИСПРАВЛЕННАЯ ПРОВЕРКА ПОЗИЦИЙ
        for sym, pos in list(open_positions.items()):
            try:
                ohlcv = fetch_ohlcv(sym, TIMEFRAME_PRIMARY, limit=50)
                if not ohlcv:
                    continue
                    
                df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v']).astype(float)
                price = float(df['c'].iloc[-1])
                open_price = float(pos['open_price'])
                
                profit_raw = (price - open_price) / open_price
                profit_net = profit_raw - ROUNDTRIP_FEE

                # ATR с защитой
                atr_series = calc_atr(df['h'], df['l'], df['c'], window=14)
                if atr_series is not None and not atr_series.empty:
                    atr_pct = atr_series.iloc[-1] / price
                else:
                    atr_pct = 0.01

                # ИСПРАВЛЕННЫЕ TP/SL
                tp_pct = max(atr_pct * TP_ATR_MULT, MIN_TP_PCT)
                sl_pct = -max(abs(atr_pct * SL_ATR_MULT), 0.01)

                trailing_hit = False
                cursor.execute("SELECT max_price FROM positions WHERE symbol=? AND status='OPEN'", (sym,))
                r = cursor.fetchone()
                max_price = float(r[0]) if (r and r[0]) else max(price, open_price)
                
                if profit_net >= TRAIL_START_NET and price <= max_price * (1 - TRAIL_GAP):
                    trailing_hit = True

                close_reason = ""
                should_close = False
                
                if profit_net >= (tp_pct - ROUNDTRIP_FEE):
                    close_reason = f"TP {profit_net*100:+.2f}%"
                    should_close = True
                elif profit_raw <= sl_pct:
                    close_reason = f"SL {profit_raw*100:+.2f}%"
                    should_close = True
                elif trailing_hit:
                    close_reason = f"TRAIL {profit_net*100:+.2f}%"
                    should_close = True

                if should_close:
                    safe_close_position(sym, close_reason)
                    cursor.execute("REPLACE INTO closed_log (symbol, last_closed_ts) VALUES (?, ?)", 
                                 (sym, int(time.time() + 300)))
                    conn.commit()
                    
            except Exception as e:
                logging.error(f"Position check error {sym}: {e}")
                continue

        # 2) ПОИСК СДЕЛОК С ПРОВЕРКОЙ МИНИМАЛЬНОЙ СУММЫ
        if can_open_new_trade() and usdt_free > MIN_TRADE_USDT:
            for sym in active_symbols:
                try:
                    if sym in open_positions:
                        continue
                    if is_in_cooldown(sym):
                        continue

                    ohlcv = fetch_ohlcv(sym, TIMEFRAME_PRIMARY, limit=25)
                    if not ohlcv or len(ohlcv) < 20:
                        continue
                        
                    df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v']).astype(float)
                    
                    ema10, ema50, rsi = calc_ema_rsi(df['c'])
                    last_close = df['c'].iloc[-1]
                    last_ema10 = ema10.iloc[-1] if not ema10.empty else 0
                    last_ema50 = ema50.iloc[-1] if not ema50.empty else 0
                    last_rsi = rsi.iloc[-1] if not rsi.empty else 0
                    
                    cond_ema = last_ema10 > last_ema50
                    cond_rsi = last_rsi > RSI_MIN
                    volume_avg = df['v'].tail(20).mean()
                    cond_volume = df['v'].iloc[-1] > volume_avg * VOLUME_MIN_MULT if volume_avg > 0 else True
                    cond_multi_tf = simple_multi_timeframe_signal(sym)
                    cond_simple_rsi = simple_rsi_strategy(sym)
                    
                    signal = (cond_ema and cond_rsi and cond_volume and cond_multi_tf) or cond_simple_rsi
                    
                    if signal:
                        usdt_for_order = equity * TRADE_PCT
                        
                        # ПРОВЕРКА МИНИМАЛЬНОЙ СУММЫ
                        if not check_min_order_value(sym, usdt_for_order):
                            logging.warning(f"Order too small for {sym}: {usdt_for_order:.2f} USDT")
                            continue
                            
                        raw_amount = usdt_for_order / last_close
                        base_amount = round_amount(sym, raw_amount)
                        min_amount = get_min_amount(sym)
                        
                        if base_amount >= min_amount:
                            try:
                                if not DRY_RUN:
                                    exchange.create_market_order(sym, 'buy', base_amount)
                                record_open(sym, base_amount, last_close)
                                safe_send(f"🎯 BUY {sym} | RSI: {last_rsi:.1f} | Size: {usdt_for_order:.2f} USDT")
                                
                                bal = fetch_balance()
                                usdt_free = float(bal['free'].get('USDT', 0) or 0)
                                
                                if not can_open_new_trade():
                                    break
                                    
                            except Exception as e:
                                error_msg = str(e)
                                if "lower limit" in error_msg:
                                    safe_send(f"⚠️ {sym}: Order too small ({usdt_for_order:.2f} USDT)")
                                else:
                                    logging.error(f"Buy error {sym}: {e}")
                        
                except Exception as e:
                    logging.error(f"Buy error {sym}: {e}")
                    continue

        current_time = time.time()
        if current_time - last_status > STATUS_INTERVAL:
            status_report()
            last_status = current_time
            
        if current_time - last_debug > 1800:
            debug_info = debug_all_signals()
            if any("BUY SIGNAL" in info for info in debug_info):
                signal_count = sum(1 for info in debug_info if "BUY SIGNAL" in info)
                safe_send(f"🔍 Auto-debug: {signal_count} buy signals found")
            last_debug = current_time

        elapsed = time.time() - cycle_start
        to_sleep = max(1.0, SCAN_INTERVAL - elapsed)
        time.sleep(to_sleep)

except KeyboardInterrupt:
    safe_send("⏹ Fixed bot v6 stopped by user")
except Exception as e:
    safe_send(f"💥 Bot crash: {e}")
    logging.exception("Fixed bot v6 crash")
finally:
    try:
        os.remove(LOCK_FILE)
    except:
        pass
    conn.close()
    safe_send("🔴 Fixed bot v6 shutdown")
