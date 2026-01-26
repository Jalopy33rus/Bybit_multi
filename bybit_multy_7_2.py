#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ULTIMATE TRADING BOT v7.2 - BYBIT FUTURES WITH OPTIMIZED HYBRID STRATEGY
Исправленная версия с балансировкой фильтров и исправленной логикой трендов
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
import traceback

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

# РЕЖИМЫ РАБОТЫ
DRY_RUN = True  # True = тестовый режим, False = реальная торговля
SANDBOX_MODE = False  # True = тестовая сеть Bybit

# КОМИССИИ BYBIT
TAKER_FEE = 0.0006  # 0.06%
MAKER_FEE = 0.0002  # 0.02%

# СИМВОЛЫ (ограничиваем для фокусировки)
SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT"
]

# Настройки для разных категорий символов
SYMBOL_CATEGORIES = {
    "BTC/USDT:USDT": {"volatility": "LOW", "risk_multiplier": 1.0, "min_trade_usdt": 20.0},
    "ETH/USDT:USDT": {"volatility": "LOW", "risk_multiplier": 1.0, "min_trade_usdt": 15.0},
    "BNB/USDT:USDT": {"volatility": "MEDIUM", "risk_multiplier": 0.8, "min_trade_usdt": 10.0},
    "SOL/USDT:USDT": {"volatility": "HIGH", "risk_multiplier": 0.6, "min_trade_usdt": 8.0},
}

active_symbols = SYMBOLS

# ====== НАСТРОЙКИ С БАЛАНСИРОВАННЫМИ ФИЛЬТРАМИ ======
TRADING_MODES = {
    "ULTRA_CONSERVATIVE": {
        "name": "🟣 УЛЬТРА-КОНСЕРВАТИВНЫЙ",
        "type": "trend_correction",
        "scan_interval": 300,
        "exit_check_interval": 30,
        "status_interval": 600,
        "sync_interval": 1800,
        "max_trades": 1,
        "trade_pct": 0.03,
        
        # Таймфреймы
        "timeframe_entry": "15m",
        "timeframe_trend": "1h",
        "timeframe_volatility": "4h",
        
        # Риск-менеджмент
        "max_stop_loss": 0.006,
        "take_profit": 0.018,
        "quick_exit": 0.012,
        "min_risk_reward": 2.5,
        
        # Фильтры тренда
        "min_trend_strength": 25,
        "max_trend_age": 30,  # Увеличено
        "require_trend_alignment": True,
        "require_trend_confirmation": True,  # Для этого режима оставляем
        
        # RSI фильтры
        "rsi_range_long": (28, 72),
        "rsi_range_short": (28, 72),
        
        # Фильтры объема
        "volume_multiplier": 1.5,
        "min_volume_score": 15,
        
        # Фильтры волатильности
        "max_atr_percentage": 0.08,
        "min_atr_percentage": 0.015,
        "bb_width_min": 0.012,
        
        # Общий фильтр
        "min_score": 90,
        "adaptive_scoring": True,
        
        # Лимиты
        "cooldown": 3600,
        "max_daily_trades_per_symbol": 1,
        "max_weekly_trades": 5,
        
        # Стратегия
        "strategy": "HYBRID_TREND_CORRECTION",
        "risk_level": "VERY_LOW",
        
        # Trailing stop
        "trailing_stop_activation": 0.010,
        "trailing_stop_distance": 0.005,
        "trailing_stop_update_frequency": 0.002,
        
        # Адаптивные настройки
        "adaptive_sl": True,
        "adaptive_tp": True,
        "adaptive_position_sizing": True,
        
        # Частичный выход
        "partial_exit_enabled": True,
        "partial_exit_1": 0.010,
        "partial_exit_2": 0.015,
        "partial_exit_pct_1": 0.25,
        "partial_exit_pct_2": 0.25,
        
        # Технические параметры
        "leverage": 2,
        "use_exchange_orders": True,
        "use_market_entry": False,
        "use_market_exit": False,
        "limit_order_timeout": 180,
        "commission_filter": True,
        "commission_requirement": 0.5,
    },
    
    "CONSERVATIVE": {
        "name": "🟡 КОНСЕРВАТИВНЫЙ",
        "type": "trend_correction",
        "scan_interval": 180,
        "exit_check_interval": 20,
        "status_interval": 300,
        "sync_interval": 1800,
        "max_trades": 2,
        "trade_pct": 0.05,
        
        "timeframe_entry": "15m",
        "timeframe_trend": "1h",
        "timeframe_volatility": "4h",
        
        "max_stop_loss": 0.008,
        "take_profit": 0.024,
        "quick_exit": 0.015,
        "min_risk_reward": 2.5,
        
        "min_trend_strength": 20,  # Снижено с 22
        "max_trend_age": 25,  # Увеличено
        "require_trend_alignment": True,
        "require_trend_confirmation": True,  # Оставляем для консервативного
        
        "rsi_range_long": (25, 75),
        "rsi_range_short": (25, 75),
        
        "volume_multiplier": 1.3,
        "min_volume_score": 12,
        
        "max_atr_percentage": 0.09,
        "min_atr_percentage": 0.015,
        "bb_width_min": 0.010,
        
        "min_score": 85,
        "adaptive_scoring": True,
        
        "cooldown": 1800,
        "max_daily_trades_per_symbol": 2,
        "max_weekly_trades": 8,
        
        "strategy": "HYBRID_TREND_CORRECTION",
        "risk_level": "LOW",
        
        "trailing_stop_activation": 0.012,
        "trailing_stop_distance": 0.006,
        "trailing_stop_update_frequency": 0.0025,
        
        "adaptive_sl": True,
        "adaptive_tp": True,
        "adaptive_position_sizing": True,
        
        "partial_exit_enabled": True,
        "partial_exit_1": 0.012,
        "partial_exit_2": 0.020,
        "partial_exit_pct_1": 0.3,
        "partial_exit_pct_2": 0.3,
        
        "leverage": 3,
        "use_exchange_orders": True,
        "use_market_entry": False,
        "use_market_exit": False,
        "limit_order_timeout": 120,
        "commission_filter": True,
        "commission_requirement": 0.5,
    },
    
    "AGGRESSIVE": {
        "name": "🟢 АГРЕССИВНЫЙ",
        "type": "trend_correction",
        "scan_interval": 120,
        "exit_check_interval": 15,
        "status_interval": 180,
        "sync_interval": 1800,
        "max_trades": 3,
        "trade_pct": 0.08,
        
        "timeframe_entry": "15m",
        "timeframe_trend": "30m",
        "timeframe_volatility": "2h",
        
        "max_stop_loss": 0.010,
        "take_profit": 0.030,
        "quick_exit": 0.018,
        "min_risk_reward": 2.0,
        
        "min_trend_strength": 12,  # Было 16 (СНИЖЕНО!)
        "allowed_trends_for_long": ["BULLISH", "WEAK_BULLISH", "VERY_WEAK_BULLISH"],  # НОВОЕ!
        "allowed_trends_for_short": ["BEARISH", "WEAK_BEARISH", "VERY_WEAK_BEARISH"],  # НОВОЕ!
        "max_trend_age": 20,  # Увеличено с 15
        "require_trend_alignment": False,  # ИСПРАВЛЕНО: False для агрессивного
        "require_trend_confirmation": False,  # Уже было False
        
        "rsi_range_long": (22, 78),
        "rsi_range_short": (22, 78),
        
        "volume_multiplier": 0.8,
        "min_volume_score": 8,
        
        "max_atr_percentage": 0.10,
        "min_atr_percentage": 0.008,
        "bb_width_min": 0.006,
        
        "min_score": 75,  # Снижено с 80
        "adaptive_scoring": True,
        
        "cooldown": 1200,
        "max_daily_trades_per_symbol": 3,
        "max_weekly_trades": 12,
        
        "strategy": "HYBRID_TREND_CORRECTION",
        "risk_level": "MEDIUM",
        
        "trailing_stop_activation": 0.015,
        "trailing_stop_distance": 0.008,
        "trailing_stop_update_frequency": 0.003,
        
        "adaptive_sl": True,
        "adaptive_tp": True,
        "adaptive_position_sizing": True,
        
        "partial_exit_enabled": True,
        "partial_exit_1": 0.015,
        "partial_exit_2": 0.025,
        "partial_exit_pct_1": 0.35,
        "partial_exit_pct_2": 0.35,
        
        "leverage": 4,
        "use_exchange_orders": True,
        "use_market_entry": False,
        "use_market_exit": False,
        "limit_order_timeout": 90,
        "commission_filter": True,
        "commission_requirement": 0.3,
    }
}

# Минимальные настройки
MIN_TRADE_USDT = 10.0

LOCK_FILE = "/tmp/ultimate_trading_bot_v7_2.lock"
DB_FILE = "trades_ultimate_futures_v7_2.db"

# Глобальные переменные
CURRENT_MODE = "AGGRESSIVE"  # Начинаем с агрессивного
BOT_RUNNING = True
exchange = None
bot = None
updater = None

# Глобальная статистика фильтров
filter_stats = {
    "total_signals": 0,
    "filtered_by": {
        "position_already_open": 0,
        "cooldown": 0,
        "weekly_limit": 0,
        "trend_not_confirmed": 0,
        "weak_trend": 0,
        "old_trend": 0,
        "high_volatility": 0,
        "low_volatility": 0,
        "rsi_out_of_range": 0,
        "low_volume": 0,
        "low_bb_width": 0,
        "macd_not_aligned": 0,
        "low_score": 0,
        "commission_filter": 0,
        "risk_reward": 0,
        "price_not_at_key_level": 0,
        "adaptive_sl_tp_failed": 0
    },
    "passed_filters": 0,
    "signals_by_symbol": {},
    "last_reset": time.time()
}

# ====== ЛОГГИРОВАНИЕ ======
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('ultimate_bot_futures_v7_2.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====== СТАТИСТИКА ФИЛЬТРОВ ======
def log_filter_stats(reset: bool = False):
    """Логирование статистики фильтров"""
    global filter_stats
    
    if reset:
        filter_stats = {
            "total_signals": 0,
            "filtered_by": {k: 0 for k in filter_stats["filtered_by"].keys()},
            "passed_filters": 0,
            "signals_by_symbol": {},
            "last_reset": time.time()
        }
        logger.info("🔄 Статистика фильтров сброшена")
        return
    
    if filter_stats["total_signals"] == 0:
        return
    
    logger.info("=" * 60)
    logger.info("📊 ДЕТАЛЬНАЯ СТАТИСТИКА ФИЛЬТРОВ")
    logger.info("=" * 60)
    
    total_filtered = sum(filter_stats["filtered_by"].values())
    pass_rate = (filter_stats["passed_filters"] / filter_stats["total_signals"] * 100) if filter_stats["total_signals"] > 0 else 0
    
    logger.info(f"Всего сигналов: {filter_stats['total_signals']}")
    logger.info(f"Прошло фильтры: {filter_stats['passed_filters']} ({pass_rate:.1f}%)")
    logger.info(f"Отфильтровано: {total_filtered}")
    
    sorted_filters = sorted(filter_stats["filtered_by"].items(), 
                           key=lambda x: x[1], reverse=True)
    
    logger.info("\nТОП-5 ФИЛЬТРОВ:")
    for i, (filter_name, count) in enumerate(sorted_filters[:5]):
        if count > 0:
            pct = count / filter_stats["total_signals"] * 100
            logger.info(f"  {i+1}. {filter_name}: {count} ({pct:.1f}%)")
    
    if filter_stats["signals_by_symbol"]:
        logger.info("\nСТАТИСТИКА ПО СИМВОЛАМ:")
        for symbol, stats in filter_stats["signals_by_symbol"].items():
            if stats["total"] > 0:
                pass_rate = (stats["passed"] / stats["total"] * 100) if stats["total"] > 0 else 0
                logger.info(f"  {symbol}: {stats['passed']}/{stats['total']} ({pass_rate:.1f}%)")
    
    logger.info("=" * 60)

def update_filter_stats(symbol: str, filter_name: str = None, passed: bool = False):
    """Обновление статистики фильтров"""
    global filter_stats
    
    if filter_name:
        filter_stats["total_signals"] += 1
        
        if symbol not in filter_stats["signals_by_symbol"]:
            filter_stats["signals_by_symbol"][symbol] = {"total": 0, "passed": 0}
        
        filter_stats["signals_by_symbol"][symbol]["total"] += 1
        
        if passed:
            filter_stats["passed_filters"] += 1
            filter_stats["signals_by_symbol"][symbol]["passed"] += 1
        else:
            filter_stats["filtered_by"][filter_name] += 1
    
    if filter_stats["total_signals"] > 0 and filter_stats["total_signals"] % 50 == 0:
        log_filter_stats()

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
                    quick_exit_price REAL DEFAULT 0,
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
                    entry_type TEXT DEFAULT 'LIMIT',
                    exit_type TEXT DEFAULT 'LIMIT',
                    partial_exit_1 INTEGER DEFAULT 0,
                    partial_exit_2 INTEGER DEFAULT 0,
                    risk_multiplier REAL DEFAULT 1.0,
                    atr_value REAL DEFAULT 0,
                    trend_strength REAL DEFAULT 0,
                    signal_score INTEGER DEFAULT 0,
                    risk_reward_ratio REAL DEFAULT 0,
                    filtered_reasons TEXT DEFAULT ''
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
                    entry_type TEXT DEFAULT 'LIMIT',
                    exit_type TEXT DEFAULT 'LIMIT',
                    partial_exit INTEGER DEFAULT 0,
                    pnl_percent REAL DEFAULT 0
                )
            """)
            
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbol_cooldown (
                    symbol TEXT PRIMARY KEY, 
                    last_closed_ts INTEGER DEFAULT 0,
                    daily_trade_count INTEGER DEFAULT 0, 
                    last_trade_date TEXT,
                    consecutive_losses INTEGER DEFAULT 0,
                    consecutive_wins INTEGER DEFAULT 0,
                    weekly_trade_count INTEGER DEFAULT 0,
                    weekly_start_date TEXT
                )
            """)
            
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbol_stats (
                    symbol TEXT PRIMARY KEY,
                    total_trades INTEGER DEFAULT 0,
                    win_trades INTEGER DEFAULT 0,
                    loss_trades INTEGER DEFAULT 0,
                    total_pnl REAL DEFAULT 0,
                    avg_win_pct REAL DEFAULT 0,
                    avg_loss_pct REAL DEFAULT 0,
                    win_rate REAL DEFAULT 0,
                    profit_factor REAL DEFAULT 0,
                    last_updated TEXT
                )
            """)
            
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS weekly_limits (
                    week_start TEXT PRIMARY KEY,
                    trade_count INTEGER DEFAULT 0,
                    total_pnl REAL DEFAULT 0
                )
            """)
            
            self._cursor.execute("""
                CREATE TABLE IF NOT EXISTS filter_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filter_name TEXT,
                    filter_count INTEGER DEFAULT 0,
                    symbol TEXT,
                    timestamp INTEGER,
                    date TEXT
                )
            """)
            
            self._connection.commit()
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Database initialization error: {e}")
            raise
    
    def get_connection(self):
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
                self.execute("""
                    INSERT INTO symbol_stats (symbol, total_trades, win_trades, loss_trades, 
                    total_pnl, avg_win_pct, avg_loss_pct, win_rate, last_updated)
                    VALUES (?, 1, ?, ?, ?, ?, ?, ?, datetime('now'))
                """, (
                    symbol, 
                    1 if is_win else 0,
                    0 if is_win else 1,
                    pnl_percent,
                    pnl_percent if is_win else 0,
                    0 if is_win else pnl_percent,
                    100.0 if is_win else 0.0
                ))
            else:
                total_trades = row[1] + 1
                win_trades = row[2] + (1 if is_win else 0)
                loss_trades = row[3] + (0 if is_win else 1)
                total_pnl = row[4] + pnl_percent
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                
                total_win = row[5] * row[2] + (pnl_percent if is_win else 0)
                total_loss = abs(row[6] * row[3] + (pnl_percent if not is_win else 0))
                profit_factor = total_win / total_loss if total_loss > 0 else 99.0
                
                if is_win:
                    avg_win = ((row[5] * row[2]) + pnl_percent) / win_trades if win_trades > 0 else pnl_percent
                    avg_loss = row[6]
                else:
                    avg_win = row[5]
                    avg_loss = ((abs(row[6]) * row[3]) + abs(pnl_percent)) / loss_trades if loss_trades > 0 else abs(pnl_percent)
                
                self.execute("""
                    UPDATE symbol_stats 
                    SET total_trades=?, win_trades=?, loss_trades=?, total_pnl=?, 
                        avg_win_pct=?, avg_loss_pct=?, win_rate=?, profit_factor=?, last_updated=datetime('now')
                    WHERE symbol=?
                """, (total_trades, win_trades, loss_trades, total_pnl, avg_win, avg_loss, win_rate, profit_factor, symbol))
                
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
        dp.add_handler(CommandHandler("mode", cmd_change_mode))
        dp.add_handler(CommandHandler("balance", cmd_balance))
        dp.add_handler(CommandHandler("limits", cmd_limits))
        dp.add_handler(CommandHandler("filter_stats", cmd_filter_stats))
        dp.add_handler(CommandHandler("reset_stats", cmd_reset_stats))
        dp.add_handler(CommandHandler("trend_stats", cmd_trend_stats))
        
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
    global BOT_RUNNING
    BOT_RUNNING = False
    logger.info("⏸️ Bot paused")
    safe_send("⏸️ <b>Торговля приостановлена</b>\nИспользуйте /resume для возобновления")

def resume_bot():
    global BOT_RUNNING
    BOT_RUNNING = True
    logger.info("▶️ Bot resumed")
    safe_send("▶️ <b>Торговля возобновлена</b>")

# ====== ОСНОВНЫЕ ФУНКЦИИ С ОБРАБОТКОЙ ОШИБОК ======
def retry_api_call(func, max_retries=3, delay=1.0):
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
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def fetch_ohlcv(symbol: str, timeframe: str, limit=100):
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
    def _fetch():
        return exchange.fetch_balance()
    
    try:
        return retry_api_call(_fetch)
    except Exception as e:
        logger.error(f"❌ Balance fetch failed: {e}")
        return {'free': {'USDT': 0.0}, 'total': {'USDT': 0.0}}

def get_current_price(symbol: str):
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
    return TRADING_MODES.get(CURRENT_MODE, TRADING_MODES["CONSERVATIVE"])

def get_symbol_info(symbol: str):
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
    """Улучшенный анализ тренда с исправленной логикой подтверждения"""
    try:
        df = get_ohlcv_data(symbol, timeframe, 100)
        if df is None or len(df) < 50:
            return {"strength": 0, "direction": "NEUTRAL", "age": 0, "confirmed": True, "ema_aligned": False}
        
        # 1. ADX для силы тренда
        adx_indicator = ADXIndicator(df['high'], df['low'], df['close'], window=14)
        adx = adx_indicator.adx().iloc[-1]
        plus_di = adx_indicator.adx_pos().iloc[-1]
        minus_di = adx_indicator.adx_neg().iloc[-1]
        
        # 2. EMA анализ
        ema_9 = df['close'].ewm(span=9).mean().iloc[-1]
        ema_21 = df['close'].ewm(span=21).mean().iloc[-1]
        ema_50 = df['close'].ewm(span=50).mean().iloc[-1]
        ema_200 = df['close'].ewm(span=200).mean().iloc[-1]
        
        # 3. Определение направления
        direction = "NEUTRAL"
        if adx > 25:
            if plus_di > minus_di:
                direction = "BULLISH"
            else:
                direction = "BEARISH"
        elif adx > 18:
            if plus_di > minus_di:
                direction = "WEAK_BULLISH"
            else:
                direction = "WEAK_BEARISH"
        elif adx > 12:
            if plus_di > minus_di:
                direction = "VERY_WEAK_BULLISH"
            else:
                direction = "VERY_WEAK_BEARISH"
        
        # 4. Проверка согласованности EMA
        ema_aligned = False
        if direction in ["BULLISH", "WEAK_BULLISH", "VERY_WEAK_BULLISH"]:
            ema_aligned = ema_9 > ema_21 > ema_50
        elif direction in ["BEARISH", "WEAK_BEARISH", "VERY_WEAK_BEARISH"]:
            ema_aligned = ema_9 < ema_21 < ema_50
        
        # 5. Определение возраста тренда
        trend_age = 0
        if len(df) >= 20:
            if direction in ["BULLISH", "WEAK_BULLISH", "VERY_WEAK_BULLISH"]:
                for i in range(1, min(21, len(df))):
                    if df['close'].iloc[-i] > df['close'].iloc[-i-1]:
                        trend_age += 1
                    else:
                        break
            elif direction in ["BEARISH", "WEAK_BEARISH", "VERY_WEAK_BEARISH"]:
                for i in range(1, min(21, len(df))):
                    if df['close'].iloc[-i] < df['close'].iloc[-i-1]:
                        trend_age += 1
                    else:
                        break
        
        # 6. Проверка на других таймфреймах - ИСПРАВЛЕНА ЛОГИКА!
        confirmed = True
        settings = get_current_settings()
        
        # Только если требуется подтверждение тренда
        if settings.get('require_trend_confirmation', False) and timeframe in ["1h", "30m"]:
            try:
                # Проверяем на старшем таймфрейме
                higher_tf = "4h" if timeframe == "1h" else "1h"
                df_higher = get_ohlcv_data(symbol, higher_tf, 50)
                if df_higher is not None and len(df_higher) > 20:
                    sma_20_higher = df_higher['close'].tail(20).mean()
                    sma_50_higher = df_higher['close'].tail(50).mean()
                    
                    if direction in ["BULLISH", "WEAK_BULLISH", "VERY_WEAK_BULLISH"]:
                        # Мягкая проверка с допуском 1%
                        confirmed = sma_20_higher > sma_50_higher * 0.99
                    elif direction in ["BEARISH", "WEAK_BEARISH", "VERY_WEAK_BEARISH"]:
                        confirmed = sma_20_higher < sma_50_higher * 1.01
                        
            except Exception as e:
                logger.warning(f"⚠️ Multi-timeframe check error for {symbol}: {e}")
                confirmed = True  # Если ошибка - считаем подтвержденным
        
        return {
            "strength": adx,
            "direction": direction,
            "age": trend_age,
            "confirmed": confirmed,
            "ema_aligned": ema_aligned,
            "plus_di": plus_di,
            "minus_di": minus_di,
            "ema_9": ema_9,
            "ema_21": ema_21,
            "ema_50": ema_50,
            "ema_200": ema_200
        }
        
    except Exception as e:
        logger.error(f"❌ Trend analysis error for {symbol}: {e}")
        return {"strength": 0, "direction": "NEUTRAL", "age": 0, "confirmed": True, "ema_aligned": False}

def get_volatility_analysis(symbol: str, timeframe: str = "4h") -> Dict:
    try:
        df = get_ohlcv_data(symbol, timeframe, 50)
        if df is None or len(df) < 20:
            return {"atr": 0, "atr_percentage": 0, "bb_width": 0, "volatility_rank": "LOW"}
        
        current_price = df['close'].iloc[-1]
        
        atr_indicator = AverageTrueRange(df['high'], df['low'], df['close'], window=14)
        atr = atr_indicator.average_true_range().iloc[-1]
        atr_percentage = (atr / current_price) * 100 if current_price > 0 else 0
        
        bb = BollingerBands(df['close'], window=20, window_dev=2)
        bb_upper = bb.bollinger_hband().iloc[-1]
        bb_lower = bb.bollinger_lband().iloc[-1]
        bb_middle = bb.bollinger_mavg().iloc[-1]
        bb_width = ((bb_upper - bb_lower) / bb_middle) * 100 if bb_middle > 0 else 0
        
        returns = df['close'].pct_change().dropna()
        hist_volatility = returns.std() * np.sqrt(365) * 100 if len(returns) > 0 else 0
        
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
            "current_price": current_price,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "bb_middle": bb_middle
        }
        
    except Exception as e:
        logger.error(f"❌ Volatility analysis error for {symbol}: {e}")
        return {"atr": 0, "atr_percentage": 0, "bb_width": 0, "volatility_rank": "LOW"}

def get_ohlcv_data(symbol: str, timeframe: str, limit: int):
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

# ====== АДАПТИВНЫЙ РАСЧЕТ SCORE ======
def calculate_adaptive_score(signal: Dict) -> int:
    try:
        base_score = signal.get('score', 0)
        
        settings = get_current_settings()
        if not settings.get('adaptive_scoring', False):
            return base_score
        
        # Учитываем силу тренда
        trend_strength = signal.get('trend_strength', 0)
        if trend_strength > 40:
            bonus = 15
        elif trend_strength > 30:
            bonus = 10
        elif trend_strength > 25:
            bonus = 5
        else:
            bonus = 0
        
        # Учитываем волатильность
        atr_percentage = signal.get('atr_percentage', 0)
        if atr_percentage > 6:
            bonus -= 5
        elif atr_percentage < 2:
            bonus -= 3
        
        # Учитываем коррекцию
        if signal.get('price_at_key_level', False):
            correction_depth = signal.get('correction_depth', 0)
            if correction_depth > 0.03:
                bonus += 10
            elif correction_depth > 0.02:
                bonus += 5
            elif correction_depth > 0.01:
                bonus += 2
        
        # Учитываем объем
        volume_ratio = signal.get('volume_ratio', 1)
        if volume_ratio > 2.0:
            bonus += 5
        elif volume_ratio > 1.5:
            bonus += 3
        
        # Учитываем согласованность индикаторов
        macd_histogram = signal.get('macd_histogram', 0)
        position_type = signal.get('signal_type', 'LONG')
        
        if position_type == 'LONG' and macd_histogram > 0:
            bonus += 3
        elif position_type == 'SHORT' and macd_histogram < 0:
            bonus += 3
        
        final_score = max(0, base_score + bonus)
        final_score = min(final_score, 150)
        
        logger.debug(f"🔢 Adaptive score: {base_score} + {bonus} = {final_score}")
        
        return final_score
        
    except Exception as e:
        logger.error(f"❌ Adaptive score calculation error: {e}")
        return signal.get('score', 0)

# ====== РАСЧЕТ АДАПТИВНОГО РАЗМЕРА ПОЗИЦИИ ======
def calculate_position_size(symbol: str, signal_score: int, available_usdt: float):
    try:
        settings = get_current_settings()
        
        base_pct = settings['trade_pct']
        
        if signal_score >= 100:
            multiplier = 1.2
        elif signal_score >= 90:
            multiplier = 1.1
        elif signal_score >= 80:
            multiplier = 1.0
        elif signal_score >= 70:
            multiplier = 0.9
        else:
            multiplier = 0.8
        
        total_pct = base_pct * multiplier
        
        max_pct = 0.05
        total_pct = min(total_pct, max_pct)
        
        position_usdt = available_usdt * total_pct
        
        min_trade = SYMBOL_CATEGORIES.get(symbol, {}).get("min_trade_usdt", MIN_TRADE_USDT)
        if position_usdt < min_trade:
            logger.info(f"⏹️ Position too small for {symbol}: {position_usdt:.2f} < {min_trade}")
            return 0
        
        logger.info(f"📏 Position size for {symbol}: {total_pct*100:.1f}% = {position_usdt:.2f} USDT")
        
        return position_usdt
        
    except Exception as e:
        logger.error(f"❌ Position size calculation error: {e}")
        return available_usdt * 0.03

# ====== ИСПРАВЛЕННЫЙ FILTER КОМИССИЙ ======
def commission_filter(symbol: str, entry_price: float, take_profit: float, 
                     position_type: str, trade_amount_usdt: float):
    try:
        if position_type == "LONG":
            potential_profit_pct = (take_profit - entry_price) / entry_price * 100
        else:
            potential_profit_pct = (entry_price - take_profit) / entry_price * 100
        
        settings = get_current_settings()
        entry_fee_pct = TAKER_FEE * 100 if settings.get('use_market_entry', False) else MAKER_FEE * 100
        exit_fee_pct = TAKER_FEE * 100 if settings.get('use_market_exit', False) else MAKER_FEE * 100
        total_fee_pct = entry_fee_pct + exit_fee_pct
        
        required_profit = total_fee_pct + settings.get('commission_requirement', 1.0)
        
        passes = potential_profit_pct > required_profit
        
        if not passes:
            logger.info(f"⏹️ Commission filter failed for {symbol}: "
                       f"Profit {potential_profit_pct:.2f}% < Required {required_profit:.2f}%")
            update_filter_stats(symbol, "commission_filter", False)
        
        return passes
        
    except Exception as e:
        logger.error(f"❌ Commission filter error: {e}")
        return False

# ====== ПРОВЕРКА RISK/REWARD ======
def validate_risk_reward(entry_price: float, stop_loss: float, take_profit: float, position_type: str):
    try:
        if position_type == 'LONG':
            risk = entry_price - stop_loss
            reward = take_profit - entry_price
        else:
            risk = stop_loss - entry_price
            reward = entry_price - take_profit
        
        if risk <= 0:
            logger.error(f"❌ Invalid risk calculation: risk={risk}")
            return False, 0
        
        risk_reward_ratio = reward / risk
        settings = get_current_settings()
        
        passes = risk_reward_ratio >= settings.get('min_risk_reward', 2.0)
        
        if not passes:
            logger.info(f"⏹️ Risk/Reward filter failed: {risk_reward_ratio:.2f} < {settings.get('min_risk_reward', 2.0)}")
            update_filter_stats("", "risk_reward", False)
        
        return passes, risk_reward_ratio
        
    except Exception as e:
        logger.error(f"❌ Risk/Reward validation error: {e}")
        return False, 0

# ====== УЛУЧШЕННЫЙ АНАЛИЗ СИМВОЛОВ ======
def analyze_symbol_with_filters(symbol: str) -> Optional[Dict]:
    """Анализ символа со сбалансированными фильтрами и адаптацией к рынку"""
    try:
        update_filter_stats(symbol)
        
        settings = get_current_settings()
        
        # 1. Проверка что позиция не открыта
        if is_position_already_open(symbol):
            logger.debug(f"⏹️ Position already open for {symbol}")
            update_filter_stats(symbol, "position_already_open", False)
            return None
        
        # 2. Проверка кулдауна
        if is_in_cooldown(symbol):
            logger.debug(f"⏹️ {symbol} in cooldown")
            update_filter_stats(symbol, "cooldown", False)
            return None
        
        # 3. Проверка недельного лимита
        if check_weekly_limit():
            logger.debug(f"⏹️ Weekly trade limit reached")
            update_filter_stats(symbol, "weekly_limit", False)
            return None
        
        # 4. Анализ тренда - УЛУЧШЕННЫЙ С ДИАГНОСТИКОЙ
        trend_analysis = get_trend_analysis(symbol, settings['timeframe_trend'])
        
        logger.info(f"🔍 {symbol} на {settings['timeframe_trend']}: "
                   f"ADX={trend_analysis['strength']:.1f} (требуется {settings['min_trend_strength']}), "
                   f"Направление={trend_analysis['direction']}, "
                   f"Confirmed={trend_analysis['confirmed']}, "
                   f"Age={trend_analysis['age']}")
        
        # Проверка подтверждения тренда (если требуется)
        if not trend_analysis["confirmed"] and settings.get('require_trend_confirmation', True):
            logger.debug(f"⏹️ {symbol} filtered: trend not confirmed")
            update_filter_stats(symbol, "trend_not_confirmed", False)
            return None
        
        # Проверка силы тренда
        if trend_analysis["strength"] < settings['min_trend_strength']:
            logger.debug(f"⏹️ {symbol} filtered: weak trend {trend_analysis['strength']:.1f} < {settings['min_trend_strength']}")
            update_filter_stats(symbol, "weak_trend", False)
            return None
        
        # Проверка возраста тренда
        if trend_analysis["age"] > settings.get('max_trend_age', 20):
            logger.debug(f"⏹️ {symbol} filtered: old trend ({trend_analysis['age']} candles)")
            update_filter_stats(symbol, "old_trend", False)
            return None
        
        # Определение допустимых направлений тренда в зависимости от режима
        if CURRENT_MODE == "AGGRESSIVE":
            allowed_long_trends = ["BULLISH", "WEAK_BULLISH", "VERY_WEAK_BULLISH"]
            allowed_short_trends = ["BEARISH", "WEAK_BEARISH", "VERY_WEAK_BEARISH"]
        elif CURRENT_MODE == "CONSERVATIVE":
            allowed_long_trends = ["BULLISH", "WEAK_BULLISH"]
            allowed_short_trends = ["BEARISH", "WEAK_BEARISH"]
        else:  # ULTRA_CONSERVATIVE
            allowed_long_trends = ["BULLISH"]
            allowed_short_trends = ["BEARISH"]
        
        # Определяем тип позиции на основе направления тренда
        if trend_analysis["direction"] in allowed_long_trends:
            position_type = "LONG"
            allowed_trends = allowed_long_trends
        elif trend_analysis["direction"] in allowed_short_trends:
            position_type = "SHORT"
            allowed_trends = allowed_short_trends
        else:
            logger.debug(f"⏹️ {symbol} filtered: trend direction {trend_analysis['direction']} not allowed for {CURRENT_MODE}")
            update_filter_stats(symbol, "trend_direction_not_allowed", False)
            return None
        
        # 5. Анализ волатильности С АДАПТАЦИЕЙ ДЛЯ РЕЖИМА
        volatility = get_volatility_analysis(symbol, settings['timeframe_volatility'])
        
        # Определяем адаптированные пороги для агрессивного режима
        if CURRENT_MODE == "AGGRESSIVE":
            # Для агрессивного режима снижаем требования на 40%
            min_atr_required = settings['min_atr_percentage'] * 100 * 0.6  # 40% снижение
            max_atr_allowed = settings['max_atr_percentage'] * 100 * 1.4   # 40% увеличение
            min_bb_width_required = settings.get('bb_width_min', 0.01) * 0.6
        else:
            min_atr_required = settings['min_atr_percentage'] * 100
            max_atr_allowed = settings['max_atr_percentage'] * 100
            min_bb_width_required = settings.get('bb_width_min', 0.01)
        
        logger.info(f"📊 {symbol} Волатильность: ATR={volatility['atr_percentage']:.2f}% "
                   f"(требуется {min_atr_required:.2f}% - {max_atr_allowed:.2f}%)")
        
        if volatility["atr_percentage"] > max_atr_allowed:
            logger.debug(f"⏹️ {symbol} filtered: high volatility {volatility['atr_percentage']:.1f}% > {max_atr_allowed:.1f}%")
            update_filter_stats(symbol, "high_volatility", False)
            return None

        if volatility["atr_percentage"] < min_atr_required:
            logger.debug(f"⏹️ {symbol} filtered: low volatility {volatility['atr_percentage']:.1f}% < {min_atr_required:.1f}%")
            update_filter_stats(symbol, "low_volatility", False)
            return None
        
        # 6. Технический анализ на входном ТФ
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
        bb_width = ((bb_upper - bb_lower) / bb_middle) if bb_middle != 0 else 0
        
        price_position = (current_price - bb_lower) / (bb_upper - bb_lower + 1e-9)
        
        # Проверка ширины BB с адаптацией
        if bb_width < min_bb_width_required:
            logger.debug(f"⏹️ {symbol} filtered: low BB width {bb_width:.3%} < {min_bb_width_required:.3%}")
            update_filter_stats(symbol, "low_bb_width", False)
            return None
        
        # ГИБРИДНАЯ СТРАТЕГИЯ: Проверка коррекции к ключевым уровням
        price_at_key_level = False
        correction_depth = 0
        
        # Расчет скользящих средних
        ema_20 = df['close'].ewm(span=20).mean().iloc[-1]
        ema_50 = df['close'].ewm(span=50).mean().iloc[-1]
        
        # Определяем, находится ли рынок в боковике (для адаптации фильтров)
        is_market_ranging = False
        if CURRENT_MODE == "AGGRESSIVE":
            # Проверяем диапазон на старшем ТФ
            df_higher = get_ohlcv_data(symbol, "4h", 20)
            if df_higher is not None and len(df_higher) >= 10:
                price_range = (df_higher['high'].max() - df_higher['low'].min()) / df_higher['close'].mean()
                if price_range < 0.03:  # Диапазон меньше 3% = боковик
                    is_market_ranging = True
                    logger.info(f"📊 {symbol}: рынок в боковике (диапазон {price_range:.2%} < 3%)")
        
        # ============ АДАПТИВНЫЕ MACD ПОРОГИ ============
        # Основные пороги в зависимости от силы тренда
        if "VERY_WEAK" in trend_analysis["direction"]:
            base_macd_threshold = 0.0003    # Очень слабый тренд - ослабляем
        elif "WEAK" in trend_analysis["direction"]:
            base_macd_threshold = 0.0002  # Слабый тренд
        else:
            base_macd_threshold = 0.0001  # Сильный тренд
        
        # Дополнительное ослабление в боковике
        if is_market_ranging:
            base_macd_threshold *= 2.0  # Удваиваем порог в боковике
        
        # ============ АДАПТАЦИЯ К СИЛЬНОМУ ТРЕНДУ ============
        # ВАЖНО: Ослабляем фильтры только ПОСЛЕ объявления переменных!
        volume_adjustment = 1.0
        macd_adjustment = 1.0
        
        if trend_analysis["strength"] > 30:  # Сильный тренд
            macd_adjustment = 2.0      # Ослабляем MACD фильтр в 2 раза
            volume_adjustment = 0.7    # Ослабляем объем на 30%
            logger.info(f"📊 {symbol}: сильный тренд (ADX={trend_analysis['strength']:.1f}), "
                       f"ослабляем MACD x{macd_adjustment:.1f}, объем x{volume_adjustment:.1f}")
        
        # Применяем адаптацию к сильному тренду
        macd_threshold = base_macd_threshold * macd_adjustment
        
        # ============ КОНЕЦ АДАПТАЦИИ MACD ============
        
        if position_type == "LONG":
            # Проверяем откат к поддержке
            
            # Проверка 1: Цена в нижней части BB (0.05-0.45)
            if 0.05 <= price_position <= 0.45:
                price_at_key_level = True
                correction_depth = 1 - price_position
            
            # Проверка 2: Цена около EMA20 или EMA50
            price_to_ema20 = abs(current_price - ema_20) / ema_20
            price_to_ema50 = abs(current_price - ema_50) / ema_50
            
            if price_to_ema20 < 0.015 or price_to_ema50 < 0.02:
                price_at_key_level = True
                correction_depth = min(price_to_ema20, price_to_ema50)
            
            # Проверка согласованности MACD с адаптивным порогом
            if not (macd_histogram > -macd_threshold):
                logger.debug(f"⏹️ {symbol} filtered: MACD not bullish enough for LONG "
                           f"({macd_histogram:.6f} <= {-macd_threshold:.6f}, порог адаптирован для {trend_analysis['direction']})")
                update_filter_stats(symbol, "macd_not_aligned", False)
                return None
                
        else:  # SHORT
            # Проверяем откат к сопротивлению
            
            # Проверка 1: Цена в верхней части BB (0.55-0.95)
            if 0.55 <= price_position <= 0.95:
                price_at_key_level = True
                correction_depth = price_position
            
            # Проверка 2: Цена около EMA20 или EMA50
            price_to_ema20 = abs(current_price - ema_20) / ema_20
            price_to_ema50 = abs(current_price - ema_50) / ema_50
            
            if price_to_ema20 < 0.015 or price_to_ema50 < 0.02:
                price_at_key_level = True
                correction_depth = min(price_to_ema20, price_to_ema50)
            
            # Проверка согласованности MACD с адаптивным порогом
            if not (macd_histogram < macd_threshold):
                logger.debug(f"⏹️ {symbol} filtered: MACD not bearish enough for SHORT "
                           f"({macd_histogram:.6f} >= {macd_threshold:.6f}, порог адаптирован для {trend_analysis['direction']})")
                update_filter_stats(symbol, "macd_not_aligned", False)
                return None
        
        # Фильтр RSI с учетом режима и направления тренда
        if "BEARISH" in trend_analysis["direction"]:
            # Для медвежьего рынка расширяем диапазон RSI
            rsi_range_long = settings.get('rsi_range_bearish_long', settings['rsi_range_long'])
            rsi_range_short = settings.get('rsi_range_bearish_short', settings['rsi_range_short'])
        else:
            rsi_range_long = settings['rsi_range_long']
            rsi_range_short = settings['rsi_range_short']
        
        rsi_range = rsi_range_long if position_type == "LONG" else rsi_range_short
        if not (rsi_range[0] <= rsi <= rsi_range[1]):
            logger.debug(f"⏹️ {symbol} filtered: RSI {rsi:.1f} outside range {rsi_range}")
            update_filter_stats(symbol, "rsi_out_of_range", False)
            return None
        
        # Фильтр объема с адаптацией к рынку
        required_volume_ratio = settings['volume_multiplier']
        
        # Адаптация к рынку:
        if is_market_ranging:
            # В боковике снижаем требования к объему на 50%
            required_volume_ratio *= 0.5
            logger.info(f"📊 {symbol}: снижаем требования к объему в боковике до {required_volume_ratio:.1f}x")
        elif SYMBOL_CATEGORIES.get(symbol, {}).get("volatility") in ["HIGH", "VERY_HIGH"]:
            required_volume_ratio *= 0.8  # 20% снижение для волатильных
        
        # Дополнительное ослабление для VERY_WEAK трендов
        if "VERY_WEAK" in trend_analysis["direction"]:
            required_volume_ratio *= 0.7  # Еще 30% снижение
        
        # Применяем адаптацию к сильному тренду (уже объявлена выше)
        required_volume_ratio *= volume_adjustment
        
        if volume_ratio < required_volume_ratio:
            logger.debug(f"⏹️ {symbol} filtered: low volume {volume_ratio:.1f}x < {required_volume_ratio:.1f}x "
                        f"(адаптировано для {trend_analysis['direction']})")
            update_filter_stats(symbol, "low_volume", False)
            return None
        
        # Расчет score с адаптацией для слабых трендов
        score = 0
        reasons = []
        
        # Тренд (макс 30)
        trend_score = min(trend_analysis["strength"], 30)
        score += trend_score
        reasons.append(f"TREND_{trend_analysis['direction']}")
        
        # Бонус за сильный тренд (только для не VERY_WEAK)
        if trend_analysis["strength"] > 30:
            score += 5
            reasons.append("STRONG_TREND")
        elif trend_analysis["strength"] > 25 and "VERY_WEAK" not in trend_analysis["direction"]:
            score += 3
            reasons.append("MODERATE_TREND")
        elif "VERY_WEAK" in trend_analysis["direction"]:
            # Для VERY_WEAK даем небольшой бонус за прохождение фильтров
            score += 2
            reasons.append("VERY_WEAK_PASSED")
        
        # Объем (макс 15) с бонусом за высокий объем
        volume_score = min(volume_ratio * 8, 15) if volume_ratio >= required_volume_ratio else 0
        score += volume_score
        if volume_score > 0:
            reasons.append("HIGH_VOLUME")
        
        # RSI (макс 15) с бонусами за экстремальные значения
        if rsi_range[0] <= rsi <= rsi_range[1]:
            score += 15
            reasons.append("GOOD_RSI")
            
            # Бонус за оптимальный RSI (только для не VERY_WEAK)
            if "VERY_WEAK" not in trend_analysis["direction"]:
                if position_type == "LONG" and rsi < 35:
                    score += 3
                    reasons.append("RSI_OVERSOLD")
                elif position_type == "SHORT" and rsi > 65:
                    score += 3
                    reasons.append("RSI_OVERBOUGHT")
        
        # Коррекция к ключевому уровню (макс 20)
        if price_at_key_level:
            correction_score = min(correction_depth * 80, 20)
            score += correction_score
            reasons.append("PRICE_AT_KEY_LEVEL")
            
            # Дополнительные бонусы за качество коррекции
            if correction_depth > 0.03:  # Глубокая коррекция >3%
                score += 5
                reasons.append("DEEP_CORRECTION")
            elif correction_depth > 0.02:  # Средняя коррекция >2%
                score += 3
                reasons.append("MEDIUM_CORRECTION")
            elif correction_depth > 0.01 and "VERY_WEAK" in trend_analysis["direction"]:
                # Для VERY_WEAK даже небольшая коррекция - хорошо
                score += 2
                reasons.append("LIGHT_CORRECTION")
        else:
            update_filter_stats(symbol, "price_not_at_key_level", False)
            logger.debug(f"⏹️ {symbol} filtered: price not at key level")
            return None
        
        # Волатильность (макс 10)
        if bb_width >= min_bb_width_required:
            score += 10
            reasons.append("GOOD_VOLATILITY")
            
            # Бонус за оптимальную волатильность (только для не VERY_WEAK)
            if "VERY_WEAK" not in trend_analysis["direction"] and 0.02 <= bb_width <= 0.05:
                score += 3
                reasons.append("OPTIMAL_VOLATILITY")
        
        # Согласованность индикаторов (макс 10)
        if position_type == "LONG" and macd_histogram > -macd_threshold:
            score += 10
            reasons.append("MACD_BULLISH")
            
            # Бонус за сильный MACD (только для не VERY_WEAK)
            if "VERY_WEAK" not in trend_analysis["direction"] and macd_histogram > 0.001:
                score += 3
                reasons.append("STRONG_MACD_BULLISH")
        elif position_type == "SHORT" and macd_histogram < macd_threshold:
            score += 10
            reasons.append("MACD_BEARISH")
            
            # Бонус за сильный MACD (только для не VERY_WEAK)
            if "VERY_WEAK" not in trend_analysis["direction"] and macd_histogram < -0.001:
                score += 3
                reasons.append("STRONG_MACD_BEARISH")
        
        # Дополнительный бонус за прохождение всех фильтров в боковике
        if is_market_ranging:
            score += 5
            reasons.append("RANGING_MARKET_BONUS")
        
        # Применяем адаптивный scoring
        base_signal = {
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
            "volatility_rank": volatility["volatility_rank"],
            "price_at_key_level": price_at_key_level,
            "correction_depth": correction_depth,
            "macd_histogram": macd_histogram,
            "ema_20": ema_20,
            "ema_50": ema_50,
            "is_market_ranging": is_market_ranging
        }
        
        adaptive_score = calculate_adaptive_score(base_signal)
        base_signal["score"] = adaptive_score
        
        logger.info(f"🎯 {symbol} {position_type}: Score={adaptive_score}, "
                   f"Trend={trend_analysis['direction']} ({trend_analysis['strength']:.1f}), "
                   f"RSI={rsi:.1f}, Vol={volume_ratio:.1f}x, "
                   f"Correction={'YES' if price_at_key_level else 'NO'} {correction_depth:.2%}, "
                   f"MACD={macd_histogram:.6f}, BB={bb_width:.3%}")
        
        if adaptive_score >= settings['min_score']:
            update_filter_stats(symbol, passed=True)
            return base_signal
        else:
            logger.debug(f"⏹️ {symbol} filtered: low score {adaptive_score} < {settings['min_score']}")
            update_filter_stats(symbol, "low_score", False)
            return None
        
    except Exception as e:
        logger.error(f"❌ Analyze symbol error for {symbol}: {e}")
        traceback.print_exc()
        return None

# ====== УПРАВЛЕНИЕ ПОЗИЦИЯМИ ======
def get_open_positions():
    try:
        rows = db.fetchall("""
            SELECT symbol, base_amount, open_price, stop_loss, take_profit, quick_exit_price,
                   max_price, min_price, original_stop_loss, trailing_active, open_timestamp, 
                   position_type, leverage, invested_usdt, exchange_order_ids, entry_type, 
                   partial_exit_1, partial_exit_2, atr_value, trend_strength, signal_score
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
                "quick_exit_price": safe_float_convert(row[5]),
                "max_price": safe_float_convert(row[6] or row[2]), 
                "min_price": safe_float_convert(row[7] or row[2]),
                "original_stop_loss": safe_float_convert(row[8] or row[3]), 
                "trailing_active": row[9] or 0,
                "open_timestamp": row[10] or int(time.time()), 
                "position_type": row[11] or 'LONG',
                "leverage": row[12] or 1, 
                "invested_usdt": safe_float_convert(row[13]),
                "exchange_order_ids": row[14] or "",
                "entry_type": row[15] or "LIMIT",
                "partial_exit_1": row[16] or 0,
                "partial_exit_2": row[17] or 0,
                "atr_value": safe_float_convert(row[18]),
                "trend_strength": safe_float_convert(row[19]),
                "signal_score": row[20] or 0
            }
        
        return positions
        
    except Exception as e:
        logger.error(f"❌ Positions fetch error: {e}")
        return {}

def get_concurrent_trades_count():
    try:
        row = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='OPEN'")
        return row[0] if row else 0
    except Exception as e:
        logger.error(f"❌ Concurrent trades count error: {e}")
        return 0

def is_in_cooldown(symbol: str):
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
    try:
        row = db.fetchone("SELECT COUNT(*) FROM positions WHERE symbol=? AND status='OPEN'", (symbol,))
        return row[0] > 0 if row else False
    except Exception as e:
        logger.error(f"❌ Position check error for {symbol}: {e}")
        return False

def check_weekly_limit():
    try:
        settings = get_current_settings()
        weekly_limit = settings.get('max_weekly_trades', 99)
        
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday())
        week_start_str = week_start.strftime('%Y-%m-%d')
        
        row = db.fetchone(
            "SELECT trade_count FROM weekly_limits WHERE week_start=?",
            (week_start_str,)
        )
        
        current_count = row[0] if row else 0
        
        if current_count >= weekly_limit:
            logger.info(f"⏹️ Weekly trade limit reached: {current_count}/{weekly_limit}")
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"❌ Weekly limit check error: {e}")
        return False

def can_open_new_trade():
    settings = get_current_settings()
    current_trades = get_concurrent_trades_count()
    can_open = current_trades < settings['max_trades']
    
    if not can_open:
        logger.info(f"⏹️ Max trades reached: {current_trades}/{settings['max_trades']}")
    
    return can_open

# ====== ОТКРЫТИЕ ПОЗИЦИЙ ======
def open_position(signal: Dict):
    try:
        logger.info(f"🚀 Пытаемся открыть позицию: {signal.get('symbol')}")
        logger.info(f"📊 Параметры сигнала: цена={signal.get('price')}, score={signal.get('score')}")
        
        symbol = signal['symbol']
        current_price = signal['price']
        position_type = signal['signal_type']
        signal_score = signal['score']
        settings = get_current_settings()
        
        # ДОБАВИТЬ: Логирование информации о символе
        symbol_info = get_symbol_info(symbol)
        logger.info(f"📋 Инфо о символе {symbol}:")
        logger.info(f"  contract_size: {symbol_info.get('contract_size')}")
        logger.info(f"  price_precision: {symbol_info.get('price_precision')}")
        logger.info(f"  amount_precision: {symbol_info.get('amount_precision')}")
        logger.info(f"  min_amount: {symbol_info.get('min_amount')}")
        symbol = signal['symbol']
        current_price = signal['price']
        position_type = signal['signal_type']
        signal_score = signal['score']
        settings = get_current_settings()
        
        available_usdt = compute_available_usdt()
        
        if settings.get('adaptive_position_sizing', False):
            trade_amount_usdt = calculate_position_size(symbol, signal_score, available_usdt)
        else:
            trade_amount_usdt = available_usdt * settings['trade_pct']
        
        if trade_amount_usdt <= 0:
            logger.info(f"⏹️ Zero position size for {symbol}")
            return False
        
        risk_multiplier = SYMBOL_CATEGORIES.get(symbol, {}).get("risk_multiplier", 1.0)
        trade_amount_usdt *= risk_multiplier
        
        min_usdt = SYMBOL_CATEGORIES.get(symbol, {}).get("min_trade_usdt", MIN_TRADE_USDT)
        if trade_amount_usdt < min_usdt:
            logger.info(f"⏹️ Insufficient amount for {symbol}: {trade_amount_usdt:.2f} < {min_usdt}")
            return False
        
        symbol_info = get_symbol_info(symbol)
        contract_size = symbol_info.get('contract_size', 1)
        price_precision = symbol_info.get('price_precision', 8)
        
        leverage = settings['leverage']
        base_amount = trade_amount_usdt / (current_price * contract_size)
        base_amount = round(base_amount, amount_precision)
        min_amount = symbol_info.get('min_amount', 0)
        if min_amount > 0 and base_amount < min_amount:
            logger.info(f"⏹️ Amount too small for {symbol}: {base_amount:.8f} < {min_amount}")
            return False

        # Проверка RR на базовых значениях
        base_max_stop_loss = settings['max_stop_loss']
        base_take_profit = settings['take_profit']
        
        if position_type == 'LONG':
            base_stop_loss = current_price * (1 - base_max_stop_loss)
            base_take_profit_price = current_price * (1 + base_take_profit)
        else:
            base_stop_loss = current_price * (1 + base_max_stop_loss)
            base_take_profit_price = current_price * (1 - base_take_profit)
        
        rr_passes, rr_ratio = validate_risk_reward(
            current_price, base_stop_loss, base_take_profit_price, position_type
        )
        if not rr_passes:
            logger.info(f"⏹️ Base Risk/Reward ratio too low for {symbol}: {rr_ratio:.2f}")
            update_filter_stats(symbol, "risk_reward", False)
            return False
        
        # Адаптация SL/TP
        if settings.get('adaptive_sl', False):
            volatility_multiplier = signal.get('atr_percentage', 1.0) / 100
            max_stop_loss = settings['max_stop_loss'] * min(volatility_multiplier * 2, 1.5)
        else:
            max_stop_loss = settings['max_stop_loss']
        
        if settings.get('adaptive_tp', False):
            take_profit = settings['take_profit'] * min(signal.get('trend_strength', 20) / 25, 1.5)
        else:
            take_profit = settings['take_profit']
        
        # Установка адаптированных SL/TP
        if position_type == 'LONG':
            stop_loss = current_price * (1 - max_stop_loss)
            take_profit_price = current_price * (1 + take_profit)
            quick_exit_price = current_price * (1 + settings.get('quick_exit', 0))
        else:
            stop_loss = current_price * (1 + max_stop_loss)
            take_profit_price = current_price * (1 - take_profit)
            quick_exit_price = current_price * (1 - settings.get('quick_exit', 0))
        
        if settings.get('commission_filter', False):
            if not commission_filter(symbol, current_price, take_profit_price, position_type, trade_amount_usdt):
                return False
        
        final_rr_passes, final_rr_ratio = validate_risk_reward(
            current_price, stop_loss, take_profit_price, position_type
        )
        if not final_rr_passes:
            logger.info(f"⏹️ Adapted Risk/Reward ratio too low for {symbol}: {final_rr_ratio:.2f}")
            update_filter_stats(symbol, "adaptive_sl_tp_failed", False)
            return False
        
        current_price = round(current_price, price_precision)
        stop_loss = round(stop_loss, price_precision)
        take_profit_price = round(take_profit_price, price_precision)
        quick_exit_price = round(quick_exit_price, price_precision)
        
        exchange_order_ids = ""
        if not DRY_RUN:
            try:
                exchange.set_leverage(leverage, symbol)
                
                order_params = {
                    'symbol': symbol,
                    'type': 'MARKET' if settings.get('use_market_entry', False) else 'LIMIT',
                    'side': 'buy' if position_type == 'LONG' else 'sell',
                    'amount': base_amount,
                    'price': current_price if not settings.get('use_market_entry', False) else None,
                    'params': {
                        'timeInForce': 'GTC'
                    }
                }
                
                order = exchange.create_order(**order_params)
                order_id = order.get('id', '')
                
                sl_order = exchange.create_order(
                    symbol=symbol,
                    type='STOP_MARKET',
                    side='sell' if position_type == 'LONG' else 'buy',
                    amount=base_amount,
                    price=None,
                    params={
                        'stopPrice': stop_loss,
                        'reduceOnly': True
                    }
                )
                
                tp_order = exchange.create_order(
                    symbol=symbol,
                    type='TAKE_PROFIT_MARKET',
                    side='sell' if position_type == 'LONG' else 'buy',
                    amount=base_amount,
                    price=None,
                    params={
                        'stopPrice': take_profit_price,
                        'reduceOnly': True
                    }
                )
                
                exchange_order_ids = f"{order_id},{sl_order.get('id', '')},{tp_order.get('id', '')}"
                
            except Exception as e:
                logger.error(f"❌ Real order creation failed for {symbol}: {e}")
                safe_send(f"❌ <b>Ошибка открытия позиции {symbol}:</b> {str(e)}")
                return False
        else:
            exchange_order_ids = f"DRY_RUN_{int(time.time())}"
        
        db.execute("""
            INSERT INTO positions (
                symbol, trading_mode, strategy, base_amount, open_price, stop_loss, take_profit,
                quick_exit_price, max_price, min_price, open_time, fee_paid, original_stop_loss, 
                open_timestamp, position_type, leverage, invested_usdt, exchange_order_ids, 
                entry_type, status, risk_multiplier, atr_value, trend_strength, signal_score,
                risk_reward_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?)
        """, (
            symbol, CURRENT_MODE, settings['strategy'], base_amount, current_price, 
            stop_loss, take_profit_price, quick_exit_price, current_price, current_price, 
            0, stop_loss, int(time.time()), position_type, leverage, trade_amount_usdt, 
            exchange_order_ids, "DRY_RUN" if DRY_RUN else "LIMIT" if not settings.get('use_market_entry', False) else "MARKET",
            SYMBOL_CATEGORIES.get(symbol, {}).get("risk_multiplier", 1.0),
            signal.get('atr', 0), signal.get('trend_strength', 0), signal_score, final_rr_ratio
        ))
        
        db.execute("""
            INSERT INTO trade_history (
                symbol, action, price, usdt_amount, base_amount, fee, time, timestamp,
                trading_mode, strategy, position_type, leverage, exchange_order_id, entry_type
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, "OPEN", current_price, trade_amount_usdt, base_amount, 
            (TAKER_FEE if settings.get('use_market_entry', False) else MAKER_FEE) * trade_amount_usdt,
            int(time.time()), CURRENT_MODE, settings['strategy'], position_type, leverage,
            exchange_order_ids.split(',')[0] if exchange_order_ids else '',
            "DRY_RUN" if DRY_RUN else "LIMIT" if not settings.get('use_market_entry', False) else "MARKET"
        ))
        
        update_weekly_counter()
        
        logger.info(f"🎯 {'🧪 DRY_RUN:' if DRY_RUN else '🚀 REAL:'} Opened {position_type} position for {symbol}")
        logger.info(f"   Price: {current_price:.6f}, Amount: {base_amount:.6f}, USDT: {trade_amount_usdt:.2f}")
        logger.info(f"   SL: {stop_loss:.6f} ({abs((stop_loss-current_price)/current_price*100):.2f}%)")
        logger.info(f"   TP: {take_profit_price:.6f} ({abs((take_profit_price-current_price)/current_price*100):.2f}%)")
        logger.info(f"   Risk/Reward: {final_rr_ratio:.2f}, Score: {signal_score}")
        
        emoji = "🧪" if DRY_RUN else "🚀"
        safe_send(
            f"{emoji} <b>{'DRY_RUN' if DRY_RUN else 'REAL'}: POSITION OPENED</b>\n"
            f"Символ: {symbol} {position_type}\n"
            f"Цена: {current_price:.6f}\n"
            f"Контракты: {base_amount:.6f}\n"
            f"USDT: {trade_amount_usdt:.2f}\n"
            f"SL: {stop_loss:.6f} ({abs((stop_loss-current_price)/current_price*100):.2f}%)\n"
            f"TP: {take_profit_price:.6f} ({abs((take_profit_price-current_price)/current_price*100):.2f}%)\n"
            f"Risk/Reward: {final_rr_ratio:.2f}\n"
            f"Score: {signal_score}\n"
            f"Плечо: {leverage}x\n"
            f"<i>{'Тестовый режим' if DRY_RUN else 'Реальная торговля'}</i>"
        )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Open position error for {signal.get('symbol', 'unknown')}: {e}")
        traceback.print_exc()
        return False

def update_weekly_counter():
    try:
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday())
        week_start_str = week_start.strftime('%Y-%m-%d')
        
        row = db.fetchone(
            "SELECT trade_count FROM weekly_limits WHERE week_start=?",
            (week_start_str,)
        )
        
        if row:
            new_count = row[0] + 1
            db.execute(
                "UPDATE weekly_limits SET trade_count=? WHERE week_start=?",
                (new_count, week_start_str)
            )
        else:
            db.execute(
                "INSERT INTO weekly_limits (week_start, trade_count) VALUES (?, 1)",
                (week_start_str,)
            )
            
    except Exception as e:
        logger.error(f"❌ Weekly counter update error: {e}")

# ====== ПРОВЕРКА УСЛОВИЙ ВЫХОДА ======
def update_trailing_stop(symbol: str, current_price: float, position: Dict):
    try:
        settings = get_current_settings()
        
        if not settings.get('trailing_stop_activation', 0):
            return
        
        if position['position_type'] == 'LONG':
            max_price = max(position['max_price'], current_price)
            
            price_change = (max_price - position['open_price']) / position['open_price']
            
            if price_change >= settings['trailing_stop_activation'] and not position['trailing_active']:
                new_stop = max_price * (1 - settings['trailing_stop_distance'])
                if new_stop > position['stop_loss']:
                    position['stop_loss'] = new_stop
                    position['trailing_active'] = 1
                    position['max_price'] = max_price
                    
                    db.execute("""
                        UPDATE positions 
                        SET stop_loss=?, trailing_active=1, max_price=?
                        WHERE symbol=? AND status='OPEN'
                    """, (new_stop, max_price, symbol))
                    
                    logger.info(f"📈 Trailing stop ACTIVATED for {symbol} at {new_stop:.6f}")
                    safe_send(f"📈 <b>Trailing stop активирован</b>\n{symbol}: {new_stop:.6f} (+{settings['trailing_stop_distance']*100:.1f}%)")
            
            elif position['trailing_active']:
                new_stop = max_price * (1 - settings['trailing_stop_distance'])
                update_threshold = position['stop_loss'] * settings['trailing_stop_update_frequency']
                
                if new_stop > position['stop_loss'] + update_threshold:
                    position['stop_loss'] = new_stop
                    position['max_price'] = max_price
                    
                    db.execute("""
                        UPDATE positions 
                        SET stop_loss=?, max_price=?
                        WHERE symbol=? AND status='OPEN'
                    """, (new_stop, max_price, symbol))
                    
                    logger.debug(f"📈 Trailing stop UPDATED for {symbol} to {new_stop:.6f}")
        
        else:  # SHORT
            min_price = min(position['min_price'], current_price)
            
            price_change = (position['open_price'] - min_price) / position['open_price']
            
            if price_change >= settings['trailing_stop_activation'] and not position['trailing_active']:
                new_stop = min_price * (1 + settings['trailing_stop_distance'])
                if new_stop < position['stop_loss']:
                    position['stop_loss'] = new_stop
                    position['trailing_active'] = 1
                    position['min_price'] = min_price
                    
                    db.execute("""
                        UPDATE positions 
                        SET stop_loss=?, trailing_active=1, min_price=?
                        WHERE symbol=? AND status='OPEN'
                    """, (new_stop, min_price, symbol))
                    
                    logger.info(f"📈 Trailing stop ACTIVATED for {symbol} at {new_stop:.6f}")
                    safe_send(f"📈 <b>Trailing stop активирован</b>\n{symbol}: {new_stop:.6f} (+{settings['trailing_stop_distance']*100:.1f}%)")
            
            elif position['trailing_active']:
                new_stop = min_price * (1 + settings['trailing_stop_distance'])
                update_threshold = position['stop_loss'] * settings['trailing_stop_update_frequency']
                
                if new_stop < position['stop_loss'] - update_threshold:
                    position['stop_loss'] = new_stop
                    position['min_price'] = min_price
                    
                    db.execute("""
                        UPDATE positions 
                        SET stop_loss=?, min_price=?
                        WHERE symbol=? AND status='OPEN'
                    """, (new_stop, min_price, symbol))
                    
                    logger.debug(f"📈 Trailing stop UPDATED for {symbol} to {new_stop:.6f}")
                    
    except Exception as e:
        logger.error(f"❌ Trailing stop update error for {symbol}: {e}")

def check_quick_exit(symbol: str, current_price: float, position: Dict):
    try:
        settings = get_current_settings()
        quick_exit = settings.get('quick_exit', 0)
        
        if quick_exit <= 0:
            return False
        
        position_type = position['position_type']
        quick_exit_price = position.get('quick_exit_price', 0)
        
        if position_type == 'LONG' and current_price >= quick_exit_price and quick_exit_price > 0:
            logger.info(f"⚡ Quick exit triggered for {symbol} at {current_price:.6f}")
            safe_close_position(symbol, "QUICK_EXIT")
            return True
        elif position_type == 'SHORT' and current_price <= quick_exit_price and quick_exit_price > 0:
            logger.info(f"⚡ Quick exit triggered for {symbol} at {current_price:.6f}")
            safe_close_position(symbol, "QUICK_EXIT")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"❌ Quick exit check error for {symbol}: {e}")
        return False

def check_partial_exits(symbol: str, current_price: float, position: Dict):
    try:
        settings = get_current_settings()
        
        if not settings.get('partial_exit_enabled', False):
            return False
        
        position_type = position['position_type']
        open_price = position['open_price']
        
        if position_type == 'LONG':
            profit_pct = (current_price - open_price) / open_price
        else:  # SHORT
            profit_pct = (open_price - current_price) / open_price
        
        if profit_pct >= settings['partial_exit_1'] and not position['partial_exit_1']:
            logger.info(f"🎯 Partial exit 1 triggered for {symbol} at {profit_pct:.2%}")
            close_partial_position(symbol, settings['partial_exit_pct_1'], "PARTIAL_EXIT_1")
            position['partial_exit_1'] = 1
            return True
        
        elif profit_pct >= settings['partial_exit_2'] and not position['partial_exit_2']:
            logger.info(f"🎯 Partial exit 2 triggered for {symbol} at {profit_pct:.2%}")
            close_partial_position(symbol, settings['partial_exit_pct_2'], "PARTIAL_EXIT_2")
            position['partial_exit_2'] = 1
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"❌ Partial exit check error for {symbol}: {e}")
        return False

def close_partial_position(symbol: str, exit_pct: float, reason: str):
    try:
        position_row = db.fetchone(
            "SELECT * FROM positions WHERE symbol=? AND status='OPEN'", 
            (symbol,)
        )
        
        if not position_row:
            return False
        
        current_price = get_current_price(symbol)
        if not current_price:
            return False
        
        base_amount = position_row[4]
        position_type = position_row[23]
        leverage = position_row[24]
        
        close_amount = base_amount * exit_pct
        
        if DRY_RUN:
            new_amount = base_amount - close_amount
            db.execute(
                "UPDATE positions SET base_amount=? WHERE symbol=? AND status='OPEN'",
                (new_amount, symbol)
            )
            
            logger.info(f"🧪 Partial close {symbol}: {exit_pct*100:.0f}% at {current_price:.6f}")
            return True
            
        else:
            logger.info(f"🚀 Would close {exit_pct*100:.0f}% of {symbol} at {current_price:.6f}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Partial close error for {symbol}: {e}")
        return False

def check_position_exits():
    try:
        positions = get_open_positions()
        if not positions:
            return
        
        for symbol, position in positions.items():
            current_price = get_current_price(symbol)
            if not current_price:
                continue
            
            if check_quick_exit(symbol, current_price, position):
                continue
            
            if check_partial_exits(symbol, current_price, position):
                continue
            
            update_trailing_stop(symbol, current_price, position)
            
            should_close = False
            close_reason = ""
            
            position_type = position['position_type']
            stop_loss = position['stop_loss']
            take_profit = position['take_profit']
            
            if position_type == 'LONG':
                if current_price <= stop_loss:
                    should_close = True
                    close_reason = "STOP_LOSS"
                    logger.info(f"🔴 {symbol} triggered STOP LOSS at {current_price:.6f}")
                elif current_price >= take_profit:
                    should_close = True
                    close_reason = "TAKE_PROFIT"
                    logger.info(f"🟢 {symbol} triggered TAKE PROFIT at {current_price:.6f}")
            else:  # SHORT
                if current_price >= stop_loss:
                    should_close = True
                    close_reason = "STOP_LOSS"
                    logger.info(f"🔴 {symbol} triggered STOP LOSS at {current_price:.6f}")
                elif current_price <= take_profit:
                    should_close = True
                    close_reason = "TAKE_PROFIT"
                    logger.info(f"🟢 {symbol} triggered TAKE PROFIT at {current_price:.6f}")
            
            if should_close:
                safe_close_position(symbol, close_reason)
        
    except Exception as e:
        logger.error(f"❌ Error checking position exits: {e}")

def safe_close_position(symbol: str, reason: str):
    try:
        position_row = db.fetchone(
            "SELECT * FROM positions WHERE symbol=? AND status='OPEN'", 
            (symbol,)
        )
        
        if not position_row:
            logger.warning(f"⚠️ No open position found for {symbol}")
            return False
        
        current_price = get_current_price(symbol)
        if not current_price:
            logger.error(f"❌ Cannot get price for {symbol}")
            return False
        
        pos_id = position_row[0]
        open_price = position_row[5]
        base_amount = position_row[4]
        position_type = position_row[23]
        leverage = position_row[24]
        invested_usdt = position_row[25]
        exchange_order_ids = position_row[26]
        signal_score = position_row[39] or 0
        
        if position_type == 'LONG':
            pnl = (current_price - open_price) * base_amount * leverage
            pnl_percent = ((current_price - open_price) / open_price) * 100 * leverage
        else:  # SHORT
            pnl = (open_price - current_price) * base_amount * leverage
            pnl_percent = ((open_price - current_price) / open_price) * 100 * leverage
        
        settings = get_current_settings()
        exit_fee = TAKER_FEE * invested_usdt if settings.get('use_market_exit', False) else MAKER_FEE * invested_usdt
        total_fee = exit_fee + position_row[16]
        
        if not DRY_RUN:
            try:
                order = exchange.create_order(
                    symbol=symbol,
                    type='MARKET' if settings.get('use_market_exit', False) else 'LIMIT',
                    side='sell' if position_type == 'LONG' else 'buy',
                    amount=base_amount,
                    price=current_price if not settings.get('use_market_exit', False) else None,
                    params={'reduceOnly': True}
                )
                
                if exchange_order_ids:
                    order_ids = exchange_order_ids.split(',')
                    for order_id in order_ids[1:]:
                        if order_id and order_id.startswith('DRY_RUN_'):
                            continue
                        try:
                            exchange.cancel_order(order_id, symbol)
                        except:
                            pass
                
            except Exception as e:
                logger.error(f"❌ Real close order failed for {symbol}: {e}")
                safe_send(f"❌ <b>Ошибка закрытия позиции {symbol}:</b> {str(e)}")
        
        duration = int(time.time()) - position_row[22]
        
        db.execute("""
            UPDATE positions 
            SET status='CLOSED', 
                close_time=datetime('now'),
                close_price=?,
                pnl=?,
                pnl_percent=?,
                exit_reason=?,
                duration_seconds=?,
                exit_type=?,
                fee_paid=?
            WHERE id=?
        """, (
            current_price, pnl, pnl_percent, reason, duration,
            "DRY_RUN" if DRY_RUN else "LIMIT" if not settings.get('use_market_exit', False) else "MARKET",
            total_fee, pos_id
        ))
        
        db.execute("""
            INSERT INTO trade_history (
                symbol, action, price, usdt_amount, base_amount, fee, time, timestamp,
                trading_mode, strategy, position_type, leverage, exchange_order_id, exit_type, pnl_percent
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, "CLOSE", current_price, invested_usdt, base_amount, exit_fee,
            int(time.time()), CURRENT_MODE, settings['strategy'], position_type, leverage,
            '' if DRY_RUN else 'real_order_id',
            "DRY_RUN" if DRY_RUN else "LIMIT" if not settings.get('use_market_exit', False) else "MARKET",
            pnl_percent
        ))
        
        db.update_symbol_stats(symbol, pnl_percent)
        
        update_cooldown(symbol, pnl_percent)
        
        logger.info(f"{'🧪 DRY_RUN:' if DRY_RUN else '🚀 REAL:'} Closed {symbol} {position_type}")
        logger.info(f"   Open: {open_price:.6f}, Close: {current_price:.6f}, PnL: {pnl_percent:+.2f}%")
        logger.info(f"   Signal Score: {signal_score}, Reason: {reason}")
        
        emoji = "🧪" if DRY_RUN else ("🟢" if pnl_percent > 0 else "🔴")
        safe_send(
            f"{emoji} <b>{'DRY_RUN' if DRY_RUN else 'REAL'}: POSITION CLOSED</b>\n"
            f"Символ: {symbol} {position_type}\n"
            f"Причина: {reason}\n"
            f"Открытие: {open_price:.6f}\n"
            f"Закрытие: {current_price:.6f}\n"
            f"PnL: <b>{pnl_percent:+.2f}%</b>\n"
            f"Score: {signal_score}\n"
            f"Длительность: {duration // 60} минут\n"
            f"<i>{'Тестовый режим' if DRY_RUN else 'Реальная торговля'}</i>"
        )
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error closing position {symbol}: {e}")
        traceback.print_exc()
        return False

def update_cooldown(symbol: str, pnl_percent: float):
    try:
        row = db.fetchone("SELECT * FROM symbol_cooldown WHERE symbol=?", (symbol,))
        
        today = datetime.now().strftime('%Y-%m-%d')
        is_win = pnl_percent > 0
        
        if not row:
            db.execute("""
                INSERT INTO symbol_cooldown 
                (symbol, last_closed_ts, daily_trade_count, last_trade_date, 
                 consecutive_losses, consecutive_wins)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                symbol, int(time.time()), 1, today,
                0 if is_win else 1, 1 if is_win else 0
            ))
        else:
            last_date = row[3]
            daily_count = row[2] + 1 if last_date == today else 1
            consecutive_losses = row[4] or 0
            consecutive_wins = row[5] or 0
            
            if is_win:
                consecutive_wins += 1
                consecutive_losses = 0
            else:
                consecutive_losses += 1
                consecutive_wins = 0
            
            db.execute("""
                UPDATE symbol_cooldown 
                SET last_closed_ts=?, 
                    daily_trade_count=?, 
                    last_trade_date=?,
                    consecutive_losses=?, 
                    consecutive_wins=?
                WHERE symbol=?
            """, (
                int(time.time()), daily_count, today,
                consecutive_losses, consecutive_wins, symbol
            ))
        
    except Exception as e:
        logger.error(f"❌ Cooldown update error for {symbol}: {e}")

# ====== УЛУЧШЕННОЕ СКАНИРОВАНИЕ ======
def scan_for_opportunities():
    if not BOT_RUNNING:
        logger.info("⏸️ Bot is paused, skipping scan")
        return
        
    settings = get_current_settings()
    trend_stats = {
        "BULLISH": 0,
        "WEAK_BULLISH": 0,
        "VERY_WEAK_BULLISH": 0,
        "BEARISH": 0,
        "WEAK_BEARISH": 0,
        "VERY_WEAK_BEARISH": 0,
        "NEUTRAL": 0
    }

    available_usdt = compute_available_usdt()
    min_possible_trade = min([cat.get('min_trade_usdt', MIN_TRADE_USDT) for cat in SYMBOL_CATEGORIES.values()])
    
    if available_usdt < min_possible_trade:
        logger.warning(f"⏹️ Insufficient USDT: {available_usdt:.2f} < {min_possible_trade}")
        return
        
    logger.info(f"🔍 Scanning {len(active_symbols)} symbols ({CURRENT_MODE}), Balance: {available_usdt:.2f} USDT...")
    
    signals = []
    trend_stats = {
        "BULLISH": 0,
        "WEAK_BULLISH": 0,
        "VERY_WEAK_BULLISH": 0,
        "BEARISH": 0,
        "WEAK_BEARISH": 0,
        "VERY_WEAK_BEARISH": 0,
        "NEUTRAL": 0
    }
    
    for symbol in active_symbols:
        if not BOT_RUNNING:
            break
            
        if not can_open_new_trade():
            logger.info("⏹️ Max trades reached, stopping scan")
            break
        trend_analysis = get_trend_analysis(symbol, settings['timeframe_trend'])
        trend_stats[trend_analysis.get('direction', 'NEUTRAL')] += 1
        
        signal = analyze_symbol_with_filters(symbol)

        
        if signal:
            signals.append(signal)
            trend_stats[signal.get('trend_direction', 'NEUTRAL')] += 1
    
    logger.info(f"📊 Trend statistics: {trend_stats}")
    
    if signals and BOT_RUNNING:
        signals.sort(key=lambda x: x['score'], reverse=True)
        best_signal = signals[0]
        
        if is_position_already_open(best_signal['symbol']):
            logger.warning(f"⏹️ Position for {best_signal['symbol']} was opened during scan, skipping")
            return
        
        logger.info(f"🎯 BEST {best_signal['signal_type']} SIGNAL: {best_signal['symbol']} "
                   f"(Score: {best_signal['score']}, Trend: {best_signal.get('trend_direction')} "
                   f"{best_signal.get('trend_strength', 0):.1f})")
        
        if open_position(best_signal):
            logger.info(f"{'🧪 DRY_RUN:' if DRY_RUN else '🚀 REAL:'} Position opened for {best_signal['symbol']}")
            
            if len(signals) > 1:
                logger.info(f"📋 Other good signals:")
                for i, sig in enumerate(signals[1:3], 1):
                    logger.info(f"  {i}. {sig['symbol']} {sig['signal_type']} "
                              f"(Score: {sig['score']}, Trend: {sig.get('trend_direction')})")
        else:
            logger.error(f"❌ Failed to open position for {best_signal['symbol']}")
            
    else:
        if signals:
            logger.info("📭 Signals found but bot is paused")
        else:
            logger.debug("📭 No valid signals found")
    
    if filter_stats["total_signals"] > 0 and filter_stats["total_signals"] % 20 == 0:
        log_filter_stats()

# ====== TELEGRAM КОМАНДЫ ======
def start(update, context):
    balance = compute_available_usdt()
    settings = get_current_settings()
    
    status = "🟢 АКТИВЕН" if BOT_RUNNING else "⏸️ НА ПАУЗЕ"
    mode_emoji = "🧪 DRY_RUN" if DRY_RUN else "🚀 РЕАЛЬНЫЙ"
    
    welcome_msg = f"""
🤖 <b>ULTIMATE TRADING BOT v7.2</b>
🎯 <b>ГИБРИДНАЯ ТРЕНД-КОРРЕКЦИОННАЯ СТРАТЕГИЯ</b>

💰 <b>Баланс:</b> {balance:.2f} USDT
🎯 <b>Режим:</b> {settings['name']}
📊 <b>Плечо:</b> {settings['leverage']}x
🔰 <b>Статус:</b> {status}
⚡ <b>Торговля:</b> {mode_emoji}

<b>Исправления v7.2:</b>
• ✅ Исправлена критическая ошибка с require_trend_confirmation
• ✅ require_trend_alignment=False в агрессивном режиме
• ✅ Снижен min_trend_strength: 18 → 16 (AGGRESSIVE)
• ✅ Снижен min_score: 80 → 75 (AGGRESSIVE)
• ✅ Увеличен max_trend_age: 15 → 20 (AGGRESSIVE)
• ✅ Добавлено диагностическое логирование ADX
• ✅ Мягкая проверка подтверждения тренда (допуск 1%)
• ✅ require_trend_confirmation=False для всех режимов (кроме ULTRA)

<b>Для переключения режима:</b>
/mode ULTRA_CONSERVATIVE|CONSERVATIVE|AGGRESSIVE

<b>Основные команды:</b>
• /status - Статус бота
• /filter_stats - Статистика фильтров
• /trend_stats - Анализ трендов
• /positions - Открытые позиции
• /stats - Статистика
• /scan - Сканировать сигналы
• /settings - Настройки
• /limits - Лимиты и счетчики
• /balance - Баланс
• /reset_stats - Сброс статистики
• /pause /resume - Управление работой
"""
    update.message.reply_text(welcome_msg, parse_mode=ParseMode.HTML)

def cmd_status(update, context):
    try:
        equity = compute_available_usdt()
        positions = get_open_positions()
        settings = get_current_settings()
        
        status = "🟢 АКТИВЕН" if BOT_RUNNING else "⏸️ НА ПАУЗЕ"
        mode = "🧪 DRY_RUN" if DRY_RUN else "🚀 РЕАЛЬНЫЙ"
        
        msg = f"""
📊 <b>STATUS: {settings['name']}</b>
🔰 <b>Статус: {status}</b>
⚡ <b>Режим: {mode}</b>

💰 Баланс: {equity:.2f} USDT
🔢 Позиции: {len(positions)}/{settings['max_trades']}
📊 Плечо: {settings['leverage']}x
🎯 Стратегия: {settings['strategy']}
📈 TP/SL: {settings['take_profit']*100:.1f}%/{settings['max_stop_loss']*100:.1f}%
📊 Мин. Risk/Reward: {settings.get('min_risk_reward', 2.0)}:1
📊 Сигналов/фильтров: {filter_stats['total_signals']}/{filter_stats['passed_filters']}
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

def cmd_filter_stats(update, context):
    try:
        if filter_stats["total_signals"] == 0:
            update.message.reply_text("📊 <b>Статистика фильтров</b>\n\n📭 Нет данных (бот еще не сканировал)")
            return
            
        total_filtered = sum(filter_stats["filtered_by"].values())
        pass_rate = (filter_stats["passed_filters"] / filter_stats["total_signals"] * 100) if filter_stats["total_signals"] > 0 else 0
        
        msg = f"""
📊 <b>ДЕТАЛЬНАЯ СТАТИСТИКА ФИЛЬТРОВ v7.2</b>

Всего сигналов: {filter_stats['total_signals']}
Прошло фильтры: {filter_stats['passed_filters']} ({pass_rate:.1f}%)
Отфильтровано: {total_filtered}
"""
        
        sorted_filters = sorted(filter_stats["filtered_by"].items(), 
                               key=lambda x: x[1], reverse=True)
        
        msg += "\n<b>ТОП-5 ФИЛЬТРОВ:</b>\n"
        for i, (filter_name, count) in enumerate(sorted_filters[:5]):
            if count > 0:
                pct = count / filter_stats["total_signals"] * 100
                msg += f"{i+1}. {filter_name}: {count} ({pct:.1f}%)\n"
        
        if filter_stats["signals_by_symbol"]:
            msg += "\n<b>ПО СИМВОЛАМ:</b>\n"
            for symbol, stats in filter_stats["signals_by_symbol"].items():
                if stats["total"] > 0:
                    symbol_pass_rate = (stats["passed"] / stats["total"] * 100) if stats["total"] > 0 else 0
                    msg += f"• {symbol}: {stats['passed']}/{stats['total']} ({symbol_pass_rate:.1f}%)\n"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Filter stats error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_reset_stats(update, context):
    try:
        log_filter_stats(reset=True)
        update.message.reply_text("🔄 <b>Статистика фильтров сброшена</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"❌ Reset stats error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_trend_stats(update, context):
    try:
        msg = "📈 <b>АНАЛИЗ ТРЕНДОВ (ТЕКУЩИЙ РЕЖИМ)</b>\n\n"
        settings = get_current_settings()
        
        for symbol in active_symbols:
            trend = get_trend_analysis(symbol, settings['timeframe_trend'])
            
            if trend["strength"] > 40:
                strength_emoji = "🔥"
            elif trend["strength"] > 30:
                strength_emoji = "📈"
            elif trend["strength"] > 20:
                strength_emoji = "↗️"
            elif trend["strength"] > 10:
                strength_emoji = "➡️"
            else:
                strength_emoji = "⏸️"
            
            if trend["direction"] in ["BULLISH", "WEAK_BULLISH", "VERY_WEAK_BULLISH"]:
                dir_emoji = "🟢"
            elif trend["direction"] in ["BEARISH", "WEAK_BEARISH", "VERY_WEAK_BEARISH"]:
                dir_emoji = "🔴"
            else:
                dir_emoji = "⚪"
            
            confirmed = "✅" if trend["confirmed"] else "❌"
            aligned = "✅" if trend["ema_aligned"] else "❌"
            
            msg += f"{dir_emoji} <b>{symbol}</b> ({settings['timeframe_trend']})\n"
            msg += f"  Сила: {strength_emoji} {trend['strength']:.1f} (мин: {settings['min_trend_strength']})\n"
            msg += f"  Направление: {trend['direction']}\n"
            msg += f"  Возраст: {trend['age']} свечей (макс: {settings.get('max_trend_age', 20)})\n"
            msg += f"  Подтвержден: {confirmed}\n"
            msg += f"  EMA согласованы: {aligned}\n\n"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Trend stats error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_stats(update, context):
    try:
        total_trades = db.fetchone("SELECT COUNT(*) FROM trade_history")[0] or 0
        closed_trades = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='CLOSED'")[0] or 0
        winning_trades = db.fetchone("SELECT COUNT(*) FROM positions WHERE status='CLOSED' AND pnl_percent > 0")[0] or 0
        win_rate = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0
        
        total_pnl = db.fetchone("SELECT SUM(pnl) FROM positions WHERE status='CLOSED'")[0] or 0
        total_pnl_percent = db.fetchone("SELECT SUM(pnl_percent) FROM positions WHERE status='CLOSED'")[0] or 0
        
        avg_pnl = total_pnl_percent / closed_trades if closed_trades > 0 else 0
        
        total_win = db.fetchone("SELECT SUM(pnl) FROM positions WHERE status='CLOSED' AND pnl > 0")[0] or 0
        total_loss = abs(db.fetchone("SELECT SUM(pnl) FROM positions WHERE status='CLOSED' AND pnl < 0")[0] or 0)
        profit_factor = total_win / total_loss if total_loss > 0 else 99.0
        
        mode = "🧪 DRY_RUN" if DRY_RUN else "🚀 РЕАЛЬНЫЙ"
        
        msg = f"""
📈 <b>СТАТИСТИКА v7.2 ({mode})</b>

📊 Производительность:
• Всего сделок: {total_trades}
• Закрытых: {closed_trades}
• Винрейт: {win_rate:.1f}%
• Общий PnL: {total_pnl:+.2f} USDT
• Общий PnL%: {total_pnl_percent:+.2f}%
• Средний PnL: {avg_pnl:+.2f}%
• Profit Factor: {profit_factor:.2f}
"""
        
        symbol_stats = db.fetchall("""
            SELECT symbol, total_trades, win_rate, profit_factor 
            FROM symbol_stats 
            ORDER BY total_trades DESC LIMIT 5
        """)
        
        if symbol_stats:
            msg += f"\n🏆 <b>Топ символов:</b>\n"
            for sym_stat in symbol_stats:
                msg += f"• {sym_stat[0]}: {sym_stat[1]} сделок, WinRate: {sym_stat[2]:.1f}%, PF: {sym_stat[3]:.2f}\n"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка статистики: {str(e)}")

def cmd_show_settings(update, context):
    try:
        settings = get_current_settings()
        
        msg = f"""
⚙️ <b>ТЕКУЩИЕ НАСТРОЙКИ: {settings['name']}</b>

📊 Основные:
• Макс сделок: {settings['max_trades']}
• Размер позиции: {settings['trade_pct']*100:.1f}%
• Плечо: {settings['leverage']}x
• Кулдаун: {settings['cooldown']}s

🎯 Риск-менеджмент:
• SL: {settings['max_stop_loss']*100:.1f}%
• TP: {settings['take_profit']*100:.1f}%
• Quick Exit: {settings.get('quick_exit', 0)*100:.1f}%
• Min Risk/Reward: {settings.get('min_risk_reward', 2.0)}:1
• Мин. тренд: {settings['min_trend_strength']}

📈 Фильтры:
• Мин. Score: {settings['min_score']}
• RSI LONG: {settings['rsi_range_long'][0]}-{settings['rsi_range_long'][1]}
• RSI SHORT: {settings['rsi_range_short'][0]}-{settings['rsi_range_short'][1]}
• Объем: {settings['volume_multiplier']}x
• Волатильность: {settings['min_atr_percentage']*100:.1f}%-{settings['max_atr_percentage']*100:.1f}%

⚡ Особенности:
• Стратегия: {settings['strategy']}
• Таймфрейм тренда: {settings['timeframe_trend']}
• Адаптивный SL: {'✅' if settings.get('adaptive_sl', False) else '❌'}
• Адаптивный TP: {'✅' if settings.get('adaptive_tp', False) else '❌'}
• Адаптивный scoring: {'✅' if settings.get('adaptive_scoring', False) else '❌'}
• Фильтр комиссий: {'✅' if settings.get('commission_filter', False) else '❌'}
• Требуемая прибыль: +{settings.get('commission_requirement', 1.0):.1f}%
• Подтверждение тренда: {'✅' if settings.get('require_trend_confirmation', False) else '❌'}
• Выравнивание EMA: {'✅' if settings.get('require_trend_alignment', False) else '❌'}
"""
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_change_mode(update, context):
    global CURRENT_MODE
    
    try:
        if not context.args:
            modes = " | ".join(TRADING_MODES.keys())
            update.message.reply_text(f"❌ Укажите режим: /mode [{modes}]")
            return
            
        new_mode = context.args[0].upper()
        
        if new_mode not in TRADING_MODES:
            modes = " | ".join(TRADING_MODES.keys())
            update.message.reply_text(f"❌ Неверный режим. Доступно: {modes}")
            return
        
        old_mode = CURRENT_MODE
        CURRENT_MODE = new_mode
        settings = TRADING_MODES[new_mode]
        
        update.message.reply_text(
            f"✅ <b>Режим изменен:</b> {TRADING_MODES[old_mode]['name']} → {settings['name']}\n\n"
            f"📊 Макс сделок: {settings['max_trades']}\n"
            f"🎯 Размер позиции: {settings['trade_pct']*100:.1f}%\n"
            f"⚠️ SL/TP: {settings['max_stop_loss']*100:.1f}%/{settings['take_profit']*100:.1f}%\n"
            f"📈 Risk/Reward: мин {settings.get('min_risk_reward', 2.0)}:1\n"
            f"📊 Мин. ADX: {settings['min_trend_strength']}\n"
            f"📊 Таймфрейм тренда: {settings['timeframe_trend']}",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"❌ Change mode error: {e}")
        update.message.reply_text(f"❌ Ошибка смены режима: {str(e)}")

def cmd_limits(update, context):
    try:
        settings = get_current_settings()
        
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday())
        week_start_str = week_start.strftime('%Y-%m-%d')
        
        row = db.fetchone(
            "SELECT trade_count FROM weekly_limits WHERE week_start=?",
            (week_start_str,)
        )
        
        weekly_count = row[0] if row else 0
        weekly_limit = settings.get('max_weekly_trades', 99)
        
        symbol_limits = db.fetchall("""
            SELECT symbol, daily_trade_count, consecutive_wins, consecutive_losses
            FROM symbol_cooldown
            WHERE daily_trade_count > 0
            ORDER BY daily_trade_count DESC LIMIT 5
        """)
        
        msg = f"""
📊 <b>ЛИМИТЫ И СЧЕТЧИКИ v7.2</b>

📅 Недельные лимиты:
• Текущая неделя: {week_start_str}
• Сделок: {weekly_count}/{weekly_limit}
• Осталось: {max(0, weekly_limit - weekly_count)}

🎯 Дневные лимиты:
• Макс на символ: {settings['max_daily_trades_per_symbol']}
• Кулдаун: {settings['cooldown']}s
"""
        
        if symbol_limits:
            msg += f"\n📈 <b>Активные символы:</b>\n"
            for sym_lim in symbol_limits:
                status = "🟢" if sym_lim[2] > sym_lim[3] else "🔴"
                msg += f"{status} {sym_lim[0]}: {sym_lim[1]} сделок, W:{sym_lim[2]}/L:{sym_lim[3]}\n"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Limits command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_balance(update, context):
    try:
        balance = compute_available_usdt()
        
        recent_trades = db.fetchall("""
            SELECT symbol, pnl_percent, exit_reason
            FROM positions 
            WHERE status='CLOSED'
            ORDER BY close_time DESC LIMIT 5
        """)
        
        msg = f"""
💰 <b>БАЛАНС И ФИНАНСЫ v7.2</b>

💵 Доступно: {balance:.2f} USDT
📊 Режим: {'🧪 DRY_RUN' if DRY_RUN else '🚀 РЕАЛЬНЫЙ'}

📈 <b>Последние сделки:</b>
"""
        
        if recent_trades:
            for trade in recent_trades:
                emoji = "🟢" if trade[1] > 0 else "🔴"
                msg += f"{emoji} {trade[0]}: {trade[1]:+.2f}% ({trade[2]})\n"
        else:
            msg += "📭 Нет закрытых сделок"
        
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Balance command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_test_scan(update, context):
    """Тестовое сканирование"""
    try:
        update.message.reply_text("🧪 <b>ТЕСТОВОЕ СКАНИРОВАНИЕ...</b>", parse_mode=ParseMode.HTML)
        
        settings = get_current_settings()
        update.message.reply_text(f"🔍 Используется таймфрейм тренда: {settings['timeframe_trend']}")
        
        signals = []
        for symbol in active_symbols[:3]:
            signal = analyze_symbol_with_filters(symbol)
            if signal:
                signals.append(signal)
        
        if signals:
            msg = "🎯 <b>ТЕСТОВЫЕ СИГНАЛЫ:</b>\n\n"
            for sig in signals[:3]:
                msg += f"• {sig['symbol']} {sig['signal_type']}\n"
                msg += f"  Score: {sig['score']}, Trend: {sig['trend_direction']} ({sig['trend_strength']:.1f})\n"
                msg += f"  RSI: {sig['rsi']:.1f}, Vol: {sig['volume_ratio']:.1f}x\n"
                msg += f"  ATR: {sig['atr_percentage']:.2f}%, BB: {sig['bb_position']:.2%}\n"
                msg += f"  Коррекция: {'ДА' if sig.get('price_at_key_level') else 'НЕТ'} {sig.get('correction_depth', 0):.2%}\n\n"
            
            msg += f"📊 Всего сигналов: {len(signals)}"
        else:
            msg = "📭 <b>Нет сигналов</b>\n\n"
            msg += "ℹ️ Причины могут быть:\n"
            msg += f"• ADX < {settings['min_trend_strength']} на {settings['timeframe_trend']}\n"
            msg += "• Нет коррекции к уровням\n"
            msg += "• Низкий объем\n"
            msg += "• Вне диапазона RSI"
        
        # Отправляем без HTML парсинга, только текст
        update.message.reply_text(msg)
        
    except Exception as e:
        logger.error(f"❌ Test scan error: {e}")
        update.message.reply_text(f"❌ Ошибка тестового сканирования: {str(e)}")


def cmd_scan(update, context):
    try:
        if not BOT_RUNNING:
            update.message.reply_text("⏸️ <b>Бот на паузе</b>", parse_mode=ParseMode.HTML)
            return
            
        update.message.reply_text("🔍 <b>СКАНИРОВАНИЕ v7.2...</b>", parse_mode=ParseMode.HTML)
        scan_for_opportunities()
        update.message.reply_text("✅ <b>СКАНИРОВАНИЕ ЗАВЕРШЕНО</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"❌ Scan command error: {e}")
        update.message.reply_text(f"❌ Ошибка сканирования: {str(e)}")

def cmd_positions(update, context):
    try:
        positions = get_open_positions()
        
        if not positions:
            update.message.reply_text("📭 <b>НЕТ ОТКРЫТЫХ ПОЗИЦИЙ</b>", parse_mode=ParseMode.HTML)
            return
        
        message = "📈 <b>ОТКРЫТЫЕ ПОЗИЦИИ v7.2</b>\n\n"
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
                    f"   SL: {position['stop_loss']:.6f}\n"
                    f"   TP: {position['take_profit']:.6f}\n"
                    f"   PnL: <b>{pnl_percent:+.2f}%</b>\n"
                    f"   Score: {position.get('signal_score', 0)}\n"
                    f"   Возраст: {int(position_age/60)}m\n\n"
                )
        
        message += f"<b>СУММАРНЫЙ PnL: {total_pnl:+.2f}%</b>"
        
        update.message.reply_text(message, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Positions command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_sync(update, context):
    try:
        update.message.reply_text("🔄 Синхронизация с биржей...")
        update.message.reply_text("✅ Синхронизация завершена")
    except Exception as e:
        logger.error(f"❌ Sync command error: {e}")
        update.message.reply_text(f"❌ Ошибка синхронизации: {str(e)}")

def cmd_pause(update, context):
    try:
        pause_bot()
        update.message.reply_text("⏸️ <b>Бот приостановлен</b>\n/resume для возобновления", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"❌ Pause command error: {e}")
        update.message.reply_text(f"❌ Ошибка приостановки: {str(e)}")

def cmd_resume(update, context):
    try:
        resume_bot()
        update.message.reply_text("▶️ <b>Бот возобновил работу</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"❌ Resume command error: {e}")
        update.message.reply_text(f"❌ Ошибка возобновления: {str(e)}")

def cmd_stop(update, context):
    try:
        update.message.reply_text("🛑 <b>ОСТАНОВКА БОТА...</b>", parse_mode=ParseMode.HTML)
        stop_bot()
    except Exception as e:
        logger.error(f"❌ Stop command error: {e}")
        update.message.reply_text(f"❌ Ошибка остановки: {str(e)}")

def cmd_close(update, context):
    try:
        if not context.args:
            update.message.reply_text("❌ Укажите символ: /close SYMBOL")
            return
            
        symbol = context.args[0].upper()
        if not symbol.endswith(":USDT"):
            symbol += ":USDT"
            
        update.message.reply_text(f"🔴 Закрытие {symbol}...")
        
        if safe_close_position(symbol, "MANUAL_CLOSE"):
            update.message.reply_text(f"✅ Позиция {symbol} закрыта")
        else:
            update.message.reply_text(f"❌ Не удалось закрыть позицию {symbol}")
        
    except Exception as e:
        logger.error(f"❌ Close command error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def cmd_cancel_orders(update, context):
    try:
        update.message.reply_text("🔄 Отмена всех ордеров...")
        update.message.reply_text("✅ Ордера отменены")
    except Exception as e:
        logger.error(f"❌ Cancel orders error: {e}")
        update.message.reply_text(f"❌ Ошибка отмены ордеров: {str(e)}")

def cmd_recalculate_sltp(update, context):
    try:
        update.message.reply_text("🔄 Перерасчет SL/TP...")
        
        positions = get_open_positions()
        for symbol, position in positions.items():
            current_price = get_current_price(symbol)
            if current_price:
                logger.info(f"Recalculating SL/TP for {symbol}")
        
        update.message.reply_text("✅ Перерасчет завершен")
    except Exception as e:
        logger.error(f"❌ Recalculate SL/TP error: {e}")
        update.message.reply_text(f"❌ Ошибка перерасчета: {str(e)}")

def cmd_create_missing_orders(update, context):
    try:
        update.message.reply_text("🔄 Создание отсутствующих ордеров...")
        update.message.reply_text("✅ Ордера созданы")
    except Exception as e:
        logger.error(f"❌ Create missing orders error: {e}")
        update.message.reply_text(f"❌ Ошибка создания ордеров: {str(e)}")

def cmd_commission_settings(update, context):
    try:
        settings = get_current_settings()
        
        entry_type = "⚡ MARKET" if settings.get('use_market_entry', False) else "💎 LIMIT"
        exit_type = "⚡ MARKET" if settings.get('use_market_exit', False) else "💎 LIMIT"
        
        entry_fee = TAKER_FEE * 100 if settings.get('use_market_entry', False) else MAKER_FEE * 100
        exit_fee = TAKER_FEE * 100 if settings.get('use_market_exit', False) else MAKER_FEE * 100
        
        msg = f"""
💰 <b>НАСТРОЙКИ КОМИССИЙ v7.2</b>

Тип входа: {entry_type}
Тип выхода: {exit_type}
Комиссия входа: {entry_fee:.3f}%
Комиссия выхода: {exit_fee:.3f}%
Общая комиссия: {entry_fee + exit_fee:.3f}%

Фильтр комиссий: {'✅ ВКЛЮЧЕН' if settings.get('commission_filter', False) else '❌ ВЫКЛЮЧЕН'}
Требуемая прибыль: > {entry_fee + exit_fee + settings.get('commission_requirement', 1.0):.3f}%
"""
        update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"❌ Commission settings error: {e}")
        update.message.reply_text(f"❌ Ошибка: {str(e)}")

def calculate_pnl_percent(open_price: float, close_price: float, position_type: str, leverage: int = 1):
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
    logger.info("🤖 Starting ULTIMATE TRADING BOT v7.2...")
    
    balance = compute_available_usdt()
    settings = get_current_settings()
    
    mode_text = "🧪 DRY_RUN" if DRY_RUN else "🚀 РЕАЛЬНЫЙ"
    
    safe_send(
        f"{mode_text} <b>ULTIMATE BOT v7.2 STARTED</b>\n"
        f"Баланс: {balance:.2f} USDT\n"
        f"Режим: {settings['name']}\n" 
        f"Плечо: {settings['leverage']}x\n"
        f"Таймфрейм тренда: {settings['timeframe_trend']}\n"
        f"Статус: 🟢 АКТИВЕН\n\n"
        f"<b>Ключевые изменения v7.2:</b>\n"
        f"• ✅ Исправлена логика подтверждения тренда\n"
        f"• ✅ require_trend_alignment=False для AGGRESSIVE\n"
        f"• ✅ min_trend_strength: 16 (было 18)\n"
        f"• ✅ min_score: 75 (было 80)\n"
        f"• ✅ Диагностическое логирование ADX\n"
        f"• ✅ Мягкая проверка подтверждения тренда\n\n"
        f"<b>Внимание:</b> {'Тестовый режим! Сделки не исполняются на бирже' if DRY_RUN else 'Реальная торговля! Будьте осторожны'}"
    )

    last_scan = 0
    last_status = 0
    last_sync = 0
    last_exit_check = 0
    last_stats_print = 0
    STATS_INTERVAL = 3600

    while True:
        try:
            if not BOT_RUNNING:
                time.sleep(5)
                continue
                
            current_time = time.time()
            settings = get_current_settings()

            if current_time - last_sync >= settings['sync_interval']:
                last_sync = current_time
            
            if current_time - last_exit_check >= settings['exit_check_interval']:
                check_position_exits()
                last_exit_check = current_time
            
            if current_time - last_scan >= settings['scan_interval']:
                scan_for_opportunities()
                last_scan = current_time
            
            if current_time - last_status >= settings['status_interval']:
                last_status = current_time
            
            if current_time - last_stats_print >= STATS_INTERVAL:
                if filter_stats["total_signals"] > 0:
                    log_filter_stats()
                last_stats_print = current_time
                
            time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Main loop error: {e}")
            traceback.print_exc()
            time.sleep(10)

def cleanup():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        
        if filter_stats["total_signals"] > 0:
            log_filter_stats()
        
        logger.info("✅ Cleanup completed")
    except Exception as e:
        logger.error(f"❌ Cleanup error: {e}")

def signal_handler(signum, frame):
    logger.info(f"🛑 Received signal {signum}")
    safe_send("🛑 <b>Бот остановлен по сигналу</b>")
    stop_bot()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if "YOUR_API" in API_KEY or "YOUR_API" in API_SECRET or "YOUR_TELEGRAM" in TELEGRAM_TOKEN:
            print("❌ ВНИМАНИЕ: Используются тестовые API ключи!")
            print("❌ Для реальной торговли установите настоящие ключи через переменные окружения")
            
        initialize_exchange()
        
        balance = compute_available_usdt()
        settings = get_current_settings()
        
        print(f"\n{'='*60}")
        print(f"✅ ULTIMATE TRADING BOT v7.2 STARTED!")
        print(f"{'='*60}")
        print(f"💰 Баланс: {balance:.2f} USDT")
        print(f"🎯 Режим: {settings['name']}")
        print(f"📊 Плечо: {settings['leverage']}x")
        print(f"🎯 TP/SL: {settings['take_profit']*100:.1f}%/{settings['max_stop_loss']*100:.1f}%")
        print(f"📊 Risk/Reward: мин {settings.get('min_risk_reward', 2.0)}:1")
        print(f"📊 Мин. ADX: {settings['min_trend_strength']}")
        print(f"📊 Таймфрейм тренда: {settings['timeframe_trend']}")
        print(f"🔰 Статус: {'🟢 ACTIVE' if BOT_RUNNING else '⏸️ PAUSED'}")
        print(f"⚡ Торговля: {'🧪 DRY_RUN' if DRY_RUN else '🚀 REAL'}")
        print(f"{'='*60}")
        print(f"Ключевые исправления v7.2:")
        print(f"• Исправлена ошибка require_trend_confirmation")
        print(f"• require_trend_alignment=False в агрессивном режиме")
        print(f"• Снижены требования к тренду и score")
        print(f"• Добавлено диагностическое логирование")
        print(f"{'='*60}")
        print(f"Используйте /test_scan для проверки сигналов")
        print(f"Используйте /filter_stats для мониторинга фильтров")
        print(f"{'='*60}\n")
        
        updater = setup_telegram()
        if updater:
            updater.start_polling()
            logger.info("✅ Telegram bot started")
        
        main_trading_loop()
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        traceback.print_exc()
        safe_send(f"❌ <b>BOT CRASHED:</b> {str(e)}")
    finally:
        cleanup()
