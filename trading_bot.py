#!/usr/bin/env python3
"""
KuCoin Futures Deploy Bot - Final Updated

Save as deploy_trading_bot.py
"""

import os
import time
import json
import traceback
import threading
import signal
import logging
from datetime import datetime, timedelta, timezone

import ccxt
import pandas as pd
import numpy as np
import requests

# -------------------------
# Logging & graceful stop
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fut-bot")
stop_event = threading.Event()

def _handle_sig(signum, frame):
    log.info(f"Received signal {signum}, shutting down gracefully...")
    stop_event.set()

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

# =========================
# CONFIG (env-driven)
# =========================
MODE = os.getenv("MODE", "paper").lower()                # "paper" or "live"
EXCHANGE_ID = os.getenv("EXCHANGE_ID", "kucoinfutures")
SYMBOLS = [s.strip() for s in os.getenv(
    "SYMBOLS",
    "ARB/USDT:USDT,LINK/USDT:USDT,SOL/USDT:USDT,ETH/USDT:USDT,BTC/USDT:USDT"
).split(",") if s.strip()]

ENTRY_TF = os.getenv("ENTRY_TF", "1h")
HTF = os.getenv("HTF", "4h")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "180"))

TOTAL_PORTFOLIO_CAPITAL = float(os.getenv("TOTAL_PORTFOLIO_CAPITAL", "10000"))
PER_COIN_ALLOCATION = float(os.getenv("PER_COIN_ALLOCATION", "0.20"))
PER_COIN_CAP_USD = TOTAL_PORTFOLIO_CAPITAL * PER_COIN_ALLOCATION

# strategy
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.02"))
RR_FIXED = float(os.getenv("RR_FIXED", "5.0"))
DYNAMIC_RR = os.getenv("DYNAMIC_RR", "true").lower() == "true"
MIN_RR = float(os.getenv("MIN_RR", "4.0"))
MAX_RR = float(os.getenv("MAX_RR", "6.0"))

ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_MULT_SL = float(os.getenv("ATR_MULT_SL", "1.5"))
USE_ATR_STOPS = os.getenv("USE_ATR_STOPS", "true").lower() == "true"
USE_H1_FILTER = os.getenv("USE_H1_FILTER", "true").lower() == "true"

# filters
USE_VOLUME_FILTER = os.getenv("USE_VOLUME_FILTER", "false").lower() == "true"
VOL_LOOKBACK = int(os.getenv("VOL_LOOKBACK", "20"))
VOL_MIN_RATIO = float(os.getenv("VOL_MIN_RATIO", "0.5"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "25"))
RSI_OVERBOUGHT = 100 - RSI_OVERSOLD
BIAS_CONFIRM_BEAR = int(os.getenv("BIAS_CONFIRM_BEAR", "2"))
COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "0.0"))

# risk/fees (KuCoin taker ~0.06% each side)
MAX_DRAWDOWN = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "100000"))  # base qty cap
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.0005"))    # 0.05%
FEE_RATE = float(os.getenv("FEE_RATE", "0.0006"))              # 0.06% per side
INCLUDE_FUNDING = os.getenv("INCLUDE_FUNDING", "true").lower() == "true"

# telegram
TELEGRAM_TOKEN_FUT = os.getenv("TELEGRAM_TOKEN_FUT", "")
TELEGRAM_CHAT_ID_FUT = os.getenv("TELEGRAM_CHAT_ID_FUT", "")

# kucoin futures keys (live only)
API_KEY = os.getenv("KUCOIN_API_KEY", "")
API_SECRET = os.getenv("KUCOIN_SECRET", "")
API_PASSPHRASE = os.getenv("KUCOIN_PASSPHRASE", "")

# scheduler
SEND_DAILY_SUMMARY = os.getenv("SEND_DAILY_SUMMARY", "true").lower() == "true"
SUMMARY_HOUR_IST = int(os.getenv("SUMMARY_HOUR", "20"))  # 8 PM IST default

SLEEP_CAP = int(os.getenv("SLEEP_CAP", "60"))  # cap huge sleeps
LOG_PREFIX = "[FUT-BOT]"

if MODE == "live":
    if not API_KEY or not API_SECRET or not API_PASSPHRASE:
        raise ValueError("Live mode requires KUCOIN_API_KEY, KUCOIN_SECRET, KUCOIN_PASSPHRASE")

# =========================
# TELEGRAM helpers
# =========================
def send_telegram_fut(msg: str):
    if not TELEGRAM_TOKEN_FUT or not TELEGRAM_CHAT_ID_FUT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN_FUT}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID_FUT, "text": msg},
            timeout=10
        )
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

# =========================
# EXCHANGE & DATA
# =========================
def get_exchange():
    cfg = {
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    }
    if MODE == "live":
        cfg.update({"apiKey": API_KEY, "secret": API_SECRET, "password": API_PASSPHRASE})
    return ccxt.kucoinfutures(cfg)

def timeframe_to_ms(tf: str) -> int:
    tf = (tf or "").strip().lower()
    if not tf:
        raise ValueError("Invalid timeframe")
    num = ''.join([c for c in tf if c.isdigit()])
    unit = ''.join([c for c in tf if c.isalpha()])
    if not num or not unit:
        raise ValueError(f"Invalid timeframe '{tf}'")
    n = int(num)
    units = {"m": 60_000, "min": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    if unit not in units:
        raise ValueError(f"Unsupported timeframe unit '{unit}' in '{tf}'")
    return n * units[unit]

def fetch_ohlcv_range(exchange, symbol, timeframe, since_ms, until_ms, limit=1500, pause=0.12):
    tf_ms = timeframe_to_ms(timeframe)
    out = []
    cursor = int(since_ms)
    last_newest = None

    while cursor < until_ms and not stop_event.is_set():
        try:
            batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
        except ccxt.RateLimitExceeded:
            time.sleep(1); continue
        except Exception as e:
            log.warning(f"[fetch_ohlcv_range] fetch error {symbol} {timeframe}: {e}")
            break

        if not batch:
            break

        out.extend(batch)

        newest = int(batch[-1][0])
        if last_newest is None or newest > last_newest:
            cursor = newest + 1
            last_newest = newest
        else:
            cursor = cursor + tf_ms

        if newest >= until_ms - tf_ms:
            break

        if pause:
            time.sleep(pause)

    # Ensure consistent empty DF shape with DatetimeIndex
    if not out:
        empty_idx = pd.DatetimeIndex([], name="timestamp")
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"], index=empty_idx)

    dedup = {}
    for r in out:
        ts = int(r[0])
        dedup[ts] = r
    rows = [dedup[k] for k in sorted(dedup.keys()) if since_ms <= k <= until_ms]

    df = pd.DataFrame(rows, columns=["timestamp", "Open", "High", "Low", "Close", "Volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
    df.set_index("timestamp", inplace=True)

    for c in ["Open", "High", "Low", "Close", "Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    return df

# funding rates
def fetch_funding_history(exchange, symbol, since_ms, until_ms):
    try:
        rates, cursor = [], int(since_ms)
        while cursor < until_ms and not stop_event.is_set():
            page = exchange.fetchFundingRateHistory(symbol, since=cursor, limit=1000)
            if not page:
                break
            rates += page
            newest = page[-1].get('timestamp') or page[-1].get('ts')
            if newest is None or newest <= cursor:
                break
            cursor = newest + 1
            time.sleep(0.1)
        if not rates:
            return None
        df = pd.DataFrame([
            {
                "ts": r.get("timestamp") or r.get("ts"),
                "rate": float(
                    r.get("fundingRate", 0.0) or
                    (r.get("info", {}).get("fundingRate", 0.0) if isinstance(r.get("info", {}), dict) else 0.0)
                ),
            }
            for r in rates
            if (r.get("timestamp") or r.get("ts")) is not None
        ])
        if df.empty:
            return None
        df["timestamp"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(None)
        df = df.drop(columns=["ts"]).set_index("timestamp").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df
    except Exception as e:
        log.warning(f"fetch_funding_history failed: {e}")
        return None

def align_funding_to_index(idx, funding_df):
    """Return a Series aligned to idx with funding rates (0.0 when missing)."""
    s = pd.Series(0.0, index=idx)
    if funding_df is None or funding_df.empty:
        return s
    fd = funding_df.copy()
    if fd.index.tz is not None:
        try:
            fd.index = fd.index.tz_convert(timezone.utc).tz_convert(None)
        except Exception:
            fd.index = fd.index.tz_localize(None)
    fd = fd.sort_index()

    for ts, row in fd.iterrows():
        try:
            j = s.index.searchsorted(ts)
            if j < len(s):
                s.iloc[j] = float(row["rate"])
        except Exception:
            continue
    return s

# =========================
# INDICATORS
# =========================
def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_atr(df, period=14):
    hl = df['High'] - df['Low']
    hc = (df['High'] - df['Close'].shift()).abs()
    lc = (df['Low'] - df['Close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def position_size_futures(price, sl, capital, risk_percent, max_trade_size):
    risk_per_trade = capital * risk_percent
    rpc = abs(price - sl)
    if rpc <= 0:
        return 0.0
    max_by_risk = (risk_per_trade / rpc)
    max_by_capital = capital / price if price > 0 else 0
    return max(min(max_by_risk, max_by_capital, max_trade_size / price), 0)

# =========================
# STATE & FILES
# =========================
def state_files_for_symbol(symbol: str):
    tag = "fut_" + symbol.replace("/", "_").replace(":", "_")
    return f"state_{tag}.json", f"{tag}_trades.csv"

def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            s = json.load(f)
        for k in ["entry_time", "last_processed_ts", "last_exit_time"]:
            if s.get(k):
                try:
                    s[k] = pd.to_datetime(s[k], utc=True).tz_convert(None)
                except Exception:
                    s[k] = pd.to_datetime(s[k])
        return s
    return {
        "capital": PER_COIN_CAP_USD,
        "position": 0,                 # 0 flat, 1 long, -1 short
        "entry_price": 0.0,
        "entry_sl": 0.0,
        "entry_tp": 0.0,
        "entry_time": None,
        "entry_size": 0.0,
        "peak_equity": PER_COIN_CAP_USD,
        "last_processed_ts": None,
        "last_exit_time": None,
        "bearish_count": 0
    }

def save_state(state_file, state):
    s = dict(state)
    for k in ["entry_time", "last_processed_ts", "last_exit_time"]:
        if s.get(k) is not None:
            try:
                s[k] = pd.to_datetime(s[k]).isoformat()
            except Exception:
                s[k] = str(s[k])
    with open(state_file, "w") as f:
        json.dump(s, f, indent=2)

def append_trade(csv_file, row):
    write_header = not os.path.exists(csv_file)
    pd.DataFrame([row]).to_csv(csv_file, mode="a", header=write_header, index=False)

# =========================
# LIVE ORDER HELPERS (used only if MODE=live)
# =========================
def place_market(exchange, symbol, side, amount, reduce_only=False):
    params = {"reduceOnly": True} if reduce_only else {}
    return exchange.create_order(symbol, type="market", side=side, amount=amount, params=params)

def avg_fill_price(order):
    p = order.get("average") or order.get("price")
    if p: return float(p)
    if "trades" in order and order["trades"]:
        notional = 0.0; qty = 0.0
        for t in order["trades"]:
            pr = float(t["price"]); am = float(t["amount"])
            notional += pr*am; qty += am
        if qty > 0: return notional / qty
    return None

def amount_to_precision(exchange, symbol, amt):
    try:
        return float(exchange.amount_to_precision(symbol, amt))
    except Exception:
        return float(amt)

# =========================
# CORE PER-BAR PROCESSOR
# =========================
def process_bar(symbol, h1, h4, state, exchange=None, funding_series=None):
    # input validation
    assert isinstance(h1.index, pd.DatetimeIndex), "h1 must have DatetimeIndex"

    h1 = h1.copy(); h4 = h4.copy()
    # indicators already calculated in worker but keep local defensive calc
    if 'Bias' not in h1.columns:
        h1['Bias'] = 0
        h1.loc[h1['Close'] > h1['Close'].shift(1), 'Bias'] = 1
        h1.loc[h1['Close'] < h1['Close'].shift(1), 'Bias'] = -1

    if 'Trend' not in h4.columns:
        h4['Trend'] = 0
        h4.loc[h4['Close'] > h4['Close'].shift(1), 'Trend'] = 1
        h4.loc[h4['Close'] < h4['Close'].shift(1), 'Trend'] = -1

    h1['H4_Trend'] = h4['Trend'].reindex(h1.index, method='ffill').fillna(0).astype(int)

    if USE_ATR_STOPS and 'ATR' not in h1.columns:
        h1['ATR'] = calculate_atr(h1, ATR_PERIOD)
    if USE_VOLUME_FILTER and 'Avg_Volume' not in h1.columns:
        h1['Avg_Volume'] = h1['Volume'].rolling(VOL_LOOKBACK).mean()
    if 'RSI' not in h1.columns:
        h1['RSI'] = calculate_rsi(h1['Close'], RSI_PERIOD)

    i = len(h1) - 1
    curr = h1.iloc[i]
    prev_close = h1['Close'].iloc[i-1] if i >= 1 else curr['Close']
    price = float(curr['Close']); open_price = float(curr['Open'])
    bias = int(curr['Bias']); h4t = int(curr['H4_Trend'])
    ts = h1.index[i]

    # funding impact at settlement bar
    if INCLUDE_FUNDING and state.get("position", 0) != 0 and funding_series is not None:
        try:
            rate = float(funding_series.iloc[i]) if i < len(funding_series) else 0.0
        except Exception:
            rate = 0.0
        if rate != 0.0 and state.get("entry_price") and state.get("entry_size"):
            notional = abs(state["entry_price"] * state["entry_size"])
            # longs pay when rate>0; shorts receive; apply negative when paid
            fee = -notional * rate if state["position"] == 1 else notional * rate
            state["capital"] += fee

    # drawdown stop: update peak BEFORE check (match live logic)
    state["peak_equity"] = max(state.get("peak_equity", state.get("capital", PER_COIN_CAP_USD)), state.get("capital", PER_COIN_CAP_USD))
    dd = (state["peak_equity"] - state.get("capital", 0.0)) / state["peak_equity"] if state.get("peak_equity", 0) > 0 else 0.0
    if dd >= MAX_DRAWDOWN and state.get("position", 0) != 0:
        # force exit
        side = "sell" if state["position"] == 1 else "buy"
        exit_price = price
        if MODE == "live" and exchange is not None:
            try:
                order = place_market(exchange, symbol, side, amount_to_precision(exchange, symbol, state["entry_size"]), reduce_only=True)
                exit_price = float(avg_fill_price(order) or price)
            except Exception as e:
                send_telegram_fut(f"❌ {symbol} forced-exit failed: {e}")
        gross = state["entry_size"] * (exit_price - state["entry_price"]) * (1 if state["position"]==1 else -1)
        pos_val = abs(exit_price * state["entry_size"]) if state.get("entry_size") else 0.0
        pnl = gross - pos_val*SLIPPAGE_RATE - pos_val*FEE_RATE
        state["capital"] += pnl
        row = {
            "Symbol": symbol, "Entry_DateTime": state.get("entry_time"),
            "Exit_DateTime": ts, "Position": "Long" if state.get("position") == 1 else "Short",
            "Entry_Price": round(state.get("entry_price",0.0),6), "Exit_Price": round(exit_price,6),
            "Take_Profit": round(state.get("entry_tp",0.0),6), "Stop_Loss": round(state.get("entry_sl",0.0),6),
            "Position_Size_Base": round(state.get("entry_size",0.0),8),
            "PnL_$": round(pnl,2), "Win": 1 if pnl>0 else 0,
            "Exit_Reason": "MAX DRAWDOWN", "Capital_After": round(state["capital"],2), "Mode": MODE
        }
        state.update({"position":0,"entry_price":0.0,"entry_sl":0.0,"entry_tp":0.0,"entry_time":None,"entry_size":0.0,"bearish_count":0})
        state["last_exit_time"] = ts
        return state, row

    trade_row = None

    # ===== EXIT LOGIC =====
    if state.get("position", 0) != 0:
        exit_flag = False; exit_reason = ""; exit_price = price
        if state["position"] == 1:
            if price >= state["entry_tp"]:
                exit_flag, exit_price, exit_reason = True, state["entry_tp"], "Take Profit"
                state["bearish_count"] = 0
            elif price <= state["entry_sl"]:
                exit_flag, exit_price, exit_reason = True, state["entry_sl"], "Stop Loss"
                state["bearish_count"] = 0
            elif USE_H1_FILTER and (h4t < 0 and bias < 0):
                exit_flag, exit_price, exit_reason = True, price, "4H Trend Reversal"
            elif bias < 0:
                state["bearish_count"] += 1
                if state["bearish_count"] >= BIAS_CONFIRM_BEAR:
                    exit_flag, exit_price, exit_reason = True, price, "Bias Reversal"
                    state["bearish_count"] = 0
            else:
                state["bearish_count"] = 0
        else:
            if price <= state["entry_tp"]:
                exit_flag, exit_price, exit_reason = True, state["entry_tp"], "Take Profit"
                state["bearish_count"] = 0
            elif price >= state["entry_sl"]:
                exit_flag, exit_price, exit_reason = True, state["entry_sl"], "Stop Loss"
                state["bearish_count"] = 0
            elif USE_H1_FILTER and (h4t > 0 and bias > 0):
                exit_flag, exit_price, exit_reason = True, price, "4H Trend Reversal"
            elif bias > 0:
                state["bearish_count"] += 1
                if state["bearish_count"] >= BIAS_CONFIRM_BEAR:
                    exit_flag, exit_price, exit_reason = True, price, "Bias Reversal"
                    state["bearish_count"] = 0
            else:
                state["bearish_count"] = 0

        if exit_flag:
            side = "sell" if state["position"]==1 else "buy"
            if MODE == "live" and exchange is not None:
                try:
                    order = place_market(exchange, symbol, side, amount_to_precision(exchange, symbol, state["entry_size"]), reduce_only=True)
                    exit_price = float(avg_fill_price(order) or price)
                except Exception as e:
                    send_telegram_fut(f"❌ {symbol} exit failed: {e}")

            gross = state["entry_size"] * (exit_price - state["entry_price"]) * (1 if state["position"]==1 else -1)
            pos_val = abs(exit_price * state["entry_size"]) if state.get("entry_size") else 0.0
            pnl = gross - pos_val*SLIPPAGE_RATE - pos_val*FEE_RATE
            state["capital"] += pnl

            trade_row = {
                "Symbol": symbol, "Entry_DateTime": state.get("entry_time"),
                "Exit_DateTime": ts, "Position": "Long" if state["position"]==1 else "Short",
                "Entry_Price": round(state.get("entry_price",0.0),6), "Exit_Price": round(exit_price,6),
                "Take_Profit": round(state.get("entry_tp",0.0),6), "Stop_Loss": round(state.get("entry_sl",0.0),6),
                "Position_Size_Base": round(state.get("entry_size",0.0),8),
                "PnL_$": round(pnl,2), "Win": 1 if pnl>0 else 0,
                "Exit_Reason": exit_reason, "Capital_After": round(state["capital"],2), "Mode": MODE
            }
            state.update({"position":0,"entry_price":0.0,"entry_sl":0.0,"entry_tp":0.0,"entry_time":None,"entry_size":0.0})
            state["last_exit_time"] = ts

            emoji = "💚" if pnl>0 else "❤️"
            send_telegram_fut(f"{emoji} EXIT {symbol} {exit_reason} @ {exit_price:.4f} | PnL ${pnl:.2f}")

    # ===== ENTRY LOGIC (mirror long/short) =====
    if state.get("position", 0) == 0:
        # cooldown
        if COOLDOWN_HOURS>0 and state.get("last_exit_time") is not None:
            try:
                if (ts - state["last_exit_time"]).total_seconds()/3600 < COOLDOWN_HOURS:
                    state["last_processed_ts"] = ts
                    return state, trade_row
            except Exception:
                pass

        bullish_sweep = (price > open_price) and (price > prev_close)
        bearish_sweep = (price < open_price) and (price < prev_close)

        vol_ok_long = True
        vol_ok_short = True
        if USE_VOLUME_FILTER and not pd.isna(h1['Volume'].iloc[i]):
            avgv = h1['Volume'].rolling(VOL_LOOKBACK).mean().iloc[i]
            if not pd.isna(avgv):
                vol_ok_long = h1['Volume'].iloc[i] >= VOL_MIN_RATIO * avgv
                vol_ok_short = vol_ok_long

        rsi = float(h1['RSI'].iloc[i]) if not pd.isna(h1['RSI'].iloc[i]) else None
        rsi_ok_long = True if rsi is None else rsi > RSI_OVERSOLD
        rsi_ok_short = True if rsi is None else rsi < RSI_OVERBOUGHT

        long_ok  = bullish_sweep and vol_ok_long  and rsi_ok_long  and ((not USE_H1_FILTER) or h4t == 1)
        short_ok = bearish_sweep and vol_ok_short and rsi_ok_short and ((not USE_H1_FILTER) or h4t == -1)

        signal = 1 if long_ok else (-1 if short_ok else 0)

        if signal != 0 and (not USE_ATR_STOPS or (USE_ATR_STOPS and not pd.isna(h1['ATR'].iloc[i]) and h1['ATR'].iloc[i] > 0)):
            if signal == 1:
                sl = price - (ATR_MULT_SL * h1['ATR'].iloc[i]) if USE_ATR_STOPS else price * (1 - min(max(price*0.0005,0.0005),0.0015))
                risk = abs(price - sl)
                rr = RR_FIXED
                if DYNAMIC_RR and USE_ATR_STOPS and i >= 6:
                    recent = float(h1['ATR'].iloc[i-5:i].mean())
                    curr = float(h1['ATR'].iloc[i])
                    if recent > 0:
                        if curr > recent*1.2: rr = MIN_RR
                        elif curr < recent*0.8: rr = MAX_RR
                tp = price + rr * risk
            else:
                sl = price + (ATR_MULT_SL * h1['ATR'].iloc[i]) if USE_ATR_STOPS else price * (1 + min(max(price*0.0005,0.0005),0.0015))
                risk = abs(sl - price)
                rr = RR_FIXED
                if DYNAMIC_RR and USE_ATR_STOPS and i >= 6:
                    recent = float(h1['ATR'].iloc[i-5:i].mean())
                    curr = float(h1['ATR'].iloc[i])
                    if recent > 0:
                        if curr > recent*1.2: rr = MIN_RR
                        elif curr < recent*0.8: rr = MAX_RR
                tp = price - rr * risk

            if risk > 0:
                size = position_size_futures(price, sl, state["capital"], RISK_PERCENT, MAX_TRADE_SIZE)
                if size > 0:
                    entry_price_used = price
                    side = "buy" if signal==1 else "sell"
                    if MODE == "live" and exchange is not None:
                        try:
                            size = amount_to_precision(exchange, symbol, size)
                            order = place_market(exchange, symbol, side, size, reduce_only=False)
                            ep = avg_fill_price(order)
                            if ep is not None: entry_price_used = float(ep)
                        except Exception as e:
                            send_telegram_fut(f"❌ {symbol} entry failed: {e}")
                            state["last_processed_ts"] = ts
                            return state, trade_row

                    state["position"] = 1 if signal==1 else -1
                    state["entry_price"] = entry_price_used
                    state["entry_sl"] = sl
                    state["entry_tp"] = tp
                    state["entry_time"] = ts
                    state["entry_size"] = size
                    state["bearish_count"] = 0

                    pos_val = abs(entry_price_used * size)
                    state["capital"] -= pos_val*SLIPPAGE_RATE
                    state["capital"] -= pos_val*FEE_RATE

                    tag = "LONG" if signal==1 else "SHORT"
                    send_telegram_fut(f"🚀 ENTRY {symbol} {tag} @ {entry_price_used:.4f} | SL {sl:.4f} | TP {tp:.4f} | RR {rr:.1f}")

    state["last_processed_ts"] = ts
    state["peak_equity"] = max(state.get("peak_equity", state.get("capital", PER_COIN_CAP_USD)), state.get("capital", PER_COIN_CAP_USD))
    return state, trade_row

# =========================
# WORKER THREAD (one per symbol)
# =========================
def worker(symbol):
    state_file, trades_csv = state_files_for_symbol(symbol)
    exchange = get_exchange()
    state = load_state(state_file)

    send_telegram_fut(f"🤖 {symbol} FUTURES bot started | {ENTRY_TF}/{HTF} | cap ${state['capital']:.2f}")

    while not stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)
            since = now - timedelta(days=LOOKBACK_DAYS)
            since_ms = int(since.timestamp()*1000); until_ms = int(now.timestamp()*1000)

            h1 = fetch_ohlcv_range(exchange, symbol, ENTRY_TF, since_ms, until_ms)
            h4 = fetch_ohlcv_range(exchange, symbol, HTF, since_ms, until_ms)
            if h1.empty or h4.empty or len(h1) < 3:
                time.sleep(30); continue

            # act on last CLOSED bar (match backtest & deploy)
            # trim latest in-progress bar so we only process closed bars
            if len(h1) >= 2:
                h1_closed = h1.iloc[:-1].copy()
                h4_closed = h4.reindex(h1_closed.index, method='ffill').copy()
            else:
                time.sleep(30); continue

            closed_ts = h1_closed.index[-1]
            if state.get("last_processed_ts") is not None and pd.to_datetime(state.get("last_processed_ts")) >= closed_ts:
                time.sleep(30); continue

            funding_series = None
            if INCLUDE_FUNDING:
                fdf = fetch_funding_history(exchange, symbol, int(h1_closed.index[0].timestamp()*1000), int(h1_closed.index[-1].timestamp()*1000))
                funding_series = align_funding_to_index(h1_closed.index, fdf) if (fdf is not None and not fdf.empty) else pd.Series(0.0, index=h1_closed.index)

            # compute indicators (done here so both backtest & live aligned)
            h1_closed['Bias'] = 0
            h1_closed.loc[h1_closed['Close'] > h1_closed['Close'].shift(1), 'Bias'] = 1
            h1_closed.loc[h1_closed['Close'] < h1_closed['Close'].shift(1), 'Bias'] = -1

            h4_closed['Trend'] = 0
            h4_closed.loc[h4_closed['Close'] > h4_closed['Close'].shift(1), 'Trend'] = 1
            h4_closed.loc[h4_closed['Close'] < h4_closed['Close'].shift(1), 'Trend'] = -1

            h1_closed['H4_Trend'] = h4_closed['Trend'].reindex(h1_closed.index, method='ffill').fillna(0).astype(int)

            if USE_ATR_STOPS:
                h1_closed['ATR'] = calculate_atr(h1_closed, ATR_PERIOD)
            if USE_VOLUME_FILTER:
                h1_closed['Avg_Volume'] = h1_closed['Volume'].rolling(VOL_LOOKBACK).mean()
            h1_closed['RSI'] = calculate_rsi(h1_closed['Close'], RSI_PERIOD)

            state, trade = process_bar(symbol, h1_closed, h4_closed, state, exchange=exchange, funding_series=funding_series)
            if trade is not None:
                append_trade(trades_csv, trade)

            save_state(state_file, state)

            # sleep till just after next bar close
            next_close = h1.index[-1] + (h1.index[-1] - h1.index[-2])  # tz-naive
            now_utc = datetime.now(timezone.utc)
            if getattr(next_close, "tzinfo", None) is None:
                next_close = next_close.replace(tzinfo=timezone.utc)
            sleep_sec = (next_close - now_utc).total_seconds() + 5
            if sleep_sec < 10: sleep_sec = 10
            if sleep_sec > 3600: sleep_sec = SLEEP_CAP

            # allow stop_event wakeup
            slept = 0
            while slept < sleep_sec and not stop_event.is_set():
                time.sleep(min(5, sleep_sec - slept))
                slept += min(5, sleep_sec - slept)

        except ccxt.RateLimitExceeded:
            time.sleep(10)
        except Exception as e:
            msg = f"{LOG_PREFIX} {symbol} ERROR: {e}"
            log.exception(msg)
            send_telegram_fut(msg)
            time.sleep(60)

# =========================
# DAILY SUMMARY (IST 20:00)
# =========================
def ist_now():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def generate_daily_summary():
    try:
        now_ist = ist_now()
        start_ist = datetime(now_ist.year, now_ist.month, now_ist.day, 0, 0, 0, tzinfo=now_ist.tzinfo)
        end_ist   = datetime(now_ist.year, now_ist.month, now_ist.day, 23, 59, 59, tzinfo=now_ist.tzinfo)
        start_utc = start_ist.astimezone(timezone.utc).replace(tzinfo=None)  # tz-naive UTC
        end_utc   = end_ist.astimezone(timezone.utc).replace(tzinfo=None)

        lines = [f"📊 FUTURES DAILY SUMMARY — {now_ist.strftime('%Y-%m-%d %I:%M %p IST')}", "-"*60]
        total_cap, total_init, pnl_today, n_today, w_today = 0.0, 0.0, 0.0, 0, 0

        for sym in SYMBOLS:
            state_file, trades_csv = state_files_for_symbol(sym)
            state = load_state(state_file) if os.path.exists(state_file) else {"capital": PER_COIN_CAP_USD, "position":0}
            cap = float(state.get("capital", PER_COIN_CAP_USD))
            initial = PER_COIN_CAP_USD

            wins = losses = wr = 0.0
            pnl_all = 0.0
            n_trades_today = wins_today = 0
            pnl_today_sym = 0.0

            if os.path.exists(trades_csv):
                df = pd.read_csv(trades_csv)

                # ✅ SAFETY: handle empty CSV or missing/invalid timestamp column
                if df.empty or 'Exit_DateTime' not in df.columns:
                    today = df.iloc[0:0]
                else:
                    try:
                        df['Exit_DateTime'] = pd.to_datetime(df['Exit_DateTime'], utc=True).dt.tz_convert(None)
                        today = df[(df['Exit_DateTime'] >= start_utc) & (df['Exit_DateTime'] <= end_utc)]
                    except Exception:
                        today = df.iloc[0:0]

                n_trades_today = len(today)
                wins_today = int(today['Win'].sum()) if n_trades_today else 0
                pnl_today_sym = float(today['PnL_$'].sum()) if n_trades_today else 0.0

                if not df.empty:
                    pnl_all = float(df['PnL_$'].sum())
                    wins = int(df['Win'].sum()); losses = len(df)-wins
                    wr = (wins/len(df)*100) if len(df) else 0.0

            total_cap += cap; total_init += initial
            pnl_today += pnl_today_sym; n_today += n_trades_today; w_today += wins_today
            roi = ((cap/initial)-1)*100 if initial>0 else 0.0

            lines.append(f"{sym}: cap ${cap:,.2f} ({roi:+.2f}%) | today {n_trades_today} trades, {wins_today} wins | PnL ${pnl_today_sym:+.2f} | all WR {wr:.1f}%")

        port_roi = ((total_cap/total_init)-1)*100 if total_init>0 else 0.0
        wr_today = (w_today/n_today*100) if n_today>0 else 0.0
        lines += ["-"*60, f"TOTAL: cap ${total_cap:,.2f} ({port_roi:+.2f}%) | today {n_today} trades | WR {wr_today:.1f}% | PnL ${pnl_today:+.2f}"]
        msg = "\n".join(lines)
        log.info(msg)
        send_telegram_fut(msg)
    except Exception as e:
        send_telegram_fut(f"❌ summary error: {e}")

def summary_scheduler():
    last_sent_date = None
    while not stop_event.is_set():
        try:
            now = ist_now()
            if now.hour == SUMMARY_HOUR_IST and (last_sent_date is None or last_sent_date != now.date()):
                generate_daily_summary()
                last_sent_date = now.date()
            time.sleep(60)
        except Exception:
            time.sleep(60)

# =========================
# MAIN
# =========================
def main():
    boot = f"""
🚀 Futures Bot Started
Mode: {MODE.upper()}
Exchange: {EXCHANGE_ID} (perps)
Symbols: {", ".join(SYMBOLS)}
TF: {ENTRY_TF}/{HTF}
Cap/coin: ${PER_COIN_CAP_USD:,.2f}
Risk: {RISK_PERCENT*100:.1f}% | Fee: {FEE_RATE*100:.3f}% | Slippage: {SLIPPAGE_RATE*100:.3f}%
Funding: {"ON" if INCLUDE_FUNDING else "OFF"}
"""
    log.info(boot)
    send_telegram_fut(boot)

    threads = []
    for sym in SYMBOLS:
        t = threading.Thread(target=worker, args=(sym,), daemon=True)
        t.start(); threads.append(t)
        time.sleep(1)

    if SEND_DAILY_SUMMARY:
        s = threading.Thread(target=summary_scheduler, daemon=True)
        s.start(); threads.append(s)

    log.info(f"✅ Running {len(threads)} threads…")
    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()

    log.info("Shutting down threads…")
    time.sleep(1)

if __name__ == "__main__":
    main()
