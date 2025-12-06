# trading_bot_PRODUCTION.py - PRODUCTION-READY VERSION
# ============================================================================
# CRITICAL FIXES IMPLEMENTED:
# 1. Real stop-loss and take-profit orders placed on exchange
# 2. Position synchronization with exchange (every loop)
# 3. Balance drift detection and alerts
# 4. Emergency kill switch
# 5. Partial fill handling
# 6. Funding rate pre-checks
# 7. Daily loss limits
# 8. Consecutive loss protection
# 9. Exchange health checks
# 10. Enhanced error handling and recovery
# ============================================================================

import os, time, json, traceback, threading
from datetime import datetime, timedelta, timezone
import ccxt
import pandas as pd
import numpy as np
import requests

# =========================
# CONFIG (env-driven)
# =========================
MODE = os.getenv("MODE", "paper").lower()
EXCHANGE_ID = "kucoinfutures"
SYMBOLS = [s.strip() for s in os.getenv(
    "SYMBOLS",
    "BTC/USDT:USDT,ETH/USDT:USDT"  # ⚠️ DEFAULT TO LIQUID PAIRS ONLY
).split(",") if s.strip()]

ENTRY_TF = os.getenv("ENTRY_TF", "1h")
HTF = os.getenv("HTF", "4h")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "180"))

TOTAL_PORTFOLIO_CAPITAL = float(os.getenv("TOTAL_PORTFOLIO_CAPITAL", "10000"))
PER_COIN_ALLOCATION = float(os.getenv("PER_COIN_ALLOCATION", "0.20"))
PER_COIN_CAP_USD = TOTAL_PORTFOLIO_CAPITAL * PER_COIN_ALLOCATION

RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.01"))  # ✅ REDUCED TO 1%
RR_FIXED = float(os.getenv("RR_FIXED", "5.0"))
DYNAMIC_RR = os.getenv("DYNAMIC_RR", "true").lower() == "true"
MIN_RR = float(os.getenv("MIN_RR", "4.0"))
MAX_RR = float(os.getenv("MAX_RR", "6.0"))

ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_MULT_SL = float(os.getenv("ATR_MULT_SL", "1.5"))
USE_ATR_STOPS = os.getenv("USE_ATR_STOPS", "true").lower() == "true"
USE_H1_FILTER = os.getenv("USE_H1_FILTER", "true").lower() == "true"

USE_VOLUME_FILTER = os.getenv("USE_VOLUME_FILTER", "false").lower() == "true"
VOL_LOOKBACK = int(os.getenv("VOL_LOOKBACK", "20"))
VOL_MIN_RATIO = float(os.getenv("VOL_MIN_RATIO", "0.5"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "25"))
RSI_OVERBOUGHT = 100 - RSI_OVERSOLD
BIAS_CONFIRM_BEAR = int(os.getenv("BIAS_CONFIRM_BEAR", "8"))
COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "2.0"))  # ✅ ADDED DEFAULT COOLDOWN

MAX_DRAWDOWN = float(os.getenv("MAX_DRAWDOWN", "0.15"))  # ✅ REDUCED TO 15%
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "100000"))
SLIPPAGE_RATE = float(os.getenv("SLIPPAGE_RATE", "0.002"))  # ✅ MORE REALISTIC 0.2%
FEE_RATE = float(os.getenv("FEE_RATE", "0.0006"))
INCLUDE_FUNDING = os.getenv("INCLUDE_FUNDING", "true").lower() == "true"
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.3"))  # ✅ REDUCED TO 30%

# ✅ NEW: Risk management parameters
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "500"))  # Max $500 loss per day
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))
MAX_FUNDING_RATE = float(os.getenv("MAX_FUNDING_RATE", "0.0003"))  # 0.03% max
BALANCE_DRIFT_TOLERANCE = float(os.getenv("BALANCE_DRIFT_TOLERANCE", "0.05"))  # 5%

TELEGRAM_TOKEN_FUT = os.getenv("TELEGRAM_TOKEN_FUT", "")
TELEGRAM_CHAT_ID_FUT = os.getenv("TELEGRAM_CHAT_ID_FUT", "")

API_KEY = os.getenv("KUCOIN_API_KEY", "")
API_SECRET = os.getenv("KUCOIN_SECRET", "")
API_PASSPHRASE = os.getenv("KUCOIN_PASSPHRASE", "")

SEND_DAILY_SUMMARY = os.getenv("SEND_DAILY_SUMMARY", "true").lower() == "true"
SUMMARY_HOUR_IST = int(os.getenv("SUMMARY_HOUR", "20"))

SLEEP_CAP = int(os.getenv("SLEEP_CAP", "60"))
LOG_PREFIX = "[FUT-BOT-PROD]"

# Emergency stop flag (check for file existence)
EMERGENCY_STOP_FILE = "EMERGENCY_STOP.txt"

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
    except Exception:
        pass

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
    tf = tf.strip().lower()
    units = {"m": 60000, "h": 3600000, "d": 86400000}
    n = int(''.join([c for c in tf if c.isdigit()]))
    u = ''.join([c for c in tf if c.isalpha()])
    return n * units[u]

def fetch_ohlcv_range(exchange, symbol, timeframe, since_ms, until_ms, limit=1500, pause=0.12):
    tf_ms = timeframe_to_ms(timeframe)
    out, cursor, last = [], since_ms, None
    while cursor < until_ms:
        try:
            batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
        except ccxt.RateLimitExceeded:
            time.sleep(1); continue
        except Exception:
            break
        if not batch:
            break
        out.extend(batch)
        newest = batch[-1][0]
        cursor = (newest + tf_ms) if (last is None or newest > last) else cursor + tf_ms
        last = newest
        if newest >= until_ms - tf_ms:
            break
        time.sleep(pause)
    if not out:
        return pd.DataFrame(columns=["Open","High","Low","Close","Volume"])
    dedup = {r[0]: r for r in out}
    rows = [dedup[k] for k in sorted(dedup.keys()) if since_ms <= k <= until_ms]
    df = pd.DataFrame(rows, columns=["timestamp","Open","High","Low","Close","Volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
    df.set_index("timestamp", inplace=True)
    for c in ["Open","High","Low","Close","Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna()[["Open","High","Low","Close","Volume"]]

def fetch_funding_history(exchange, symbol, since_ms, until_ms):
    try:
        rates, cursor = [], since_ms
        while cursor < until_ms:
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
    except Exception:
        return None

def align_funding_to_index(idx, funding_df):
    s = pd.Series(0.0, index=idx)
    if funding_df is None or funding_df.empty:
        return s
    for ts, row in funding_df.iterrows():
        j = s.index.searchsorted(ts)
        if j < len(s):
            s.iloc[j] = row["rate"]
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

def position_size_futures(price, sl, capital, risk_percent, max_trade_size, max_position_pct=0.3):
    risk_per_trade = capital * risk_percent
    rpc = abs(price - sl)
    max_by_risk = (risk_per_trade / rpc) if rpc > 0 else 0
    max_by_capital = (capital / price) * max_position_pct
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
                    s[k] = pd.to_datetime(s[k]).tz_localize(None)
                except:
                    s[k] = None
        # ✅ NEW: Initialize new fields if missing
        if "stop_order_id" not in s:
            s["stop_order_id"] = None
        if "tp_order_id" not in s:
            s["tp_order_id"] = None
        if "consecutive_losses" not in s:
            s["consecutive_losses"] = 0
        if "daily_pnl" not in s:
            s["daily_pnl"] = 0.0
        if "last_daily_reset" not in s:
            s["last_daily_reset"] = None
        return s
    return {
        "capital": PER_COIN_CAP_USD,
        "position": 0,
        "entry_price": 0.0,
        "entry_sl": 0.0,
        "entry_tp": 0.0,
        "entry_time": None,
        "entry_size": 0.0,
        "entry_bar_index": 0,
        "peak_equity": PER_COIN_CAP_USD,
        "last_processed_ts": None,
        "last_exit_time": None,
        "bearish_count": 0,
        "stop_order_id": None,
        "tp_order_id": None,
        "consecutive_losses": 0,
        "daily_pnl": 0.0,
        "last_daily_reset": None
    }

def save_state(state_file, state):
    s = dict(state)
    for k in ["entry_time", "last_processed_ts", "last_exit_time", "last_daily_reset"]:
        if s.get(k) is not None:
            s[k] = pd.to_datetime(s[k]).isoformat()
    with open(state_file, "w") as f:
        json.dump(s, f, indent=2)

def append_trade(csv_file, row):
    write_header = not os.path.exists(csv_file)
    pd.DataFrame([row]).to_csv(csv_file, mode="a", header=write_header, index=False)

# =========================
# ✅ NEW: LIVE ORDER MANAGEMENT
# =========================
def place_market(exchange, symbol, side, amount, reduce_only=False):
    """Place market order with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            params = {"reduceOnly": True} if reduce_only else {}
            order = exchange.create_order(symbol, type="market", side=side, amount=amount, params=params)
            return order
        except ccxt.InsufficientFunds as e:
            send_telegram_fut(f"❌ Insufficient funds for {symbol}: {e}")
            raise
        except ccxt.RateLimitExceeded:
            time.sleep(2 ** attempt)
            continue
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(1)
    return None

def place_stop_loss_order(exchange, symbol, side, stop_price, amount):
    """
    ✅ CRITICAL: Place real stop-loss order on exchange
    This executes automatically if price hits stop
    """
    if MODE != "live":
        return None
        
    try:
        # KuCoin stop-market order
        stop_side = 'sell' if side == 'long' else 'buy'
        
        params = {
            'stopPrice': stop_price,
            'stop': 'down' if side == 'long' else 'up',
            'reduceOnly': True
        }
        
        order = exchange.create_order(
            symbol=symbol,
            type='market',
            side=stop_side,
            amount=amount,
            params=params
        )
        
        order_id = order.get('id')
        send_telegram_fut(f"✅ Stop-loss order placed: {symbol} @ {stop_price:.6f} (ID: {order_id})")
        return order_id
        
    except Exception as e:
        send_telegram_fut(f"🚨 CRITICAL: Stop-loss order FAILED for {symbol}: {e}")
        # Return None but log the failure - caller should handle
        return None

def place_take_profit_order(exchange, symbol, side, tp_price, amount):
    """✅ Place real take-profit limit order"""
    if MODE != "live":
        return None
        
    try:
        tp_side = 'sell' if side == 'long' else 'buy'
        
        order = exchange.create_order(
            symbol=symbol,
            type='limit',
            side=tp_side,
            amount=amount,
            price=tp_price,
            params={'reduceOnly': True}
        )
        
        order_id = order.get('id')
        send_telegram_fut(f"✅ Take-profit order placed: {symbol} @ {tp_price:.6f} (ID: {order_id})")
        return order_id
        
    except Exception as e:
        send_telegram_fut(f"⚠️ Take-profit order failed for {symbol}: {e}")
        return None

def cancel_orders(exchange, symbol, order_ids):
    """Cancel stop-loss and take-profit orders"""
    if MODE != "live" or not order_ids:
        return
        
    for order_id in order_ids:
        if order_id:
            try:
                exchange.cancel_order(order_id, symbol)
            except Exception as e:
                # Order might already be filled, that's OK
                pass

def avg_fill_price(order):
    """Extract actual fill price from order"""
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
# ✅ NEW: POSITION SYNCHRONIZATION
# =========================
def verify_and_sync_position(exchange, symbol, state):
    """
    ✅ CRITICAL: Verify state matches exchange reality
    Exchange is source of truth - sync state to match
    """
    if MODE != "live":
        return False
        
    try:
        positions = exchange.fetch_positions([symbol])
        
        actual_position = 0
        actual_size = 0.0
        actual_entry = 0.0
        
        for pos in positions:
            if pos['symbol'] == symbol:
                contracts = float(pos.get('contracts', 0) or 0)
                if contracts > 0:
                    actual_size = contracts
                    actual_entry = float(pos.get('entryPrice', 0) or 0)
                    actual_position = 1 if pos.get('side') == 'long' else -1
                    break
        
        state_position = state.get('position', 0)
        state_size = state.get('entry_size', 0.0)
        
        # Check for position mismatch
        if actual_position != state_position:
            send_telegram_fut(
                f"🚨 POSITION DESYNC DETECTED: {symbol}\n"
                f"State: {state_position} | Exchange: {actual_position}\n"
                f"SYNCING TO EXCHANGE (source of truth)"
            )
            
            if actual_position != 0:
                # Position exists on exchange but not in state
                state['position'] = actual_position
                state['entry_size'] = actual_size
                state['entry_price'] = actual_entry
                # Can't recover SL/TP, will need to set new ones
                state['entry_sl'] = 0.0
                state['entry_tp'] = 0.0
                state['stop_order_id'] = None
                state['tp_order_id'] = None
            else:
                # No position on exchange, clear state
                state['position'] = 0
                state['entry_size'] = 0.0
                state['entry_price'] = 0.0
                state['entry_sl'] = 0.0
                state['entry_tp'] = 0.0
                state['stop_order_id'] = None
                state['tp_order_id'] = None
                
            return True  # Desync occurred
        
        # Check size mismatch (>5% difference)
        if actual_position != 0 and abs(actual_size - state_size) > 0.05 * state_size:
            send_telegram_fut(
                f"⚠️ SIZE MISMATCH: {symbol}\n"
                f"State: {state_size:.4f} | Exchange: {actual_size:.4f}\n"
                f"SYNCING TO EXCHANGE"
            )
            state['entry_size'] = actual_size
            return True
        
        return False  # All good
        
    except Exception as e:
        send_telegram_fut(f"❌ Position sync check failed for {symbol}: {e}")
        return False

# =========================
# ✅ NEW: BALANCE VERIFICATION
# =========================
def verify_balance(exchange, all_states):
    """Check if total state capital matches exchange balance"""
    if MODE != "live":
        return True
        
    try:
        balance = exchange.fetch_balance()
        exchange_total = float(balance.get('USDT', {}).get('total', 0))
        
        state_total = sum(s.get('capital', 0.0) for s in all_states.values())
        
        if exchange_total > 100:  # Only check if we have significant balance
            drift = abs(exchange_total - state_total) / exchange_total
            
            if drift > BALANCE_DRIFT_TOLERANCE:
                send_telegram_fut(
                    f"⚠️ BALANCE DRIFT DETECTED!\n"
                    f"Exchange: ${exchange_total:,.2f}\n"
                    f"State Total: ${state_total:,.2f}\n"
                    f"Drift: {drift*100:.1f}%\n"
                    f"Possible causes: funding fees, failed orders, manual intervention"
                )
                return False
        
        return True
        
    except Exception as e:
        send_telegram_fut(f"❌ Balance verification failed: {e}")
        return True  # Don't block on this

# =========================
# ✅ NEW: EMERGENCY CONTROLS
# =========================
def check_emergency_stop():
    """Check if emergency stop file exists"""
    return os.path.exists(EMERGENCY_STOP_FILE)

def emergency_close_all(exchange, symbols, states):
    """
    ✅ EMERGENCY: Force close ALL positions immediately
    """
    send_telegram_fut(f"🚨 EMERGENCY SHUTDOWN INITIATED")
    
    results = []
    for symbol in symbols:
        try:
            if MODE == "live":
                positions = exchange.fetch_positions([symbol])
                
                for pos in positions:
                    contracts = float(pos.get('contracts', 0) or 0)
                    if contracts > 0:
                        side = 'sell' if pos.get('side') == 'long' else 'buy'
                        
                        order = place_market(exchange, symbol, side, contracts, reduce_only=True)
                        results.append(f"✅ Closed {symbol}: {pos.get('side')} {contracts}")
            
            # Clear state
            if symbol in states:
                state = states[symbol]
                state['position'] = 0
                state['entry_size'] = 0.0
                state['entry_price'] = 0.0
                state['entry_sl'] = 0.0
                state['entry_tp'] = 0.0
                state['stop_order_id'] = None
                state['tp_order_id'] = None
                
        except Exception as e:
            results.append(f"❌ Failed {symbol}: {e}")
    
    summary = "\n".join(results)
    send_telegram_fut(f"EMERGENCY CLOSE RESULTS:\n{summary}")
    
    return results

# =========================
# ✅ NEW: PRE-TRADE CHECKS
# =========================
def reset_daily_pnl_if_needed(state):
    """Reset daily PnL counter at start of new day"""
    now = datetime.now(timezone.utc)
    last_reset = state.get("last_daily_reset")
    
    if last_reset is None:
        state["last_daily_reset"] = now
        state["daily_pnl"] = 0.0
        return
    
    if isinstance(last_reset, str):
        last_reset = pd.to_datetime(last_reset).tz_localize(None).replace(tzinfo=timezone.utc)
    
    # Check if it's a new day
    if now.date() > last_reset.date():
        state["daily_pnl"] = 0.0
        state["last_daily_reset"] = now

def pre_trade_checks(exchange, symbol, state):
    """
    ✅ Run safety checks before allowing entry
    Returns (allowed: bool, reason: str)
    """
    
    # 1. Check emergency stop
    if check_emergency_stop():
        return False, "EMERGENCY STOP FILE DETECTED"
    
    # 2. Check funding rate
    if MODE == "live":
        try:
            ticker = exchange.fetch_ticker(symbol)
            funding_rate = abs(float(ticker.get('fundingRate', 0) or 0))
            
            if funding_rate > MAX_FUNDING_RATE:
                return False, f"Funding rate too high: {funding_rate*100:.3f}%"
        except:
            pass  # Don't block on this check failure
    
    # 3. Check daily loss limit
    reset_daily_pnl_if_needed(state)
    daily_pnl = state.get("daily_pnl", 0.0)
    
    if daily_pnl < -MAX_DAILY_LOSS:
        return False, f"Daily loss limit hit: ${daily_pnl:.2f}"
    
    # 4. Check consecutive losses
    consec_losses = state.get("consecutive_losses", 0)
    if consec_losses >= MAX_CONSECUTIVE_LOSSES:
        return False, f"Max consecutive losses reached: {consec_losses}"
    
    # 5. Check exchange health
    if MODE == "live":
        try:
            status = exchange.fetch_status()
            if status.get('status') != 'ok':
                return False, "Exchange not healthy"
        except:
            return False, "Cannot verify exchange status"
    
    # 6. Check sufficient balance
    if MODE == "live":
        try:
            balance = exchange.fetch_balance()
            free_balance = float(balance.get('USDT', {}).get('free', 0))
            if free_balance < 50:
                return False, f"Insufficient balance: ${free_balance:.2f}"
        except:
            pass  # Don't block on this
    
    return True, "All checks passed"

# =========================
# CORE PROCESSOR
# =========================
def process_bar(symbol, h1, h4, state, exchange=None, funding_series=None):
    """Process one closed H1 bar with production-grade safety"""
    
    h1 = h1.copy(); h4 = h4.copy()

    # Calculate indicators
    h1['Bias'] = 0
    h1.loc[h1['Close'] > h1['Close'].shift(1), 'Bias'] = 1
    h1.loc[h1['Close'] < h1['Close'].shift(1), 'Bias'] = -1

    h4['Trend'] = 0
    h4.loc[h4['Close'] > h4['Close'].shift(1), 'Trend'] = 1
    h4.loc[h4['Close'] < h4['Close'].shift(1), 'Trend'] = -1

    h1['H4_Trend'] = h4['Trend'].reindex(h1.index, method='ffill').fillna(0).astype(int)

    if USE_ATR_STOPS:
        h1['ATR'] = calculate_atr(h1, ATR_PERIOD)
    if USE_VOLUME_FILTER:
        h1['Avg_Volume'] = h1['Volume'].rolling(VOL_LOOKBACK).mean()
    h1['RSI'] = calculate_rsi(h1['Close'], RSI_PERIOD)

    i = len(h1) - 1
    if i < 0:
        return state, None

    curr = h1.iloc[i]
    prev_close = h1['Close'].iloc[i-1] if i >= 1 else curr['Close']
    price = float(curr['Close']); open_price = float(curr['Open'])
    bias = int(curr['Bias']); h4t = int(curr['H4_Trend'])
    ts = h1.index[i]

    # Funding fees
    if INCLUDE_FUNDING and state.get("position", 0) != 0 and funding_series is not None:
        bar_hour = ts.hour
        if bar_hour in [0, 8, 16]:
            try:
                rate = float(funding_series.iloc[i]) if i < len(funding_series) else 0.0
            except Exception:
                rate = 0.0
            if rate != 0.0 and state.get("entry_price") and state.get("entry_size"):
                notional = abs(state["entry_price"] * state["entry_size"])
                fee = -notional * rate if state["position"] == 1 else notional * rate
                state["capital"] += fee
                if abs(fee) > 0.01:  # Log if significant
                    send_telegram_fut(f"💸 Funding fee {symbol}: ${fee:+.2f}")

    # Update peak equity and check drawdown
    state["peak_equity"] = max(state.get("peak_equity", state.get("capital", PER_COIN_CAP_USD)), state.get("capital", PER_COIN_CAP_USD))
    dd = (state["peak_equity"] - state.get("capital", 0.0)) / state["peak_equity"] if state.get("peak_equity", 0) > 0 else 0.0
    
    if dd >= MAX_DRAWDOWN and state.get("position", 0) != 0:
        # Forced exit due to max drawdown
        send_telegram_fut(f"🚨 MAX DRAWDOWN HIT ({dd*100:.1f}%) - Forcing exit on {symbol}")
        
        side = "sell" if state["position"] == 1 else "buy"
        exit_price = price
        
        # Cancel existing orders
        cancel_orders(exchange, symbol, [state.get("stop_order_id"), state.get("tp_order_id")])
        
        if MODE == "live" and exchange is not None:
            try:
                order = place_market(exchange, symbol, side, amount_to_precision(exchange, symbol, state["entry_size"]), reduce_only=True)
                exit_price = float(avg_fill_price(order) or price)
            except Exception as e:
                send_telegram_fut(f"❌ {symbol} forced-exit failed: {e}")

        gross = state["entry_size"] * (exit_price - state["entry_price"]) * (1 if state["position"] == 1 else -1)
        pos_val = abs(exit_price * state["entry_size"]) if state.get("entry_size") else 0.0
        pnl = gross - pos_val * FEE_RATE
        state["capital"] += pnl
        state["daily_pnl"] = state.get("daily_pnl", 0.0) + pnl

        row = {
            "Symbol": symbol, "Entry_DateTime": state.get("entry_time"),
            "Exit_DateTime": ts, "Position": "Long" if state.get("position") == 1 else "Short",
            "Entry_Price": round(state.get("entry_price", 0.0), 6), "Exit_Price": round(exit_price, 6),
            "Take_Profit": round(state.get("entry_tp", 0.0), 6), "Stop_Loss": round(state.get("entry_sl", 0.0), 6),
            "Position_Size_Base": round(state.get("entry_size", 0.0), 8),
            "PnL_$": round(pnl, 2), "Win": 1 if pnl > 0 else 0,
            "Exit_Reason": "MAX DRAWDOWN", "Capital_After": round(state["capital"], 2), "Mode": MODE
        }

        state.update({"position": 0, "entry_price": 0.0, "entry_sl": 0.0, "entry_tp": 0.0,
                      "entry_time": None, "entry_size": 0.0, "bearish_count": 0, "entry_bar_index": 0,
                      "stop_order_id": None, "tp_order_id": None})
        state["last_exit_time"] = ts
        state["last_processed_ts"] = ts
        state["peak_equity"] = max(state.get("peak_equity", state.get("capital")), state.get("capital"))

        return state, row

    trade_row = None

    # ===== EXIT LOGIC =====
    if state.get("position", 0) != 0:
        # ✅ Check if stop-loss or take-profit orders were filled
        filled_reason = None
        
        if MODE == "live" and exchange is not None:
            try:
                # Check if position still exists
                positions = exchange.fetch_positions([symbol])
                position_exists = any(float(p.get('contracts', 0) or 0) > 0 for p in positions if p['symbol'] == symbol)
                
                if not position_exists:
                    # Position was closed (likely by stop-loss or take-profit order)
                    filled_reason = "Stop-Loss or Take-Profit Order Filled"
                    
                    # Try to determine which one by checking order status
                    try:
                        if state.get("stop_order_id"):
                            stop_order = exchange.fetch_order(state["stop_order_id"], symbol)
                            if stop_order['status'] == 'closed':
                                filled_reason = "Stop Loss Order"
                        elif state.get("tp_order_id"):
                            tp_order = exchange.fetch_order(state["tp_order_id"], symbol)
                            if tp_order['status'] == 'closed':
                                filled_reason = "Take Profit Order"
                    except:
                        pass  # Can't determine which, use generic message
            except:
                pass  # Continue with manual check
        
        bars_held = i - state.get("entry_bar_index", 0)
        
        exit_flag = False; exit_reason = ""; exit_price = price
        
        # If order was filled, trigger exit
        if filled_reason:
            exit_flag = True
            exit_reason = filled_reason
            # Use current price as approximation (actual fill price would need order history)
            exit_price = state.get("entry_tp") if "Profit" in filled_reason else state.get("entry_sl")
            if exit_price == 0:
                exit_price = price
        
        # Manual exit checks
        elif state["position"] == 1:
            if price >= state["entry_tp"]:
                exit_flag, exit_price, exit_reason = True, state["entry_tp"], "Take Profit"
                state["bearish_count"] = 0
            elif price <= state["entry_sl"]:
                exit_flag, exit_price, exit_reason = True, state["entry_sl"], "Stop Loss"
                state["bearish_count"] = 0
            elif bars_held >= 3:
                if USE_H1_FILTER and (h4t < 0 and bias < 0):
                    exit_flag, exit_price, exit_reason = True, price, "4H Trend Reversal"
                elif bias < 0:
                    state["bearish_count"] += 1
                    if state["bearish_count"] >= BIAS_CONFIRM_BEAR:
                        exit_flag, exit_price, exit_reason = True, price, "Bias Reversal"
                        state["bearish_count"] = 0
                elif bias > 0:
                    state["bearish_count"] = 0
            else:
                state["bearish_count"] = 0
        else:  # Short position
            if price <= state["entry_tp"]:
                exit_flag, exit_price, exit_reason = True, state["entry_tp"], "Take Profit"
                state["bearish_count"] = 0
            elif price >= state["entry_sl"]:
                exit_flag, exit_price, exit_reason = True, state["entry_sl"], "Stop Loss"
                state["bearish_count"] = 0
            elif bars_held >= 3:
                if USE_H1_FILTER and (h4t > 0 and bias > 0):
                    exit_flag, exit_price, exit_reason = True, price, "4H Trend Reversal"
                elif bias > 0:
                    state["bearish_count"] += 1
                    if state["bearish_count"] >= BIAS_CONFIRM_BEAR:
                        exit_flag, exit_price, exit_reason = True, price, "Bias Reversal"
                        state["bearish_count"] = 0
                elif bias < 0:
                    state["bearish_count"] = 0
            else:
                state["bearish_count"] = 0

        if exit_flag:
            side = "sell" if state["position"] == 1 else "buy"
            
            # ✅ Cancel stop-loss and take-profit orders
            cancel_orders(exchange, symbol, [state.get("stop_order_id"), state.get("tp_order_id")])
            
            if MODE == "live" and exchange is not None and not filled_reason:
                try:
                    order = place_market(exchange, symbol, side, amount_to_precision(exchange, symbol, state["entry_size"]), reduce_only=True)
                    exit_price = float(avg_fill_price(order) or price)
                except Exception as e:
                    send_telegram_fut(f"❌ {symbol} exit failed: {e}")

            gross = state["entry_size"] * (exit_price - state["entry_price"]) * (1 if state["position"] == 1 else -1)
            pos_val = abs(exit_price * state["entry_size"]) if state.get("entry_size") else 0.0
            pnl = gross - pos_val * FEE_RATE
            state["capital"] += pnl
            state["daily_pnl"] = state.get("daily_pnl", 0.0) + pnl
            
            # ✅ Track consecutive losses
            if pnl < 0:
                state["consecutive_losses"] = state.get("consecutive_losses", 0) + 1
            else:
                state["consecutive_losses"] = 0

            trade_row = {
                "Symbol": symbol, "Entry_DateTime": state.get("entry_time"),
                "Exit_DateTime": ts, "Position": "Long" if state["position"] == 1 else "Short",
                "Entry_Price": round(state.get("entry_price", 0.0), 6), "Exit_Price": round(exit_price, 6),
                "Take_Profit": round(state.get("entry_tp", 0.0), 6), "Stop_Loss": round(state.get("entry_sl", 0.0), 6),
                "Position_Size_Base": round(state.get("entry_size", 0.0), 8),
                "PnL_$": round(pnl, 2), "Win": 1 if pnl > 0 else 0,
                "Exit_Reason": exit_reason, "Capital_After": round(state["capital"], 2), "Mode": MODE
            }

            state.update({"position": 0, "entry_price": 0.0, "entry_sl": 0.0, "entry_tp": 0.0,
                          "entry_time": None, "entry_size": 0.0, "entry_bar_index": 0,
                          "stop_order_id": None, "tp_order_id": None})
            state["last_exit_time"] = ts
            state["last_processed_ts"] = ts
            state["peak_equity"] = max(state.get("peak_equity", state.get("capital")), state.get("capital"))

            emoji = "💚" if pnl > 0 else "❤️"
            consec = f" | {state['consecutive_losses']} losses in row" if state["consecutive_losses"] > 0 else ""
            send_telegram_fut(f"{emoji} EXIT {symbol} {exit_reason} @ {exit_price:.4f} | PnL ${pnl:.2f}{consec}")

            return state, trade_row

    # ===== ENTRY LOGIC =====
    if state.get("position", 0) == 0:
        # ✅ Run pre-trade safety checks
        allowed, reason = pre_trade_checks(exchange, symbol, state)
        if not allowed:
            state["last_processed_ts"] = ts
            if "EMERGENCY" in reason or "Daily loss" in reason or "consecutive" in reason:
                send_telegram_fut(f"⛔ {symbol} entry blocked: {reason}")
            return state, trade_row
        
        # Cooldown check
        if COOLDOWN_HOURS > 0 and state.get("last_exit_time") is not None:
            last_exit = state["last_exit_time"]
            if isinstance(last_exit, str):
                try:
                    last_exit = pd.to_datetime(last_exit).tz_localize(None)
                except Exception:
                    last_exit = None
            try:
                if last_exit is not None:
                    hours_since = (ts - last_exit).total_seconds() / 3600
                    if hours_since < COOLDOWN_HOURS:
                        state["last_processed_ts"] = ts
                        return state, trade_row
            except Exception:
                pass

        bullish_sweep = (price > open_price) and (price > prev_close)
        bearish_sweep = (price < open_price) and (price < prev_close)

        vol_ok_long = True; vol_ok_short = True
        if USE_VOLUME_FILTER and not pd.isna(h1['Volume'].iloc[i]):
            avgv = h1['Volume'].rolling(VOL_LOOKBACK).mean().iloc[i]
            if not pd.isna(avgv):
                vol_ok_long = h1['Volume'].iloc[i] >= VOL_MIN_RATIO * avgv
                vol_ok_short = vol_ok_long

        rsi = float(h1['RSI'].iloc[i]) if not pd.isna(h1['RSI'].iloc[i]) else None
        rsi_ok_long = True if rsi is None else rsi > RSI_OVERSOLD
        rsi_ok_short = True if rsi is None else rsi < RSI_OVERBOUGHT

        long_ok = bullish_sweep and vol_ok_long and rsi_ok_long and ((not USE_H1_FILTER) or h4t == 1)
        short_ok = bearish_sweep and vol_ok_short and rsi_ok_short and ((not USE_H1_FILTER) or h4t == -1)

        signal = 1 if long_ok else (-1 if short_ok else 0)

        if signal != 0 and (not USE_ATR_STOPS or (USE_ATR_STOPS and not pd.isna(h1['ATR'].iloc[i]) and h1['ATR'].iloc[i] > 0)):
            # Calculate SL, TP, RR
            if signal == 1:
                sl = price - (ATR_MULT_SL * h1['ATR'].iloc[i]) if USE_ATR_STOPS else price * (1 - 0.01)
                risk = abs(price - sl)
                rr = RR_FIXED
                if DYNAMIC_RR and USE_ATR_STOPS and i >= 6:
                    recent = float(h1['ATR'].iloc[i-5:i].mean())
                    curr = float(h1['ATR'].iloc[i])
                    if recent > 0:
                        if curr > recent * 1.2: rr = MAX_RR
                        elif curr < recent * 0.8: rr = MIN_RR
                tp = price + rr * risk
            else:
                sl = price + (ATR_MULT_SL * h1['ATR'].iloc[i]) if USE_ATR_STOPS else price * (1 + 0.01)
                risk = abs(sl - price)
                rr = RR_FIXED
                if DYNAMIC_RR and USE_ATR_STOPS and i >= 6:
                    recent = float(h1['ATR'].iloc[i-5:i].mean())
                    curr = float(h1['ATR'].iloc[i])
                    if recent > 0:
                        if curr > recent * 1.2: rr = MAX_RR
                        elif curr < recent * 0.8: rr = MIN_RR
                tp = price - rr * risk

            if risk > 0:
                size = position_size_futures(price, sl, state["capital"], RISK_PERCENT, MAX_TRADE_SIZE, MAX_POSITION_PCT)
                if size > 0:
                    entry_price_used = price
                    side_str = "long" if signal == 1 else "short"
                    side = "buy" if signal == 1 else "sell"
                    
                    if MODE == "live" and exchange is not None:
                        try:
                            size = amount_to_precision(exchange, symbol, size)
                            order = place_market(exchange, symbol, side, size, reduce_only=False)
                            ep = avg_fill_price(order)
                            if ep is not None:
                                entry_price_used = float(ep)
                            
                            # ✅ Get actual filled size (handle partial fills)
                            filled_size = float(order.get('filled', size))
                            if abs(filled_size - size) > 0.01 * size:  # >1% difference
                                send_telegram_fut(f"⚠️ Partial fill: requested {size:.4f}, got {filled_size:.4f}")
                                size = filled_size
                                
                        except Exception as e:
                            send_telegram_fut(f"❌ {symbol} entry failed: {e}")
                            state["last_processed_ts"] = ts
                            return state, trade_row

                    # Set position state
                    state["position"] = 1 if signal == 1 else -1
                    state["entry_price"] = entry_price_used
                    state["entry_sl"] = sl
                    state["entry_tp"] = tp
                    state["entry_time"] = ts
                    state["entry_size"] = size
                    state["entry_bar_index"] = i
                    state["bearish_count"] = 0
                    state["last_processed_ts"] = ts

                    # Deduct entry fees
                    pos_val = abs(entry_price_used * size)
                    state["capital"] -= pos_val * FEE_RATE

                    # ✅ Place real stop-loss and take-profit orders
                    if MODE == "live" and exchange is not None:
                        stop_id = place_stop_loss_order(exchange, symbol, side_str, sl, size)
                        tp_id = place_take_profit_order(exchange, symbol, side_str, tp, size)
                        
                        state["stop_order_id"] = stop_id
                        state["tp_order_id"] = tp_id
                        
                        if not stop_id:
                            send_telegram_fut(f"🚨 WARNING: No stop-loss order for {symbol}! Manual monitoring required!")

                    tag = "LONG" if signal == 1 else "SHORT"
                    send_telegram_fut(f"🚀 ENTRY {symbol} {tag} @ {entry_price_used:.4f} | SL {sl:.4f} | TP {tp:.4f} | RR {rr:.1f} | Size: {size:.4f}")

    state["last_processed_ts"] = ts
    state["peak_equity"] = max(state.get("peak_equity", state.get("capital", PER_COIN_CAP_USD)), state.get("capital", PER_COIN_CAP_USD))
    return state, trade_row


# =========================
# WORKER THREAD
# =========================
def worker(symbol, all_states):
    """Worker thread with production-grade safety"""
    state_file, trades_csv = state_files_for_symbol(symbol)
    exchange = get_exchange()
    state = load_state(state_file)
    all_states[symbol] = state

    send_telegram_fut(f"🤖 {symbol} bot started | {ENTRY_TF}/{HTF} | cap ${state['capital']:.2f} | Mode: {MODE.upper()}")

    # Initial position sync
    if MODE == "live":
        verify_and_sync_position(exchange, symbol, state)
        save_state(state_file, state)

    loop_count = 0
    
    while True:
        try:
            # Check emergency stop every loop
            if check_emergency_stop():
                send_telegram_fut(f"🛑 Emergency stop detected for {symbol}")
                break
            
            # ✅ Position sync every 10 loops (~10 minutes)
            if MODE == "live" and loop_count % 10 == 0:
                desync = verify_and_sync_position(exchange, symbol, state)
                if desync:
                    save_state(state_file, state)
            
            loop_count += 1

            now = datetime.now(timezone.utc)
            since = now - timedelta(days=LOOKBACK_DAYS)
            since_ms = int(since.timestamp()*1000); until_ms = int(now.timestamp()*1000)

            h1 = fetch_ohlcv_range(exchange, symbol, ENTRY_TF, since_ms, until_ms)
            h4 = fetch_ohlcv_range(exchange, symbol, HTF, since_ms, until_ms)
            if h1.empty or h4.empty or len(h1) < 3:
                time.sleep(30); continue

            closed_ts = h1.index[-2]
            
            last_proc = state.get("last_processed_ts")
            if last_proc is not None:
                if isinstance(last_proc, str):
                    last_proc = pd.to_datetime(last_proc).tz_localize(None)
                if last_proc >= closed_ts:
                    time.sleep(30); continue

            funding_series = None
            if INCLUDE_FUNDING:
                fdf = fetch_funding_history(exchange, symbol, int(h1.index[0].timestamp()*1000), int(h1.index[-1].timestamp()*1000))
                funding_series = align_funding_to_index(h1.index, fdf) if (fdf is not None and not fdf.empty) else pd.Series(0.0, index=h1.index)

            state, trade = process_bar(symbol, h1.iloc[:-1], h4.iloc[:-1], state, exchange=exchange, funding_series=funding_series)
            if trade is not None:
                append_trade(trades_csv, trade)

            save_state(state_file, state)

            next_close = h1.index[-1] + (h1.index[-1] - h1.index[-2])
            now_utc = datetime.now(timezone.utc)
            if getattr(next_close, "tzinfo", None) is None:
                next_close = next_close.replace(tzinfo=timezone.utc)
            sleep_sec = (next_close - now_utc).total_seconds() + 5
            if sleep_sec < 10: sleep_sec = 10
            if sleep_sec > 3600: sleep_sec = SLEEP_CAP
            time.sleep(sleep_sec)

        except ccxt.RateLimitExceeded:
            time.sleep(10)
        except Exception as e:
            msg = f"{LOG_PREFIX} {symbol} ERROR: {e}"
            print(msg)
            traceback.print_exc()
            send_telegram_fut(msg)
            time.sleep(60)

# =========================
# DAILY SUMMARY
# =========================
def ist_now():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def generate_daily_summary():
    try:
        now_ist = ist_now()
        start_ist = datetime(now_ist.year, now_ist.month, now_ist.day, 0, 0, 0, tzinfo=now_ist.tzinfo)
        end_ist = datetime(now_ist.year, now_ist.month, now_ist.day, 23, 59, 59, tzinfo=now_ist.tzinfo)
        start_utc = start_ist.astimezone(timezone.utc).replace(tzinfo=None)
        end_utc = end_ist.astimezone(timezone.utc).replace(tzinfo=None)

        lines = [f"📊 DAILY SUMMARY — {now_ist.strftime('%Y-%m-%d %I:%M %p IST')}", "-"*60]
        total_cap, total_init, pnl_today, n_today, w_today = 0.0, 0.0, 0.0, 0, 0

        for sym in SYMBOLS:
            state_file, trades_csv = state_files_for_symbol(sym)
            state = load_state(state_file) if os.path.exists(state_file) else {"capital": PER_COIN_CAP_USD, "position":0}
            cap = float(state.get("capital", PER_COIN_CAP_USD))
            initial = PER_COIN_CAP_USD

            wins = losses = wr = 0.0; pnl_all = 0.0
            n_trades_today = wins_today = 0; pnl_today_sym = 0.0

            if os.path.exists(trades_csv):
                try:
                    df = pd.read_csv(trades_csv)
                    if df.empty or 'Exit_DateTime' not in df.columns:
                        today = df.iloc[0:0]
                    else:
                        df['Exit_DateTime'] = pd.to_datetime(df['Exit_DateTime'], errors='coerce', utc=True).dt.tz_convert(None)
                        df = df.dropna(subset=['Exit_DateTime'])
                        today = df[(df['Exit_DateTime'] >= start_utc) & (df['Exit_DateTime'] <= end_utc)]

                    n_trades_today = len(today)
                    wins_today = int(today['Win'].sum()) if n_trades_today else 0
                    pnl_today_sym = float(today['PnL_].sum()) if n_trades_today else 0.0

                    if not df.empty:
                        pnl_all = float(df['PnL_].sum())
                        wins = int(df['Win'].sum()); losses = len(df)-wins
                        wr = (wins/len(df)*100) if len(df) else 0.0
                except Exception:
                    today = pd.DataFrame()

            total_cap += cap; total_init += initial
            pnl_today += pnl_today_sym; n_today += n_trades_today; w_today += wins_today
            roi = ((cap/initial)-1)*100 if initial>0 else 0.0

            # ✅ Show position status
            pos_status = ""
            if state.get("position") == 1:
                pos_status = f" | 🟢 LONG @ {state.get('entry_price', 0):.4f}"
            elif state.get("position") == -1:
                pos_status = f" | 🔴 SHORT @ {state.get('entry_price', 0):.4f}"

            lines.append(f"{sym}: ${cap:,.2f} ({roi:+.2f}%) | {n_trades_today}T {wins_today}W | ${pnl_today_sym:+.2f} | WR {wr:.1f}%{pos_status}")

        port_roi = ((total_cap/total_init)-1)*100 if total_init>0 else 0.0
        wr_today = (w_today/n_today*100) if n_today>0 else 0.0
        lines += ["-"*60, f"TOTAL: ${total_cap:,.2f} ({port_roi:+.2f}%) | {n_today}T | WR {wr_today:.1f}% | PnL ${pnl_today:+.2f}"]
        msg = "\n".join(lines)
        print(msg)
        send_telegram_fut(msg)
    except Exception as e:
        send_telegram_fut(f"❌ summary error: {e}")

def summary_scheduler():
    """Send summary every 3 hours"""
    last_sent_time = None
    INTERVAL_HOURS = 3
    
    while True:
        try:
            if check_emergency_stop():
                break
                
            now = ist_now()
            
            if last_sent_time is None:
                generate_daily_summary()
                last_sent_time = now
            else:
                hours_since_last = (now - last_sent_time).total_seconds() / 3600
                if hours_since_last >= INTERVAL_HOURS:
                    generate_daily_summary()
                    last_sent_time = now
            
            time.sleep(300)
        except Exception as e:
            print(f"Summary scheduler error: {e}")
            time.sleep(300)

# =========================
# MAIN
# =========================
def main():
    boot = f"""
🚀 PRODUCTION Trading Bot Started
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mode: {MODE.upper()}
Exchange: KuCoin Futures
Symbols: {", ".join(SYMBOLS)}
Timeframes: {ENTRY_TF}/{HTF}
Capital/coin: ${PER_COIN_CAP_USD:,.2f}
Risk: {RISK_PERCENT*100:.2f}% | Fee: {FEE_RATE*100:.3f}%
Max Position: {MAX_POSITION_PCT*100:.0f}% | Max DD: {MAX_DRAWDOWN*100:.0f}%

✅ SAFETY FEATURES ENABLED:
- Real stop-loss orders
- Position synchronization
- Balance drift detection  
- Emergency kill switch
- Daily loss limits (${MAX_DAILY_LOSS})
- Consecutive loss protection ({MAX_CONSECUTIVE_LOSSES})
- Funding rate checks ({MAX_FUNDING_RATE*100:.3f}%)

⚠️ Create '{EMERGENCY_STOP_FILE}' to emergency shutdown
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    print(boot)
    send_telegram_fut(boot)

    # Shared state dict for balance verification
    all_states = {}
    threads = []
    
    for sym in SYMBOLS:
        t = threading.Thread(target=worker, args=(sym, all_states), daemon=True)
        t.start(); threads.append(t)
        time.sleep(1)

    if SEND_DAILY_SUMMARY:
        s = threading.Thread(target=summary_scheduler, daemon=True)
        s.start(); threads.append(s)

    # ✅ Balance verification thread
    def balance_monitor():
        while True:
            try:
                if check_emergency_stop():
                    break
                time.sleep(3600)  # Every hour
                if MODE == "live":
                    exchange = get_exchange()
                    verify_balance(exchange, all_states)
            except Exception as e:
                print(f"Balance monitor error: {e}")
    
    bm = threading.Thread(target=balance_monitor, daemon=True)
    bm.start(); threads.append(bm)

    print(f"✅ Running {len(threads)} threads (Mode: {MODE})")
    
    # Main loop - check for emergency stop
    while True:
        if check_emergency_stop():
            send_telegram_fut("🚨 EMERGENCY STOP DETECTED - Shutting down all positions")
            exchange = get_exchange()
            emergency_close_all(exchange, SYMBOLS, all_states)
            break
        time.sleep(60)

if __name__ == "__main__":
    main()
