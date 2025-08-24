import os
import time
import json
import requests
from datetime import datetime
from fastapi import FastAPI
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

app = FastAPI()

BYBIT_API_URL = "https://api.bybit.com/v5/market/kline"
SYMBOLS_URL = "https://api.bybit.com/v5/market/instruments-info?category=linear"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL_SECONDS", 300))  # 5 minutes default
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MACD_FAST = int(os.getenv("MACD_FAST", 12))
MACD_SLOW = int(os.getenv("MACD_SLOW", 26))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", 9))

INTERVALS = {"4h": 240, "1h": 60, "15m": 15}

STATE_FILE = "state.json"
MAX_WORKERS = 10  # safe concurrency


# ------------------ Utilities ------------------

def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print("Telegram error:", e)


def safe_get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r
            print("HTTP error:", r.status_code, r.text)
        except Exception as e:
            print("Request error:", e)
        time.sleep(2 * (attempt + 1))
    return None


def fetch_symbols():
    r = safe_get(SYMBOLS_URL)
    if not r:
        return []
    try:
        data = r.json()
        return [s["symbol"] for s in data["result"]["list"] if s["quoteCoin"] == "USDT"]
    except Exception as e:
        print("Symbol parse error:", e)
        return []


def fetch_ohlcv(symbol, interval, limit=200):
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    r = safe_get(BYBIT_API_URL, params=params)
    if not r:
        return pd.DataFrame()
    try:
        data = r.json()
        if "result" not in data or "list" not in data["result"]:
            return pd.DataFrame()
        df = pd.DataFrame(data["result"]["list"], columns=["time","open","high","low","close","volume","turnover"])
        df = df.astype(float)
        df.sort_values("time", inplace=True)
        return df
    except Exception as e:
        print("Parse error:", e)
        return pd.DataFrame()


def macd(df):
    df["ema_fast"] = df["close"].ewm(span=MACD_FAST, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["macd"] = df["ema_fast"] - df["ema_slow"]
    df["signal"] = df["macd"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    df["hist"] = df["macd"] - df["signal"]
    return df


def check_first_green(df):
    if len(df) < 3:
        return False
    return df.iloc[-2]["hist"] < 0 and df.iloc[-1]["hist"] > 0


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


# ------------------ Scanner ------------------

def process_symbol(symbol, state, force_run=False):
    results = []
    df4h = fetch_ohlcv(symbol, INTERVALS["4h"])
    if df4h.empty:
        return state, results

    df4h = macd(df4h)
    if check_first_green(df4h):
        key = f"{symbol}-4h"
        last_time = state.get(key, 0)
        candle_time = int(df4h.iloc[-1]["time"])

        if last_time != candle_time or force_run:
            msg = f"⚡ Signal: {symbol} → First Green MACD Histogram on 4H"
            results.append(msg)
            state[key] = candle_time
            state[f"{symbol}-4h-strong"] = False
        else:
            alignment = []
            for tf in ["1h", "15m"]:
                dft = fetch_ohlcv(symbol, INTERVALS[tf])
                if dft.empty:
                    continue
                dft = macd(dft)
                if check_first_green(dft):
                    alignment.append(tf)

            if alignment and not state.get(f"{symbol}-4h-strong", False):
                msg = f"🚀 STRONG SIGNAL: {symbol} → 4H aligned with {alignment}"
                results.append(msg)
                state[f"{symbol}-4h-strong"] = True
    else:
        state.pop(f"{symbol}-4h-strong", None)

    return state, results


def scanner(force_run=False):
    state = load_state()
    symbols = fetch_symbols()
    if not symbols:
        print("No symbols fetched.")
        return

    print(f"Scanning {len(symbols)} symbols...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_symbol, symbol, state.copy(), force_run): symbol for symbol in symbols}
        for future in as_completed(futures):
            try:
                new_state, results = future.result()
                state.update(new_state)
                for msg in results:
                    send_telegram(msg)
            except Exception as e:
                print("Symbol error:", futures[future], e)

    save_state(state)


# ------------------ Background ------------------

@app.on_event("startup")
def start_loop():
    import threading

    threading.Thread(target=lambda: scanner(force_run=True), daemon=True).start()

    def loop():
        while True:
            try:
                scanner()
            except Exception as e:
                print("Error in scanner:", e)
            time.sleep(SCAN_INTERVAL)
    threading.Thread(target=loop, daemon=True).start()

    def pinger():
        url = os.getenv("SELF_URL")
        if not url:
            return
        while True:
            try:
                requests.get(url, timeout=10)
                print("Self-pinged", url)
            except Exception as e:
                print("Ping error:", e)
            time.sleep(300)
    threading.Thread(target=pinger, daemon=True).start()


@app.api_route("/", methods=["GET", "HEAD"])
def root():
    return {"status": "ok"}
