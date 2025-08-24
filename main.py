import os
import time
import json
import requests
from fastapi import FastAPI
import pandas as pd

app = FastAPI()

BYBIT_API_URL = "https://api.bybit.com/v5/market/kline"
SYMBOLS_URL = "https://api.bybit.com/v5/market/instruments-info?category=linear"

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL_SECONDS", 300))  # 5 minutes default
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MACD_FAST = int(os.getenv("MACD_FAST", 12))
MACD_SLOW = int(os.getenv("MACD_SLOW", 26))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", 9))

INTERVALS = {
    "4h": 240,
    "1h": 60,
    "15m": 15
}

STATE_FILE = "state.json"
BATCH_SIZE = 250   # scan 250 per minute

def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print("Telegram error:", e)

def fetch_symbols():
    r = requests.get(SYMBOLS_URL)
    data = r.json()
    return [s["symbol"] for s in data["result"]["list"] if s["quoteCoin"] == "USDT"]

def fetch_ohlcv(symbol, interval, limit=200):
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BYBIT_API_URL, params=params)
    data = r.json()
    if "result" not in data or "list" not in data["result"]:
        return pd.DataFrame()
    df = pd.DataFrame(data["result"]["list"], columns=["time","open","high","low","close","volume","turnover"])
    df = df.astype(float)
    df.sort_values("time", inplace=True)
    return df

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

def check_alignment(symbol):
    results = {}
    for label, minutes in INTERVALS.items():
        df = fetch_ohlcv(symbol, minutes)
        if df.empty:
            continue
        df = macd(df)
        results[label] = check_first_green(df)
    return results

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def scanner():
    state = load_state()
    symbols = fetch_symbols()
    print(f"Scanning {len(symbols)} symbols...")

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i+BATCH_SIZE]
        print(f"Processing batch {i//BATCH_SIZE+1}: {len(batch)} symbols")

        for symbol in batch:
            alignment = check_alignment(symbol)
            if alignment.get("4h"):
                key = f"{symbol}-4h"
                last_time = state.get(key, 0)
                df = fetch_ohlcv(symbol, INTERVALS["4h"])
                candle_time = int(df.iloc[-1]["time"])
                if last_time != candle_time:
                    if alignment.get("1h") or alignment.get("15m"):
                        label = "STRONG SIGNAL"
                    else:
                        label = "Signal"
                    msg = f"{label}: {symbol} → First Green MACD Histogram on 4H (confirm 1h/15m={alignment})"
                    send_telegram(msg)
                    state[key] = candle_time

        save_state(state)
        time.sleep(60)  # wait 1 minute before next batch

@app.on_event("startup")
def start_loop():
    import threading
    def loop():
        while True:
            try:
                scanner()
            except Exception as e:
                print("Error in scanner:", e)
            time.sleep(SCAN_INTERVAL)
    threading.Thread(target=loop, daemon=True).start()

    # --- Self-pinger every 5 mins ---
    def pinger():
        url = os.getenv("SELF_URL")
        if not url:
            return
        while True:
            try:
                requests.get(url)
                print("Self-pinged", url)
            except Exception as e:
                print("Ping error:", e)
            time.sleep(300)
    threading.Thread(target=pinger, daemon=True).start()

@app.get("/")
def root():
    return {"status": "ok"}
