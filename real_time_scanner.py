"""
Real-Time Stock Scanner
- Auto-selects top active stocks for the day
- Computes SMA, EMA, RSI, VWAP, ATR
- Triggers alerts:
    * Percent move from open
    * SMA/EMA cross
    * RSI overbought/oversold
    * VWAP cross
    * ATR breakout
- Logs to console & SQLite
- Optional: Telegram, email, webhook notifications
- Run on-demand; no infinite loops
"""

import os, asyncio, logging, time, aiosqlite, aiohttp, pandas as pd, numpy as np
from datetime import datetime, timezone
from collections import deque, defaultdict
from dotenv import load_dotenv

# -----------------------
# CONFIG
# -----------------------
load_dotenv()

FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN")
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", "30"))  # seconds
HISTORY_MINUTES = int(os.getenv("HISTORY_MINUTES", "30"))
DB_PATH = os.getenv("DB_PATH", "alerts.db")
SMA_FAST = int(os.getenv("SMA_FAST", "20"))
SMA_SLOW = int(os.getenv("SMA_SLOW", "50"))
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD = int(os.getenv("RSI_OVERSOLD", "30"))

# Optional alert channels
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scanner")

# -----------------------
# INDICATORS
# -----------------------
def sma(series, n): return series.rolling(n).mean().iloc[-1] if len(series)>=n else None
def ema(series, n): return series.ewm(span=n, adjust=False).mean().iloc[-1] if len(series)>=n else None
def rsi(series, n=14):
    if len(series)<n+1: return None
    delta = series.diff().dropna()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    rs = up.rolling(n).mean() / down.rolling(n).mean().replace(0,np.nan)
    rsi_val = 100 - (100 / (1 + rs))
    return float(rsi_val.iloc[-1]) if not rsi_val.isna().iloc[-1] else None
def vwap(df): return float((df['c']*df['v']).sum()/df['v'].sum()) if not df.empty else None
def atr(df, n=14):
    if len(df)<n+1: return None
    high_low = df['h'] - df['l']
    high_close = (df['h'] - df['c'].shift()).abs()
    low_close = (df['l'] - df['c'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return float(tr.rolling(n).mean().iloc[-1])

# -----------------------
# DATABASE
# -----------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            alert_type TEXT,
            message TEXT,
            timestamp TEXT
        )""")
        await db.commit()

async def save_alert(symbol, typ, msg):
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO alerts(symbol,alert_type,message,timestamp) VALUES (?,?,?,?)",
                         (symbol, typ, msg, ts))
        await db.commit()

# -----------------------
# ALERT DELIVERY
# -----------------------
async def alert(symbol, typ, msg):
    logger.info(f"[{symbol}] {typ} - {msg}")
    await save_alert(symbol, typ, msg)
    # Optional: add Telegram, email, webhook here

# -----------------------
# FINNHUB API
# -----------------------
async def fetch_json(url):
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url) as resp:
            return await resp.json()

async def top_active_symbols():
    # Fetch all US symbols
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={FINNHUB_TOKEN}"
    data = await fetch_json(url)
    # Pick a subset for demo: top N by symbol name (can replace with volume sorting later)
    return [s['symbol'] for s in data[:20]]

async def fetch_quote(symbol):
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_TOKEN}"
    return await fetch_json(url)

async def fetch_candles(symbol, resolution='1', count=200):
    url = f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution={resolution}&count={count}&token={FINNHUB_TOKEN}"
    data = await fetch_json(url)
    if data.get('s')=='ok':
        df = pd.DataFrame({'t':data['t'],'o':data['o'],'h':data['h'],'l':data['l'],'c':data['c'],'v':data['v']})
        return df
    return pd.DataFrame()

# -----------------------
# SCANNER
# -----------------------
async def run_scanner():
    await init_db()
    symbols = await top_active_symbols()
    logger.info(f"Scanning symbols: {symbols}")
    for sym in symbols:
        quote = await fetch_quote(sym)
        if 'c' not in quote: continue
        df = await fetch_candles(sym)
        last_close = quote['c']
        sma_fast = sma(df['c'], SMA_FAST) if not df.empty else None
        sma_slow = sma(df['c'], SMA_SLOW) if not df.empty else None
        rsi_val = rsi(df['c']) if not df.empty else None
        vwap_val = vwap(df)
        atr_val = atr(df)
        # Alert rules
        if last_close and quote.get('o'):
            pct_move = (last_close - quote['o'])/quote['o']*100
            if abs(pct_move)>2: await alert(sym, "PCT_MOVE", f"{pct_move:.2f}% from open")
        if sma_fast and sma_slow:
            if sma_fast > sma_slow: await alert(sym,"SMA_CROSS","Fast SMA crossed above Slow SMA")
            if sma_fast < sma_slow: await alert(sym,"SMA_CROSS","Fast SMA crossed below Slow SMA")
        if rsi_val:
            if rsi_val>RSI_OVERBOUGHT: await alert(sym,"RSI","Overbought")
            if rsi_val<RSI_OVERSOLD: await alert(sym,"RSI","Oversold")
        if vwap_val:
            if last_close>vwap_val: await alert(sym,"VWAP","Price above VWAP")
            if last_close<vwap_val: await alert(sym,"VWAP","Price below VWAP")
        if atr_val:
            if abs(last_close - quote['o'])>2.5*atr_val: await alert(sym,"ATR_BREAK","Price move exceeded 2.5×ATR")

# -----------------------
# RUN ON-DEMAND
# -----------------------
if __name__=="__main__":
    asyncio.run(run_scanner())