# config.py
POLL_INTERVAL_SECONDS = 1.0

# Желательный базовый список бирж (именa ccxt). Можно добавлять/удалять.
EXCHANGES = [
    "binance",
    "huobi",
    "lbank",
    "ascendex",
    "kucoin",
    "okx",
    "mexc",
    "bybit",
    "gateio",
    "bitget",
    "bingx",
    "bitstamp",
    "bitmart",
    # "hitbtc"
]

DEFAULT_PAIR = "SUI/USDT"

# Варианты пар, которые будем пробовать (по приоритету)
PAIRS_ALTERNATIVES = {
    "*": ["SUI/USDT", "SUI/USDC", "SUI/USDT:USDT", "SUIUSDT"]
}

PAIRS_ALTERNATIVES.update({
    "huobi": ["SUI/USDT", "SUI/USDC"],
    "gateio": ["SUI/USDT", "SUI/USDC"],
    "bingx": ["SUI/USDT", "SUI/USDC"]
})

API_KEYS = {}
import os
REQUEST_TIMEOUT = 0  # ms
MAX_CONCURRENT_REQUESTS = 200
PERCENT_DECIMALS = 4
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "0"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "200"))
PERCENT_DECIMALS = int(os.getenv("PERCENT_DECIMALS", "4"))

# Redis snapshot config (fast path for realtime prices)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# If enabled, server first tries to read snapshots from Redis before falling back to REST
USE_REDIS_SNAPSHOT = os.getenv("USE_REDIS_SNAPSHOT", "1") == "1"
# how fresh snapshot must be (ms)
SNAPSHOT_MAX_AGE_MS = int(os.getenv("SNAPSHOT_MAX_AGE_MS", "500"))