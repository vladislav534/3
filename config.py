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
    "hitbtc"
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

REQUEST_TIMEOUT = 20_000  # ms
MAX_CONCURRENT_REQUESTS = 4
PERCENT_DECIMALS = 4
