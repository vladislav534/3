
import aioredis
import time
from typing import Optional, Dict, Any
from config import REDIS_URL

_redis = None

async def get_redis():
    global _redis
    if _redis is None:
        _redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis

async def read_snapshot(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Read snapshot:{symbol} from Redis.
    Expected hash fields: price, ts, exchange
    Returns normalized dict or None.
    """
    r = await get_redis()
    key = f"snapshot:{symbol}"
    data = await r.hgetall(key)
    if not data:
        return None
    try:
        price = float(data.get("price")) if data.get("price") else None
    except Exception:
        price = None
    try:
        ts = int(data.get("ts")) if data.get("ts") else None
    except Exception:
        ts = None
    return {"symbol": symbol, "price": price, "ts": ts, "exchange": data.get("exchange")}

async def list_snapshot_symbols() -> list:
    """
    Lightweight helper for PoC: returns symbols present as keys snapshot:*.
    For production prefer maintaining a set of symbols to avoid KEYS.
    """
    r = await get_redis()
    keys = await r.keys("snapshot:*")
    return [k.split("snapshot:", 1)[1] for k in keys]

