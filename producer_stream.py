#!/usr/bin/env python3
# producer_stream.py
"""
Simple producer example: writes simulated ticks to Redis Stream and updates snapshot hashes.
Usage:
  python producer_stream.py --loop 0.2
"""
import asyncio
import json
import time
import argparse
import os
import signal

import aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_KEY = os.getenv("STREAM_KEY", "stream:ticks")    # global stream
SNAPSHOT_PREFIX = os.getenv("SNAPSHOT_PREFIX", "snapshot:")

TEST_SYMBOLS = [
    ("BINANCE", "SUI/USDT", 0.0),
    ("BINANCE", "BTC/USDT", 30000.0),
    ("BINANCE", "ETH/USDT", 2000.0),
]

async def run_once(redis, now_ts_ms, prices_map):
    # push events to stream and update snapshot
    for exch, sym, base in TEST_SYMBOLS:
        # simulate price variation if provided
        price = prices_map.get(sym, base) if prices_map else base
        rec = {"e": exch, "s": sym, "ts": str(now_ts_ms), "p": str(price)}
        # XADD: use compact JSON in single field 'j'
        await redis.xadd(STREAM_KEY, {"j": json.dumps(rec)}, maxlen=10000, approximate=True)
        # update snapshot hash for quick reads
        key = f"{SNAPSHOT_PREFIX}{sym.replace('/', '')}"
        await redis.hset(key, mapping={"price": str(price), "ts": str(now_ts_ms), "exchange": exch})
    return

async def loop(interval: float):
    r = await aioredis.from_url(REDIS_URL, decode_responses=True)
    print("Producer connected to", REDIS_URL, "stream:", STREAM_KEY)
    stopped = False
    def stop(sig, frame):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGINT, stop)
    counter = 0
    try:
        while not stopped:
            ts = int(time.time() * 1000)
            # simple wobble
            prices = {}
            for _, sym, base in TEST_SYMBOLS:
                prices[sym] = base + ((counter % 11) - 5)
            await run_once(r, ts, prices)
            counter += 1
            await asyncio.sleep(interval)
    finally:
        await r.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--loop", type=float, default=0.5, help="interval seconds between writes")
    args = p.parse_args()
    asyncio.run(loop(args.loop))

if __name__ == "__main__":
    main()
