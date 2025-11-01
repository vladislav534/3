#!/usr/bin/env python3
"""
test_snapshot.py

Независимый smoke test для Redis snapshot path.

Что делает:
- подключается к Redis (по REDIS_URL из окружения или использует localhost),
- записывает пару тестовых snapshot ключей (BTCUSDT, ETHUSDT) с полями price, ts, exchange,
- читает ключи обратно и выводит результаты,
- опционально: режим --loop N (в секундах) для периодической записи (имитатор producer).

Запуск:
  python test_snapshot.py           # однократно записать и проверить
  python test_snapshot.py --loop 1  # писать каждые 1s (Ctrl+C чтобы остановить)
"""
import argparse
import asyncio
import os
import time
import signal
import sys
import json

try:
    import aioredis
except Exception as e:
    print("aioredis is required. Install: pip install aioredis", file=sys.stderr)
    raise

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

TEST_SYMBOLS = [
    ("BTCUSDT", "binance", 30000.0),
    ("ETHUSDT", "binance", 2000.0),
]

async def write_snapshots(redis, base_prices=None):
    ts = int(time.time() * 1000)
    base_prices = base_prices or {}
    for sym, exch, default in TEST_SYMBOLS:
        price = base_prices.get(sym, default)
        key = f"snapshot:{sym}"
        await redis.hset(key, mapping={"price": str(price), "ts": str(ts), "exchange": exch})
    return ts

async def read_snapshots(redis):
    keys = await redis.keys("snapshot:*")
    out = {}
    for k in keys:
        sym = k.split("snapshot:", 1)[1]
        data = await redis.hgetall(k)
        out[sym] = data
    return out

async def single_run():
    r = await aioredis.from_url(REDIS_URL, decode_responses=True)
    print(f"Connected to Redis at {REDIS_URL}")
    ts = await write_snapshots(r)
    print(f"Wrote snapshots at ts={ts}")
    out = await read_snapshots(r)
    print("Snapshots read back:")
    print(json.dumps(out, indent=2, ensure_ascii=False))
    await r.close()

async def loop_run(interval_s: float):
    r = await aioredis.from_url(REDIS_URL, decode_responses=True)
    print(f"Loop mode: writing to Redis {REDIS_URL} every {interval_s}s (Ctrl+C to stop)")
    stopped = False

    def _on_sigint(sig, frame):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, _on_sigint)
    counter = 0
    try:
        while not stopped:
            # simple price variation to simulate updates
            delta = (counter % 10) - 5
            prices = {sym: base + delta for sym, _, base in TEST_SYMBOLS}
            ts = await write_snapshots(r, base_prices=prices)
            print(f"[{time.strftime('%H:%M:%S')}] wrote ts={ts} prices={prices}")
            counter += 1
            await asyncio.sleep(interval_s)
    finally:
        await r.close()
        print("Stopped loop writer.")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--loop", type=float, default=0.0, help="Write snapshots repeatedly every N seconds (0 = single run)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.loop and args.loop > 0:
        asyncio.run(loop_run(args.loop))
    else:
        asyncio.run(single_run())
