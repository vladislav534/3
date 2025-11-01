#!/usr/bin/env python3
"""
dump_pairs_live.py

Подключается к WebSocket сервера /ws, слушает сообщения типа "update"
и постоянно обновляет CSV файл с парами (cheaper, expensive, percent).

По умолчанию файл перезаписывается на каждое update (snapshot mode).
Опция --append включает режим дописывания (history mode).

Использование:
  pip install websockets
  python dump_pairs_live.py --ws ws://127.0.0.1:8000/ws --out outputs/pairs.csv

Опции:
  --debounce-ms N    : минимальный интервал между физическими записями файла (ms). По умолчанию 200.
  --min-pct P        : минимальный процент (в абсолютном значении) для включения пары (чтобы не засорять файл мелкими шумами). По умолчанию 0.0 (включать все).
  --append           : включить режим дописывания (history); по умолчанию False (перезапись snapshot).
"""
import asyncio
import json
import csv
import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import websockets

def compute_pct(a: float, b: float) -> float:
    denom = (a + b) / 2.0
    if denom == 0:
        return 0.0
    return 100.0 * (b - a) / denom

def build_rows_from_prices(prices: Dict[str, float], min_pct: float = 0.0) -> List[Tuple[str, str, float]]:
    labels = list(prices.keys())
    rows: List[Tuple[str, str, float]] = []
    for i in range(len(labels)):
        for j in range(i+1, len(labels)):
            li = labels[i]
            lj = labels[j]
            try:
                pi = float(prices[li])
                pj = float(prices[lj])
            except Exception:
                continue
            if not (pi > 0 and pj > 0):
                continue
            if pi <= pj:
                cheaper, expensive, pct = li, lj, compute_pct(pi, pj)
            else:
                cheaper, expensive, pct = lj, li, compute_pct(pj, pi)
            if abs(pct) >= abs(min_pct):
                rows.append((cheaper, expensive, pct))
    rows.sort(key=lambda x: x[2], reverse=True)
    return rows

async def write_snapshot_csv(path: str, rows: List[Tuple[str, str, float]], append: bool = False):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    if append:
        # append mode: write rows with timestamp per row
        tmp = None
        try:
            with open(path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for cheaper, expensive, pct in rows:
                    now_ms = int(time.time() * 1000)
                    writer.writerow([datetime.utcfromtimestamp(now_ms/1000.0).isoformat() + "Z", now_ms, cheaper, expensive, f"{pct:.6f}"])
        except Exception:
            raise
    else:
        # snapshot mode: write header + all rows (atomic replace)
        tmp = path + ".tmp"
        with open(tmp, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["ts_iso", "ts_ms", "cheaper", "expensive", "percent"])
            now_ms = int(time.time() * 1000)
            for cheaper, expensive, pct in rows:
                writer.writerow([datetime.utcfromtimestamp(now_ms/1000.0).isoformat() + "Z", now_ms, cheaper, expensive, f"{pct:.6f}"])
        os.replace(tmp, path)

async def consume_loop(ws_url: str, out_path: str, debounce_ms: int, min_pct: float, append: bool):
    backoff = 1.0
    last_write_ts = 0.0
    pending_rows: Optional[List[Tuple[str,str,float]]] = None

    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                print(f"[{datetime.utcnow().isoformat()}] Connected to {ws_url}", file=sys.stderr)
                backoff = 1.0
                async for msg in ws:
                    try:
                        payload = json.loads(msg)
                    except Exception:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    typ = payload.get("type")
                    if typ != "update":
                        continue
                    # flatten prices if nested
                    prices = payload.get("prices") or {}
                    if prices and any(isinstance(v, dict) for v in prices.values()):
                        flat = {}
                        for k,v in prices.items():
                            if isinstance(v, dict):
                                flat[k] = v.get("price")
                            else:
                                flat[k] = v
                        prices = flat
                    rows = build_rows_from_prices(prices, min_pct)
                    # debounce writes: batch frequent updates within debounce_ms
                    now = time.time() * 1000.0
                    pending_rows = rows
                    if (now - last_write_ts) >= debounce_ms:
                        # perform write immediately
                        await write_snapshot_csv(out_path, pending_rows, append)
                        last_write_ts = time.time() * 1000.0
                        pending_rows = None
                        # print to stderr for visibility
                        print(f"[{datetime.utcnow().isoformat()}] Wrote {out_path} ({len(rows)} rows)", file=sys.stderr)
                    # else: wait for next tick or flush on timeout below
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.InvalidURI,
                ConnectionRefusedError, OSError) as e:
            print(f"[{datetime.utcnow().isoformat()}] WebSocket connection error: {e}; reconnect in {backoff:.1f}s", file=sys.stderr)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.8, 30.0)
        except Exception as e:
            print(f"[{datetime.utcnow().isoformat()}] Unexpected error: {e}", file=sys.stderr)
            await asyncio.sleep(2.0)

async def periodic_flush(out_path: str, interval_ms: int, pending_getter, append: bool):
    """
    Optional: if you want to flush pending_rows at intervals even if debounce didn't pass.
    Not used in current main, but left as hook.
    """
    while True:
        await asyncio.sleep(interval_ms/1000.0)
        rows = pending_getter()
        if rows:
            await write_snapshot_csv(out_path, rows, append)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ws", default="ws://127.0.0.1:8000/ws", help="WebSocket URL")
    p.add_argument("--out", default="outputs/pairs.csv", help="Output CSV path")
    p.add_argument("--debounce-ms", type=int, default=200, help="Minimal ms between physical writes")
    p.add_argument("--min-pct", type=float, default=0.0, help="Minimal percent difference to include")
    p.add_argument("--append", action="store_true", help="Append (history) mode instead of snapshot overwrite")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        asyncio.run(consume_loop(args.ws, args.out, args.debounce_ms, args.min_pct, args.append))
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)

if __name__ == "__main__":
    main()
