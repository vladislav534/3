#!/usr/bin/env python3
import os
import json
import time
import logging
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime

import uvicorn
import aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from fetcher import fetch_all_prices, close_all_exchanges
from calculator import build_pairwise_table
from config import POLL_INTERVAL_SECONDS, USE_REDIS_SNAPSHOT, SNAPSHOT_MAX_AGE_MS
from redis_client import read_snapshot, list_snapshot_symbols
from exchanges import get_candidate_exchanges

# Stream constants
STREAM_KEY = os.getenv("STREAM_KEY", "stream:ticks")
STREAM_GROUP = os.getenv("STREAM_GROUP", "srv_group")
STREAM_CONSUMER = f"srv_consumer_{os.getpid()}"
SNAPSHOT_PREFIX = os.getenv("SNAPSHOT_PREFIX", "snapshot:")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# App + logging
app = FastAPI()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("server")


# Broadcaster: central place for managing WS clients and latest in-memory prices
class Broadcaster:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        # latest: label -> {"price": float, "ts_source": int, "ts_server": int, "latency_exchange_ms": int}
        self.latest: Dict[str, Dict[str, Any]] = {}

    async def add(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)
        logger.info("Client connected, total=%d", len(self.clients))

    def remove(self, ws: WebSocket):
        self.clients.discard(ws)
        logger.info("Client disconnected, total=%d", len(self.clients))

    async def broadcast_json(self, payload: Dict[str, Any]):
        # avoid logging full payload to reduce I/O and log noise
        try:
            t = payload.get("type")
            n_prices = None
            if isinstance(payload.get("prices"), dict):
                n_prices = len(payload["prices"])
            summary = {"type": t, "n_prices": n_prices, "ts_server": payload.get("ts_server")}
            logger.debug("WS OUT summary: %s", summary)
        except Exception:
            logger.debug("WS OUT summary unavailable")

        to_remove = []
        text = None
        for ws in list(self.clients):
            try:
                # prepare serialized text once per broadcast to reduce overhead
                if text is None:
                    text = json.dumps(payload, default=str)
                await ws.send_text(text)
            except Exception:
                to_remove.append(ws)
        for ws in to_remove:
            self.remove(ws)


    def update_latest(self, label: str, price: float, ts_source: int):
        ts_server = int(time.time() * 1000)
        latency_exchange_ms = max(0, ts_server - (int(ts_source) if ts_source else ts_server))
        self.latest[label] = {
            "price": price,
            "ts_source": int(ts_source) if ts_source else ts_server,
            "ts_server": ts_server,
            "latency_exchange_ms": latency_exchange_ms,
        }


brdc = Broadcaster()

import sqlite3

DB_PATH = os.getenv("PAIRS_DB", "outputs/pairs.db")

def _ensure_db(path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=5, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS current_pairs (
        pair TEXT PRIMARY KEY,
        cheaper TEXT NOT NULL,
        expensive TEXT NOT NULL,
        percent REAL NOT NULL,
        ts_ms INTEGER NOT NULL,
        ts_iso TEXT NOT NULL
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_pairs_ts ON current_pairs(ts_ms);")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pair TEXT NOT NULL,
        cheaper TEXT NOT NULL,
        expensive TEXT NOT NULL,
        percent REAL NOT NULL,
        ts_ms INTEGER NOT NULL,
        ts_iso TEXT NOT NULL
    );
    """)
    conn.commit()
    return conn

# Backward-compatible lightweight ConnectionManager (kept for other code paths)
class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active.add(websocket)
        logger.info("Client connected, total=%d", len(self.active))

    def disconnect(self, websocket: WebSocket):
        self.active.discard(websocket)
        logger.info("Client disconnected, total=%d", len(self.active))

    async def send_json(self, data: Dict[str, Any]):
        # log small summary only
        try:
            t = data.get("type")
            logger.debug("ConnMgr OUT type=%s", t)
            js = json.dumps(data, default=str)
        except Exception:
            js = str(data)
        remove = []
        for ws in list(self.active):
            try:
                await ws.send_text(js)
            except Exception:
                remove.append(ws)
        for ws in remove:
            self.disconnect(ws)


manager = ConnectionManager()

# Global state
_current_pair: Optional[str] = None  # optional override
# store last payload/labels for new clients
globals()["_latest_payload"] = None
globals()["_latest_labels"] = []


# Serve static files (index.html)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutting down: closing exchange instances and stream consumer")
    # stop stream consumer if running
    stop_evt = globals().get("_stream_consumer_stop")
    task = globals().get("_stream_consumer_task")
    if stop_evt and isinstance(stop_evt, asyncio.Event):
        stop_evt.set()
    if task:
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except Exception:
            try:
                task.cancel()
            except Exception:
                pass
    csv_stop = globals().get("_csv_dumper_stop")
    csv_task = globals().get("_csv_dumper_task")
    if csv_stop and isinstance(csv_stop, asyncio.Event):
        csv_stop.set()
    if csv_task:
        try:
            await asyncio.wait_for(csv_task, timeout=3.0)
        except Exception:
            try:
                csv_task.cancel()
            except Exception:
                pass
    # close ccxt exchanges
    try:
        await close_all_exchanges()
    except Exception:
        logger.exception("Error closing exchanges")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await brdc.add(websocket)
    try:
        # send initial candidate labels
        try:
            candidates = get_candidate_exchanges()
            if not isinstance(candidates, list):
                candidates = list(candidates)
            await websocket.send_text(json.dumps({"type": "labels", "labels": candidates}))
            globals()["_latest_labels"] = candidates
            logger.info("Sent initial candidate labels to client, count=%d", len(candidates))
        except Exception:
            logger.exception("Failed to send initial candidate labels")

        # send cached latest payload if exists and warm ticks
        try:
            last = globals().get("_latest_payload")
            if last:
                await websocket.send_text(json.dumps(last, default=str))
                logger.info("Sent cached latest payload to new client")
            # warm with individual latest ticks
            for lbl, info in brdc.latest.items():
                # send tick with metadata
                await websocket.send_text(json.dumps({
                    "type": "tick",
                    "label": lbl,
                    "price": info["price"],
                    "ts_source": info.get("ts_source"),
                    "ts_server": info.get("ts_server"),
                    "latency_exchange_ms": info.get("latency_exchange_ms"),
                }))
        except Exception:
            logger.debug("Failed to send cached payload / warm ticks to new client")

        # receive messages from client (pair set/clear, ping_client)
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "set_pair":
                    global _current_pair
                    _current_pair = msg.get("pair")
                    logger.info("Pair override set to %s", _current_pair)
                elif msg.get("type") == "clear_pair":
                    _current_pair = None
                    logger.info("Pair override cleared")
                elif msg.get("type") == "ping_client":
                    # echo back with server ts for RTT measurement
                    ts_client = msg.get("ts_client")
                    await websocket.send_text(json.dumps({
                        "type": "pong",
                        "ts_client": ts_client,
                        "ts_server": int(time.time() * 1000)
                    }))
            except Exception:
                logger.debug("Failed to parse ws client message")
    except WebSocketDisconnect:
        brdc.remove(websocket)


def normalize_table_obj(table_obj, labels_fallback):
    """
    Normalize table_obj into canonical dict:
      { "labels": [...], "matrix": [[str,...],[...]], "mean": maybe }
    """
    def _cell_to_str(v):
        if v is None:
            return "-"
        if isinstance(v, (int, float)):
            return f"{v:.4f}"
        if isinstance(v, str):
            return v
        try:
            return str(v)
        except Exception:
            return "-"

    if isinstance(table_obj, dict):
        labels = table_obj.get("labels") or labels_fallback or []
        matrix = table_obj.get("matrix") or []
        mean = table_obj.get("mean", None)
    else:
        labels = getattr(table_obj, "labels", None) or labels_fallback or []
        matrix = getattr(table_obj, "matrix", None) or []
        mean = getattr(table_obj, "mean", None)

    labels = [str(l) for l in labels]
    n = len(labels)
    norm_matrix = []

    if isinstance(matrix, list) and matrix and all(isinstance(row, (list, tuple)) for row in matrix):
        for i in range(n):
            if i < len(matrix):
                row = matrix[i] or []
                new_row = []
                for j in range(n):
                    if j < len(row):
                        new_row.append(_cell_to_str(row[j]))
                    else:
                        new_row.append("-")
                norm_matrix.append(new_row)
            else:
                norm_matrix.append(["-"] * n)
    else:
        norm_matrix = [["-"] * n for _ in range(n)]

    for i in range(n):
        if i >= len(norm_matrix):
            norm_matrix.append(["-"] * n)
        else:
            row = norm_matrix[i]
            if len(row) < n:
                row.extend(["-"] * (n - len(row)))
            elif len(row) > n:
                norm_matrix[i] = [str(x) for x in row[:n]]
            norm_matrix[i] = [_cell_to_str(c) for c in norm_matrix[i]]

    out = {"labels": labels, "matrix": norm_matrix}
    if mean is not None:
        out["mean"] = mean
    return out


async def poll_loop():
    """
    Poll fallback loop: queries Redis snapshot fast-path, otherwise fetch_all_prices REST fallback.
    """
    global _current_pair
    logger.info("Starting poll loop with interval %.2f s", POLL_INTERVAL_SECONDS)
    # optional startup test payload to help client initialization
    await asyncio.sleep(0.5)
    try:
        test_labels = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]
        test_update = {
            "type": "update",
            "pair": None,
            "prices": {test_labels[0]: 0, test_labels[1]: 0},
            "table": {"labels": test_labels, "matrix": [["-", "-"], ["-", "-"]], "mean": None},
            "ts_server": int(time.time() * 1000),
        }
        await brdc.broadcast_json({"type": "labels", "labels": test_labels})
        await brdc.broadcast_json(test_update)
    except Exception:
        logger.debug("Failed to send startup test payloads")

    while True:
        try:
            # prices may be either: label -> float (from fetcher) or label -> dict (from snapshot)
            prices: Dict[str, Any] = {}
            labels: List[str] = []
            mean = None
            used_redis = False

            if USE_REDIS_SNAPSHOT:
                try:
                    symbols = await list_snapshot_symbols()
                    now_ms = int(time.time() * 1000)
                    for sym in symbols:
                        snap = await read_snapshot(sym)
                        if not snap:
                            continue
                        price = snap.get("price")
                        ts = snap.get("ts")
                        if price is None:
                            continue
                        fresh = True
                        if ts is not None:
                            try:
                                age = now_ms - int(ts)
                                if age > SNAPSHOT_MAX_AGE_MS:
                                    fresh = False
                            except Exception:
                                fresh = False
                        if fresh:
                            exch = (snap.get("exchange") or "").upper()
                            label = f"{exch}:{sym}" if exch else str(sym)
                            try:
                                price_val = float(price)
                            except Exception:
                                continue
                            # timestamps: ts_source comes from producer (ms)
                            try:
                                ts_source = int(ts) if ts else int(time.time() * 1000)
                            except Exception:
                                ts_source = int(time.time() * 1000)
                            ts_server = int(time.time() * 1000)
                            latency_exchange_ms = max(0, ts_server - ts_source)
                            prices[label] = {
                                "price": price_val,
                                "ts_source": ts_source,
                                "ts_server": ts_server,
                                "latency_exchange_ms": latency_exchange_ms,
                            }
                            # warm in-memory latest
                            brdc.update_latest(label, price_val, ts_source)
                    if prices:
                        labels = list(prices.keys())
                        used_redis = True
                except Exception:
                    logger.exception("Error reading snapshots from Redis, falling back to REST")
                    used_redis = False

            if not used_redis:
                res = await fetch_all_prices(pair_override=_current_pair)
                labels = res.get("exchanges", [])
                prices = res.get("prices", {})
                mean = res.get("mean")

            # build pairwise table; build_pairwise_table expects labels list and prices mapping label->float
            # if prices contain dicts (snapshot path), provide a flat map for table builder
            prices_for_table: Dict[str, float] = {}
            for k, v in prices.items():
                if isinstance(v, dict):
                    prices_for_table[k] = v.get("price")
                else:
                    prices_for_table[k] = v

            table_obj = build_pairwise_table(list(prices_for_table.keys()), prices_for_table)
            table_payload = normalize_table_obj(table_obj, list(prices_for_table.keys()))

            ts_now = int(time.time() * 1000)

            # split prices into flat and meta for backward compatibility
            prices_flat: Dict[str, float] = {}
            prices_meta: Dict[str, Dict[str, Any]] = {}
            for lbl, v in prices.items():
                if isinstance(v, dict):
                    prices_flat[lbl] = v["price"]
                    prices_meta[lbl] = {
                        "ts_source": v.get("ts_source"),
                        "ts_server": v.get("ts_server"),
                        "latency_exchange_ms": v.get("latency_exchange_ms"),
                    }
                else:
                    prices_flat[lbl] = v
                    prices_meta[lbl] = {"ts_source": None, "ts_server": ts_now, "latency_exchange_ms": None}

            payload = {
                "type": "update",
                "pair": _current_pair,
                "prices": prices_flat,
                "prices_meta": prices_meta,
                "table": table_payload,
                "ts_server": ts_now,
            }

            logger.info(
                "Broadcasting update labels=%d prices=%d clients=%d (redis=%s)",
                len(table_payload.get("labels", [])),
                len(prices_flat),
                len(brdc.clients),
                used_redis,
            )

            globals()["_latest_labels"] = table_payload.get("labels", [])
            globals()["_latest_payload"] = payload

            await brdc.broadcast_json(payload)
        except Exception as e:
            logger.exception("Poll loop error: %s", e)
        await asyncio.sleep(max(1.0, POLL_INTERVAL_SECONDS))


async def ensure_stream_group(redis):
    try:
        await redis.xgroup_create(STREAM_KEY, STREAM_GROUP, id="0-0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" in str(e) or "exists" in str(e).lower():
            return
        logger.debug("xgroup create warning: %s", e)


async def stream_consumer_task(stop_event: asyncio.Event):
    """
    Read Redis Stream (XREADGROUP) and broadcast tick deltas as they arrive.
    Keeps in-memory latest map updated for new clients.
    """
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await ensure_stream_group(redis)
        while not stop_event.is_set():
            try:
                resp = await redis.xreadgroup(STREAM_GROUP, STREAM_CONSUMER, streams={STREAM_KEY: ">"}, count=100, block=1000)
                if not resp:
                    continue
                # resp -> list of (stream_name, [(id, {field: value}), ...])
                for stream_name, messages in resp:
                    for msg_id, fields in messages:
                        j = fields.get("j")
                        if not j:
                            await redis.xack(STREAM_KEY, STREAM_GROUP, msg_id)
                            continue
                        try:
                            rec = json.loads(j)
                        except Exception:
                            rec = None
                        if not rec:
                            await redis.xack(STREAM_KEY, STREAM_GROUP, msg_id)
                            continue

                        exch = (rec.get("e") or "").upper()
                        sym = rec.get("s")
                        # ts provided by producer (source timestamp, ms)
                        try:
                            ts_source = int(rec.get("ts", 0) or 0)
                        except Exception:
                            ts_source = int(time.time() * 1000)
                        try:
                            price = float(rec.get("p") or 0)
                        except Exception:
                            price = 0.0

                        label = f"{exch}:{sym}"

                        # update in-memory latest map (stores both price and ts_source/ts_server/latency)
                        try:
                            brdc.update_latest(label, price, ts_source)
                        except Exception:
                            logger.debug("Failed to update broadcaster.latest for %s", label)

                        # compute server-side timestamps and latency
                        ts_server = int(time.time() * 1000)
                        latency_exchange_ms = max(0, ts_server - ts_source)

                        # broadcast tick delta including ts_source and ts_server
                        payload = {
                            "type": "tick",
                            "label": label,
                            "price": price,
                            "ts_source": ts_source,
                            "ts_server": ts_server,
                            "latency_exchange_ms": latency_exchange_ms,
                        }
                        await brdc.broadcast_json(payload)
                        await redis.xack(STREAM_KEY, STREAM_GROUP, msg_id)
            except Exception as e:
                logger.exception("stream_consumer loop error: %s", e)
                await asyncio.sleep(0.5)
    finally:
        await redis.close()
import csv
from typing import Tuple

def _compute_pct(a: float, b: float) -> float:
    denom = (a + b) / 2.0
    if denom == 0:
        return 0.0
    return 100.0 * (b - a) / denom

def _build_pair_rows_from_latest(latest: Dict[str, Dict], min_pct: float = 0.0) -> List[Tuple[str, str, float]]:
    labels = list(latest.keys())
    rows: List[Tuple[str, str, float]] = []
    for i in range(len(labels)):
        for j in range(i + 1, len(labels)):
            li = labels[i]
            lj = labels[j]
            try:
                pi = float(latest[li].get("price"))
                pj = float(latest[lj].get("price"))
            except Exception:
                continue
            if not (pi > 0 and pj > 0):
                continue
            if pi <= pj:
                cheaper, expensive, pct = li, lj, _compute_pct(pi, pj)
            else:
                cheaper, expensive, pct = lj, li, _compute_pct(pj, pi)
            if abs(pct) >= abs(min_pct):
                rows.append((cheaper, expensive, pct))
    rows.sort(key=lambda x: x[2], reverse=True)
    return rows

async def sqlite_dumper_task(out_db: str = DB_PATH,
                             interval_ms: int = 200,
                             min_pct: float = 0.0,
                             stop_event: Optional[asyncio.Event] = None,
                             write_history: bool = False):
    conn = _ensure_db(out_db)
    cur = conn.cursor()
    last_write_ts = 0.0

    try:
        while True:
            if stop_event and stop_event.is_set():
                break

            now_ms = time.time() * 1000.0

            # source: prefer brdc.latest; fallback to _latest_payload.prices
            latest_copy: Dict[str, Dict] = {}
            if brdc.latest:
                for lbl, info in brdc.latest.items():
                    p = info.get("price")
                    if p is None:
                        continue
                    try:
                        latest_copy[lbl] = {"price": float(p)}
                    except Exception:
                        continue
            else:
                payload = globals().get("_latest_payload") or {}
                prices = payload.get("prices") or {}
                for lbl, val in prices.items():
                    try:
                        latest_copy[lbl] = {"price": float(val)}
                    except Exception:
                        continue

            rows = _build_pair_rows_from_latest(latest_copy, min_pct=min_pct)

            if (now_ms - last_write_ts) >= interval_ms:
                tsms = int(time.time() * 1000)
                tsiso = datetime.utcfromtimestamp(tsms / 1000.0).isoformat() + "Z"
                with conn:
                    # upsert snapshot
                    for cheaper, expensive, pct in rows:
                        pair_key = f"{cheaper}|{expensive}"
                        conn.execute(
                            "INSERT INTO current_pairs(pair, cheaper, expensive, percent, ts_ms, ts_iso) "
                            "VALUES(?,?,?,?,?,?) "
                            "ON CONFLICT(pair) DO UPDATE SET "
                            "cheaper=excluded.cheaper, "
                            "expensive=excluded.expensive, "
                            "percent=excluded.percent, "
                            "ts_ms=excluded.ts_ms, "
                            "ts_iso=excluded.ts_iso",
                            (pair_key, cheaper, expensive, float(pct), tsms, tsiso)
                        )
                    # optional history append
                    if write_history and rows:
                        conn.executemany(
                            "INSERT INTO history(pair, cheaper, expensive, percent, ts_ms, ts_iso) VALUES(?,?,?,?,?,?)",
                            [(f"{c}|{e}", c, e, float(p), tsms, tsiso) for c, e, p in rows]
                        )

                last_write_ts = time.time() * 1000.0
                logger.debug("sqlite_dumper: wrote %d rows to %s", len(rows), out_db)


            await asyncio.sleep(max(0.05, interval_ms / 1000.0))
    except Exception:
        logger.exception("sqlite_dumper_task error")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@app.on_event("startup")
async def on_startup():
    logger.info("Starting poll loop with interval %.2f s", POLL_INTERVAL_SECONDS)

    asyncio.create_task(poll_loop())

    stop_evt = asyncio.Event()
    task = asyncio.create_task(stream_consumer_task(stop_evt))
    globals()["_stream_consumer_task"] = task
    globals()["_stream_consumer_stop"] = stop_evt

    # start SQLite dumper
    sqlite_stop = asyncio.Event()
    sqlite_task = asyncio.create_task(sqlite_dumper_task(
        out_db=DB_PATH,
        interval_ms=200,
        min_pct=0.0,
        stop_event=sqlite_stop,
        write_history=False  # set True if you want history table populated
    ))
    globals()["_sqlite_dumper_task"] = sqlite_task
    globals()["_sqlite_dumper_stop"] = sqlite_stop

    async def ensure_first_broadcast():
        await asyncio.sleep(0.1)
        last = globals().get("_latest_payload")
        if last:
            last["ts_server"] = int(time.time() * 1000)
            await brdc.broadcast_json(last)

    asyncio.create_task(ensure_first_broadcast())



@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutting down: closing exchange instances and stream consumer")

    stop_evt = globals().get("_stream_consumer_stop")
    task = globals().get("_stream_consumer_task")
    if stop_evt and isinstance(stop_evt, asyncio.Event):
        stop_evt.set()
    if task:
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except Exception:
            try:
                task.cancel()
            except Exception:
                pass

    # stop SQLite dumper
    sqlite_stop = globals().get("_sqlite_dumper_stop")
    sqlite_task = globals().get("_sqlite_dumper_task")
    if sqlite_stop and isinstance(sqlite_stop, asyncio.Event):
        sqlite_stop.set()
    if sqlite_task:
        try:
            await asyncio.wait_for(sqlite_task, timeout=3.0)
        except Exception:
            try:
                sqlite_task.cancel()
            except Exception:
                pass

    try:
        await close_all_exchanges()
    except Exception:
        logger.exception("Error closing exchanges")




if __name__ == "__main__":
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=False)
