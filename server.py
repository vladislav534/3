# server.py
import asyncio
import json
import logging
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from fetcher import fetch_all_prices, close_all_exchanges
from calculator import build_pairwise_table
from config import POLL_INTERVAL_SECONDS, USE_REDIS_SNAPSHOT, SNAPSHOT_MAX_AGE_MS
from redis_client import read_snapshot, list_snapshot_symbols
from redis_client import read_snapshot, list_snapshot_symbols
from exchanges import get_candidate_exchanges

app = FastAPI()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("server")

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutting down: closing exchange instances")
    await close_all_exchanges()

# Serve static files (index.html)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

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

    async def send_json(self, data):
        remove = []
        for ws in list(self.active):
            try:
                await ws.send_text(json.dumps(data))
            except Exception:
                remove.append(ws)
        for ws in remove:
            self.disconnect(ws)

manager = ConnectionManager()

# Global state
_current_pair = None  # if set via WS message, overrides defaults

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # send initial config: candidate exchange labels (validated once)
        try:
            candidates = get_candidate_exchanges()
            await websocket.send_text(json.dumps({"type": "labels", "labels": candidates}))
        except Exception:
            # best-effort; continue even if sending initial labels fails
            pass
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
            except Exception:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)

async def poll_loop():
    """
    Основной loop — опрашивает биржи и рассылает результаты всем подключённым клиентам.
    """
    global _current_pair
    while True:
        try:
            prices = {}
            labels = []
            mean = None
            used_redis = False
            if USE_REDIS_SNAPSHOT:
                try:
                    # list available snapshot symbols (PoC). For production replace with maintained set.
                    symbols = await list_snapshot_symbols()
                    now_ms = int(asyncio.get_event_loop().time() * 1000)
                    for sym in symbols:
                        snap = await read_snapshot(sym)
                        if not snap:
                            continue
                        price = snap.get("price")
                        ts = snap.get("ts")
                        if price is None:
                            continue
                        # freshness check
                        fresh = True
                        if ts is not None:
                            age = now_ms - int(ts)
                            if age > SNAPSHOT_MAX_AGE_MS:
                                fresh = False
                        if fresh:
                            prices[sym] = price
                    if prices:
                        labels = list(prices.keys())
                        used_redis = True
                except Exception:
                    # if anything goes wrong reading Redis, fallback to REST
                    used_redis = False
            if not used_redis:
                res = await fetch_all_prices(pair_override=_current_pair)
                labels = res.get("exchanges", [])
                prices = res.get("prices", {})
                mean = res.get("mean")
            # строим квадратную попарную таблицу (симметричная формула)
            table = build_pairwise_table(labels, prices)
            payload = {
                "type": "update",
                "pair": _current_pair,
                "prices": prices,
                "table": table
            }
            await manager.send_json(payload)
        except Exception as e:
            logger.exception("Poll loop error: %s", e)
        await asyncio.sleep(max(1.0, POLL_INTERVAL_SECONDS))

@app.on_event("startup")
async def on_startup():
    logger.info("Starting poll loop with interval %.2f s", POLL_INTERVAL_SECONDS)
    asyncio.create_task(poll_loop())

if __name__ == "__main__":
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=False)
