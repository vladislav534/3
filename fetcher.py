# fetcher.py
import asyncio
import logging
import math
from typing import Dict, List, Optional, Tuple

import ccxt.async_support as ccxt

from config import (
    EXCHANGES,
    DEFAULT_PAIR,
    PAIRS_ALTERNATIVES,
    REQUEST_TIMEOUT,
    MAX_CONCURRENT_REQUESTS,
)

logger = logging.getLogger("fetcher")
logging.basicConfig(level=logging.INFO)

# cache of exchange instances to reuse across calls
_EXCHANGES_INST: Dict[str, ccxt.Exchange] = {}
# protect concurrent creation
_ex_create_lock = asyncio.Lock()


async def _ensure_exchange(name: str) -> Optional[ccxt.Exchange]:
    """
    Return a cached async ccxt exchange instance or create it.
    If creation fails, return None.
    """
    global _EXCHANGES_INST
    if name in _EXCHANGES_INST:
        return _EXCHANGES_INST[name]
    async with _ex_create_lock:
        if name in _EXCHANGES_INST:
            return _EXCHANGES_INST[name]
        try:
            ex_cls = getattr(ccxt, name)
        except AttributeError:
            logger.warning("Exchange %s not found in ccxt, skipping", name)
            return None
        try:
            inst = ex_cls({
                "enableRateLimit": True,
                "timeout": max(REQUEST_TIMEOUT, 10) * 1000,
            })
            # some exchanges require load_markets to be called once
            try:
                await inst.load_markets()
            except Exception:
                # some exchanges fail on load_markets in sandbox/dev; ignore here
                logger.debug("load_markets failed for %s (ignored)", name, exc_info=True)
            _EXCHANGES_INST[name] = inst
            logger.info("Created exchange instance %s", name)
            return inst
        except Exception:
            logger.exception("Failed to create exchange instance %s", name)
            return None


async def close_all_exchanges():
    """
    Close and release all cached exchange instances.
    """
    global _EXCHANGES_INST
    exs = list(_EXCHANGES_INST.items())
    _EXCHANGES_INST = {}
    errs = []
    for name, inst in exs:
        try:
            await inst.close()
            logger.info("Closed exchange %s", name)
        except Exception as e:
            errs.append((name, e))
            logger.warning("Error closing exchange %s: %s", name, e)
    if errs:
        logger.warning("Some exchanges failed to close: %s", errs)


def _make_candidate_pairs_for_exchange(exchange_name: str, override_pair: Optional[str]) -> List[str]:
    """
    Build priority list of pair strings to try for a given exchange.
    override_pair: if provided, try only this and its variants first.
    Uses PAIRS_ALTERNATIVES; falls back to DEFAULT_PAIR.
    """
    if override_pair:
        # try override as-is first, then fallbacks in config
        cand = []
        cand.append(override_pair)
        # if there is alt list for the exchange, append them
        if exchange_name in PAIRS_ALTERNATIVES:
            cand.extend(PAIRS_ALTERNATIVES[exchange_name])
        # global star alternatives
        cand.extend(PAIRS_ALTERNATIVES.get("*", []))
        # finally default
        cand.append(DEFAULT_PAIR)
        # unique preserving order
        seen = set()
        out = []
        for p in cand:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out
    # no override
    if exchange_name in PAIRS_ALTERNATIVES:
        return PAIRS_ALTERNATIVES[exchange_name] + PAIRS_ALTERNATIVES.get("*", []) + [DEFAULT_PAIR]
    return PAIRS_ALTERNATIVES.get("*", []) + [DEFAULT_PAIR]


async def _fetch_price_for_exchange(
    exchange_name: str,
    pair_candidates: List[str],
    semaphore: asyncio.Semaphore,
) -> Optional[Tuple[str, float, str]]:
    """
    Try to fetch ticker/price for the first pair that the exchange supports.
    Returns tuple (label, price, used_pair) where label is 'EXCHANGE:PAIR' (uppercase exchange),
    or None if no price could be fetched.
    """
    inst = await _ensure_exchange(exchange_name)
    if not inst:
        return None

    async with semaphore:
        for candidate in pair_candidates:
            # ccxt expects symbol like "SUI/USDT" or "SUI/USDT:USDT" depending on exchange,
            # try a few normalizations:
            symbols_to_try = [candidate]
            if "/" not in candidate and ":" not in candidate:
                # maybe user passed "SUIUSDT" -> try with slash
                symbols_to_try.append(f"{candidate[:3]}/{candidate[3:]}" if len(candidate) >= 6 else candidate)
            # uppercase symbols
            symbols_to_try = list(dict.fromkeys([s.upper() for s in symbols_to_try]))

            for sym in symbols_to_try:
                try:
                    # use asyncio.wait_for to enforce per-request timeout (seconds)
                    # REQUEST_TIMEOUT in config is seconds (int)
                    timeout_s = max(1, int(REQUEST_TIMEOUT))
                    res = await asyncio.wait_for(inst.fetch_ticker(sym), timeout=timeout_s)
                    # fetch_ticker returns structure with 'last' or 'close'
                    price = res.get("last") or res.get("close") or res.get("price") or None
                    if price is None:
                        # sometimes tickers have 'info' nested fields
                        info = res.get("info", {})
                        # try common fields
                        price = info.get("lastPrice") or info.get("last") or info.get("price")
                    if price is None:
                        continue
                    # build label using exchange uppercased
                    label = f"{exchange_name.upper()}:{sym}"
                    try:
                        price_val = float(price)
                    except Exception:
                        continue
                    logger.debug("Fetched %s -> %s from %s", sym, price_val, exchange_name)
                    return label, price_val, sym
                except asyncio.TimeoutError:
                    logger.debug("Timeout fetching %s from %s", sym, exchange_name)
                except ccxt.BaseError as e:
                    # ccxt-specific errors: symbol not available, DDoS protection, etc.
                    msg = str(e)
                    # if it's symbol not found / not available, try next candidate
                    logger.debug("ccxt error for %s@%s: %s", sym, exchange_name, msg)
                except Exception:
                    logger.exception("Unexpected error fetching %s from %s", sym, exchange_name)
        # no candidate yielded price
        logger.info("No supported pair found for exchange %s (tried %s)", exchange_name, pair_candidates)
        return None


async def fetch_all_prices(pair_override: Optional[str] = None) -> Dict:
    """
    Query all configured exchanges concurrently (bounded by MAX_CONCURRENT_REQUESTS)
    and return:
      {
        "exchanges": [labels...],    # labels in same order as prices keys
        "prices": { label: price, ... },
        "mean": float|None
      }
    label format: "EXCHANGE:PAIR" (e.g., "BINANCE:SUI/USDT")
    """
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []
    for ex in EXCHANGES:
        pair_cands = _make_candidate_pairs_for_exchange(ex, pair_override)
        tasks.append(_fetch_price_for_exchange(ex, pair_cands, sem))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    prices: Dict[str, float] = {}
    labels: List[str] = []
    for idx, res in enumerate(results):
        # If the task raised or was cancelled, skip and log
        if isinstance(res, Exception):
            # CancelledError is a subclass of Exception; handle explicitly for clarity
            if isinstance(res, asyncio.CancelledError):
                logger.debug("fetch task %d was cancelled", idx)
            else:
                logger.warning("Exception during fetch_all_prices task[%d]: %s", idx, res)
            continue

        # valid successful result: unpack and record
        if not res:
            continue
        try:
            label, price_val, used_pair = res
        except Exception:
            logger.warning("Unexpected fetch task result format at idx %d: %r", idx, res)
            continue

        # ensure unique labels
        if label in prices:
            logger.debug("Duplicate label %s, skipping", label)
            continue
        prices[label] = price_val
        labels.append(label)

    mean = None
    if prices:
        vals = list(prices.values())
        mean = float(sum(vals) / len(vals))

    return {"exchanges": labels, "prices": prices, "mean": mean}

