# fetcher.py
import asyncio
import logging
import math
from typing import Dict, List, Optional, Tuple, Any, Mapping

import ccxt.async_support as ccxt

from config import (
    EXCHANGES,
    DEFAULT_PAIR,
    PAIRS_ALTERNATIVES,
    REQUEST_TIMEOUT,
    MAX_CONCURRENT_REQUESTS,
)

logger = logging.getLogger("fetcher")
# logging.basicConfig(level=logging.INFO)

# cache of exchange instances to reuse across calls
_EXCHANGES_INST: Dict[str, ccxt.Exchange] = {}
_ex_create_lock = asyncio.Lock()

# markets + symbol resolution caches
_MARKETS_CACHE: Dict[str, Dict[str, Any]] = {}          # ex.id -> markets
_SYMBOL_MAP_CACHE: Dict[str, Dict[str, str]] = {}       # ex.id -> canonical "BASE/QUOTE" -> exchange symbol
import re

def _sanitize_error_message(exc: Exception, max_len: int = 200) -> str:
    """
    Return a short, safe string for logging from an exception.
    - prefer short exception type + message
    - strip HTML tags if present
    - truncate to max_len
    """
    if exc is None:
        return ""
    msg = ""
    try:
        msg = str(exc)
    except Exception:
        msg = repr(exc)

    # strip obvious HTML tags — leaves plain text if possible
    try:
        # remove tags
        msg_nohtml = re.sub(r"<[^>]+>", "", msg)
        # collapse whitespace/newlines
        msg_nohtml = re.sub(r"\s+", " ", msg_nohtml).strip()
        # truncate
        if len(msg_nohtml) > max_len:
            msg_nohtml = msg_nohtml[: max_len - 3] + "..."
        return msg_nohtml
    except Exception:
        # fallback to truncated raw message
        return (msg[: max_len - 3] + "...") if len(msg) > max_len else msg


async def _ensure_exchange(name: str) -> Optional[ccxt.Exchange]:
    """
    Return a cached async ccxt exchange instance or create it.
    """
    if name in _EXCHANGES_INST:
        return _EXCHANGES_INST[name]
    async with _ex_create_lock:
        if name in _EXCHANGES_INST:
            return _EXCHANGES_INST[name]
        try:
            ex_cls = getattr(ccxt, name)
        except AttributeError:
            logger.debug("Exchange %s not found in ccxt, skipping", name)
            return None
        try:
            inst = ex_cls({
                "enableRateLimit": True,
                # ccxt expects timeout in ms
                "timeout": max(REQUEST_TIMEOUT, 5) * 1000,
            })
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
    exs = list(_EXCHANGES_INST.items())
    _EXCHANGES_INST.clear()
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
    """
    if override_pair:
        cand: List[str] = [override_pair]
        cand.extend(PAIRS_ALTERNATIVES.get(exchange_name, []))
        cand.extend(PAIRS_ALTERNATIVES.get("*", []))
        cand.append(DEFAULT_PAIR)
        seen = set(); out = []
        for p in cand:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out
    return PAIRS_ALTERNATIVES.get(exchange_name, []) + PAIRS_ALTERNATIVES.get("*", []) + [DEFAULT_PAIR]


async def _ensure_markets(inst: ccxt.Exchange, type_hint: Optional[str] = None) -> None:
    """
    Load markets and cache them, preferring spot for exchanges with multi-market behavior.
    Prevents OKX base/quote NoneType errors.
    """
    exid = inst.id.lower()
    params: Dict[str, Any] = {}
    if exid in ("okx", "bybit", "bitget", "gateio"):
        # Prefer spot unless explicitly asked otherwise
        params["type"] = type_hint or "spot"

    try:
        await inst.load_markets(params)
    except Exception:
        # fallback without params
        await inst.load_markets()

    _MARKETS_CACHE[inst.id] = inst.markets or {}


def _safe_price_from_ticker(t: Mapping[str, Any]) -> Optional[float]:
    """
    Robust extraction of a numeric price from various ticker dict shapes.
    """
    if not t or not isinstance(t, Mapping):
        return None

    # common direct fields
    for k in ("last", "lastPrice", "close", "price", "p"):
        v = t.get(k)
        if v is None:
            continue
        try:
            val = float(str(v).replace(",", ".").strip())
            if math.isfinite(val) and val > 0:
                return val
        except Exception:
            continue

    # nested info
    info = t.get("info")
    if isinstance(info, Mapping):
        for k in ("last", "lastPrice", "close", "price"):
            v = info.get(k)
            if v is None:
                continue
            try:
                val = float(str(v).replace(",", ".").strip())
                if math.isfinite(val) and val > 0:
                    return val
            except Exception:
                continue

    # bid/ask average
    bid = t.get("bid") or t.get("bestBid")
    ask = t.get("ask") or t.get("bestAsk")
    try:
        if bid is not None and ask is not None:
            val = (float(str(bid).replace(",", ".")) + float(str(ask).replace(",", "."))) / 2.0
            if math.isfinite(val) and val > 0:
                return val
    except Exception:
        pass

    return None


def _resolve_symbol(inst: ccxt.Exchange, canonical: str) -> Optional[str]:
    """
    Map canonical 'BASE/QUOTE' to an exchange-specific symbol present in inst.markets.
    Handles OKX/HitBTC/LBank/Bybit/Bitget/Gate naming differences.
    """
    exid = inst.id.lower()
    markets = _MARKETS_CACHE.get(inst.id) or inst.markets or {}
    cache = _SYMBOL_MAP_CACHE.setdefault(inst.id, {})

    if canonical in cache:
        return cache[canonical]

    # direct hit
    if canonical in markets:
        cache[canonical] = canonical
        return canonical

    base, quote = None, None
    try:
        base, quote = canonical.split("/")
    except Exception:
        # if canonical is not 'BASE/QUOTE', try to infer but prefer strict format in config
        return None

    candidates = set()

    # Exchange-specific variants
    if exid == "hitbtc":
        candidates.update({f"{base}{quote}", canonical})
    elif exid == "lbank":
        candidates.update({
            f"{base}_{quote}".lower(),
            f"{base}{quote}".upper(),
            f"{base}{quote}".lower(),
            canonical
        })
    elif exid in ("bybit", "bitget", "gateio"):
        candidates.update({canonical, f"{base}-{quote}"})
    elif exid == "okx":
        # OKX spot should be canonical; derivatives have suffixes; we want spot
        candidates.update({canonical})

    # test candidates against markets keys
    for c in list(candidates):
        if c in markets:
            cache[canonical] = c
            return c

    # last-resort: search by base/quote inside market info (spot only)
    for sym, info in markets.items():
        try:
            if (info.get("base") == base and info.get("quote") == quote and (info.get("type") or "spot") == "spot"):
                cache[canonical] = sym
                return sym
        except Exception:
            continue

    return None


async def _fetch_price_for_exchange(
    exchange_name: str,
    pair_candidates: List[str],
    semaphore: asyncio.Semaphore,
) -> Optional[Tuple[str, float, str]]:
    """
    Try to fetch ticker/price for the first supported pair.
    Returns (label, price, used_pair) where label is 'EXCHANGE:BASE/QUOTE'.
    """
    inst = await _ensure_exchange(exchange_name)
    if not inst:
        return None

    # ensure markets loaded and cached
    await _ensure_markets(inst, type_hint="spot")

    async with semaphore:
        for candidate in pair_candidates:
            # enforce canonical format 'BASE/QUOTE' for resolution
            cand = candidate if "/" in candidate else f"{candidate[:3]}/{candidate[3:]}" if len(candidate) >= 6 else candidate
            canonical = cand.upper()

            # resolve to exchange-specific symbol
            sym = _resolve_symbol(inst, canonical)
            if not sym:
                logger.debug("Symbol %s not found in markets for %s", canonical, exchange_name)
                continue

            # strict per-request timeout (seconds)
            timeout_s = max(1, int(REQUEST_TIMEOUT))
            ticker = None
            try:
                ticker = await asyncio.wait_for(inst.fetch_ticker(sym), timeout=timeout_s)
            except asyncio.TimeoutError:
                logger.debug("Timeout fetching %s from %s", sym, exchange_name)
            except ccxt.DDoSProtection as e:
                logger.warning("DDoS/Rate limit from %s when fetching %s: %s", exchange_name, sym, e)
            except ccxt.NetworkError as e:
                logger.debug("Network error fetching %s from %s: %s", sym, exchange_name, e)
            except ccxt.ExchangeError as e:
                # symbol not available, deprecated API, etc. — логируем в debug, чтобы не спамить
                logger.debug("Exchange error for %s@%s: %s", sym, exchange_name, e)
            except Exception:
                # действительно неожиданные проблемы — поднимаем warning с трассой (или exception)
                logger.exception("Unexpected error fetching %s from %s", sym, exchange_name)

                continue

            price = _safe_price_from_ticker(ticker) if ticker else None
            if price is None:
                logger.debug("No valid price from %s@%s; ticker=%s", sym, exchange_name, ticker)
                continue

            # label stays canonical for consistency
            label = f"{exchange_name.upper()}:{canonical}"
            logger.debug("Fetched %s -> %s from %s (api sym=%s)", canonical, price, exchange_name, sym)
            return label, float(price), canonical

        logger.info("No supported pair found for exchange %s (tried %s)", exchange_name, pair_candidates)
        return None


async def fetch_all_prices(pair_override: Optional[str] = None) -> Dict:
    """
    Query all configured exchanges concurrently and return:
      {
        "exchanges": [labels...],
        "prices": { label: price, ... },
        "mean": float|None
      }
    label format: "EXCHANGE:BASE/QUOTE"
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
        if isinstance(res, Exception):
            if isinstance(res, asyncio.CancelledError):
                logger.debug("fetch task %d was cancelled", idx)
            else:
                safe = _sanitize_error_message(res, max_len=240)
                # логируем кратко; level warning только для неожиданных ошибок
                logger.warning("fetch_all_prices task[%d] error: %s", idx, safe)
            continue



        if not res:
            continue

        try:
            label, price_val, used_pair = res
        except Exception:
            logger.warning("Unexpected fetch task result format at idx %d: %r", idx, res)
            continue

        if label in prices:
            logger.debug("Duplicate label %s, skipping", label)
            continue
        prices[label] = float(price_val)
        labels.append(label)

    mean: Optional[float] = None
    if prices:
        vals = list(prices.values())
        mean = float(sum(vals) / len(vals)) if vals else None

    return {"exchanges": labels, "prices": prices, "mean": mean}
