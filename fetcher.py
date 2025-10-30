# fetcher.py
import asyncio
import logging
from typing import Dict, Optional, List
import ccxt.async_support as ccxt
from aiohttp import ClientError
from config import REQUEST_TIMEOUT, MAX_CONCURRENT_REQUESTS
from exchanges import get_candidate_exchanges, get_pair_alternatives_for_exchange, get_api_credentials

logger = logging.getLogger("fetcher")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

_EXCHANGE_INSTANCES: Dict[str, ccxt.Exchange] = {}
_MARKETS_LOADED: Dict[str, Dict] = {}
_MISSING_PAIR_LOGGED: Dict[str, bool] = {}

async def _get_or_create_exchange(exchange_id: str):
    ex = _EXCHANGE_INSTANCES.get(exchange_id)
    if ex:
        return ex
    if not hasattr(ccxt, exchange_id):
        raise ValueError(f"Exchange {exchange_id} not found in ccxt")
    cls = getattr(ccxt, exchange_id)
    creds = get_api_credentials(exchange_id)
    ex = cls({
        "timeout": REQUEST_TIMEOUT,
        "enableRateLimit": True,
        **creds
    })
    _EXCHANGE_INSTANCES[exchange_id] = ex
    return ex

async def _load_markets_once(ex):
    eid = getattr(ex, "id", None)
    if not eid:
        return None
    if eid in _MARKETS_LOADED:
        return _MARKETS_LOADED[eid]
    try:
        markets = await ex.load_markets()
        _MARKETS_LOADED[eid] = markets
        return markets
    except Exception as e:
        logger.debug("Could not load markets for %s: %s", eid, e)
        return None

def _choose_candidate_from_markets(markets: Dict, base_symbol: str) -> Optional[str]:
    base_upper = base_symbol.upper()
    for sym, meta in markets.items():
        try:
            if meta.get("base", "").upper() == base_upper:
                return sym
        except Exception:
            continue
    return None

async def _find_valid_symbol(ex, alternatives: List[str], base_symbol: str) -> Optional[str]:
    markets = await _load_markets_once(ex)
    # альтернативы
    for cand in alternatives:
        for v in (cand, cand.upper(), cand.lower()):
            if markets and v in markets:
                return v
    # поиск по базе
    if markets:
        pick = _choose_candidate_from_markets(markets, base_symbol)
        if pick:
            return pick
    return None

async def _fetch_price_for_exchange_instance(ex, exchange_id: str, symbol: str, retries: int = 2) -> Optional[float]:
    for attempt in range(1, retries + 2):
        try:
            ticker = await ex.fetch_ticker(symbol)
            if ticker is None:
                raise RuntimeError("Empty ticker")
            bid = ticker.get("bid")
            ask = ticker.get("ask")
            last = ticker.get("last")
            price = None
            if bid and ask:
                price = (bid + ask) / 2.0
            elif last:
                price = last
            # Protection: treat zero or extremely small prices as missing
            EPS = 1e-12
            if price is None or (isinstance(price, (int, float)) and price <= EPS):
                return None
            return float(price)
        except (ccxt.NetworkError, ccxt.RequestTimeout, ClientError) as e:
            logger.warning("Network error %s on %s/%s attempt %d", e, exchange_id, symbol, attempt)
            await asyncio.sleep(min(5.0, 0.5 * (2 ** (attempt - 1))))
        except ccxt.ExchangeError as e:
            msg = str(e).lower()
            logger.warning("Exchange error %s on %s/%s attempt %d", e, exchange_id, symbol, attempt)
            if "symbol" in msg or "not" in msg:
                return None
            await asyncio.sleep(0.5 * attempt)
        except Exception as e:
            logger.exception("Unexpected error fetching %s %s: %s", exchange_id, symbol, e)
            await asyncio.sleep(0.5 * attempt)
    logger.error("Failed to fetch after retries: %s %s", exchange_id, symbol)
    return None

async def fetch_price_for_exchange(exchange_id: str, pair_hint: Optional[str] = None) -> Optional[float]:
    await _semaphore.acquire()
    try:
        try:
            ex = await _get_or_create_exchange(exchange_id)
        except ValueError as e:
            logger.error("Exchange %s not available: %s", exchange_id, e)
            return None

        alternatives = get_pair_alternatives_for_exchange(exchange_id)
        if pair_hint:
            # если передали override — ставим его в начало кандидатов
            alternatives = [pair_hint] + [p for p in alternatives if p != pair_hint]

        symbol = await _find_valid_symbol(ex, alternatives, base_symbol="SUI")
        if not symbol:
            if not _MISSING_PAIR_LOGGED.get(exchange_id):
                logger.warning("No suitable pair found for %s; skipping for this asset", exchange_id)
                _MISSING_PAIR_LOGGED[exchange_id] = True
            return None

        return await _fetch_price_for_exchange_instance(ex, exchange_id, symbol)
    finally:
        _semaphore.release()

async def filter_exchanges_supporting_asset(candidate_exs: List[str], asset_base: str = "SUI") -> List[str]:
    """
    Возвращает список бирж из кандидатов, на которых есть markets с base == asset_base
    (или хотя бы один из pair alternatives). Используется перед массовым fetch.
    """
    supported = []
    tasks = []
    for ex_id in candidate_exs:
        tasks.append(_get_or_create_exchange(ex_id))
    # Создаём/инициализируем инстансы последовательно, но не вызываем load_markets тут параллельно для контроля
    created = []
    for t_ex_id, task in zip(candidate_exs, tasks):
        try:
            ex = await task
            created.append((t_ex_id, ex))
        except Exception as e:
            logger.debug("Could not create instance for %s: %s", t_ex_id, e)

    # Проверяем markets по очереди (чтобы не перегружать сеть)
    for ex_id, ex in created:
        try:
            markets = await _load_markets_once(ex)
            has = False
            if not markets:
                # попробуем alternatives presence
                alternatives = get_pair_alternatives_for_exchange(ex_id)
                for cand in alternatives:
                    if markets and (cand in markets or cand.upper() in markets or cand.lower() in markets):
                        has = True; break
            else:
                # есть markets — ищем base == asset_base
                for sym, meta in markets.items():
                    if meta.get("base", "").upper() == asset_base.upper():
                        has = True
                        break
            if has:
                supported.append(ex_id)
        except Exception as e:
            logger.debug("Error checking markets for %s: %s", ex_id, e)
    return supported

async def fetch_all_prices(pair_override: Optional[str] = None) -> Dict:
    """
    Возвращает структуру:
    {
      "exchanges": [list_of_exchanges_in_table_order],
      "prices": {ex: price_or_None},
      "mean": mean_price_or_None
    }
    Биржи без пары для текущего актива автоматически исключаются.
    """
    candidate_exs = get_candidate_exchanges()
    # если есть override pair, извлечём базовый символ (до '/')
    base_symbol = None
    if pair_override and '/' in pair_override:
        base_symbol = pair_override.split('/', 1)[0]
    else:
        base_symbol = "SUI"

    supported = await filter_exchanges_supporting_asset(candidate_exs, asset_base=base_symbol)
    prices_tasks = [fetch_price_for_exchange(ex, pair_override) for ex in supported]
    results = await asyncio.gather(*prices_tasks, return_exceptions=True)

    prices = {}
    for ex, r in zip(supported, results):
        if isinstance(r, Exception):
            logger.error("Task for %s raised exception: %s", ex, r)
            prices[ex] = None
        else:
            prices[ex] = r

    vals = [v for v in prices.values() if v is not None]
    mean = sum(vals) / len(vals) if vals else None

    return {"exchanges": supported, "prices": prices, "mean": mean}

async def close_all_exchanges():
    tasks = []
    for ex in list(_EXCHANGE_INSTANCES.values()):
        try:
            tasks.append(ex.close())
        except Exception as e:
            logger.debug("Error scheduling close for %s: %s", getattr(ex, 'id', '?'), e)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _EXCHANGE_INSTANCES.clear()
    _MARKETS_LOADED.clear()
    logger.info("All exchange instances closed and caches cleared")
