# exchanges.py
from typing import Dict, List
import ccxt
from config import EXCHANGES, DEFAULT_PAIR, PAIRS_ALTERNATIVES, API_KEYS

ALIASES = {
    "htx": "huobi",
    "okex": "okx",
    "gate": "gateio",   # поддержка варианта "gate"
    "bitmart": "bitmart"
}

def _normalize_id(ex_id: str) -> str:
    return ALIASES.get(ex_id.strip().lower(), ex_id.strip().lower())

# Валидируем доступные в ccxt идентификаторы один раз
def _validate_once() -> List[str]:
    supported = set(ccxt.exchanges)
    result = []
    seen = set()
    for ex in EXCHANGES:
        nid = _normalize_id(ex)
        if nid in supported and nid not in seen:
            result.append(nid); seen.add(nid)
        else:
            alt = nid.replace('-', '').replace(' ', '')
            if alt in supported and alt not in seen:
                result.append(alt); seen.add(alt)
            else:
                print(f"[WARN] Exchange '{ex}' normalized to '{nid}' is NOT found in ccxt; it will be skipped")
    print(f"[INFO] Final desired exchange candidates ({len(result)}): {result}")
    return result

_VALIDATED_CANDIDATES = _validate_once()

def get_candidate_exchanges() -> List[str]:
    # Возвращает желательный набор бирж (валидированный против ccxt), без фильтра по активу
    return _VALIDATED_CANDIDATES.copy()

def get_pair_alternatives_for_exchange(exchange_id: str):
    return PAIRS_ALTERNATIVES.get(exchange_id) or PAIRS_ALTERNATIVES.get(exchange_id.lower()) or PAIRS_ALTERNATIVES.get("*", [DEFAULT_PAIR])

def get_api_credentials(exchange_id: str) -> Dict:
    return API_KEYS.get(exchange_id) or API_KEYS.get(exchange_id.lower()) or {}
