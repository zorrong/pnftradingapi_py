from fastapi import APIRouter, Query, Header
from typing import Optional, List
from adapters.mt5 import fetch_mt5_ohlcv, fetch_mt5_symbols
from lang import en as lang_en, vin as lang_vi
import time

router = APIRouter(prefix="/mt5", tags=["MT5"])

# In-memory TTL cache
CACHE_TTL_SECONDS = 30
_CACHE = {}

def _pick_lang(lang: str | None, accept_language: str | None):
    code = (lang or "").lower()
    if code in ("vin", "vi", "vn", "vietnamese"):
        return lang_vi, "vin"
    if code in ("en", "english"):
        return lang_en, "en"
    al = (accept_language or "").lower()
    if "vi" in al or "vn" in al:
        return lang_vi, "vin"
    return lang_en, "en"

def _cache_key(name: str, **params) -> str:
    items = sorted((k, v) for k, v in params.items())
    return f"{name}|" + "&".join(f"{k}={v}" for k, v in items)

def _cache_get(key: str):
    now = time.time()
    entry = _CACHE.get(key)
    if not entry:
        return None
    exp, val = entry
    if exp > now:
        return val
    _CACHE.pop(key, None)
    return None

def _cache_set(key: str, value: object, ttl: int | None):
    ttl_eff = CACHE_TTL_SECONDS if ttl is None else max(0, int(ttl))
    if ttl_eff == 0:
        return
    _CACHE[key] = (time.time() + ttl_eff, value)

@router.get("/ohlcv")
async def ohlcv_mt5(
    symbol: str = Query(..., description="Example: EURUSD"),
    interval: str = Query("1h", description="1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 1w, 1M"),
    days: int | None = Query(7, description="Count of days if from/to not provided"),
    from_ts: int | None = Query(None, description="Start Epoch seconds"),
    to_ts: int | None = Query(None, description="End Epoch seconds"),
    limit: int | None = Query(None, description="Max candles"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    # If the lang module doesn't have mt5 specific keys, we might need to fallback or use generic string
    # Assuming generic structure for now.
    
    key = _cache_key("mt5_ohlcv", symbol=symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    candles = await fetch_mt5_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
    
    title = f"MT5 {symbol}" 
    # Try to verify if lang_mod has support, if not just use hardcoded English or simple string
    
    resp = {
        "lang": lang_code,
        "title": title,
        "source": "mt5",
        "symbol": symbol,
        "interval": interval,
        "count": len(candles),
        "candles": candles
    }
    _cache_set(key, resp, cache_ttl)
    return resp

@router.get("/symbols")
async def symbols_mt5(
    cache_ttl: int | None = Query(300, description="Cache TTL (seconds)"),
):
    key = "mt5_symbols"
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    symbols = await fetch_mt5_symbols()
    
    resp = {
        "count": len(symbols),
        "source": "mt5",
        "symbols": symbols
    }
    _cache_set(key, resp, cache_ttl)
    return resp
