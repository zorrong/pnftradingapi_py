from fastapi import APIRouter, Query, Header
from adapters.binance import fetch_binance_ohlcv, fetch_binance_symbols
from adapters.kucoin import fetch_kucoin_ohlcv, fetch_kucoin_symbols
from adapters.gateio import fetch_gateio_ohlcv, fetch_gateio_symbols
from adapters.mexc import fetch_mexc_ohlcv, fetch_mexc_symbols
from adapters.bybit import fetch_bybit_ohlcv, fetch_bybit_symbols
from adapters.bitfinex import fetch_bitfinex_ohlcv, fetch_bitfinex_symbols
from adapters.coinbase import fetch_coinbase_ohlcv, fetch_coinbase_symbols
from adapters.okx import fetch_okx_ohlcv, fetch_okx_symbols
import time
import asyncio
from typing import Any, Dict, Tuple
from lang import en as lang_en, vin as lang_vi

router = APIRouter(prefix="/crypto", tags=["Crypto"])

# In-memory TTL cache đơn giản
CACHE_TTL_SECONDS = 30
_CACHE: Dict[str, Tuple[float, Any]] = {}
_COMMON_QUOTES = ["USDT", "USDC", "BTC", "ETH", "USD", "BUSD", "FDUSD"]


def _pick_lang(lang: str | None, accept_language: str | None) -> tuple[Any, str]:
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
    # hết hạn
    _CACHE.pop(key, None)
    return None


def _cache_set(key: str, value: Any, ttl: int | None):
    ttl_eff = CACHE_TTL_SECONDS if ttl is None else max(0, int(ttl))
    if ttl_eff == 0:
        return
    _CACHE[key] = (time.time() + ttl_eff, value)


def _split_base_quote(symbol: str) -> Tuple[str, str]:
    s = symbol.upper()
    if "-" in s:
        base, quote = s.split("-", 1)
        return base, quote
    if "_" in s:
        base, quote = s.split("_", 1)
        return base, quote
    for q in _COMMON_QUOTES:
        if s.endswith(q) and len(s) > len(q):
            return s[: -len(q)], q
    return s, ""


def _normalize_symbol(exchange: str, symbol: str) -> str:
    base, quote = _split_base_quote(symbol)
    if not base or not quote:
        return symbol.upper()
    if exchange in ("binance", "mexc", "bybit"):
        return f"{base}{quote}"
    if exchange == "kucoin":
        return f"{base}-{quote}"
    if exchange == "gateio":
        return f"{base}_{quote}"
    if exchange == "okx":
        return f"{base}-{quote}"  # OKX uses BTC-USDT
    if exchange == "coinbase":
        return f"{base}-{quote}"  # Coinbase uses BTC-USD
    if exchange == "bitfinex":
        return f"{base}-{quote}"  # Adapter will handle conversion to 'tBTCUSD'
    return symbol.upper()


@router.get("/ohlcv/binance")
async def ohlcv_binance(
    symbol: str = Query(..., description="Example: BTCUSDT"),
    interval: str = Query("1h", description="1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M"),
    days: int | None = Query(7, description="Count of days if from/to not provided"),
    from_ts: int | None = Query(None, description="Start Epoch seconds"),
    to_ts: int | None = Query(None, description="End Epoch seconds"),
    limit: int | None = Query(None, description="Max candles (1-1000)"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t
    key = _cache_key("binance", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_binance_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
    resp = {"lang": lang_code, "title": t("crypto.ohlcv.binance"), "source": "binance", "symbol": symbol, "interval": interval, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/kucoin")
async def ohlcv_kucoin(
    symbol: str = Query(..., description="Example: BTC-USDT"),
    interval: str = Query("1h", description="1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,1w,1M"),
    days: int | None = Query(7, description="Count of days if from/to not provided"),
    from_ts: int | None = Query(None, description="Start Epoch seconds"),
    to_ts: int | None = Query(None, description="End Epoch seconds"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t
    key = _cache_key("kucoin", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_kucoin_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts)
    resp = {"lang": lang_code, "title": t("crypto.ohlcv.kucoin"), "source": "kucoin", "symbol": symbol, "interval": interval, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/gateio")
async def ohlcv_gateio(
    symbol: str = Query(..., description="Example: BTC_USDT"),
    interval: str = Query("1h", description="1m,5m,15m,30m,1h,4h,8h,1d,7d"),
    days: int | None = Query(30, description="Count of days if from/to not provided"),
    from_ts: int | None = Query(None, description="Start Epoch seconds"),
    to_ts: int | None = Query(None, description="End Epoch seconds"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t
    key = _cache_key("gateio", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_gateio_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts)
    resp = {"lang": lang_code, "title": t("crypto.ohlcv.gateio"), "source": "gateio", "symbol": symbol, "interval": interval, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/mexc")
async def ohlcv_mexc(
    symbol: str = Query(..., description="Example: BTCUSDT"),
    interval: str = Query("1h", description="1m,5m,15m,30m,1h,4h,8h,1d,1w,1M"),
    days: int | None = Query(7, description="Count of days if from/to not provided"),
    from_ts: int | None = Query(None, description="Start Epoch seconds"),
    to_ts: int | None = Query(None, description="End Epoch seconds"),
    limit: int | None = Query(None, description="Max candles (1-1000)"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t
    key = _cache_key("mexc", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_mexc_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
    resp = {"lang": lang_code, "title": t("crypto.ohlcv.mexc"), "source": "mexc", "symbol": symbol, "interval": interval, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/bybit")
async def ohlcv_bybit(
    symbol: str = Query(..., description="Example: BTCUSDT (spot)"),
    interval: str = Query("1h", description="1m,3m,5m,15m,30m,1h,2h,4h,6h,12h,1d"),
    category: str = Query("spot", description="spot | linear | inverse | option"),
    days: int | None = Query(7, description="Count of days if from/to not provided"),
    from_ts: int | None = Query(None, description="Start Epoch seconds"),
    to_ts: int | None = Query(None, description="End Epoch seconds"),
    limit: int | None = Query(None, description="Max candles (1-1000)"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t
    key = _cache_key("bybit", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit, category=category, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_bybit_ohlcv(symbol, interval=interval, category=category, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
    resp = {"lang": lang_code, "title": t("crypto.ohlcv.bybit"), "source": "bybit", "symbol": symbol, "interval": interval, "category": category, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/bitfinex")
async def ohlcv_bitfinex(
    symbol: str = Query(..., description="Example: BTC-USD"),
    interval: str = Query("1h", description="1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 1W, 14D, 1M"),
    days: int | None = Query(7),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
    limit: int | None = Query(100),
    cache_ttl: int | None = Query(None),
    lang: str | None = Query(None),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    key = _cache_key("bitfinex", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_bitfinex_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
    resp = {"lang": lang_code, "title": "Bitfinex Data", "source": "bitfinex", "symbol": symbol, "interval": interval, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/coinbase")
async def ohlcv_coinbase(
    symbol: str = Query(..., description="Example: BTC-USD"),
    interval: str = Query("1h", description="1m, 5m, 15m, 1h, 6h, 1d"),
    days: int | None = Query(7),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
    limit: int | None = Query(300),
    cache_ttl: int | None = Query(None),
    lang: str | None = Query(None),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    key = _cache_key("coinbase", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_coinbase_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
    resp = {"lang": lang_code, "title": "Coinbase Data", "source": "coinbase", "symbol": symbol, "interval": interval, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/okx")
async def ohlcv_okx(
    symbol: str = Query(..., description="Example: BTC-USDT"),
    interval: str = Query("1h", description="1m,3m,5m,15m,30m,1H,2H,4H..."),
    days: int | None = Query(7),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
    limit: int | None = Query(100),
    cache_ttl: int | None = Query(None),
    lang: str | None = Query(None),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    key = _cache_key("okx", symbol=symbol.upper(), interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit, lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached
    candles = await fetch_okx_ohlcv(symbol, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
    resp = {"lang": lang_code, "title": "OKX Data", "source": "okx", "symbol": symbol, "interval": interval, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv")
async def ohlcv_unified(
    symbol: str = Query(..., description="Example: BTC-USDT"),
    interval: str = Query("1h", description="1h, 1d, etc."),
    sources: str = Query("binance,bybit,gateio,kucoin,mexc,bitfinex,coinbase,okx", description="Priority list of exchanges"),
    days: int | None = Query(7),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
    limit: int | None = Query(None),
    category: str = Query("spot", description="Bybit category: spot|linear|inverse"),
    cache_ttl: int | None = Query(None, description="Cache TTL in seconds"),
    lang: str | None = Query(None),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    """
    Unified Crypto OHLCV Endpoint.
    Iterates through 'sources' list and returns the first valid result found (Fallback strategy).
    """
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t
    
    srcs = [s.strip().lower() for s in sources.split(",") if s.strip()]
    
    key = _cache_key(
        "unified",
        sources=",".join(srcs), symbol=symbol.upper(), interval=interval, days=days,
        from_ts=from_ts, to_ts=to_ts, limit=limit, category=category, lang=lang_code
    )
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    final_candles = []
    used_source = None
    error_log = {}

    for ex in srcs:
        try:
            candles = []
            if ex == "binance":
                sym = _normalize_symbol("binance", symbol)
                candles = await fetch_binance_ohlcv(sym, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
            elif ex == "kucoin":
                sym = _normalize_symbol("kucoin", symbol)
                candles = await fetch_kucoin_ohlcv(sym, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts)
            elif ex == "gateio":
                sym = _normalize_symbol("gateio", symbol)
                candles = await fetch_gateio_ohlcv(sym, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts)
            elif ex == "mexc":
                sym = _normalize_symbol("mexc", symbol)
                candles = await fetch_mexc_ohlcv(sym, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
            elif ex == "bybit":
                sym = _normalize_symbol("bybit", symbol)
                candles = await fetch_bybit_ohlcv(sym, interval=interval, category=category, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
            elif ex == "bitfinex":
                sym = _normalize_symbol("bitfinex", symbol)
                candles = await fetch_bitfinex_ohlcv(sym, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
            elif ex == "coinbase":
                sym = _normalize_symbol("coinbase", symbol)
                candles = await fetch_coinbase_ohlcv(sym, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
            elif ex == "okx":
                sym = _normalize_symbol("okx", symbol)
                candles = await fetch_okx_ohlcv(sym, interval=interval, days=days, from_ts=from_ts, to_ts=to_ts, limit=limit)
            else:
                continue
            
            if candles and len(candles) > 0:
                final_candles = candles
                used_source = ex
                break # Found data, stop searching
            else:
                error_log[ex] = "No data returned"

        except Exception as e:
            error_log[ex] = str(e)
            continue

    if not final_candles:
        # If no data found from any source
        resp = {
            "lang": lang_code,
            "error": "No data found from any source",
            "sources_tried": srcs,
            "details": error_log
        }
        return resp

    resp = {
        "lang": lang_code,
        "title": t("crypto.ohlcv.unified"),
        "symbol": symbol,
        "interval": interval,
        "source_used": used_source,
        "count": len(final_candles),
        "candles": final_candles
    }
    
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/symbols")
async def get_crypto_symbols(
    exchanges: str = Query("binance,kucoin,gateio,mexc,bybit,bitfinex,coinbase,okx", description="Exchanges to scan"),
    cache_ttl: int | None = Query(3600, description="Cache TTL in seconds (default 1h)"),
):
    """
    Get a unified, unique list of crypto symbols (BASE-QUOTE) from selected exchanges.
    Merging strategy: Combine all, remove duplicates.
    """
    exs = [e.strip().lower() for e in exchanges.split(",") if e.strip()]
    exs.sort()
    
    key = f"crypto_symbols|{','.join(exs)}"
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    tasks = []
    for ex in exs:
        if ex == "binance":
            tasks.append(fetch_binance_symbols())
        elif ex == "kucoin":
            tasks.append(fetch_kucoin_symbols())
        elif ex == "gateio":
            tasks.append(fetch_gateio_symbols())
        elif ex == "mexc":
            tasks.append(fetch_mexc_symbols())
        elif ex == "bybit":
            tasks.append(fetch_bybit_symbols(category="spot"))
        elif ex == "bitfinex":
            tasks.append(fetch_bitfinex_symbols())
        elif ex == "coinbase":
            tasks.append(fetch_coinbase_symbols())
        elif ex == "okx":
            tasks.append(fetch_okx_symbols())
    
    if not tasks:
        return {"count": 0, "symbols": []}

    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    unique_set = set()
    source_stats = {}
    
    for ex_name, res in zip(exs, results):
        if isinstance(res, list):
            count_before = len(unique_set)
            unique_set.update(res)
            count_added = len(unique_set) - count_before
            source_stats[ex_name] = {"total": len(res), "new_added": count_added}
        else:
             source_stats[ex_name] = {"error": str(res)}
             
    final_list = sorted(list(unique_set))
    
    resp = {
        "count": len(final_list),
        "exchanges": exs,
        "stats": source_stats,
        "symbols": final_list
    }
    
    _cache_set(key, resp, cache_ttl)
    return resp



