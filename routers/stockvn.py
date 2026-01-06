from fastapi import APIRouter, Query, Header
from adapters.dnse import fetch_dnse_ohlcv
from adapters.ssi import fetch_ssi_daily_ohlcv, fetch_ssi_securities_details, fetch_ssi_intraday_ohlcv, fetch_ssi_securities_list
import time
import asyncio
import datetime as dt
from typing import Any, Dict, Tuple
from lang import en as lang_en, vin as lang_vi

router = APIRouter(prefix="/stockvn", tags=["StocksVN"])

# Cache đơn giản cho endpoint thông tin công ty & ohlcv
_CACHE: Dict[str, Tuple[float, Any]] = {}
CACHE_TTL_SECONDS = 60

def _normalize_resolution(res: str) -> str:
    """Normalize user input to internal standard codes:
    - Minutes: "1", "5", "15", "30", "60"
    - Daily: "1D"
    - Weekly: "1W"
    - Monthly: "1M"
    """
    r = res.strip().upper()
    if r in ("1", "1M", "1m"): return "1"
    if r in ("5", "5M", "5m"): return "5"
    if r in ("15", "15M", "15m"): return "15"
    if r in ("30", "30M", "30m"): return "30"
    if r in ("60", "1H", "1h", "60M", "60m"): return "60"
    if r in ("120", "2H", "2h"): return "120"
    if r in ("240", "4H", "4h"): return "240"
    
    if r in ("1D", "D", "d", "DAY", "day"): return "1D"
    if r in ("1W", "W", "w", "WEEK", "week"): return "1W"
    if r in ("1MON", "M", "MONTH", "month"): return "1M"
    
    return r # Fallback


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
    _CACHE.pop(key, None)
    return None


def _cache_set(key: str, value: Any, ttl: int | None):
    ttl_eff = CACHE_TTL_SECONDS if ttl is None else max(0, int(ttl))
    if ttl_eff == 0:
        return
    _CACHE[key] = (time.time() + ttl_eff, value)


def _resolve_dates(days: int | None, from_ts: int | None, to_ts: int | None) -> tuple[str, str]:
    now = dt.datetime.now(dt.timezone.utc)
    end_date = dt.datetime.fromtimestamp(to_ts, dt.timezone.utc).strftime('%Y-%m-%d') if to_ts else now.strftime('%Y-%m-%d')
    if from_ts:
        start_date = dt.datetime.fromtimestamp(from_ts, dt.timezone.utc).strftime('%Y-%m-%d')
    elif days:
        start_date = (now - dt.timedelta(days=int(days))).strftime('%Y-%m-%d')
    else:
        start_date = (now - dt.timedelta(days=365)).strftime('%Y-%m-%d')
    return start_date, end_date


@router.get("/ohlcv/dnse")
async def ohlcv_dnse(
    symbol: str = Query(..., description="Stock or Derivative Symbol"),
    market: str = Query("stock", description="stock | derivative | index"),
    resolution: str = Query("D", description="1,5,15,30,60,120,240,D,W,M"),
    days: int | None = Query(365, description="Number of days if from/to not provided"),
    from_ts: int | None = Query(None, description="Start Epoch seconds"),
    to_ts: int | None = Query(None, description="End Epoch seconds"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t
    key = _cache_key(
        "ohlcv_dnse", symbol=symbol.upper(), market=market, resolution=resolution, days=days, from_ts=from_ts, to_ts=to_ts, lang=lang_code
    )
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    candles = await fetch_dnse_ohlcv(symbol, market=market, resolution=resolution, days=days, from_ts=from_ts, to_ts=to_ts)
    resp = {"lang": lang_code, "title": t("stockvn.ohlcv.dnse"), "source": "dnse", "symbol": symbol, "market": market, "resolution": resolution, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/ohlcv/ssi")
async def ohlcv_ssi(
    symbol: str = Query(..., description="Stock Symbol"),
    resolution: str = Query("1D", description="Resolution: 1, 5, 15, 30, 60, 1D"),
    start_date: str | None = Query(None, description="YYYY-MM-DD"),
    end_date: str | None = Query(None, description="YYYY-MM-DD"),
    days: int | None = Query(None, description="If provided, start_date = today - days"),
    from_ts: int | None = Query(None, description="If provided, converted to start_date"),
    to_ts: int | None = Query(None, description="If provided, converted to end_date"),
    cache_ttl: int | None = Query(None, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    # Normalize resolution
    norm_res = _normalize_resolution(resolution)
    
    # Suy ra ngày nếu cần
    if not start_date and not end_date and (days or from_ts or to_ts):
        start_date, end_date = _resolve_dates(days, from_ts, to_ts)

    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t

    key = _cache_key("ohlcv_ssi", symbol=symbol.upper(), resolution=norm_res, start_date=start_date or "", end_date=end_date or "", lang=lang_code)
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    # Decide Intraday vs Daily
    is_intraday = norm_res in ("1", "5", "15", "30", "60", "120", "240")
    
    if is_intraday:
        candles = await fetch_ssi_intraday_ohlcv(symbol, resolution=norm_res, days=days, from_ts=from_ts, to_ts=to_ts)
    else:
        candles = await fetch_ssi_daily_ohlcv(symbol, start_date=start_date, end_date=end_date)

    resp = {"lang": lang_code, "title": t("stockvn.ohlcv.ssi"), "source": "ssi", "symbol": symbol, "resolution": norm_res, "count": len(candles), "candles": candles}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/securities/details")
async def securities_details(
    market: str | None = Query(None, description="HOSE | HNX | UPCOM | DER (optional)"),
    symbol: str | None = Query(None, description="Stock Symbol (optional)"),
    page_index: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    cache_ttl: int | None = Query(120, description="Cache TTL (seconds), 0 to disable"),
    lang: str | None = Query(None, description="Language: en | vin"),
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t

    key = _cache_key(
        "ssi_securities_details", market=market or "", symbol=symbol or "", page_index=page_index, page_size=page_size, lang=lang_code
    )
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    result = await fetch_ssi_securities_details(
        market=market,
        symbol=symbol,
        page_index=page_index,
        page_size=page_size,
    )

    # Gắn nhãn ngôn ngữ
    resp = {"lang": lang_code, "title": t("stockvn.securities.details"), **(result if isinstance(result, dict) else {"data": result})}
    _cache_set(key, resp, cache_ttl)
    return resp


@router.get("/symbols")
async def list_symbols(
    market: str | None = Query(None, description="HOSE | HNX | UPCOM | DER"),
    cache_ttl: int = 3600,
):
    """
    Get all symbols (Schools, ETFs, CWs) from SSI.
    """
    # Cache key
    key = _cache_key("stockvn_symbols", market=market or "")
    cached = _cache_get(key)
    if cached is not None and cache_ttl > 0:
        return cached

    # Fetch all pages
    all_symbols = []
    page_index = 1
    page_size = 1000
    
    while True:
        chunk = await fetch_ssi_securities_list(market=market, page_index=page_index, page_size=page_size)
        if not chunk:
            break
        all_symbols.extend(chunk)
        if len(chunk) < page_size:
            break
        page_index += 1
        # Safety break
        if page_index > 20: 
            break
    
    # Normalize/Clean up if needed.
    # SSI returns: {Market, Symbol, StockName, StockEnName, ...}
    # We might want to just return the list or map to a standard format
    
    resp = {
        "count": len(all_symbols),
        "data": all_symbols
    }
    
    _cache_set(key, resp, cache_ttl)
    return resp





from adapters.vci import fetch_vci_ohlcv

# ... (rest of imports)

@router.get("/ohlcv")
async def ohlcv_stockvn_unified(
    symbol: str = Query(...),
    market: str = "stock",
    resolution: str = "1D",
    sources: str = "dnse,ssi,vci",
    days: int | None = 365,
    from_ts: int | None = None,
    to_ts: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    cache_ttl: int | None = None,
    lang: str | None = None,
    accept_language: str | None = Header(None, alias="Accept-Language"),
):
    """
    Unified Stock VN OHLCV Endpoint.
    Lấy dữ liệu theo thứ tự ưu tiên (Fallback). Nguồn nào có dữ liệu trước sẽ được trả về.
    """
    lang_mod, lang_code = _pick_lang(lang, accept_language)
    t = lang_mod.t

    srcs = [s.strip().lower() for s in sources.split(",") if s.strip()]
    
    key = _cache_key(
        "ohlcv_unified_stock",
        sources=",".join(srcs), symbol=symbol.upper(), market=market, resolution=resolution,
        days=days, from_ts=from_ts, to_ts=to_ts, start_date=start_date or "", end_date=end_date or "", 
        lang=lang_code,
    )
    cached = _cache_get(key) if cache_ttl != 0 else None
    if cached is not None:
        return cached

    # Date resolution
    norm_res = _normalize_resolution(resolution)
    
    final_start, final_end = start_date, end_date
    if (not final_start and not final_end) and (days or from_ts or to_ts):
        final_start, final_end = _resolve_dates(days, from_ts, to_ts)

    final_candles = []
    used_source = None
    error_log = {}

    for s in srcs:
        try:
            candles = []
            if s == "dnse":
                # DNSE adapter handles mapping internally, but passing normalized code is safer
                # DNSE adapter expects: '1', '1H' (which it maps from '60'), '1D', 'W'
                # Our norm_res has "60". We might need to adjust for DNSE specifically if needed,
                # BUT we already updated DNSE adapter to map '60' -> '1H'. So passing "60" is fine.
                candles = await fetch_dnse_ohlcv(symbol, market=market, resolution=norm_res, days=days, from_ts=from_ts, to_ts=to_ts)
            elif s == "ssi":
                # Decide Intraday vs Daily
                is_intraday = norm_res in ("1", "5", "15", "30", "60", "120", "240")
                
                if is_intraday:
                    candles = await fetch_ssi_intraday_ohlcv(symbol, resolution=norm_res, days=days, from_ts=from_ts, to_ts=to_ts)
                else:
                    # Daily / Weekly / Monthly -> fetch_ssi_daily_ohlcv
                    # Note: SSI daily endpoint primarily returns Daily. Weekly/Monthly might not be natively supported aggregations.
                    candles = await fetch_ssi_daily_ohlcv(symbol, start_date=final_start, end_date=final_end)
                    
            elif s == "vci":
                # Pass resolution directly. Adapter will handle mapping or default to 1D
                candles = await fetch_vci_ohlcv(symbol, start_date=final_start, end_date=final_end, resolution=resolution)
            else:
                continue

            if candles and len(candles) > 0:
                final_candles = candles
                used_source = s
                break
            else:
                error_log[s] = "No data or empty"
        except Exception as e:
            error_log[s] = str(e)
            continue
            
    if not final_candles:
        return {
            "lang": lang_code,
            "error": "No data found",
            "sources_tried": srcs,
            "details": error_log
        }

    resp = {
        "lang": lang_code,
        "title": t("stockvn.ohlcv.parallel"), # Reuse key or new
        "symbol": symbol,
        "source_used": used_source,
        "count": len(final_candles),
        "candles": final_candles,
    }

    _cache_set(key, resp, cache_ttl)
    return resp




