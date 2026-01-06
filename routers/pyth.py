from fastapi import APIRouter, Query, Path
from typing import Literal
from adapters.hermes import (
    list_benchmarks_symbols,
    get_benchmarks_candles,
    get_snapshot,
    search_price_feeds,
)

router = APIRouter(prefix="/pyth", tags=["Pyth Network"])

# --- Generic Endpoints ---

@router.get("/symbols")
async def list_all_symbols(query: str | None = Query(None, description="Filter by text")):
    """List ALL Pyth symbols."""
    return await list_benchmarks_symbols(query=query)

@router.get("/ohlcv")
async def generic_ohlcv(
    symbol: str = Query(..., description="Symbol, e.g. Crypto.BTC/USD"),
    resolution: str = Query("1", description="Resolution: 1, 5, 60, D, W, M"),
    days: int | None = Query(3),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
):
    """Generic OHLCV fetcher."""
    return await get_benchmarks_candles(symbol, resolution, days, from_ts, to_ts)



@router.get("/feeds/search")
async def feeds_search(q: str | None = None):
    return await search_price_feeds(query=q)


# --- Categorized Endpoints ---

async def _list_by_type(asset_type: str, query: str | None):
    return await list_benchmarks_symbols(query=query, asset_type=asset_type)

# 1. Commodity (includes Metals, Oil, etc.)
@router.get("/commodity/symbols")
async def list_commodity_symbols(query: str | None = None):
    """List Commodity symbols (Metals, Oil, etc.). Pyth type: 'Commodity'"""
    return await _list_by_type("Commodity", query)

@router.get("/commodity/ohlcv")
async def get_commodity_ohlcv(
    symbol: str = Query(..., description="Symbol (e.g. XAUUSD or UKOILSPOT)"),
    resolution: str = Query("1", description="Resolution"),
    days: int | None = Query(3),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
):
    """Get OHLCV for Commodity symbols."""
    return await get_benchmarks_candles(symbol, resolution, days, from_ts, to_ts)


# 2. Crypto
@router.get("/crypto/symbols")
async def list_crypto_symbols(query: str | None = None):
    """List Crypto symbols. Pyth type: 'Crypto'"""
    return await _list_by_type("Crypto", query)

@router.get("/crypto/ohlcv")
async def get_crypto_ohlcv(
    symbol: str = Query(..., description="Symbol (e.g. BTC/USD)"),
    resolution: str = Query("1", description="Resolution"),
    days: int | None = Query(3),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
):
    """Get OHLCV for Crypto symbols."""
    return await get_benchmarks_candles(symbol, resolution, days, from_ts, to_ts)


# 3. Stock (Equity/StockUS)
@router.get("/stock/symbols")
async def list_stock_symbols(query: str | None = None):
    """List Stock symbols. Pyth type: 'Equity'"""
    return await _list_by_type("Equity", query)

@router.get("/stock/ohlcv")
async def get_stock_ohlcv(
    symbol: str = Query(..., description="Symbol (e.g. AAPL/USD)"),
    resolution: str = Query("1", description="Resolution"),
    days: int | None = Query(3),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
):
    """Get OHLCV for Stock symbols."""
    return await get_benchmarks_candles(symbol, resolution, days, from_ts, to_ts)


# 4. Forex
@router.get("/forex/symbols")
async def list_forex_symbols(query: str | None = None):
    """List Forex symbols. Pyth type: 'forex'"""
    return await _list_by_type("forex", query)

@router.get("/forex/ohlcv")
async def get_forex_ohlcv(
    symbol: str = Query(..., description="Symbol (e.g. EUR/USD)"),
    resolution: str = Query("1", description="Resolution"),
    days: int | None = Query(3),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
):
    """Get OHLCV for Forex symbols."""
    return await get_benchmarks_candles(symbol, resolution, days, from_ts, to_ts)


# 5. Bond
@router.get("/bond/symbols")
async def list_bond_symbols(query: str | None = None):
    """List Bond symbols. Pyth type: 'Bond' (or similar)"""
    # Note: If 'Bond' type isn't used by Pyth, this might return empty. 
    # Common types: Equity, FX, Crypto, Metal, Commodity.
    # If users specifically asked for Bond, we try 'Bond'.
    return await _list_by_type("Bond", query)

@router.get("/bond/ohlcv")
async def get_bond_ohlcv(
    symbol: str = Query(..., description="Symbol (e.g. Bond Ticker)"),
    resolution: str = Query("1", description="Resolution"),
    days: int | None = Query(3),
    from_ts: int | None = Query(None),
    to_ts: int | None = Query(None),
):
    """Get OHLCV for Bond symbols."""
    return await get_benchmarks_candles(symbol, resolution, days, from_ts, to_ts)