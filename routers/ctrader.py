from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from adapters.ctrader import ctrader_adapter
import logging

router = APIRouter(
    prefix="/ctrader",
    tags=["Ctrader"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger("api.ctrader")

@router.on_event("startup")
async def startup_event():
    # Start the adapter in background if not already running
    # We can check if thread is alive, or just call start_background which is safe-ish
    # But better to just call it.
    ctrader_adapter.start_background()

@router.get("/symbols")
async def get_symbols():
    if not ctrader_adapter.is_connected:
        raise HTTPException(status_code=503, detail="cTrader not connected")
    if not ctrader_adapter.symbols:
        ctrader_adapter.fetch_symbols() # Trigger fetch if empty
        return {"count": 0, "message": "Fetching symbols...", "data": []}
    
    # Return simplified list
    out = []
    for s in ctrader_adapter.symbols:
        out.append({
            "id": s.symbolId,
            "name": s.symbolName,
            # "digits": s.digits, # LightSymbol does not have digits
            "description": s.description
        })
    return {"count": len(out), "data": out}

@router.get("/ohlcv")
async def get_ohlcv(
    symbol_id: Optional[int] = None,
    symbol: Optional[str] = None,
    period: str = "h1", 
    days: int = 7
):
    import datetime
    
    # Resolve symbol to ID if needed
    if symbol_id is None:
        if symbol:
            # Try to find symbol by name
            # Case-insensitive match? cTrader symbols are usually exact but let's be careful.
            found = None
            if not ctrader_adapter.symbols:
                 # Try to trigger fetch if empty, though might be async race
                 ctrader_adapter.fetch_symbols()
                 # We can't easily wait here without a sleepLoop, but let's assume it's loaded if connected
            
            s_map = {s.symbolName: s.symbolId for s in ctrader_adapter.symbols}
            # Try exact match
            sid = s_map.get(symbol)
            if not sid:
                # Try partial/case-insensitive?
                for sname, s_id in s_map.items():
                    if sname.lower() == symbol.lower():
                        sid = s_id
                        break
            
            if sid:
                symbol_id = sid
            else:
                raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")
        else:
            raise HTTPException(status_code=400, detail="Either symbol_id or symbol must be provided")

    to_ts = int(datetime.datetime.now().timestamp())
    from_ts = to_ts - (days * 24 * 60 * 60)

    try:
        # Fetch symbol details if needed for digits
        digits = 5
        try:
            details = await ctrader_adapter.get_symbol_details([symbol_id])
            if details and details[0]:
                 digits = details[0].digits
        except Exception as e:
            logger.warning(f"Could not fetch symbol details for {symbol_id}, using default digits: {e}")

        raw_bars = await ctrader_adapter.get_candles(symbol_id, period, from_ts, to_ts)
        
        div = 10 ** digits if digits else 100000.0
        
        data = []
        for bar in raw_bars:
            # Low is the base
            low = bar.low
            open_ = low + optional_val(bar.deltaOpen)
            high = low + optional_val(bar.deltaHigh)
            close_ = low + optional_val(bar.deltaClose)
            
            # cTrader Trendbar timestamp is usually execution time? 
            # Check documentation: utcTimestampInMinutes
            ts_sec = bar.utcTimestampInMinutes * 60
            iso = datetime.datetime.utcfromtimestamp(ts_sec).isoformat() + "Z"
            
            data.append({
                "time": iso,
                "open": open_ / div,
                "high": high / div,
                "low": low / div,
                "close": close_ / div,
                "volume": bar.volume
            })
            
        return data

    except Exception as e:
        logger.error(f"Error fetching candles: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def optional_val(v):
    return v if v is not None else 0
