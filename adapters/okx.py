
import httpx
import datetime
from typing import List, Dict, Optional

OKX_API_URL = "https://www.okx.com"

async def fetch_okx_symbols() -> List[str]:
    """Fetch symbols from OKX and normalize to BASE-QUOTE."""
    url = f"{OKX_API_URL}/api/v5/public/instruments"
    params = {"instType": "SPOT"}
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("code") != "0":
                return []
            
            # data['data'] list of { "instId": "BTC-USDT", ... }
            raw = data.get("data", [])
            symbols = []
            for item in raw:
                inst_id = item.get("instId")
                if inst_id:
                    symbols.append(inst_id) # OKX uses '-' separator standard
            return sorted(symbols)
        except Exception:
            return []

async def fetch_okx_ohlcv(
    symbol: str, # BASE-QUOTE
    interval: str = "1h",
    days: int | None = 7,
    from_ts: int | None = None,
    to_ts: int | None = None,
    limit: int | None = 100
) -> List[Dict]:
    # Map intervals
    # OKX: 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M, 3M
    # Note casing: '1H', '1D'.
    res_map = {
        "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1H", "1H": "1H", "2h": "2H", "4h": "4H",
        "1d": "1D", "1D": "1D", "1w": "1W", "1M": "1M"
    }
    bar = res_map.get(interval, "1H")
    
    # Calculate timestamps (OKX can use 'after'/'before' pagination or just list recent)
    # If we want specific range, it's harder with just 'limit'.
    # OKX /market/candles gets recent data. /market/history-candles gets history.
    # We'll use /market/candles for recent.
    # params: after, before (timestamps).
    
    url = f"{OKX_API_URL}/api/v5/market/candles"
    
    params = {
        "instId": symbol,
        "bar": bar,
        "limit": limit or 100
    }
    # OKX pagination logic is 'after' (older than) / 'before' (newer than) the ID (ts).
    # If we just fetch, we get latest.
    # To get proper range, usage is complex without pagination loop.
    # For now, simplistic fetch of latest N candles.
    
    candles = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("code") != "0":
                return []
                
            raw_data = data.get("data", [])
            # [ts, o, h, l, c, vol, ...]
            # Descending order (Newest first).
            for row in raw_data:
                ts = int(row[0])
                iso = datetime.datetime.fromtimestamp(ts / 1000, datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                candles.append({
                    "time": iso,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5])
                })
        except Exception:
            return []
            
    # Return ascending
    return candles[::-1]
