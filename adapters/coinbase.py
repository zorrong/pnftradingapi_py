
import httpx
import datetime
from typing import List, Dict, Optional

# Using Coinbase Exchange (Pro) Public API
COINBASE_API_URL = "https://api.exchange.coinbase.com"

async def fetch_coinbase_symbols() -> List[str]:
    """Fetch symbols from Coinbase and normalize to BASE-QUOTE."""
    url = f"{COINBASE_API_URL}/products"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            # data is list of dicts: { "id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD", ... }
            symbols = []
            for item in data:
                if "id" in item:
                    symbols.append(item["id"]) # Already BASE-QUOTE
            return sorted(symbols)
        except Exception:
            return []

async def fetch_coinbase_ohlcv(
    symbol: str, # BASE-QUOTE
    interval: str = "1h",
    days: int | None = 7,
    from_ts: int | None = None,
    to_ts: int | None = None,
    limit: int | None = 300 # Coinbase max 300 per request
) -> List[Dict]:
    # Map intervals to seconds
    # Supported: 60, 300, 900, 3600, 21600, 86400
    res_map = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "6h": 21600,
        "1d": 86400, "1D": 86400
    }
    granularity = res_map.get(interval, 3600)
    
    import time
    now_ts = int(time.time())
    
    if to_ts is None:
        to_ts = now_ts
    if from_ts is None:
        if days:
            from_ts = to_ts - (days * 86400)
        else:
            from_ts = to_ts - (7 * 86400)
            
    # Coinbase API candles usually return latest first (desc).
    # And it takes `start` and `end` in ISO or Epoch?
    # Docs: start, end must be ISO 8601.
    start_iso = datetime.datetime.fromtimestamp(from_ts, datetime.timezone.utc).isoformat()
    end_iso = datetime.datetime.fromtimestamp(to_ts, datetime.timezone.utc).isoformat()
    
    url = f"{COINBASE_API_URL}/products/{symbol}/candles"
    params = {
        "granularity": granularity,
        "start": start_iso,
        "end": end_iso
    }
    
    candles = []
    async with httpx.AsyncClient() as client:
        try:
            # Coinbase user-agent is often required to avoid 403
            headers = {"User-Agent": "penef-trading-bot/1.0"}
            r = await client.get(url, params=params, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            # Response: [ [ time, low, high, open, close, volume ], ... ]
            # Ordered new -> old.
            for row in data:
                ts = row[0]
                iso = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                candles.append({
                    "time": iso,
                    "low": float(row[1]),
                    "high": float(row[2]),
                    "open": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5])
                })
        except Exception:
            return []
            
    # Reverse to return Old -> New
    return candles[::-1]
