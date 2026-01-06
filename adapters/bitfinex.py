
import httpx
import datetime
from typing import List, Dict, Optional

BITFINEX_API_URL = "https://api-pub.bitfinex.com/v2"

async def fetch_bitfinex_symbols() -> List[str]:
    """Fetch symbols from Bitfinex and normalize to BASE-QUOTE."""
    url = f"{BITFINEX_API_URL}/conf/pub:list:pair:exchange"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data and isinstance(data[0], list):
                # Format: [['1INCH:USD', '1INCH:UST', ...]]
                raw_symbols = data[0]
            else:
                 return []
            
            symbols = []
            for s in raw_symbols:
                # Bitfinex symbols often look like 'BTCUSD' (if 3 chars) or '1INCH:USD' (if >3).
                # Actually newer endpoint usually returns 'tBTCUSD'.
                # The doc says `pub:list:pair:exchange` returns pairs like "BTCUSD".
                # If they are 6 chars, usually it's 3+3.
                # If they have colon, split by colon.
                
                # Normalization logic:
                # 1. If has ':', split.
                # 2. If 6 chars, assume 3:3.
                # 3. Else, harder to guess without list of currencies.
                # Let's try basic heuristics.
                base, quote = "", ""
                if ":" in s:
                    parts = s.split(":")
                    if len(parts) == 2:
                        base, quote = parts[0], parts[1]
                elif len(s) == 6:
                    base, quote = s[:3], s[3:]
                else:
                    # Skip obscure ones for now or treat as is? 
                    # Many pairs are just concatenated.
                    # We might skip inconsistent lengths for safety or manual map common quotes?
                    for q in ["USD", "USDT", "BTC", "ETH", "EUR", "JPY", "GBP"]:
                        if s.endswith(q):
                            base = s[:-len(q)]
                            quote = q
                            break
                            
                if base and quote:
                    symbols.append(f"{base}-{quote}")
            return sorted(list(set(symbols)))

        except Exception:
            return []

async def fetch_bitfinex_ohlcv(
    symbol: str, # Expected Format: BASE-QUOTE e.g. BTC-USD
    interval: str = "1h",
    days: int | None = 7,
    from_ts: int | None = None,
    to_ts: int | None = None,
    limit: int | None = 1000
) -> List[Dict]:
    # Map intervals
    # Bitfinex: 1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 1W, 14D, 1M
    res_map = {
        "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1h", "3h": "3h", "6h": "6h", "12h": "12h",
        "1d": "1D", "1D": "1D", "7d": "1W", "1w": "1W", "1M": "1M"
    }
    tf = res_map.get(interval, "1h") # Default 1h
    
    # Normalize symbol for Bitfinex: 'tBTCUSD'
    # Assume symbol is BASE-QUOTE. Remove '-'. Prefix 't'.
    # If quote is USDT, Bitfinex often uses UST for Tether (check!).
    # Actually Bitfinex symbol lists show "UST" for USDT often in older API, but v2 supports UST and USDT?
    # Actually, often 'UST' on Bitfinex = USDT. 'USD' = USD.
    # Let's treat it simply first.
    base, quote = symbol.split("-")
    if quote == "USDT":
        quote = "UST" # Common mapping for Bitfinex
        
    pair = f"t{base}{quote}"
    
    # Resolving time
    # Bitfinex uses ms
    import time
    now_ms = int(time.time() * 1000)
    
    start_ms = None
    end_ms = now_ms
    
    if to_ts:
        end_ms = int(to_ts * 1000)
    
    if from_ts:
        start_ms = int(from_ts * 1000)
    elif days:
        start_ms = end_ms - (days * 24 * 60 * 60 * 1000)
    else:
        start_ms = end_ms - (7 * 24 * 60 * 60 * 1000)

    # limit max 10000 but reasonable is 100-1000
    if not limit: 
        limit = 100
    
    # API: /candles/trade:1m:tBTCUSD/hist?start=...&end=...&limit=...
    url = f"{BITFINEX_API_URL}/candles/trade:{tf}:{pair}/hist"
    params = {
        "start": start_ms,
        "end": end_ms,
        "limit": limit,
        "sort": 1 # 1 for old to new? No, Bitfinex default is new to old.
        # "sort": -1 is new to old. "sort": 1 is old to new. 
        # Check docs: "sort" -> if = 1, it results in standard ascending sorting.
    }
    
    candles = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            # Response: [[MTS, OPEN, CLOSE, HIGH, LOW, VOLUME], ...]
            # Note: Bitfinex Close is idx 2, High idx 3, Low idx 4.
            # Wait, verify order: [ MTS, OPEN, CLOSE, HIGH, LOW, VOLUME ]
            for row in data:
                # Skip if data is incomplete
                if len(row) < 6: continue
                # Convert MTS to ISO
                mts = row[0]
                iso = datetime.datetime.fromtimestamp(mts / 1000, datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                candles.append({
                    "time": iso,
                    "open": float(row[1]),
                    "close": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "volume": float(row[5])
                })
        except Exception:
            return []
            
    # Bitfinex logic: if sort=1, generic asc.
    # We want asc.
    return candles
