from typing import List, Dict, Optional
import datetime
import httpx
import time

# Direct implementation of VCI (Vietcap) API used by vnstock
VCI_BASE_URL = "https://trading.vietcap.com.vn/api/chart/OHLCChart/gap-chart"

# Mapping from common resolution to VCI resolution strings
VCI_RESOLUTION_MAP = {
    "1D": "ONE_DAY",
    "D": "ONE_DAY",
    "1W": "ONE_WEEK",
    "W": "ONE_WEEK",
    "1M": "ONE_MONTH",
    "M": "ONE_MONTH",
    "1H": "ONE_HOUR",
    "1h": "ONE_HOUR",
    "30m": "30m", # VCI might treat < 1D differently or map logic needed
    "15m": "15m",
    "5m": "5m",
    "1m": "1m"
}

# Mapping specific VCI resolutions to internal constants if needed based on const.py
# _INTERVAL_MAP in const.py: '1D': 'ONE_DAY', '1m': 'ONE_MINUTE', etc.
# However, for gap-chart, let's stick to what we saw or defaults.
# If resolution is minute-based, url might be different?
# In quote.py: if interval in ['1m'..'30m'] -> url might be different or resolution param differs.
# For now, we focus on 1D/Daily as primary use case.

async def fetch_vci_ohlcv(
    symbol: str, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None,
    resolution: str = "1D"
) -> List[Dict]:
    """
    Fetch OHLCV directly from VCI API.
    
    Args:
        symbol: Stock symbol (e.g. "HPG")
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        resolution: "1D" (default)
        
    Returns:
        List of dicts: time, open, high, low, close, volume
    """
    
    # Resolve dates to timestamps (ms)
    # VCI expects `from` and `to` in milliseconds
    
    if not end_date:
        dt_end = datetime.datetime.now()
    else:
        try:
            dt_end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            # Set to end of day
            dt_end = dt_end.replace(hour=23, minute=59, second=59)
        except:
            dt_end = datetime.datetime.now()

    if not start_date:
        dt_start = dt_end - datetime.timedelta(days=365)
    else:
        try:
            dt_start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        except:
            dt_start = dt_end - datetime.timedelta(days=365)

    from_ts = int(dt_start.timestamp() * 1000)
    to_ts = int(dt_end.timestamp() * 1000)
    
    res_param = VCI_RESOLUTION_MAP.get(resolution, "ONE_DAY")
    
    # If using minute resolution, VCI logic in vnstock is complex (resampling). 
    # For gap-chart, it supports basic resolutions.
    
    params = {
        "symbol": symbol.upper(),
        "resolution": res_param,
        "from": from_ts,
        "to": to_ts
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Accept": "application/json",
        "Origin": "https://trading.vietcap.com.vn",
        "Referer": "https://trading.vietcap.com.vn/trung-tam-phan-tich"
    }

    out = []
    async with httpx.AsyncClient() as client:
        try:
            # VCI uses POST for gap-chart
            resp = await client.post(VCI_BASE_URL, json=params, headers=headers, timeout=10)
            if resp.status_code != 200:
                print(f"VCI Error: {resp.status_code} - {resp.text}")
                return out
                
            data = resp.json()
            # Expected format: {"t": [...], "o": [...], "h": [...], "l": [...], "c": [...], "v": [...]}
            # or maybe distinct objects.
            
            # Check structure
            if "t" in data and isinstance(data["t"], list):
                times = data["t"]
                opens = data.get("o", [])
                highs = data.get("h", [])
                lows = data.get("l", [])
                closes = data.get("c", [])
                vols = data.get("v", [])
                
                count = len(times)
                for i in range(count):
                    # Time is usually unix timestamp in seconds or ms
                    # VCI usually returns ms?
                    ts = times[i]
                    # Check if seconds or ms
                    if ts > 100000000000: # ms
                         ts = ts / 1000.0
                    
                    iso = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                    
                    out.append({
                        "time": iso,
                        "open": float(opens[i]) / 1000.0 if i < len(opens) else 0.0,
                        "high": float(highs[i]) / 1000.0 if i < len(highs) else 0.0,
                        "low": float(lows[i]) / 1000.0 if i < len(lows) else 0.0,
                        "close": float(closes[i]) / 1000.0 if i < len(closes) else 0.0,
                        "volume": float(vols[i]) if i < len(vols) else 0.0,
                        "source": "vci"
                    })
            else:
                # Some APIs return list of objects
                pass

        except Exception as e:
            print(f"VCI Fetch Error: {e}")
            return []
            
    return out
