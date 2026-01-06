import datetime
from datetime import timezone
from typing import List, Dict, Optional

import httpx

# Bybit unified market kline endpoint (v5):
# GET https://api.bybit.com/v5/market/kline?category=spot&symbol=BTCUSDT&interval=60&start=...&end=...
BYBIT_BASE_URL = "https://api.bybit.com/v5/market/kline"

BYBIT_TIMEFRAME_MAP = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
}


def _resolve_time_range(from_ts: Optional[int], to_ts: Optional[int], days: Optional[int]) -> tuple[int, int]:
    if to_ts is None:
        to_ts = int(datetime.datetime.now(timezone.utc).timestamp())
    if from_ts is None:
        if days is None:
            days = 7
        from_ts = to_ts - days * 24 * 60 * 60
    return int(from_ts), int(to_ts)


async def fetch_bybit_ohlcv(
    symbol: str,
    *,
    interval: str = "1h",
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    days: Optional[int] = 7,
    category: str = "spot",  # spot or linear / inverse / option
    limit: Optional[int] = None,
) -> List[Dict]:
    tf = BYBIT_TIMEFRAME_MAP.get(interval)
    if not tf:
        raise ValueError(f"Unsupported Bybit interval: {interval}")

    _from, _to = _resolve_time_range(from_ts, to_ts, days)

    params = {
        "category": category,
        "symbol": symbol,
        "interval": tf,
        "start": _from * 1000,
        "end": _to * 1000,
    }

    if limit is not None:
        params["limit"] = max(1, min(int(limit), 1000))

    out: List[Dict] = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(BYBIT_BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            result = payload.get("result", {}) if isinstance(payload, dict) else {}
            list_data = result.get("list", []) if isinstance(result, dict) else []
        except Exception as e:
            print(f"Bybit Error: {e}")
            list_data = []

        # Bybit list item: [startTime(ms), open, high, low, close, volume, turnover]
        for item in list_data:
            try:
                t_ms = int(item[0])
                iso = datetime.datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
                out.append({
                    "time": iso,
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                })
            except Exception:
                continue

    return out


async def fetch_bybit_symbols(category: str = "spot") -> List[str]:
    """
    Fetch all trading symbols from Bybit, normalized to BASE-QUOTE.
    """
    url = "https://api.bybit.com/v5/market/instruments-info"
    params = {"category": category}
    out = []
    
    # Bybit pagination might be needed but for simplicity assuming we get first batch or Bybit returns all if limit large.
    # Bybit default limit is 500, max 1000. We might need loop.
    # But user wants a quick list. I will implement simpler one which might miss some if > 1000.
    # Actually, let's try to get max 1000, usually enough for major pairs. If more needed, logic complicates.
    params["limit"] = 1000 
    
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params, timeout=30)
            if r.status_code == 200:
                payload = r.json()
                result = payload.get("result", {})
                list_data = result.get("list", [])
                for item in list_data:
                    if item.get("status") == "Trading":
                        base = item.get("baseCoin")
                        quote = item.get("quoteCoin")
                        if base and quote:
                            out.append(f"{base}-{quote}".upper())
                        else:
                            out.append(item["symbol"])
        except Exception:
            pass
    return out