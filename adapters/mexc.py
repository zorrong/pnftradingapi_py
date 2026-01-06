import datetime
from datetime import timezone
from typing import List, Dict, Optional

import httpx

MEXC_BASE_URL = "https://api.mexc.com/api/v3/klines"

MEXC_TIMEFRAME_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "4h": "4h",
    "8h": "8h",
    "1d": "1d",
    "1w": "1w",
    "1M": "1M",
}


def _resolve_time_range(from_ts: Optional[int], to_ts: Optional[int], days: Optional[int]) -> tuple[int, int]:
    if to_ts is None:
        to_ts = int(datetime.datetime.now(timezone.utc).timestamp())
    if from_ts is None:
        if days is None:
            days = 7
        from_ts = to_ts - days * 24 * 60 * 60
    return int(from_ts), int(to_ts)


async def fetch_mexc_ohlcv(
    symbol: str,
    *,
    interval: str = "1h",
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    days: Optional[int] = 7,
    limit: Optional[int] = None,
) -> List[Dict]:
    tf = MEXC_TIMEFRAME_MAP.get(interval)
    if not tf:
        raise ValueError(f"Unsupported MEXC interval: {interval}")

    _from, _to = _resolve_time_range(from_ts, to_ts, days)
    params = {
        "symbol": symbol,
        "interval": tf,
        "startTime": _from * 1000,
        "endTime": _to * 1000,
    }

    if limit is not None:
        params["limit"] = max(1, min(int(limit), 1000))

    out: List[Dict] = []
    async with httpx.AsyncClient() as client:
        try:
            # Debug log
            # print(f"MEXC Requesting: {MEXC_BASE_URL} params={params}")
            r = await client.get(MEXC_BASE_URL, params=params, timeout=30)
            if r.status_code != 200:
                print(f"MEXC Error: {r.status_code} - {r.text}")
                return out
                
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                print(f"MEXC Unexpected data format: {data}")
                return out
                
            for k in data:
                # MEXC kline array format similar to Binance
                # [ openTime, open, high, low, close, volume, closeTime, ... ]
                try:
                    t_ms = int(k[0])
                    iso = datetime.datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
                    out.append({
                        "time": iso,
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                    })
                except Exception as e:
                    print(f"MEXC Parse Error: {e}")
                    continue
        except httpx.HTTPError as e:
            print(f"MEXC HTTP Error: {e}")
            return out
    return out


async def fetch_mexc_symbols() -> List[str]:
    """
    Fetch all trading symbols from MEXC, normalized to BASE-QUOTE.
    """
    url = "https://api.mexc.com/api/v3/exchangeInfo"
    out = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                symbols = data.get("symbols", [])
                for s in symbols:
                    if s.get("status") == "ENABLED":
                        base = s.get("baseAsset")
                        quote = s.get("quoteAsset")
                        if base and quote:
                            out.append(f"{base}-{quote}".upper())
                        else:
                            out.append(s["symbol"])
        except Exception:
            pass
    return out