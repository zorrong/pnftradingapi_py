import datetime
from datetime import timezone
from typing import List, Dict, Optional

import httpx

BINANCE_BASE_URL = "https://api.binance.com/api/v3/klines"

# Supported intervals for Binance spot klines
BINANCE_TIMEFRAME_MAP = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
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


async def fetch_binance_ohlcv(
    symbol: str,
    *,
    interval: str = "1m",
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    days: Optional[int] = 7,
    limit: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch OHLCV from Binance spot klines and return standardized list of dicts.

    - symbol: e.g., "BTCUSDT"
    - interval: one of BINANCE_TIMEFRAME_MAP keys
    - from_ts/to_ts: Unix seconds (converted to ms for Binance)
    - If from/to omitted, computed from days
    - Returns list[{time, open, high, low, close, volume}] with ISO time (UTC)
    """
    tf = BINANCE_TIMEFRAME_MAP.get(interval)
    if not tf:
        raise ValueError(f"Unsupported Binance interval: {interval}")

    _from, _to = _resolve_time_range(from_ts, to_ts, days)
    params = {
        "symbol": symbol,
        "interval": tf,
        "startTime": _from * 1000,
        "endTime": _to * 1000,
    }

    if limit is None and (from_ts is None or to_ts is None):
        # When not using explicit time range, set a reasonable limit based on days
        minutes_map = {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480, "12h": 720,
            "1d": 1440, "3d": 4320, "1w": 10080, "1M": 43200,
        }
        mins = minutes_map.get(tf, 60)
        approx = int((days or 7) * 1440 / max(1, mins))
        params.pop("startTime", None)
        params.pop("endTime", None)
        params["limit"] = max(100, min(approx, 1000))
    elif limit is not None:
        params["limit"] = max(1, min(int(limit), 1000))

    out: List[Dict] = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(BINANCE_BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                return out
            for k in data:
                # kline array format
                # [0] openTime, [1] open, [2] high, [3] low, [4] close, [5] volume, [6] closeTime, ...
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
                except Exception:
                    continue
        except httpx.HTTPError:
            return out
    return out


async def fetch_binance_symbols() -> List[str]:
    """
    Fetch all trading symbols from Binance, normalized to BASE-QUOTE format (e.g. BTC-USDT).
    """
    url = "https://api.binance.com/api/v3/exchangeInfo"
    out = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                symbols = data.get("symbols", [])
                for s in symbols:
                    if s.get("status") == "TRADING":
                        base = s.get("baseAsset")
                        quote = s.get("quoteAsset")
                        if base and quote:
                            out.append(f"{base}-{quote}".upper())
                        else:
                            # Fallback if fields missing
                            out.append(s["symbol"])
        except Exception:
            pass
    return out