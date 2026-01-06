import datetime
from datetime import timezone
from typing import List, Dict, Optional

import httpx

GATEIO_API_URL = "https://api.gateio.ws/api/v4/spot/candlesticks"

GATEIO_TIMEFRAME_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "4h": "4h", "8h": "8h", "1d": "1d", "7d": "7d"
}


def _resolve_time_range(from_ts: Optional[int], to_ts: Optional[int], days: Optional[int]) -> tuple[int, int]:
    if to_ts is None:
        to_ts = int(datetime.datetime.now(timezone.utc).timestamp())
    if from_ts is None:
        if days is None:
            days = 30
        from_ts = to_ts - days * 24 * 60 * 60
    return int(from_ts), int(to_ts)


async def fetch_gateio_ohlcv(
    symbol: str,
    *,
    interval: str = "1h",
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    days: Optional[int] = 30,
) -> List[Dict]:
    tf = GATEIO_TIMEFRAME_MAP.get(interval)
    if not tf:
        raise ValueError(f"Unsupported Gate.io interval: {interval}")

    _from, _to = _resolve_time_range(from_ts, to_ts, days)

    headers = {
        "Accept": "application/json",
        "User-Agent": "api-hub/1.0",
    }

    # Primary: use from/to
    params = {
        "currency_pair": symbol,
        "interval": tf,
        "from": _from,
        "to": _to,
    }

    out: List[Dict] = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(GATEIO_API_URL, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception:
            data = []

        # If empty or dict-wrapped, fallback with limit
        if isinstance(data, dict):
            data = data.get("data", [])

        if not data:
            try:
                # estimate limit from days/interval
                minutes_map = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "8h": 480, "1d": 1440, "7d": 10080}
                mins = minutes_map.get(tf, 60)
                approx = int((days or 30) * 1440 / mins)
                params2 = {"currency_pair": symbol, "interval": tf, "limit": max(100, min(approx, 1000))}
                r2 = await client.get(GATEIO_API_URL, params=params2, headers=headers, timeout=30)
                r2.raise_for_status()
                data = r2.json()
                if isinstance(data, dict):
                    data = data.get("data", [])
            except Exception:
                data = []

        if not isinstance(data, list):
            return out

        # Gate.io candlestick array format: [t, o, h, l, c, v]
        for item in data:
            try:
                t = int(item[0])
                iso = datetime.datetime.fromtimestamp(t, tz=timezone.utc).isoformat().replace("+00:00", "Z")
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


async def fetch_gateio_symbols() -> List[str]:
    """
    Fetch all trading symbols from Gate.io, normalized to BASE-QUOTE.
    Gate.io uses BASE_QUOTE (e.g. BTC_USDT), we will convert to BTC-USDT.
    """
    url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    out = []
    headers = {
        "Accept": "application/json",
        "User-Agent": "api-hub/1.0",
    }
    
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                data = r.json()
                # list of dicts: {"id": "BTC_USDT", "trade_status": "tradable", ...}
                for item in data:
                    if item.get("trade_status") == "tradable":
                        s_id = item["id"]
                        # Convert to BASE-QUOTE
                        if "_" in s_id and "-" not in s_id:
                            s_id = s_id.replace("_", "-")
                        out.append(s_id)
        except Exception:
            pass
    return out