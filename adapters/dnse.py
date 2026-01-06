import datetime
from datetime import timezone
from typing import List, Dict, Optional

import httpx

# DNSE Chart API base URLs per market type
_DNSE_BASE_URLS = {
    "derivative": "https://api.dnse.com.vn/chart-api/v2/ohlcs/derivative",
    "stock": "https://api.dnse.com.vn/chart-api/v2/ohlcs/stock",
    "index": "https://api.dnse.com.vn/chart-api/v2/ohlcs/index",
}


def _pick_base_url(market: str) -> str:
    m = (market or "stock").strip().lower()
    return _DNSE_BASE_URLS.get(m, _DNSE_BASE_URLS["stock"])  # default stock


def _resolve_time_range(days: Optional[int], from_ts: Optional[int], to_ts: Optional[int]) -> tuple[int, int]:
    now = int(datetime.datetime.now(timezone.utc).timestamp())
    if to_ts is None:
        to_ts = now
    if from_ts is None:
        if days is None:
            days = 7
        from_ts = to_ts - days * 24 * 60 * 60
    if from_ts > to_ts:
        from_ts, to_ts = to_ts, from_ts
    return int(from_ts), int(to_ts)


async def fetch_dnse_ohlcv(
    symbol: str,
    *,
    market: str = "stock",
    resolution: str = "1",
    days: Optional[int] = 7,
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
) -> List[Dict]:
    """Fetch OHLCV candles from DNSE chart-api and normalize to
    [{time, open, high, low, close, volume}].

    - market: "stock" | "index" | "derivative"
    - resolution: string minutes (e.g. "1", "5", "15", "60", ...)
    - days/from_ts/to_ts: time window (epoch seconds)
    """
    base_url = _pick_base_url(market)
    start, end = _resolve_time_range(days, from_ts, to_ts)

    # Normalize resolution
    res_map = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1D": "1D",
        "D": "1D",
        "W": "W",
        "60": "1H",
        "1h": "1H",
        "1H": "1H"
    }
    # Check exact match or use provided key
    final_res = res_map.get(resolution, resolution)

    params = {
        "from": start,
        "to": end,
        "symbol": symbol,
        "resolution": final_res,
    }

    async with httpx.AsyncClient() as client:
        # Debug print

        r = await client.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json() if r.content else {}

    if not isinstance(data, dict):
        return []

    t = data.get("t") or []
    o = data.get("o") or []
    h = data.get("h") or []
    l = data.get("l") or []
    c = data.get("c") or []
    v = data.get("v") or []

    n = min(len(t), len(o), len(h), len(l), len(c), len(v))
    out: List[Dict] = []
    for i in range(n):
        ts = int(t[i])
        # Normalize time to ISO-8601 in UTC
        iso = datetime.datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        out.append(
            {
                "time": iso,
                "open": float(o[i]),
                "high": float(h[i]),
                "low": float(l[i]),
                "close": float(c[i]),
                "volume": float(v[i]) if v[i] is not None else 0.0,
            }
        )
    return out