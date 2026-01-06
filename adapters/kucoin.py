import datetime
from datetime import timezone
from typing import List, Dict, Optional

import httpx

KUCOIN_BASE_URL = "https://api.kucoin.com/api/v1/market/candles"

# KuCoin intervals use TradingView-like format: 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week, 1month
KUCOIN_TIMEFRAME_MAP = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "2h": "2hour",
    "4h": "4hour",
    "6h": "6hour",
    "8h": "8hour",
    "12h": "12hour",
    "1d": "1day",
    "1w": "1week",
    "1M": "1month",
}


def _resolve_time_range(from_ts: Optional[int], to_ts: Optional[int], days: Optional[int]) -> tuple[int, int]:
    if to_ts is None:
        to_ts = int(datetime.datetime.now(timezone.utc).timestamp())
    if from_ts is None:
        if days is None:
            days = 7
        from_ts = to_ts - days * 24 * 60 * 60
    return int(from_ts), int(to_ts)


async def fetch_kucoin_ohlcv(
    symbol: str,
    *,
    interval: str = "1h",
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    days: Optional[int] = 7,
) -> List[Dict]:
    tf = KUCOIN_TIMEFRAME_MAP.get(interval)
    if not tf:
        raise ValueError(f"Unsupported KuCoin interval: {interval}")

    _from, _to = _resolve_time_range(from_ts, to_ts, days)

    # KuCoin API expects 'type' like '1hour' and 'symbol' like 'BTC-USDT'
    params = {
        "type": tf,
        "symbol": symbol,
        "startAt": _from,
        "endAt": _to,
    }

    out: List[Dict] = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(KUCOIN_BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            data = payload.get("data", []) if isinstance(payload, dict) else []
        except Exception:
            data = []

        # KuCoin returns list of arrays: [time, open, close, high, low, volume, turnover]
        # time is in seconds
        for item in data:
            try:
                t = int(item[0])
                iso = datetime.datetime.fromtimestamp(t, tz=timezone.utc).isoformat().replace("+00:00", "Z")
                out.append({
                    "time": iso,
                    "open": float(item[1]),
                    "high": float(item[3]),
                    "low": float(item[4]),
                    "close": float(item[2]),
                    "volume": float(item[5]),
                })
            except Exception:
                continue

    return out


async def fetch_kucoin_symbols() -> List[str]:
    """
    Fetch all trading symbols from KuCoin, normalized to BASE-QUOTE.
    """
    url = "https://api.kucoin.com/api/v1/symbols"
    out = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, timeout=30)
            if r.status_code == 200:
                payload = r.json()
                data = payload.get("data", [])
                for item in data:
                    if item.get("enableTrading"):
                        # KuCoin symbols are already BASE-QUOTE usually (e.g. BTC-USDT)
                        out.append(item["symbol"])
        except Exception:
            pass
    return out