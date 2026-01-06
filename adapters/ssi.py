import os
import datetime
from datetime import timezone, timedelta
from typing import List, Dict, Optional

import httpx

# Auth endpoints (primary + fallbacks)
AUTH_URL = "https://fc-data.ssi.com.vn/api/v2/Market/AccessToken"
ALT_AUTH_URL = "https://fc-data.ssi.com.vn/api/v2/Token"
ALT_AUTH_URL2 = "https://fc-data.ssi.com.vn/v2.0/Token"
DATA_URL = "https://fc-data.ssi.com.vn/api/v2/Market/DailyOhlc"
SECURITIES_DETAILS_URL = "https://fc-data.ssi.com.vn/api/v2/Market/SecuritiesDetails"
INTRADAY_URL = "https://fc-data.ssi.com.vn/api/v2/Market/IntradayOhlc"


def _load_config_json() -> dict:
    # Prefer adapters/config.json; still allow env override
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
        cfg_path = os.path.abspath(cfg_path)
        if os.path.exists(cfg_path):
            import json
            with open(cfg_path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _get_ssi_auth_url() -> str:
    cfg = _load_config_json()
    return os.getenv("SSI_AUTH_URL") or cfg.get("ssi_auth_url") or AUTH_URL


def _get_ssi_auth_alt_urls() -> list[str]:
    cfg = _load_config_json()
    cands = []
    v1 = os.getenv("SSI_AUTH_URL_ALT") or cfg.get("ssi_auth_url_alt") or ALT_AUTH_URL
    if v1:
        cands.append(v1)
    if ALT_AUTH_URL2 not in cands:
        cands.append(ALT_AUTH_URL2)
    return cands


def _get_ssi_data_url() -> str:
    cfg = _load_config_json()
    return os.getenv("SSI_DATA_URL") or cfg.get("ssi_data_url") or DATA_URL


def _get_ssi_securities_details_url() -> str:
    cfg = _load_config_json()
    return os.getenv("SSI_SECURITIES_DETAILS_URL") or cfg.get("ssi_securities_details_url") or SECURITIES_DETAILS_URL


def _get_ssi_intraday_url() -> str:
    cfg = _load_config_json()
    return os.getenv("SSI_INTRADAY_URL") or cfg.get("ssi_intraday_url") or INTRADAY_URL


async def _request_with_retries(
    method: str,
    url: str,
    *,
    retries: int = 3,
    backoff_factor: float = 0.6,
    timeout: float | int = 30,
    **kwargs,
):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.request(method, url, timeout=timeout, **kwargs)
                return r
        except Exception as e:
            last_exc = e
            if attempt < retries:
                import asyncio
                sleep_s = backoff_factor * (2 ** (attempt - 1))
                await asyncio.sleep(sleep_s)
            else:
                raise last_exc


async def get_access_token(consumer_id: Optional[str] = None, consumer_secret: Optional[str] = None) -> Optional[str]:
    # inputs via params -> env -> data/config.json
    if not consumer_id:
        consumer_id = os.getenv("SSI_CONSUMER_ID")
    if not consumer_secret:
        consumer_secret = os.getenv("SSI_CONSUMER_SECRET")
    if not consumer_id or not consumer_secret:
        cfg = _load_config_json()
        consumer_id = consumer_id or cfg.get("ssi_consumer_id")
        consumer_secret = consumer_secret or cfg.get("ssi_consumer_secret")
    if not consumer_id or not consumer_secret:
        return None

    headers = {"Content-Type": "application/json"}
    payload = {"consumerID": consumer_id, "consumerSecret": consumer_secret}

    candidates = [_get_ssi_auth_url(), *_get_ssi_auth_alt_urls()]
    for url in candidates:
        try:
            r = await _request_with_retries("POST", url, headers=headers, json=payload, timeout=20, retries=4)
            if r is None:
                continue
            r.raise_for_status()
            data = r.json() if r.content else {}
            token = (data.get("data") or {}).get("accessToken") if isinstance(data, dict) else None
            if token:
                return token
        except Exception:
            continue
    return None


def _to_ddmmyyyy(s: str) -> str:
    try:
        if 'T' in s:
            dt = datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
            return dt.strftime('%d/%m/%Y')
        return datetime.datetime.strptime(s, '%Y-%m-%d').strftime('%d/%m/%Y')
    except Exception:
        try:
            return datetime.datetime.strptime(s, '%d/%m/%Y').strftime('%d/%m/%Y')
        except Exception:
            return s


def _resolve_time_range(days: Optional[int], from_ts: Optional[int], to_ts: Optional[int]) -> tuple[int, int]:
    now = int(datetime.datetime.now(tz=timezone.utc).timestamp())
    _to = int(to_ts) if to_ts else now
    if from_ts:
        _from = int(from_ts)
    else:
        d = days if days is not None else 7
        _from = _to - int(d * 86400)
    if _from > _to:
        _from, _to = _to, _from
    return _from, _to


async def fetch_ssi_daily_ohlcv(
    symbol: str,
    *,
    start_date: Optional[str] = None,  # YYYY-MM-DD
    end_date: Optional[str] = None,    # YYYY-MM-DD
    consumer_id: Optional[str] = None,
    consumer_secret: Optional[str] = None,
) -> List[Dict]:
    if end_date is None:
        end_date = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d')
    if start_date is None:
        start_date = (datetime.datetime.now(timezone.utc) - datetime.timedelta(days=3650)).strftime('%Y-%m-%d')

    token = await get_access_token(consumer_id, consumer_secret)
    if not token:
        return []

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    data_url = _get_ssi_data_url()

    from_ddmmyyyy = _to_ddmmyyyy(start_date)
    to_ddmmyyyy = _to_ddmmyyyy(end_date)

    page_index = 1
    page_size = 100
    out: List[Dict] = []

    async with httpx.AsyncClient() as client:
        while True:
            params = {
                "symbol": symbol,
                "fromDate": from_ddmmyyyy,
                "toDate": to_ddmmyyyy,
                "pageIndex": page_index,
                "pageSize": page_size,
                "orderBy": "asc",
            }
            try:
                r = await client.get(data_url, headers=headers, params=params, timeout=30)
                r.raise_for_status()
                payload = r.json() if r.content else {}
                data = payload.get("data", []) if isinstance(payload, dict) else []
                if not data:
                    break
                for rec in data:
                    date_str = rec.get("Date") or rec.get("TradingDate")
                    if not date_str:
                        continue
                    try:
                        # Deterministic parse
                        if isinstance(date_str, str) and '-' in date_str:
                            dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        else:
                            dt = datetime.datetime.strptime(str(date_str), '%d/%m/%Y')
                        iso = dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                    except Exception:
                        continue
                    # Indices usually don't need division by 1000
                    div = 1000.0
                    sym_u = symbol.upper()
                    if sym_u in ["VNINDEX", "VN30", "VN100", "HNX", "HNX30", "UPCOM", "VNXALL", "VNSI", "VNMID", "VNSML"]:
                        div = 1.0
                    
                    out.append(
                        {
                            "time": iso,
                            "open": float(rec.get("Open", 0)) / div,
                            "high": float(rec.get("High", 0)) / div,
                            "low": float(rec.get("Low", 0)) / div,
                            "close": float(rec.get("Close", 0)) / div,
                            "volume": float(rec.get("Volume", 0)),
                        }
                    )
                if len(data) < page_size:
                    break
                page_index += 1
            except httpx.HTTPStatusError as http_err:
                if http_err.response is not None and http_err.response.status_code == 401:
                    # Try refresh token once
                    token = await get_access_token(consumer_id, consumer_secret)
                    if not token:
                        break
                    headers["Authorization"] = f"Bearer {token}"
                    continue
                break
            except httpx.HTTPError:
                break
    return out


async def fetch_ssi_intraday_ohlcv(
    symbol: str,
    *,
    resolution: str = "60",  # minutes: "1","5","15","30","60","120","240"
    days: Optional[int] = 7,
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    consumer_id: Optional[str] = None,
    consumer_secret: Optional[str] = None,
) -> List[Dict]:
    """Fetch SSI intraday and aggregate to given resolution in minutes.

    The source endpoint returns intraday records with fields TradingDate (dd/mm/yyyy) and Time (HH:MM:SS),
    plus Open, High, Low, Close, Volume. We combine TradingDate+Time to timestamp (assumed Asia/Ho_Chi_Minh, UTC+7)
    then resample into fixed-minute buckets.
    """
    token = await get_access_token(consumer_id, consumer_secret)
    if not token:
        return []

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    url = _get_ssi_intraday_url()

    start_sec, end_sec = _resolve_time_range(days, from_ts, to_ts)
    # Convert to local dates (Vietnam) for query window
    tz_vn = datetime.timezone(timedelta(hours=7))
    start_dt_vn = datetime.datetime.fromtimestamp(start_sec, tz=timezone.utc).astimezone(tz_vn)
    end_dt_vn = datetime.datetime.fromtimestamp(end_sec, tz=timezone.utc).astimezone(tz_vn)

    from_ddmmyyyy = start_dt_vn.strftime('%d/%m/%Y')
    to_ddmmyyyy = end_dt_vn.strftime('%d/%m/%Y')

    page_index = 1
    page_size = 1000

    # Aggregation buckets
    try:
        res_min = max(1, int(resolution))
    except Exception:
        res_min = 60
    bucket_sec = res_min * 60

    buckets: Dict[int, Dict[str, float]] = {}
    order_keys: Dict[int, int] = {}  # to track first/last for open/close ordering
    ordinal = 0

    async with httpx.AsyncClient() as client:
        while True:
            params = {
                "symbol": symbol,
                "fromDate": from_ddmmyyyy,
                "toDate": to_ddmmyyyy,
                "pageIndex": page_index,
                "pageSize": page_size,
            }
            try:
                r = await client.get(url, headers=headers, params=params, timeout=30)
                r.raise_for_status()
                payload = r.json() if r.content else {}
                data = payload.get("data", []) if isinstance(payload, dict) else []
                if not data:
                    break
                for rec in data:
                    date_str = rec.get("TradingDate") or rec.get("Date")
                    time_str = rec.get("Time") or rec.get("TradingTime")
                    if not date_str or not time_str:
                        continue
                    try:
                        dt_local = datetime.datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S").replace(tzinfo=tz_vn)
                        dt_utc = dt_local.astimezone(timezone.utc)
                        ts = int(dt_utc.timestamp())
                    except Exception:
                        continue

                    # Clip to requested time range
                    if ts < start_sec or ts > end_sec:
                        continue

                    b_start = (ts // bucket_sec) * bucket_sec
                    
                    div = 1000.0
                    sym_u = symbol.upper()
                    if sym_u in ["VNINDEX", "VN30", "VN100", "HNX", "HNX30", "UPCOM", "VNXALL", "VNSI", "VNMID", "VNSML"]:
                        div = 1.0

                    o = float(rec.get("Open", 0)) / div
                    h = float(rec.get("High", 0)) / div
                    l = float(rec.get("Low", 0)) / div
                    c = float(rec.get("Close", 0)) / div
                    v = float(rec.get("Volume", 0))

                    if b_start not in buckets:
                        buckets[b_start] = {"open": o, "high": h, "low": l, "close": c, "volume": v}
                        order_keys[b_start] = ordinal
                    else:
                        bk = buckets[b_start]
                        # update high/low
                        bk["high"] = max(bk["high"], h)
                        bk["low"] = min(bk["low"], l)
                        # update close as last seen in time order
                        if ordinal >= order_keys[b_start]:
                            bk["close"] = c
                            order_keys[b_start] = ordinal
                        # accumulate volume
                        bk["volume"] += v
                    ordinal += 1

                if len(data) < page_size:
                    break
                page_index += 1
            except httpx.HTTPStatusError as http_err:
                if http_err.response is not None and http_err.response.status_code == 401:
                    token = await get_access_token(consumer_id, consumer_secret)
                    if not token:
                        break
                    headers["Authorization"] = f"Bearer {token}"
                    continue
                break
            except httpx.HTTPError:
                break

    # Build sorted output
    out: List[Dict] = []
    for b_start in sorted(buckets.keys()):
        bk = buckets[b_start]
        iso = datetime.datetime.fromtimestamp(b_start, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        out.append({
            "time": iso,
            "open": float(bk["open"]),
            "high": float(bk["high"]),
            "low": float(bk["low"]),
            "close": float(bk["close"]),
            "volume": float(bk["volume"]),
        })
    return out


async def fetch_ssi_securities_details(
    *,
    market: Optional[str] = None,  # HOSE | HNX | UPCOM | DER (optional)
    symbol: Optional[str] = None,  # Mã CK (optional)
    page_index: int = 1,
    page_size: int = 50,
    consumer_id: Optional[str] = None,
    consumer_secret: Optional[str] = None,
) -> Dict:
    token = await get_access_token(consumer_id, consumer_secret)
    if not token:
        return {"data": [], "message": "Missing SSI credentials", "status": 401}

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    url = _get_ssi_securities_details_url()

    params = {
        "pageIndex": max(1, int(page_index)),
        "pageSize": max(1, int(page_size)),
    }
    if market:
        params["market"] = market
    if symbol:
        params["symbol"] = symbol

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json() if r.content else {}
        except httpx.HTTPStatusError as http_err:
            if http_err.response is not None and http_err.response.status_code == 401:
                # refresh token once
                token = await get_access_token(consumer_id, consumer_secret)
                if not token:
                    return {"data": [], "message": "Unauthorized", "status": 401}
                headers["Authorization"] = f"Bearer {token}"
                try:
                    r = await client.get(url, headers=headers, params=params, timeout=30)
                    r.raise_for_status()
                    payload = r.json() if r.content else {}
                except Exception as e:
                    return {"data": [], "message": str(e), "status": 500}
            else:
                return {"data": [], "message": str(http_err), "status": getattr(http_err.response, 'status_code', 500)}
        except Exception as e:
            return {"data": [], "message": str(e), "status": 500}

    return payload if isinstance(payload, dict) else {"data": [], "message": "Invalid response", "status": 500}


SECURITIES_URL = "https://fc-data.ssi.com.vn/api/v2/Market/Securities"

def _get_ssi_securities_url() -> str:
    cfg = _load_config_json()
    return os.getenv("SSI_SECURITIES_URL") or cfg.get("ssi_securities_url") or SECURITIES_URL

async def fetch_ssi_securities_list(
    market: Optional[str] = None,
    page_index: int = 1,
    page_size: int = 1000,
    consumer_id: Optional[str] = None,
    consumer_secret: Optional[str] = None,
) -> List[Dict]:
    """Fetch list of securities from SSI.
    By default (page_size=1000), it tries to fetch a large chunk.
    If you want ALL, you might need to handle pagination outside or use a loop here.
    """
    token = await get_access_token(consumer_id, consumer_secret)
    if not token:
        return []

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    url = _get_ssi_securities_url()

    params = {
        "pageIndex": max(1, int(page_index)),
        "pageSize": max(10, int(page_size)),
    }
    if market:
        params["market"] = market

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, dict) and "data" in payload:
                return payload["data"]
            return []
        except Exception:
            return []
