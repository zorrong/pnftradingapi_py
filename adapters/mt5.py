import datetime
from datetime import timezone
import json
import logging
import os
from typing import List, Dict, Optional, Any
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO, filename="debug_mt5_adapter.log", filemode="a")
logger = logging.getLogger(__name__)

# Base URL for the MT5 REST API Gateway
MT5_BASE_URL = os.environ.get("MT5_BASE_URL", "https://mt5.mtapi.io")

# Global token storage
_MT5_TOKEN: Optional[str] = None
_MT5_TOKEN_EXPIRY: float = 0
_MT5_CONFIG: Dict[str, Any] = {}

# Timeframe mapping (Standard str -> MT5 int minutes)
MT5_TIMEFRAME_MAP = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "1w": 10080,
    "1M": 43200,
}

def _load_config():
    global _MT5_CONFIG
    try:
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                full_conf = json.load(f)
                _MT5_CONFIG = full_conf.get("mt5", {})
    except Exception as e:
        logger.error(f"Failed to load mt5 config: {e}")

async def _get_token() -> Optional[str]:
    """
    Get valid auth token. Connects if necessary.
    Uses /ConnectEx if 'server' is present in config, otherwise /Connect.
    """
    global _MT5_TOKEN, _MT5_TOKEN_EXPIRY, _MT5_CONFIG
    
    now = datetime.datetime.now().timestamp()
    if _MT5_TOKEN and now < _MT5_TOKEN_EXPIRY:
        return _MT5_TOKEN

    if not _MT5_CONFIG:
        _load_config()

    user = _MT5_CONFIG.get("user")
    password = _MT5_CONFIG.get("password")
    
    # Check for server param (ConnectEx) vs host/port (Connect)
    server = _MT5_CONFIG.get("server")
    host = _MT5_CONFIG.get("host")
    port = _MT5_CONFIG.get("port", 443)
    
    if not user or not password:
        logger.warning(f"MT5 config missing credentials")
        return None

    if server:
        # Use ConnectEx
        url = f"{MT5_BASE_URL}/ConnectEx"
        params = {
            "user": user,
            "password": password,
            "server": server,
            "connectTimeoutSeconds": 60,
            "connectTimeoutClusterMemberSeconds": 20
        }
        logger.info(f"Connecting to {url} with server={server}, user={user}")
    elif host:
        # Use Connect
        url = f"{MT5_BASE_URL}/Connect"
        params = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "connectTimeoutSeconds": 60
        }
        logger.info(f"Connecting to {url} with host={host}, user={user}")
    else:
        logger.warning("MT5 config missing 'server' or 'host'")
        return None

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params, timeout=70)
            if r.status_code == 200:
                token = r.text.strip().replace('"', '') 
                _MT5_TOKEN = token
                _MT5_TOKEN_EXPIRY = now + 6 * 3600
                logger.info("MT5 Connected successfully")
                return token
            else:
                logger.error(f"MT5 Connect failed: {r.status_code} {r.text}")
                return None
        except Exception as e:
            logger.error(f"MT5 Connect exception: {e}")
            return None

async def fetch_mt5_ohlcv(
    symbol: str,
    *,
    interval: str = "1h",
    days: Optional[int] = 7,
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch price history from MT5.
    """
    token = await _get_token()
    if not token:
        logger.error("No token available in fetch_mt5_ohlcv")
        return []

    tf = MT5_TIMEFRAME_MAP.get(interval, 60)
    
    now = datetime.datetime.now(timezone.utc)
    
    if to_ts:
        dt_to = datetime.datetime.fromtimestamp(to_ts, tz=timezone.utc)
    else:
        dt_to = now
        
    if from_ts:
        dt_from = datetime.datetime.fromtimestamp(from_ts, tz=timezone.utc)
    else:
        if days is None: 
            days = 7
        dt_from = dt_to - datetime.timedelta(days=days)

    s_from = dt_from.strftime("%Y-%m-%dT%H:%M:%S")
    s_to = dt_to.strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "id": token,
        "symbol": symbol,
        "from": s_from,
        "to": s_to,
        "timeFrame": tf
    }
    
    url = f"{MT5_BASE_URL}/PriceHistory"
    
    out = []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params, timeout=30)
            if r.status_code != 200:
                logger.error(f"MT5 PriceHistory failed: {r.status_code} {r.text}")
                return []
            
            data = r.json()
            if not isinstance(data, list):
                logger.error(f"MT5 PriceHistory response is not a list: {type(data)}")
                return []
            
            for bar in data:
                t_str = bar.get("time")
                out.append({
                    "time": t_str,
                    "open": bar.get("openPrice"),
                    "high": bar.get("highPrice"),
                    "low": bar.get("lowPrice"),
                    "close": bar.get("closePrice"),
                    "volume": bar.get("volume") or bar.get("tickVolume") or 0
                })
                
        except Exception as e:
            logger.error(f"Error fetching MT5 OHLCV: {e}")
            
    return out

async def fetch_mt5_symbols() -> List[str]:
    token = await _get_token()
    if not token:
        logger.error("No token for symbols")
        return []

    url = f"{MT5_BASE_URL}/SymbolList"
    params = {"id": token}
    
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return [str(s) for s in data]
            else:
                logger.error(f"SymbolList failed: {r.status_code} {r.text}")
        except Exception as e:
            logger.error(f"Error fetching MT5 symbols: {e}")
            
    return []
