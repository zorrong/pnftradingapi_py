# English translations for MetaStock API Hub

LANG = "en"

# Key-value mapping for UI/API texts
TEXTS: dict[str, str] = {
    # General
    "app_title": "MetaStock API Hub",
    "ok": "OK",
    "cancel": "Cancel",
    "search": "Search",

    # Stock VN
    "stockvn.ohlcv.dnse": "Fetch OHLCV from DNSE",
    "stockvn.ohlcv.ssi": "Fetch daily OHLCV from SSI",
    "stockvn.securities.details": "Fetch Vietnam securities/company details",
    "stockvn.securities.search": "Search Vietnam symbols/companies",

    # Crypto
    "crypto.ohlcv.binance": "Fetch OHLCV from Binance",
    "crypto.ohlcv.kucoin": "Fetch OHLCV from KuCoin",
    "crypto.ohlcv.gateio": "Fetch OHLCV from Gate.io",
    "crypto.ohlcv.mexc": "Fetch OHLCV from MEXC",
    "crypto.ohlcv.bybit": "Fetch OHLCV from Bybit",
    "crypto.ohlcv.parallel": "Fetch OHLCV in parallel from multiple exchanges",

    # Common parameters
    "param.symbol": "Symbol",
    "param.market": "Market",
    "param.interval": "Interval",
    "param.resolution": "Resolution",
    "param.days": "Days",
    "param.from_ts": "From (epoch seconds)",
    "param.to_ts": "To (epoch seconds)",
    "param.limit": "Limit",

    # Errors / Messages
    "error.missing_credentials": "Missing credentials",
    "error.unauthorized": "Unauthorized",
    "error.bad_request": "Bad request",
}


def t(key: str, default: str | None = None) -> str:
    """Translate a key to English; fallback to default or key itself."""
    return TEXTS.get(key, default if default is not None else key)