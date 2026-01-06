# Vietnamese translations for MetaStock API Hub

LANG = "vi"

TEXTS: dict[str, str] = {
    # Chung
    "app_title": "MetaStock API Hub",
    "ok": "Đồng ý",
    "cancel": "Huỷ",
    "search": "Tìm kiếm",

    # Stock VN
    "stockvn.ohlcv.dnse": "Lấy OHLCV từ DNSE",
    "stockvn.ohlcv.ssi": "Lấy OHLCV ngày từ SSI",
    "stockvn.securities.details": "Lấy thông tin công ty/chứng khoán Việt Nam",
    "stockvn.securities.search": "Tìm kiếm mã cổ phiếu/doanh nghiệp Việt Nam",

    # Crypto
    "crypto.ohlcv.binance": "Lấy OHLCV từ Binance",
    "crypto.ohlcv.kucoin": "Lấy OHLCV từ KuCoin",
    "crypto.ohlcv.gateio": "Lấy OHLCV từ Gate.io",
    "crypto.ohlcv.mexc": "Lấy OHLCV từ MEXC",
    "crypto.ohlcv.bybit": "Lấy OHLCV từ Bybit",
    "crypto.ohlcv.parallel": "Lấy OHLCV song song từ nhiều sàn",

    # Tham số chung
    "param.symbol": "Mã",
    "param.market": "Sàn/Thị trường",
    "param.interval": "Chu kỳ",
    "param.resolution": "Khung thời gian",
    "param.days": "Số ngày",
    "param.from_ts": "Từ (epoch seconds)",
    "param.to_ts": "Đến (epoch seconds)",
    "param.limit": "Số nến",

    # Lỗi / Thông điệp
    "error.missing_credentials": "Thiếu thông tin đăng nhập",
    "error.unauthorized": "Không được phép",
    "error.bad_request": "Yêu cầu không hợp lệ",
}


def t(key: str, default: str | None = None) -> str:
    """Dịch khoá sang tiếng Việt; fallback về default hoặc chính khoá."""
    return TEXTS.get(key, default if default is not None else key)