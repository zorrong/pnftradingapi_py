import httpx

HERMES_URL = "https://hermes.pyth.network/v2"

# Feed ID cho XAU/USD (chuẩn từ Pyth Insights)
XAU_FEED_ID = "0x765d2ba906dbc32ca17cc11f5310a89e9ee1f6420508c63861f2f8ba4ee34bb2"

async def get_snapshot(feed_id: str):
    """Lấy snapshot giá mới nhất từ Hermes cho một feed id.
    Trả về dict {price, conf, publish_time} với price/conf đã scale theo expo.
    """
    url = f"{HERMES_URL}/updates/price/latest"
    params = {"ids[]": feed_id, "parsed": "true"}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
    parsed = payload.get("parsed", []) if isinstance(payload, dict) else []
    if not parsed:
        raise ValueError("No parsed price found for the given feed id")
    p = parsed[0].get("price", {})
    price_int = int(p.get("price", 0))
    conf_int = int(p.get("conf", 0))
    expo = int(p.get("expo", 0))
    publish_time = int(p.get("publish_time", 0))
    scale = 10 ** expo  # expo có thể âm -> float
    return {
        "price": price_int * scale,
        "conf": conf_int * scale,
        "publish_time": publish_time,
    }

# Benchmarks TradingView shim để lấy dữ liệu lịch sử (candles)
BENCHMARKS_TV_URL = "https://benchmarks.pyth.network/v1/shims/tradingview"

async def _resolve_xau_symbol(client: httpx.AsyncClient) -> str:
    """Tìm symbol TradingView phù hợp cho XAU/USD từ Benchmarks.
    Ưu tiên dùng trực tiếp Metal.XAU/USD (theo link người dùng cung cấp), nếu không có sẽ dò symbol_info; cuối cùng fallback "Metal.XAU/USD".
    """
    # 1) Thử trực tiếp symbol người dùng cung cấp
    try:
        sym_check = "Metal.XAU/USD"
        r0 = await client.get(f"{BENCHMARKS_TV_URL}/symbols", params={"symbol": sym_check}, timeout=15)
        if r0.status_code == 200:
            data0 = r0.json()
            # UDF /symbols thường trả về object có name hoặc symbol
            name = str(data0.get("name") or data0.get("symbol") or "").strip()
            if name.upper() == sym_check.upper():
                return sym_check
    except Exception:
        pass

    # 2) Dò theo symbol_info như trước
    url = f"{BENCHMARKS_TV_URL}/symbol_info"
    try:
        r = await client.get(url, timeout=30)
        r.raise_for_status()
        info = r.json()
        if isinstance(info, dict) and "symbol" in info and isinstance(info["symbol"], list):
            symbols = info["symbol"]
            descriptions = info.get("description", [])
            for i, sym in enumerate(symbols):
                desc = descriptions[i] if i < len(descriptions) else ""
                if ("XAU/USD" in str(sym).upper()) or ("XAUUSD" in str(sym).upper()) or ("XAU/USD" in str(desc).upper()):
                    return sym
        if isinstance(info, dict) and "data" in info and isinstance(info["data"], list):
            for item in info["data"]:
                sym = item.get("symbol", "")
                desc = item.get("description", "")
                if ("XAU/USD" in str(sym).upper()) or ("XAUUSD" in str(sym).upper()) or ("XAU/USD" in str(desc).upper()):
                    return sym
    except Exception:
        pass

    # 3) Fallback
    return "Metal.XAU/USD"

async def _fetch_tradingview_history(
    client: httpx.AsyncClient,
    *,
    symbol: str,
    resolution: str,
    from_ts: int,
    to_ts: int,
):
    """Gọi endpoint TradingView history để lấy candles.
    Trả về list các candle: [{t, o, h, l, c, v}]
    """
    params = {
        "symbol": symbol,
        "resolution": resolution,  # phút: "5" => 5m
        "from": from_ts,
        "to": to_ts,
    }
    r = await client.get(f"{BENCHMARKS_TV_URL}/history", params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    # UDF response tiêu chuẩn: { s: "ok"|"no_data", t:[], o:[], h:[], l:[], c:[], v:[] }
    if not isinstance(data, dict) or data.get("s") != "ok":
        return []
    t = data.get("t", [])
    o = data.get("o", [])
    h = data.get("h", [])
    l = data.get("l", [])
    c = data.get("c", [])
    v = data.get("v", []) or [None] * len(t)
    candles = []
    for i in range(min(len(t), len(o), len(h), len(l), len(c), len(v))):
        candles.append(
            {
                "t": t[i],  # epoch seconds
                "o": o[i],
                "h": h[i],
                "l": l[i],
                "c": c[i],
                "v": v[i],
            }
        )
    return candles

async def get_xau_usd_candles(
    resolution: str,
    days: int | None = 3,
    from_ts: int | None = None,
    to_ts: int | None = None,
):
    """Lấy dữ liệu nến cho XAU/USD với mọi khung thời gian mà source hỗ trợ.

    - resolution: truyền thẳng theo chuẩn TradingView shim, ví dụ: "1", "3", "5", "15", "30", "60", "120", "240", "D", "W", "M".
    - days: số ngày gần nhất (nếu không cung cấp from_ts/to_ts). Mặc định 3.
    - from_ts, to_ts: epoch seconds. Nếu cung cấp thì ưu tiên dùng cặp này.
    """
    import time

    now = int(time.time())
    if from_ts is not None or to_ts is not None:
        # Ưu tiên dùng from/to nếu có
        if from_ts is None and to_ts is not None and days is not None:
            from_ts = to_ts - days * 24 * 60 * 60
        if to_ts is None and from_ts is not None:
            to_ts = now
    else:
        # Mặc định dùng days gần nhất
        if days is None:
            days = 3
        to_ts = now
        from_ts = to_ts - days * 24 * 60 * 60

    if from_ts > to_ts:
        # Hoán đổi nếu nhập ngược
        from_ts, to_ts = to_ts, from_ts

    async with httpx.AsyncClient() as client:
        symbol = await _resolve_xau_symbol(client)
        candles = await _fetch_tradingview_history(
            client,
            symbol=symbol,
            resolution=resolution,
            from_ts=int(from_ts),
            to_ts=int(to_ts),
        )
    return {"symbol": symbol, "resolution": resolution, "from": int(from_ts), "to": int(to_ts), "candles": candles}

    """Lấy dữ liệu nến 5 phút cho XAU/USD trong 3 ngày gần nhất từ Pyth Benchmarks TradingView shim."""
    import time

    to_ts = int(time.time())
    from_ts = to_ts - 3 * 24 * 60 * 60  # 3 ngày
    async with httpx.AsyncClient() as client:
        symbol = await _resolve_xau_symbol(client)
        candles = await _fetch_tradingview_history(
            client,
            symbol=symbol,
            resolution="5",  # 5 phút
            from_ts=from_ts,
            to_ts=to_ts,
        )
    return {"symbol": symbol, "resolution": "5", "from": from_ts, "to": to_ts, "candles": candles}

async def _load_symbol_info(client: httpx.AsyncClient):
    """Tải toàn bộ symbol_info từ Benchmarks TradingView shim và chuẩn hoá về list các item {symbol, description}."""
    url = f"{BENCHMARKS_TV_URL}/symbol_info"
    r = await client.get(url, timeout=60)
    r.raise_for_status()
    info = r.json()
    items = []
    if isinstance(info, dict) and "symbol" in info and isinstance(info["symbol"], list):
        symbols = info["symbol"]
        descriptions = info.get("description", [])
        types = info.get("type", [])
        for i, sym in enumerate(symbols):
            desc = descriptions[i] if i < len(descriptions) else ""
            typ = types[i] if i < len(types) else "unknown"
            items.append({"symbol": sym, "description": desc, "type": typ})
    elif isinstance(info, dict) and "data" in info and isinstance(info["data"], list):
        for item in info["data"]:
            items.append({
                "symbol": item.get("symbol", ""),
                "description": item.get("description", ""),
                "type": item.get("type", "unknown"),
            })
    return items

async def list_benchmarks_symbols(query: str | None = None, asset_type: str | None = None):
    """Liệt kê tất cả mã (symbols) từ Pyth Benchmarks TradingView shim.
    - query: substring filter (symbol/description).
    - asset_type: filter exact match (case-insensitive) on 'type' field (e.g. 'Crypto', 'Metal', 'FX').
    """
    async with httpx.AsyncClient() as client:
        items = await _load_symbol_info(client)
    
    if query:
        q = str(query).strip().lower()
        items = [it for it in items if q in str(it.get("symbol", "")).lower() or q in str(it.get("description", "")).lower()]
    
    if asset_type:
        t = str(asset_type).strip().lower()
        items = [it for it in items if str(it.get("type", "")).lower() == t]
        
    return {"count": len(items), "symbols": items}

async def _resolve_symbol_generic(client: httpx.AsyncClient, raw: str) -> str:
    """Resolve một symbol bất kỳ (ví dụ 'Metal.XAG/USD' hoặc 'XAG/USD').
    - Nếu raw đã là symbol đầy đủ và tồn tại, trả về ngay.
    - Nếu không, tìm kiếm gần đúng (contains) trong symbol và description; trả về kết quả đầu tiên.
    """
    raw = str(raw).strip()
    items = await _load_symbol_info(client)
    # 1) thử khớp chính xác theo symbol (không phân biệt hoa thường)
    for it in items:
        if str(it.get("symbol", "")).upper() == raw.upper():
            return it.get("symbol")
    # 2) nếu raw dạng rút gọn (không chứa '.'), thử tìm contains
    ru = raw.upper()
    for it in items:
        symu = str(it.get("symbol", "")).upper()
        descu = str(it.get("description", "")).upper()
        if ru in symu or ru in descu:
            return it.get("symbol")
    # 3) fallback: trả về raw (để cho server upstream phản hồi lỗi nếu không hợp lệ)
    return raw

async def get_benchmarks_candles(
    symbol: str,
    resolution: str,
    days: int | None = 3,
    from_ts: int | None = None,
    to_ts: int | None = None,
):
    """Lấy candles cho bất kỳ symbol nào từ Benchmarks TradingView shim.

    - symbol: có thể là symbol đầy đủ (vd: "Metal.XAG/USD") hoặc rút gọn (vd: "XAG/USD").
    - resolution: TradingView style ("1","5","60","D","W","M", ...)
    - days hoặc from_ts/to_ts: giống hàm get_xau_usd_candles
    """
    import time

    now = int(time.time())
    if from_ts is not None or to_ts is not None:
        if from_ts is None and to_ts is not None and days is not None:
            from_ts = to_ts - days * 24 * 60 * 60
        if to_ts is None and from_ts is not None:
            to_ts = now
    else:
        if days is None:
            days = 3
        to_ts = now
        from_ts = to_ts - days * 24 * 60 * 60

    if from_ts > to_ts:
        from_ts, to_ts = to_ts, from_ts

    async with httpx.AsyncClient() as client:
        resolved = await _resolve_symbol_generic(client, symbol)
        candles = await _fetch_tradingview_history(
            client,
            symbol=resolved,
            resolution=resolution,
            from_ts=int(from_ts),
            to_ts=int(to_ts),
        )
    return {"symbol": resolved, "resolution": resolution, "from": int(from_ts), "to": int(to_ts), "candles": candles}


async def search_price_feeds(
    query: str | None = None,
    page_size: int | None = 50,
    continuation_token: str | None = None,
):
    """Tìm kiếm danh sách price feeds trên Hermes (trả về nguyên payload từ Hermes).
    - query: chuỗi tìm kiếm (symbol/miêu tả tuỳ Hermes hỗ trợ)
    - page_size: số lượng mỗi trang (nếu Hermes hỗ trợ)
    - continuation_token: phân trang theo token (nếu Hermes hỗ trợ)
    """
    url = f"{HERMES_URL}/price_feeds"
    params: dict[str, str | int] = {}
    if query:
        params["query"] = query
    if page_size:
        params["page_size"] = page_size
    if continuation_token:
        params["continuation_token"] = continuation_token
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
    return payload
