# pnfTradingAPI_Py

A unified FastAPI-based trading data API that aggregates data from multiple cryptocurrency exchanges and Vietnamese stock market sources.

## Features

- **Multi-source Crypto Data**: Support for Binance, Bybit, Gate.io, KuCoin, MEXC, OKX, Bitfinex, and Coinbase
- **Vietnamese Stock Market**: Integration with DNSE, SSI, and VCI
- **Global Markets**: Pyth Network Hermes adapter for commodities, forex, and more
- **Trading Platforms**: cTrader and MetaTrader 5 integration
- **Real-time Data**: WebSocket support for live market data

## Project Structure

```
pnfTradingAPI_Py/
├── adapters/          # Data adapters for various external APIs
├── lang/              # Localization files
├── logic/             # Business logic
├── routers/           # FastAPI routers
├── main.py            # Main application entry point
├── requirements.txt   # Python dependencies
└── pyproject.toml     # Project configuration
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/zorrong/pnfTradingAPI_Py.git
cd pnfTradingAPI_Py
```

2. Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
Create a `.env` file with your API credentials for various data sources.

## Usage

Start the API server:
```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

### API Documentation

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Available Endpoints

- `/crypto/ohlcv` - Cryptocurrency OHLCV data
- `/stockvn/ohlcv` - Vietnamese stock OHLCV data
- `/stockvn/symbols` - List of Vietnamese stock symbols
- `/pyth/ohlcv` - Global markets data via Pyth Network
- `/mt5/ohlcv` - MetaTrader 5 data
- `/ws/realtime/{source}/{symbol}` - WebSocket for real-time data

## Supported Data Sources

### Cryptocurrency
- Binance
- Bybit
- Gate.io
- KuCoin
- MEXC
- OKX
- Bitfinex
- Coinbase

### Vietnamese Stocks
- DNSE
- SSI
- VCI (VietCap)

### Global Markets
- Pyth Network (Commodities, Forex, etc.)

### Trading Platforms
- cTrader
- MetaTrader 5

## License

MIT License

## Author

zorrong
