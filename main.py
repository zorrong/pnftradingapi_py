from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import crypto, stockvn, pyth, realtime, ctrader, mt5

app = FastAPI(title="pnfTrading API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(crypto.router)
app.include_router(stockvn.router)
app.include_router(pyth.router)
app.include_router(realtime.router)
app.include_router(ctrader.router)
app.include_router(mt5.router)

@app.get("/")
def read_root():
    return {
        "message": "Welcome to pnfTrading API",
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "endpoints": [
            "/crypto/ohlcv",
            "/stockvn/ohlcv",
            "/stockvn/symbols",
            "/pyth/ohlcv",
            "/mt5/ohlcv",
            "/ws/realtime/{source}/{symbol}"
        ]
    }
