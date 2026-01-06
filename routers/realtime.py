from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from adapters.dnse_realtime import dnse_manager, set_loop
import asyncio
import logging

router = APIRouter()
logger = logging.getLogger("realtime")

@router.on_event("startup")
async def startup_event():
    # Set the running loop for MQTT threadsafe calls
    set_loop(asyncio.get_running_loop())
    
    # Authenticate & Connect DNSE
    # Note: Credentials must be in ENV or Hardcoded.
    # For now, we assume ENV is set or we skip.
    await dnse_manager.authenticate()
    dnse_manager.connect()

@router.websocket("/ws/realtime/{source}/{symbol}")
async def websocket_endpoint(websocket: WebSocket, source: str, symbol: str):
    await websocket.accept()
    
    symbol_u = symbol.upper()
    
    async def sender(data):
        try:
            # Forward data to client
            await websocket.send_json({"source": source, "symbol": symbol_u, "data": data})
        except Exception:
            # Connection likely closed
            pass

    try:
        if source == "dnse":
            await dnse_manager.subscribe(symbol_u, sender)
            logger.info(f"Client subscribed to DNSE {symbol_u}")
        elif source == "ssi":
            # TODO: Implement SSI Manager similar to DNSE
            await websocket.send_json({"error": "SSI Realtime not implemented yet"})
        else:
            await websocket.send_json({"error": "Invalid source"})
            await websocket.close()
            return

        # Keep connection open
        while True:
            # Wait for client messages (ping/keepalive)
            # If client disconnects, receive_text will raise
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info(f"Client disconnected {symbol_u}")
    finally:
        if source == "dnse":
            await dnse_manager.unsubscribe(symbol_u, sender)
