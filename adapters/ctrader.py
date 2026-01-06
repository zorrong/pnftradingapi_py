import asyncio
import threading
import uuid
import json
import os
import logging
from typing import Dict, Optional, List, Any
from twisted.internet import reactor
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAPayloadType, ProtoOATrendbarPeriod
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq,
    ProtoOASymbolsListReq,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOAGetTrendbarsReq,
    ProtoOASymbolByIdReq,
    ProtoOARefreshTokenReq,
    ProtoOARefreshTokenRes,
)

logger = logging.getLogger("ctrader_adapter")

class CTraderAdapter:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CTraderAdapter, cls).__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        self.load_config()
        self.is_connected = False
        self.is_authorized = False
        self.symbols = [] # Light symbols
        self.full_symbols: Dict[int, Any] = {} # Map ID -> ProtoOASymbol
        # Map clientMsgId -> asyncio.Future
        self._pending_requests: Dict[str, asyncio.Future] = {}
        
        # Initialize Protobuf helper
        Protobuf.populate()
        
        self.client = Client(
            EndPoints.PROTOBUF_LIVE_HOST if self.is_live else EndPoints.PROTOBUF_DEMO_HOST,
            EndPoints.PROTOBUF_PORT,
            TcpProtocol
        )
        self.client.setMessageReceivedCallback(self.on_message_received)
        self.client.setConnectedCallback(self.on_connected)
        self.client.setDisconnectedCallback(self.on_disconnected)
        # self.client.onError(self.on_error) will rely on logs for now
        
    def load_config(self):
        # Load from env or config.json
        self.client_id = os.getenv("CTRADER_CLIENT_ID")
        self.client_secret = os.getenv("CTRADER_CLIENT_SECRET")
        self.access_token = os.getenv("CTRADER_ACCESS_TOKEN")
        self.refresh_token = os.getenv("CTRADER_REFRESH_TOKEN")
        self.account_id = os.getenv("CTRADER_ACCOUNT_ID")
        self.is_live = str(os.getenv("CTRADER_IS_LIVE", "false")).lower() == "true"

        if not self.client_id or not self.access_token:
            try:
                cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
                if os.path.exists(cfg_path):
                    with open(cfg_path, "r") as f:
                        data = json.load(f)
                        self.client_id = self.client_id or data.get("ctrader_client_id")
                        self.client_secret = self.client_secret or data.get("ctrader_client_secret")
                        self.access_token = self.access_token or data.get("ctrader_access_token")
                        self.refresh_token = self.refresh_token or data.get("ctrader_refresh_token")
                        self.account_id = self.account_id or data.get("ctrader_account_id")
                        self.is_live = self.is_live or data.get("ctrader_is_live", False)
            except Exception as e:
                logger.error(f"Error loading config: {e}")

    def update_tokens_in_file(self, new_access_token: str, new_refresh_token: str):
         self.access_token = new_access_token
         self.refresh_token = new_refresh_token
         try:
             cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
             if os.path.exists(cfg_path):
                 with open(cfg_path, "r+") as f:
                     data = json.load(f)
                     data["ctrader_access_token"] = new_access_token
                     data["ctrader_refresh_token"] = new_refresh_token
                     f.seek(0)
                     json.dump(data, f, indent=4)
                     f.truncate()
                 logger.info("Tokens updated in config.json")
         except Exception as e:
             logger.error(f"Failed to save new tokens: {e}")

    def start_background(self):
        """Start the Twisted reactor in a separate thread."""
        if not self.client_id:
             logger.warning("CTrader Adapter not configured.")
             return

        def run_twisted():
            logger.info("Starting cTrader Reactor thread...")
            self.client.startService()
            if not reactor.running:
                 reactor.run(installSignalHandlers=False)

        t = threading.Thread(target=run_twisted, daemon=True)
        t.start()

    def on_connected(self, client):
        logger.info("cTrader Connected")
        self.is_connected = True
        self.authorize_app()

    def on_disconnected(self, client, reason):
        logger.warning(f"cTrader Disconnected: {reason}")
        self.is_connected = False
        self.is_authorized = False

    def on_error(self, client, failure):
        logger.error(f"cTrader Error: {failure}")

    def on_message_received(self, client, message):
        # Handle Pending Requests first
        if message.clientMsgId and message.clientMsgId in self._pending_requests:
             fut = self._pending_requests.pop(message.clientMsgId)
             if not fut.done():
                 try:
                     payload = Protobuf.extract(message)
                     # Must be thread-safe for asyncio loop
                     loop = fut.get_loop()
                     loop.call_soon_threadsafe(fut.set_result, payload)
                 except Exception as e:
                     loop = fut.get_loop()
                     loop.call_soon_threadsafe(fut.set_exception, e)
        
        # General Handlers
        if message.payloadType == ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_RES:
            logger.info("Application Authorized")
            self.fetch_account_list()
            self.authorize_account()
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES:
            accounts_list = Protobuf.extract(message)
            logger.info(f"Available Accounts found.")
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_RES:
            logger.info(f"Account {self.account_id} Authorized")
            self.is_authorized = True
            self.fetch_symbols()
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_RES:
            logger.info("Received Symbols List")
            data = Protobuf.extract(message)
            self.symbols = data.symbol
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_REFRESH_TOKEN_RES:
             logger.info("Token Refreshed")
             data = Protobuf.extract(message)
             self.update_tokens_in_file(data.accessToken, data.refreshToken)
             # Re-auth
             self.authorize_account()
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ERROR_RES:
            err = Protobuf.extract(message)
            logger.error(f"cTrader API Error: {err.errorCode} - {err.description}")
            if err.errorCode in ["CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"]:
                logger.info("Access Toke expired/invalid. Attempting Refresh...")
                self.refresh_access_token()

    def refresh_access_token(self):
        if not self.refresh_token:
            logger.error("No Refresh Token available")
            return
        
        msg = ProtoOARefreshTokenReq(refreshToken=self.refresh_token)
        reactor.callFromThread(self.client.send, msg)

    def authorize_app(self):
        if not self.client_id or not self.client_secret:
            return
        msg = ProtoOAApplicationAuthReq(
            clientId=self.client_id,
            clientSecret=self.client_secret
        )
        self.client.send(msg)

    def authorize_account(self):
        if not self.access_token or not self.account_id:
            return
        msg = ProtoOAAccountAuthReq(
            accessToken=self.access_token,
            ctidTraderAccountId=int(self.account_id)
        )
        self.client.send(msg)
        
    def fetch_symbols(self):
        if not self.account_id:
            return
        msg = ProtoOASymbolsListReq(
            ctidTraderAccountId=int(self.account_id),
            includeArchivedSymbols=False
        )
        self.client.send(msg)

    def fetch_account_list(self):
        if not self.access_token:
            return
        msg = ProtoOAGetAccountListByAccessTokenReq(
            accessToken=self.access_token
        )
        self.client.send(msg)

    async def get_symbol_details(self, symbol_ids: List[int]) -> List[Any]:
        if not self.is_authorized:
            raise Exception("cTrader not authorized")
            
        # Check cache first
        missing = [sid for sid in symbol_ids if sid not in self.full_symbols]
        if not missing:
            return [self.full_symbols[sid] for sid in symbol_ids]

        req_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_requests[req_id] = fut

        msg = ProtoOASymbolByIdReq(
            ctidTraderAccountId=int(self.account_id),
            symbolId=missing
        )
        # msg.clientMsgId = req_id # Error: not a field of payload
        reactor.callFromThread(self.client.send, msg, clientMsgId=req_id)

        try:
            res = await asyncio.wait_for(fut, timeout=10.0)
            # Update cache
            for sym in res.symbol:
                self.full_symbols[sym.symbolId] = sym
            
            return [self.full_symbols.get(sid) for sid in symbol_ids if sid in self.full_symbols]
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            raise Exception("Timeout fetching symbol details")

    async def get_candles(self, symbol_id: int, period: str, from_ts: int, to_ts: int) -> List[Any]:
        if not self.is_authorized:
            raise Exception("cTrader not authorized yet")

        req_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_requests[req_id] = fut

        # Map period string to ENUM
        period_map = {
            "m1": ProtoOATrendbarPeriod.M1,
            "m5": ProtoOATrendbarPeriod.M5,
            "m15": ProtoOATrendbarPeriod.M15,
            "h1": ProtoOATrendbarPeriod.H1,
            "d1": ProtoOATrendbarPeriod.D1,
        }
        p_enum = period_map.get(period.lower(), ProtoOATrendbarPeriod.H1)

        msg = ProtoOAGetTrendbarsReq(
            ctidTraderAccountId=int(self.account_id),
            fromTimestamp=int(from_ts * 1000), # ms
            toTimestamp=int(to_ts * 1000),
            period=p_enum,
            symbolId=int(symbol_id),
        )
        # msg.clientMsgId = req_id # Error
        
        # Send via thread-safe call passing clientMsgId
        reactor.callFromThread(self.client.send, msg, clientMsgId=req_id)

        try:
            res = await asyncio.wait_for(fut, timeout=10.0)
            return res.trendbar
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            raise Exception("Timeout waiting for cTrader response")

# Singleton instance
ctrader_adapter = CTraderAdapter()

if __name__ == "__main__":
    # Simple standalone test mode
    logging.basicConfig(level=logging.INFO)
    ctrader_adapter.start_background()
    import time
    while True:
        time.sleep(1)
