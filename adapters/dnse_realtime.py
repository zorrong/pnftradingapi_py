import asyncio
import json
import logging
import ssl
import time
import httpx
from typing import Dict, Set, Optional, Callable
import os

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

from adapters.dnse_types import Tick, StockInfo, TopPrice

logger = logging.getLogger("dnse_realtime")

class DNSERealtimeManager:
    """
    Singleton Manager for DNSE MQTT Realtime Data.
    Handles authentication, connection, and broadcasting to WebSocket clients.
    """
    
    _instance = None
    
    BROKER_HOST = "datafeed-lts-krx.dnse.com.vn"
    BROKER_PORT = 443
    AUTH_URL = "https://api.dnse.com.vn/user-service/api/auth"
    USER_INFO_URL = "https://api.dnse.com.vn/user-service/api/me"
    
    # Topics
    TOPIC_TICK = "plaintext/quotes/krx/mdds/tick/v1/roundlot/symbol/{symbol}"
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DNSERealtimeManager, cls).__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        # Env > adapters/config.json > adapters/.env
        u = os.getenv("DNSE_USERNAME")
        p = os.getenv("DNSE_PASSWORD")
        
        # Try loading from config.json
        if not u or not p:
            try:
                import json
                cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
                if os.path.exists(cfg_path):
                    with open(cfg_path, "r") as f:
                        # Handle case where file might have multiple JSON objects (invalid standard JSON)
                        # We read content and try to find keys or parse
                        content = f.read()
                        try:
                            data = json.loads(content)
                            u = u or data.get("usernameEntrade") or data.get("dnse_username")
                            p = p or data.get("password") or data.get("dnse_password")
                        except:
                            # If invalid JSON (e.g. appended objects), simple string search
                            if "usernameEntrade" in content and not u:
                                import re
                                m = re.search(r'"usernameEntrade"\s*:\s*"([^"]+)"', content)
                                if m: u = m.group(1)
                            if "password" in content and not p:
                                import re
                                m = re.search(r'"password"\s*:\s*"([^"]+)"', content)
                                if m: p = m.group(1)
            except Exception:
                pass

        # Try loading from .env
        if not u or not p:
            try:
                env_path = os.path.join(os.path.dirname(__file__), ".env")
                if os.path.exists(env_path):
                    with open(env_path, "r") as f:
                        for line in f:
                            if "usernameEntrade=" in line:
                                u = line.split("usernameEntrade=")[1].strip()
                            if "password=" in line:
                                p = line.split("password=")[1].strip()
            except Exception:
                pass
        
        self.username = u
        self.password = p
        self.token = None
        self.investor_id = None
        self.client = None
        self.is_connected = False
        
        # Callbacks map: symbol -> set of async callback functions
        self.subscribers: Dict[str, Set[Callable]] = {}
        self.active_subscriptions: Set[str] = set()

    async def authenticate(self) -> bool:
        """Authenticate using requests (sync) or httpx (async)."""
        if not self.username or not self.password:
            logger.warning("DNSE credentials missing in env (DNSE_USERNAME, DNSE_PASSWORD).")
            return False

        try:
            async with httpx.AsyncClient() as http:
                # 1. Login
                payload = {"username": self.username, "password": self.password}
                resp = await http.post(self.AUTH_URL, json=payload)
                resp.raise_for_status()
                data = resp.json()
                self.token = data.get("token")
                
                # 2. WhoAmI -> InvestorID
                if self.token:
                    resp_me = await http.get(
                        self.USER_INFO_URL, 
                        headers={'Authorization': f'Bearer {self.token}'}
                    )
                    resp_me.raise_for_status()
                    self.investor_id = str(resp_me.json().get("investorId"))
                    logger.info("DNSE Authenticated successfully.")
                    return True
        except Exception as e:
            logger.error(f"DNSE Auth Failed: {e}")
        return False

    def connect(self):
        """Connect to MQTT Broker in background."""
        if mqtt is None:
            logger.error("paho-mqtt not installed.")
            return

        if not self.token or not self.investor_id:
            logger.error("No token/investorID. Call authenticate() first.")
            return

        if self.client and self.is_connected:
            return

        client_id_suffix = str(int(time.time()))[-4:]
        client_id = f"dnse-sub-{client_id_suffix}"
        
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id,
            protocol=mqtt.MQTTv5,
            transport="websockets"
        )
        self.client.username_pw_set(self.investor_id, self.token)
        self.client.tls_set(cert_reqs=ssl.CERT_NONE)
        self.client.tls_insecure_set(True)
        self.client.ws_set_options(path="/wss")
        
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        try:
            self.client.connect(self.BROKER_HOST, self.BROKER_PORT, keepalive=60)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"MQTT Connect Error: {e}")

    def _on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("DNSE MQTT Connected.")
            self.is_connected = True
            # Resubscribe active
            for sym in self.active_subscriptions:
                self._subscribe_mqtt(sym)
        else:
            logger.error(f"DNSE MQTT Connect Failed: {rc}")

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        logger.warning(f"DNSE MQTT Disconnected: {rc}")
        self.is_connected = False

    def _on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = json.loads(msg.payload.decode('utf-8'))
            
            # Simple parsing of symbol from topic or payload
            # Topic: plaintext/quotes/krx/mdds/tick/v1/roundlot/symbol/VNM
            parts = topic.split('/')
            symbol = ""
            if "symbol" in parts:
                idx = parts.index("symbol")
                if idx + 1 < len(parts):
                    symbol = parts[idx+1]
            
            if not symbol:
                return

            # Broadcast to subscribers
            listeners = self.subscribers.get(symbol)
            if listeners:
                # We need to run async callbacks from this sync thread.
                # Since we are in separate thread (mqtt loop), we need to schedule into main loop.
                # OR simpler: fire and forget using a global loop helper?
                # Actually, in FastAPI websockets, we probably just want to Put into a Queue.
                # Handling async from sync callback is tricky. 
                # Best approach: Use a shared Queue or run_coroutine_threadsafe.
                
                # For clean architecture, we'll assume the listeners are synchronous wrappers 
                # or we use a janus queue. But simplistic approach for now:
                
                asyncio.run_coroutine_threadsafe(self._broadcast(symbol, payload), loop) 

        except Exception as e:
            logger.error(f"Msg Error: {e}")

    async def _broadcast(self, symbol, payload):
        if symbol in self.subscribers:
            # Copy set to avoid size change during iteration
            for cb in list(self.subscribers[symbol]):
                try:
                    await cb(payload)
                except Exception:
                    pass

    def _subscribe_mqtt(self, symbol: str):
        if self.client and self.is_connected:
            t = self.TOPIC_TICK.format(symbol=symbol)
            self.client.subscribe(t)
            logger.info(f"MQTT Subscribed: {t}")

    async def subscribe(self, symbol: str, callback: Callable):
        """Register a callback for a symbol."""
        if symbol not in self.subscribers:
            self.subscribers[symbol] = set()
            self.active_subscriptions.add(symbol)
            self._subscribe_mqtt(symbol)
        
        self.subscribers[symbol].add(callback)

    async def unsubscribe(self, symbol: str, callback: Callable):
        if symbol in self.subscribers:
            self.subscribers[symbol].discard(callback)
            if not self.subscribers[symbol]:
                del self.subscribers[symbol]
                self.active_subscriptions.discard(symbol)
                # Optional: Unsubscribe MQTT to save bandwidth
                # self.client.unsubscribe(...)

# Global Loop reference hack for threadsafe calls
loop = asyncio.new_event_loop()
def set_loop(l):
    global loop
    loop = l

dnse_manager = DNSERealtimeManager()
