import asyncio
import websockets
import json

async def test_realtime_dnse():
    uri = "ws://localhost:8000/ws/realtime/dnse/VNM"
    print(f"Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected! Waiting for data (Tick/StockInfo)...")
            print("Press Ctrl+C to stop.")
            
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                print(f"Received: {json.dumps(data, indent=2)}")
                
    except ConnectionRefusedError:
        print("Connection failed. Is the server running on port 8000?")
    except websockets.exceptions.ConnectionClosed:
        print("Connection closed by server.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    try:
        # Install websockets if needed: pip install websockets
        asyncio.run(test_realtime_dnse())
    except KeyboardInterrupt:
        print("\nTest stopped.")
