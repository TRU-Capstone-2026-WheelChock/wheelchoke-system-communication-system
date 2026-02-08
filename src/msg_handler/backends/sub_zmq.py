import zmq
import zmq.asyncio
from ..sub_base import BaseSubscriber, AsyncBaseSubscriber
from ..schemas import SensorMessage

# --- 同期版 (Sync) ---
class ZmqSubscriber(BaseSubscriber):
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.SUB)
        self._running = True

    def connect(self):
        self.socket.connect(self.endpoint)
        self.socket.subscribe("") 

    def __iter__(self):
        if self.socket.closed:
            self.connect()
            
        while self._running:
            try:
                json_str = self.socket.recv_string()
                yield SensorMessage.model_validate_json(json_str)
            except zmq.ZMQError:
                break
            except Exception as e:
                print(f"Error receiving message: {e}")
                continue

    def close(self):
        self._running = False
        self.socket.close()
        self.ctx.term()


# --- 非同期版 (Async) ---
class AsyncZmqSubscriber(AsyncBaseSubscriber):
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.SUB)
        self._running = True

    async def connect(self):
        self.socket.connect(self.endpoint)
        self.socket.subscribe("")

    async def __aiter__(self):
        if self.socket.closed:
            await self.connect()

        while self._running:
            try:
                json_str = await self.socket.recv_string()
                yield SensorMessage.model_validate_json(json_str)
            except zmq.ZMQError:
                break
            except Exception as e:
                print(f"Async receive error: {e}")
                continue

    async def close(self):
        self._running = False
        self.socket.close()
        self.ctx.term()