# src/msg_handler/backends/zmq.py
import zmq
import logging
from ..base import IBasePublisher
from ..schemas import SensorMessage

logger = logging.getLogger(__name__)

class ZmqPublisher(IBasePublisher):
    def __init__(self, endpoint: str = "tcp://*:5555"):
        super().__init__()
        self.endpoint = endpoint
        self.ctx = None
        self.socket = None

    def __enter__(self):
        logger.info("Preparing to connect")
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        logger.info("Ending: self distracting.")
        self.close()

    def connect(self):
        if self.socket:
            logger.warning(f"Already connected to {self.endpoint}")
            return
        
        logger.info(f"Connecting to ZMQ endpoint: {self.endpoint}")
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PUB)

        try:
            self.socket.bind(self.endpoint)
            logger.info("ZMQ Socket bound successfully.")
        except zmq.ZMQError as e:
            raise ConnectionError(f"Failed to bind to {self.endpoint}: {e}")


    def send_raw(self, data: str):
        if not isinstance(data, str):
            raise TypeError(f"payload must be str! payload datatype is {type(data)}")

        if not self.socket:
            logger.info("Attempted to send data but socket is not connected.")
            raise ConnectionError("Not connected. Call connect() first.")
        self.socket.send_string(data)

    def close(self):
        
        if self.socket:
            self.socket.close()
            self.socket = None
        if self.ctx:
            self.ctx.term()
            self.ctx = None