# src/msg_handler/backends/zmq.py
import zmq
import logging
from ..pub_base import BasePublisher
from ..schemas import SensorMessage

logger = logging.getLogger(__name__)

class ZmqPublisher(BasePublisher):
    def __init__(self, endpoint: str = "tcp://*:5555", is_client : bool = True):
        """
        Args:
            endpoint (str): ZMQ connection string.
            is_client (bool): 
                If True, performs 'connect' (Initiate connection).
                If False, performs 'bind' (Wait for connections).
        """

        super().__init__()
        self.endpoint = endpoint
        self.is_client = is_client
        self.ctx = None
        self.socket = None

    def _connect_impl(self):
        if self.socket:
            logger.warning(f"Already connected to {self.endpoint}")
            return
        
        logger.info(f"Connecting to ZMQ endpoint: {self.endpoint}")
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PUB)

        try:
            if self.is_client:
                self.socket.connect(self.endpoint)
                logger.info(f"ZMQ Socket bound successfully to {self.endpoint}")
            else:
                self.socket.bind(self.endpoint)
                logger.info(f"ZMQ Socket connected successfully to {self.endpoint}")
        except zmq.ZMQError as e:

            self.socket.close() # failed -> self destruction!
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