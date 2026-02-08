# src/msg_handler/backends/zmq.py
import zmq
from ..base import BasePublisher
from ..schemas import SensorMessage

class ZmqPublisher(BasePublisher):
    def __init__(self, endpoint: str = "tcp://*:5555"):
        self.endpoint = endpoint
        self.ctx = None
        self.socket = None

    def connect(self):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.bind(self.endpoint)

    def send(self, msg: SensorMessage):
        """
        send informati
        
        :param self: Description
        :param msg: Description
        :type msg: SensorMessage
        """
        if not self.socket:
            raise ConnectionError("Not connected. Call connect() first.")
        self.socket.send_string(msg.model_dump_json())

    def close(self):
        if self.socket:
            self.socket.close()
        if self.ctx:
            self.ctx.term()