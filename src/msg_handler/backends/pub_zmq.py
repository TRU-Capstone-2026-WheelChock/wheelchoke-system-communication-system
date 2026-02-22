import zmq
import zmq.asyncio
import logging
from ..pub_base import BasePublisher, AsyncBasePublisher

logger = logging.getLogger(__name__)


class ZmqPublisher(BasePublisher):
    """
    ZeroMQ implementation of a publisher.

    Attributes:
        endpoint: ZMQ connection string.
        is_connect: If True, performs 'connect'. If False, performs 'bind'.
        context: Optional shared ZMQ context.
        hwm: PUB socket send high water mark.
        is_own_context: True when context is internally created.
    """

    def __init__(
        self,
        endpoint: str = "tcp://localhost:5555",
        is_connect: bool = True,
        context: zmq.Context | None = None,
        hwm: int = 1000,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.is_connect = is_connect
        self.hwm = hwm
        self.ctx = context
        self.is_own_context: bool = context is None
        self.socket = None

    def _connect_impl(self):
        """Internal logic to establish the ZMQ PUB socket connection."""
        if self.socket and not self.socket.closed:
            logger.warning(f"Already connected to {self.endpoint}")
            return

        logger.info(f"Connecting to ZMQ endpoint: {self.endpoint}")
        if self.ctx is None:
            self.ctx = zmq.Context()

        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.setsockopt(zmq.SNDHWM, self.hwm)

        try:
            if self.is_connect:
                self.socket.connect(self.endpoint)
                logger.info(f"ZMQ Socket connected successfully to {self.endpoint}")
            else:
                self.socket.bind(self.endpoint)
                logger.info(f"ZMQ Socket bound successfully to {self.endpoint}")
        except zmq.ZMQError as e:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
            raise ConnectionError(f"Failed to setup ZMQ on {self.endpoint}: {e}")

    def send_raw(self, data: str):
        """
        Sends a raw string message via ZMQ.

        Note:
            Requires the socket to be initialized first.
        """
        if not isinstance(data, str):
            raise TypeError(f"Payload must be str! Received: {type(data)}")

        if not self.socket:
            logger.error("Attempted to send data but socket is not connected.")
            raise ConnectionError("Not connected. Call connect() first.")
        self.socket.send_string(data)

    def close(self):
        """Close socket. Terminate context only when this instance owns it."""
        if self.socket:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None

        if self.ctx and self.is_own_context:
            self.ctx.term()
            self.ctx = None

class AsyncZmqPublisher(AsyncBasePublisher):
    """
    Asynchronous ZeroMQ implementation of a publisher.

    Attributes:
        endpoint: ZMQ connection string.
        is_connect: If True, performs 'connect'. If False, performs 'bind'.
        context: Optional shared ZMQ asyncio context.
        hwm: PUB socket send high water mark.
        is_own_context: True when context is internally created.
    """

    def __init__(
        self,
        endpoint: str = "tcp://localhost:5555",
        is_connect: bool = True,
        context: zmq.asyncio.Context | None = None,
        hwm: int = 1000,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.is_connect = is_connect
        self.hwm = hwm
        self.ctx = context
        self.is_own_context: bool = context is None
        self.socket = None

    async def _connect_impl(self):
        """Internal logic to establish the async ZMQ PUB socket connection."""
        if self.socket and not self.socket.closed:
            logger.warning(f"Already connected to {self.endpoint}")
            return

        logger.info(f"Connecting to ZMQ endpoint: {self.endpoint}")
        if self.ctx is None:
            self.ctx = zmq.asyncio.Context()

        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.setsockopt(zmq.SNDHWM, self.hwm)

        try:
            if self.is_connect:
                self.socket.connect(self.endpoint)
                logger.info(f"ZMQ Socket connected successfully to {self.endpoint}")
            else:
                self.socket.bind(self.endpoint)
                logger.info(f"ZMQ Socket bound successfully to {self.endpoint}")
        except zmq.ZMQError as e:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
            raise ConnectionError(f"Failed to setup ZMQ on {self.endpoint}: {e}")

    async def send_raw(self, data: str):
        """
        Sends a raw string message via async ZMQ.

        Note:
            Requires the socket to be connected first.
        """
        if not isinstance(data, str):
            raise TypeError(f"Payload must be str! Received: {type(data)}")

        if not self.socket:
            logger.error("Attempted to send data but socket is not connected.")
            raise ConnectionError("Not connected. Call connect() first.")
        await self.socket.send_string(data)

    async def close(self):
        """Close socket. Terminate context only when this instance owns it."""
        if self.socket:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
        if self.ctx and self.is_own_context:
            self.ctx.term()
            self.ctx = None
