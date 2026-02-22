import zmq
import zmq.asyncio
from ..sub_base import BaseSubscriber, AsyncBaseSubscriber
from ..schemas import ExpectedMessageType, parse_message_json


def _unwrap_topic_prefixed_payload(raw_msg: str) -> str:
    """Return JSON payload part from `topic + space + json`, or raw_msg if already JSON."""
    if raw_msg.lstrip().startswith("{"):
        return raw_msg

    topic_and_payload = raw_msg.split(" ", 1)
    if len(topic_and_payload) == 2 and topic_and_payload[1].lstrip().startswith("{"):
        return topic_and_payload[1]

    return raw_msg


class ZmqSubscriber(BaseSubscriber):
    """
    Synchronous ZeroMQ implementation of a subscriber.

    Attributes:
        endpoint: ZMQ endpoint string.
        is_bind: Whether to bind (True) or connect (False).
        topics: List of topic strings to filter.
        expected_type: Expected message kind for parsing.
        context: Optional shared ZMQ context.
        hwm: SUB socket receive high water mark.
        is_own_context: True when context is internally created.
    """

    def __init__(
        self,
        endpoint: str,
        is_bind=True,
        topics: list[str] | None = None,
        expected_type: ExpectedMessageType = "auto",
        context: zmq.Context | None = None,
        hwm: int = 1000,
    ):
        self.endpoint = endpoint
        self.topics = topics or [""]
        self.is_bind = is_bind
        self.expected_type = expected_type
        self.hwm = hwm
        self.ctx = context
        self.is_own_context: bool = context is None
        self.socket = None
        self._running = True

    def connect(self):
        """Set up ZMQ socket and subscribe to topics."""
        if not self.socket or self.socket.closed:
            if self.ctx is None:
                self.ctx = zmq.Context()
            self.socket = self.ctx.socket(zmq.SUB)
            self.socket.setsockopt(zmq.RCVHWM, self.hwm)

        try:    
            if self.is_bind:
                self.socket.bind(self.endpoint)
            else:
                self.socket.connect(self.endpoint)
            for t in self.topics:
                self.socket.subscribe(t)
        except zmq.ZMQError as e:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
            hint_msg = "Did you try to bind multiple subscribers to the same address & port?"
            raise ConnectionError(f"Failed to setup ZMQ Subscriber on {self.endpoint}: {e}, {hint_msg}")

    def __iter__(self):
        """Blocking generator that yields parsed messages by expected_type."""
        if not self.socket or self.socket.closed:
            self.connect()

        while self._running:
            try:
                raw_msg = self.socket.recv_string()
                json_str = _unwrap_topic_prefixed_payload(raw_msg)
                yield parse_message_json(json_str, expected_type=self.expected_type)
            except zmq.ZMQError:
                break
            except Exception as e:
                print(f"Error receiving message: {e}")
                continue

    def close(self):
        """Stop processing and terminate ZMQ context."""
        self._running = False
        if self.socket:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
        if self.ctx and self.is_own_context:
            self.ctx.term()
            self.ctx = None


class AsyncZmqSubscriber(AsyncBaseSubscriber):
    """
    Asynchronous ZeroMQ implementation of a subscriber.

    Attributes:
        endpoint: ZMQ endpoint string.
        is_bind: Whether to bind (True) or connect (False).
        topics: List of topic strings to filter.
        expected_type: Expected message kind for parsing.
        context: Optional shared ZMQ asyncio context.
        hwm: SUB socket receive high water mark.
        is_own_context: True when context is internally created.
    """

    def __init__(
        self,
        endpoint: str,
        is_bind=True,
        topics: list[str] | None = None,
        expected_type: ExpectedMessageType = "auto",
        context: zmq.asyncio.Context | None = None,
        hwm: int = 1000,
    ):
        self.endpoint = endpoint
        self.is_bind = is_bind
        self.topics = topics or [""]
        self.expected_type = expected_type
        self.hwm = hwm
        self.ctx = context
        self.is_own_context: bool = context is None
        self.socket = None
        self._running = True

    async def connect(self):
        """Set up async ZMQ socket and subscribe to topics."""
        if not self.socket or self.socket.closed:
            if self.ctx is None:
                self.ctx = zmq.asyncio.Context()
            self.socket = self.ctx.socket(zmq.SUB)
            self.socket.setsockopt(zmq.RCVHWM, self.hwm)
        
        try:    
            if self.is_bind:
                self.socket.bind(self.endpoint)
            else:
                self.socket.connect(self.endpoint)
            for t in self.topics:
                self.socket.subscribe(t)
        except zmq.ZMQError as e:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
            hint_msg = "Did you try to bind multiple subscribers to the same address & port?"
            raise ConnectionError(f"Failed to setup ZMQ Subscriber on {self.endpoint}: {e}, {hint_msg}")


    async def __aiter__(self):
        """Async generator that yields parsed messages by expected_type."""
        if not self.socket or self.socket.closed:
            await self.connect()

        while self._running:
            try:
                raw_msg = await self.socket.recv_string()
                json_str = _unwrap_topic_prefixed_payload(raw_msg)
                yield parse_message_json(json_str, expected_type=self.expected_type)
            except zmq.ZMQError:
                break
            except Exception as e:
                print(f"Async receive error: {e}")
                continue

    async def close(self):
        """Stop processing and terminate ZMQ context asynchronously."""
        self._running = False
        if self.socket:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None
        if self.ctx and self.is_own_context:
            self.ctx.term()
            self.ctx = None
