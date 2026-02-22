# src/msg_handler/base.py
import logging
from abc import ABC, abstractmethod
from .schemas import SupportedMessage

logger = logging.getLogger(__name__)


class BasePublisher(ABC):
    """Interface for sending data"""

    def __init__(self):
        self._seq_no = 0

    def __enter__(self):
        self._connect_impl()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        """
        [Deprecated] This is for Manual connection.
        Please use 'with' statement for automatic resource management.
        """
        logger.warning(
            "⚠️ Direct call to connect() is deprecated. "
            "Use 'with Publisher(...) as pub:' context manager."
        )
        self._connect_impl()

    @abstractmethod
    def _connect_impl(self):
        """Actual connection logic to be implemented by subclasses."""
        pass

    def send(self, msg: SupportedMessage):
        """Send a validated supported message. Recommended for most use cases.

        Args:
            msg (SupportedMessage): The validated message object to be sent.

        Note:
            If `msg.sequence_no` exists and is `0`, it is auto-assigned.
        """
        try:
            # apply sequence number only if sequence num is not set and msg has sequence_no attribute
            if hasattr(msg, "sequence_no") and msg.sequence_no==0:
                setattr(msg, "sequence_no", self._seq_no)
            
            msg_type = getattr(msg, "data_type", msg.__class__.__name__)
            msg_seq = getattr(msg, "sequence_no", "N/A")
            logger.debug(f"Publishing msg {msg_seq} (type: {msg_type})")
            logger.debug(f"content is {msg.model_dump_json()}")

            self.send_raw(msg.model_dump_json())
            self._seq_no += 1
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise

    @abstractmethod
    def send_raw(self, data: str):
        """Send a raw string directly. Useful for testing, debugging, or
            sending custom payloads that don't follow the standard schema.

        Args:
            data (str): The raw string to be transmitted.

        Note:
            This is for testing purpose. Notify to team member if you use this for actual code.
        """
        pass

    @abstractmethod
    def close(self):
        """Closing connection completely."""
        pass


class AsyncBasePublisher(ABC):
    """Async interface for sending data."""

    def __init__(self):
        pass

    async def __aenter__(self):
        await self._connect_impl()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        """
        [Deprecated] This is for Manual connection.
        Please use 'async with' statement for automatic resource management.
        """
        logger.warning(
            "⚠️ Direct call to connect() is deprecated. "
            "Use 'async with get_async_publisher(...) as pub:' context manager."
        )
        await self._connect_impl()

    @abstractmethod
    async def _connect_impl(self):
        """Actual async connection logic to be implemented by subclasses."""
        pass

    async def send(self, msg: SupportedMessage):
        """Send a validated supported message asynchronously.

        Args:
            msg (SupportedMessage): The validated message object to be sent.
        """
        try:
            msg_type = getattr(msg, "data_type", msg.__class__.__name__)
            logger.debug(f"Publishing message (type: {msg_type})")

            msg_json = msg.model_dump_json()
            logger.debug(f"content is {msg_json}")

            await self.send_raw(msg_json)
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise

    @abstractmethod
    async def send_raw(self, data: str):
        """Send a raw string directly. Useful for testing, debugging, or
            sending custom payloads that don't follow the standard schema.

        Args:
            data (str): The raw string to be transmitted.

        Note:
            This is for testing purpose. Notify to team member if you use this for actual code.
        """
        pass

    @abstractmethod
    async def close(self):
        """Closing connection completely."""
        pass
