from typing import Literal, Union
import zmq
import zmq.asyncio
from pydantic import BaseModel
from .pub_base import BasePublisher, AsyncBasePublisher
from .backends.pub_zmq import ZmqPublisher, AsyncZmqPublisher


class ZmqPubOptions(BaseModel):
    """
    Configuration options for ZeroMQ publisher.

    Attributes:
        backend_type: Fixed literal for ZMQ backend.
        endpoint: ZMQ connection string.
        is_connect: If True, uses 'connect'; if False, uses 'bind'.
        hwm: PUB socket send high water mark.

    Note:
        'bind' for permanent infrastructure (start these first); 'connect' for temporary or secondary components.
    """

    backend_type: Literal["zmq"] = "zmq"
    endpoint: str = "tcp://localhost:5555"
    is_connect: bool = True
    hwm: int = 1000


class MqttPubOptions(BaseModel):
    """
    Configuration options for MQTT publisher.

    Attributes:
        backend_type: Fixed literal for MQTT backend.
        broker_url: URL of the MQTT broker.
        port: Connection port.
    """

    backend_type: Literal["mqtt"] = "mqtt"
    broker_url: str
    port: int


PublisherOptions = Union[ZmqPubOptions, MqttPubOptions]


def get_publisher(
    options: PublisherOptions,
    context: zmq.Context | None = None
) -> BasePublisher:
    """
    Factory method to create a publisher instance.

    Args:
        options: Configuration object for the chosen backend.
        context: Optional shared ZMQ context.
    """
    if options.backend_type == "zmq":
        return ZmqPublisher(
            endpoint=options.endpoint,
            is_connect=options.is_connect,
            context=context,
            hwm=options.hwm,
        )
    elif options.backend_type == "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    else:
        raise ValueError(f"Unknown backend type: {options.backend_type}")


def get_async_publisher(
    options: PublisherOptions,
    context: zmq.asyncio.Context | None = None
) -> AsyncBasePublisher:
    """
    Factory method to create an async publisher instance.

    Args:
        options: Configuration object for the chosen backend.
        context: Optional shared ZMQ asyncio context.
    """
    if options.backend_type == "zmq":
        return AsyncZmqPublisher(
            endpoint=options.endpoint,
            is_connect=options.is_connect,
            context=context,
            hwm=options.hwm,
        )
    elif options.backend_type == "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    else:
        raise ValueError(f"Unknown backend type: {options.backend_type}")
