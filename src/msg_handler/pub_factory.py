from typing import Literal, Union
import zmq
import zmq.asyncio
from pydantic import BaseModel, ConfigDict
from .pub_base import BasePublisher, AsyncBasePublisher
from .backends.pub_zmq import ZmqPublisher, AsyncZmqPublisher


class ZmqPubOptions(BaseModel):
    """
    Configuration options for ZeroMQ publisher.

    Attributes:
        backend_type: Fixed literal for ZMQ backend.
        endpoint: ZMQ connection string.
        is_connect: If True, uses 'connect'; if False, uses 'bind'.
        context: Optional shared ZMQ non/asyncio context.
        hwm: PUB socket send high water mark.

    Note:
        'bind' for permanent infrastructure (start these first); 'connect' for temporary or secondary components.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    backend_type: Literal["zmq"] = "zmq"
    endpoint: str = "tcp://localhost:5555"
    is_connect: bool = True
    context: zmq.asyncio.Context | zmq.Context | None = None
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
    options: PublisherOptions
) -> BasePublisher:
    """
    Factory method to create a publisher instance.

    Args:
        options: Configuration object for the chosen backend.
    """
    if options.backend_type == "zmq":
        context = options.context
        if context is None:
            sync_context = None
        elif isinstance(context, zmq.asyncio.Context):
            raise TypeError(
                "ZmqPubOptions.context must be zmq.Context for sync publisher."
            )
        else:
            sync_context = context

        return ZmqPublisher(
            endpoint=options.endpoint,
            is_connect=options.is_connect,
            context=sync_context,
            hwm=options.hwm,
        )
    elif options.backend_type == "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    else:
        raise ValueError(f"Unknown backend type: {options.backend_type}")


def get_async_publisher(
    options: PublisherOptions
) -> AsyncBasePublisher:
    """
    Factory method to create an async publisher instance.

    Args:
        options: Configuration object for the chosen backend.
    """
    if options.backend_type == "zmq":
        context = options.context
        if context is None:
            async_context = None
        elif isinstance(context, zmq.asyncio.Context):
            async_context = context
        else:
            raise TypeError(
                "ZmqPubOptions.context must be zmq.asyncio.Context for async publisher."
            )

        return AsyncZmqPublisher(
            endpoint=options.endpoint,
            is_connect=options.is_connect,
            context=async_context,
            hwm=options.hwm,
        )
    elif options.backend_type == "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    else:
        raise ValueError(f"Unknown backend type: {options.backend_type}")
