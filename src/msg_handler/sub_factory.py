import os
from typing import Literal, Union, List
import zmq
import zmq.asyncio
from pydantic import BaseModel, Field, ConfigDict
from .sub_base import BaseSubscriber, AsyncBaseSubscriber
from .backends.sub_zmq import ZmqSubscriber, AsyncZmqSubscriber
from .schemas import ExpectedMessageType

BACKEND_TYPE = os.getenv("MSG_BACKEND", "zmq")


class ZmqSubOptions(BaseModel):
    """
    Configuration options for ZeroMQ subscriber.

    Attributes:
        backend_type: Fixed literal for ZMQ backend.
        endpoint: ZMQ connection string (e.g., tcp://localhost:5555).
        topics: List of topics to subscribe to.
        is_bind: Whether to bind or connect to the endpoint.
        expected_type: Expected message kind to parse ("auto" | "sensor" | "display" | "motor").
        hwm: SUB socket receive high water mark.
    Note:
        'bind' for permanent infrastructure (start these first); 'connect' for temporary or secondary components.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    backend_type: Literal["zmq"] = "zmq"
    endpoint: str = "tcp://localhost:5555"
    topics: List[str] = Field(default_factory=lambda: [""])
    is_bind: bool = True
    expected_type: ExpectedMessageType = "auto"
    hwm: int = 1000
    context: zmq.Context |zmq.asyncio.Context| None = None


class MqttSubOptions(BaseModel):
    """
    Configuration options for MQTT subscriber.NOT USABLE

    Attributes:
        backend_type: Fixed literal for MQTT backend.
        broker_url: URL of the MQTT broker.
        port: Connection port.
        topics: List of MQTT topics to subscribe to.
    """

    backend_type: Literal["mqtt"] = "mqtt"
    broker_url: str
    port: int = 1883
    topics: List[str] = Field(default_factory=list)


SubscriberOptions = Union[ZmqSubOptions, MqttSubOptions]


def get_subscriber(
    options: SubscriberOptions
) -> BaseSubscriber:
    """
    Factory method to create a synchronous subscriber.

    Args:
        options: Configuration object for the chosen backend.
        context: Optional shared ZMQ context.
        Note:
            `options.hwm` is applied as SUB receive high water mark.
    """
    if options.backend_type == "zmq":
        context = options.context
        if context is None:
            sync_context = None
        elif isinstance(context, zmq.asyncio.Context):
            raise TypeError(
                "ZmqSubOptions.context must be zmq.Context for sync subscriber."
            )
        else:
            sync_context = context

        return ZmqSubscriber(
            endpoint=options.endpoint,
            topics=options.topics,
            is_bind=options.is_bind,
            expected_type=options.expected_type,
            context=sync_context,
            hwm=options.hwm,
        )
    elif options.backend_type == "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    else:
        raise ValueError(f"Unknown backend type: {options.backend_type}")


def get_async_subscriber(
    options: SubscriberOptions
) -> AsyncBaseSubscriber:
    """
    Factory method to create an asynchronous subscriber.

    Args:
        options: Configuration object for the chosen backend.
        context: Optional shared ZMQ asyncio context.
        Note:
            `options.hwm` is applied as SUB receive high water mark.
    """
    if options.backend_type == "zmq":
        context = options.context
        if context is None:
            async_context = None
        elif isinstance(context, zmq.asyncio.Context):
            async_context = context
        else:
            raise TypeError(
                "ZmqSubOptions.context must be zmq.asyncio.Context for async subscriber."
            )

        return AsyncZmqSubscriber(
            endpoint=options.endpoint,
            topics=options.topics,
            is_bind=options.is_bind,
            expected_type=options.expected_type,
            context=async_context,
            hwm=options.hwm,
        )
    elif options.backend_type == "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    else:
        raise ValueError(f"Unknown backend type: {options.backend_type}")
