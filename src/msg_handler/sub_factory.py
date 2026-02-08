import os
from .sub_base import BaseSubscriber, AsyncBaseSubscriber


BACKEND_TYPE = os.getenv("MSG_BACKEND", "zmq")

def get_subscriber(endpoint: str) -> BaseSubscriber:

    if BACKEND_TYPE == "zmq":
        from .backends.sub_zmq import ZmqSubscriber
        return ZmqSubscriber(endpoint)
    else:
        raise ValueError(f"Unknown backend: {BACKEND_TYPE}")

def get_async_subscriber(endpoint: str) -> AsyncBaseSubscriber:

    if BACKEND_TYPE == "zmq":
        from .backends.sub_zmq import AsyncZmqSubscriber
        return AsyncZmqSubscriber(endpoint)
    else:
        raise ValueError(f"Unknown backend: {BACKEND_TYPE}")