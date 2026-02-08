import os
from .pub_base import BasePublisher
from .backends.pub_zmq import ZmqPublisher

def get_publisher(backend_type: str = "zmq", **kwargs) -> BasePublisher:
    """
    Factory 
    """
    backend = backend_type.lower()

    if backend == "zmq":
        return ZmqPublisher(**kwargs)
    
    elif backend == "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    
    else:
        raise ValueError(f"Unknown backend type: {backend_type}")