import os
from .pub_base import BasePublisher
from .backends.pub_zmq import ZmqPublisher

from typing import Literal, Union, Unpack

from pydantic import BaseModel

class ZmqOptions(BaseModel):
    backend_type : Literal["zmq"] = "zmq"
    endpoint : str = "tcp://*:5555"
    is_client : bool = True

class MqttOptions(BaseModel):
    backend_type: Literal["mqtt"] = "mqtt"
    broker_url: str
    port: int   

PublisherOptions = Union[ZmqOptions, MqttOptions]

def get_publisher(options: PublisherOptions) -> BasePublisher:
    """
    Factory 
    """


    if options.backend_type == "zmq":
        return ZmqPublisher(
            endpoint=options.endpoint,
            is_client=options.is_client
        )
    
    elif options.backend_type== "mqtt":
        raise NotImplementedError("MQTT backend is not implemented yet.")
    
    else:
        raise ValueError(f"Unknown backend type: {options.backend_type}")