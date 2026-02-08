# src/msg_handler/base.py
from abc import ABC, abstractmethod
from .schemas import SensorMessage

class IBasePublisher(ABC):
    """Interface for sending data"""
    
    @abstractmethod
    def connect(self):
        """connect to another data component"""
        pass

    @abstractmethod
    def send(self, msg: SensorMessage):
        """Send a validated SensorMessage. Recommended for most use cases.

        Args:
            msg (SensorMessage): The validated message object to be sent.
        """
        pass

    
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
        """Closing connection completely. It is closing connection."""
        pass