from abc import ABC, abstractmethod
from typing import Iterator, AsyncIterator
from .schemas import SensorMessage

class BaseSubscriber(ABC):
    
    @abstractmethod
    def connect(self):

        pass

    @abstractmethod
    def __iter__(self) -> Iterator[SensorMessage]:

        pass

    @abstractmethod
    def close(self):

        pass

class AsyncBaseSubscriber(ABC):


    @abstractmethod
    async def connect(self):

        pass

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[SensorMessage]:
        """
        """
        pass

    @abstractmethod
    async def close(self):
        pass


    