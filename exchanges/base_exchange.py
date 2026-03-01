from abc import ABC, abstractmethod

class BaseExchange(ABC):
    @abstractmethod
    def get_trades(self, symbol: str, start_time: str, end_time: str):
        pass


