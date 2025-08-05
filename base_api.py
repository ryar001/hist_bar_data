from abc import ABC, abstractmethod

class BaseApi(ABC):
    """
    Abstract base class for historical data APIs.
    """
    def __init__(self, exchange):
        self.exchange = exchange

    @abstractmethod
    def get_hist_bars(self, symbol, interval):
        pass

    @abstractmethod
    def _standardize_symbol(self, symbol):
        pass

    @abstractmethod
    def _standardize_interval(self, interval):
        pass

    @abstractmethod
    def _parse_data(self, data):
        pass
