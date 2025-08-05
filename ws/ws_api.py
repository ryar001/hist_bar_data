from abc import ABC, abstractmethod

class BaseWsApi(ABC):
    def __init__(self, exchange_name):
        self.exchange_name = exchange_name

    @abstractmethod
    def subscribe_ohlcv(self, symbol, interval, callback):
        pass

    @abstractmethod
    def subscribe_trades(self, symbol, callback):
        pass

    @abstractmethod
    def subscribe_depth(self, symbol, callback):
        pass

    # Add other common subscription methods as needed

from .tiger_ws_api import TigerWsApi

class WsApi:
    def __init__(self):
        self.apis = {}
        self.register_api('tiger', TigerWsApi())

    def register_api(self, exchange_name, api_instance):
        if not isinstance(api_instance, BaseWsApi):
            raise ValueError("API instance must inherit from BaseWsApi")
        self.apis[exchange_name] = api_instance

    def subscribe_ohlcv(self, exchange, symbol, interval, callback):
        api = self.apis.get(exchange)
        if not api:
            raise ValueError(f"No WebSocket API registered for exchange: {exchange}")
        api.subscribe_ohlcv(symbol, interval, callback)

    def subscribe_trades(self, exchange, symbol, callback):
        api = self.apis.get(exchange)
        if not api:
            raise ValueError(f"No WebSocket API registered for exchange: {exchange}")
        api.subscribe_trades(symbol, callback)

    def subscribe_depth(self, exchange, symbol, callback):
        api = self.apis.get(exchange)
        if not api:
            raise ValueError(f"No WebSocket API registered for exchange: {exchange}")
        api.subscribe_depth(symbol, callback)
