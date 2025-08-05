from .REST_api.binance_api import BinanceApi
from .REST_api.okx_api import OkxApi
from .REST_api.xt_api import XtApi
from .REST_api.tiger_api import TigerApi
from .ohclv_data import OhclvData

class HistApi:
    """
    A manager class to call different REST APIs to get historic data.
    """
    def __init__(self, exchange):
        if exchange == 'binance':
            self.api = BinanceApi()
        elif exchange == 'okx':
            self.api = OkxApi()
        elif exchange == 'xt':
            self.api = XtApi()
        elif exchange == 'tiger':
            self.api = TigerApi()
        else:
            raise ValueError(f'Exchange {exchange} is not supported.')

    def get_hist_bars(self, symbol, interval):
        raw_data = self.api.get_hist_bars(symbol, interval)
        return OhclvData(raw_data)
