from tigeropen.quote.quote_client import QuoteClient
from .base_api import BaseApi
from . import hist_bar_const as hbc

# TODO: Replace with your actual Tiger API credentials
TIGER_LICENSE = 'your_license'
TIGER_PRIVATE_KEY = 'your_private_key'
TIGER_ACCOUNT = 'your_account'

class TigerApi(BaseApi):
    """
    Tiger specific API for historical data.
    """
    def __init__(self):
        super().__init__('tiger')
        self.client = QuoteClient(license=TIGER_LICENSE, private_key=TIGER_PRIVATE_KEY, tiger_id=TIGER_ACCOUNT)

    def get_hist_bars(self, symbol, interval, start_time=None, end_time=None):
        standardized_symbol = self._standardize_symbol(symbol)
        standardized_interval = self._standardize_interval(interval)

        bars = self.client.get_bars(symbols=[standardized_symbol], period=standardized_interval, begin_time=start_time, end_time=end_time)
        return self._parse_data(bars)

    def _standardize_symbol(self, symbol):
        # Symbol format for Tiger depends on the market.
        # For now, we assume the user provides the correct symbol.
        return symbol

    def _standardize_interval(self, interval):
        interval_map = {
            hbc.INTERVAL_1MINUTE: '1min',
            hbc.INTERVAL_5MINUTE: '5min',
            hbc.INTERVAL_15MINUTE: '15min',
            hbc.INTERVAL_30MINUTE: '30min',
            hbc.INTERVAL_1HOUR: '60min',
            hbc.INTERVAL_1DAY: 'day',
        }
        return interval_map.get(interval, interval)

    def _parse_data(self, data):
        if data.empty:
            return {}

        parsed_data = {
            hbc.OHLCV_TIMESTAMP: data['time'].tolist(),
            hbc.OHLCV_OPEN: data['open'].tolist(),
            hbc.OHLCV_HIGH: data['high'].tolist(),
            hbc.OHLCV_LOW: data['low'].tolist(),
            hbc.OHLCV_CLOSE: data['close'].tolist(),
            hbc.OHLCV_VOLUME: data['volume'].tolist(),
        }
        return parsed_data
