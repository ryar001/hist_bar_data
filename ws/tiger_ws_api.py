from tigeropen.push.push_client import PushClient
from tigeropen.common.util.common_utils import get_logger
from ..hist_bar_const import (
    OHLCV_TIMESTAMP,
    OHLCV_OPEN,
    OHLCV_HIGH,
    OHLCV_LOW,
    OHLCV_CLOSE,
    OHLCV_VOLUME,
)
from .ws_api import BaseWsApi

# TODO: Replace with your actual Tiger API credentials
TIGER_LICENSE = 'your_license'
TIGER_PRIVATE_KEY = 'your_private_key'
TIGER_ACCOUNT = 'your_account'

_logger = get_logger(__name__)

class TigerWsApi(BaseWsApi):
    def __init__(self):
        super().__init__('tiger')
        self.push_client = PushClient(license=TIGER_LICENSE, private_key=TIGER_PRIVATE_KEY, tiger_id=TIGER_ACCOUNT)
        self.ohlcv_callbacks = {}

        # Register the kline_changed callback
        self.push_client.kline_changed = self._on_kline_changed

    def _on_kline_changed(self, kline_data):
        symbol = kline_data.symbol
        # Assuming interval is not directly available in kline_data, 
        # so we might need to infer it or pass it through the callback registration.
        # For now, we'll just pass the symbol.
        
        # Standardize the kline data
        standardized_data = {
            OHLCV_TIMESTAMP: kline_data.time,
            OHLCV_OPEN: kline_data.open,
            OHLCV_HIGH: kline_data.high,
            OHLCV_LOW: kline_data.low,
            OHLCV_CLOSE: kline_data.close,
            OHLCV_VOLUME: kline_data.volume,
            # Add other fields if necessary and standardize their names
        }

        # Call the registered callback for this symbol
        if symbol in self.ohlcv_callbacks:
            for callback in self.ohlcv_callbacks[symbol]:
                callback(standardized_data)
        else:
            _logger.warning(f"No OHLCV callback registered for symbol: {symbol}")

    def subscribe_ohlcv(self, symbol, interval, callback):
        # Tiger API's subscribe_kline only takes symbols, not interval.
        # We'll subscribe to the symbol and assume the callback handles the interval filtering if needed.
        # The interval parameter here is for standardization with BaseWsApi.
        
        if symbol not in self.ohlcv_callbacks:
            self.ohlcv_callbacks[symbol] = []
        self.ohlcv_callbacks[symbol].append(callback)

        self.push_client.subscribe_kline(symbols=[symbol])
        _logger.info(f"Subscribed to OHLCV for {symbol} (interval: {interval})")

    def subscribe_trades(self, symbol, callback):
        _logger.warning("Tiger API does not have a direct 'subscribe_trades' method in the provided documentation.")
        pass

    def subscribe_depth(self, symbol, callback):
        _logger.warning("Tiger API does not have a direct 'subscribe_depth' method in the provided documentation.")
        pass
