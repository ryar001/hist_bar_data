import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from hist_market_data.REST_api.tiger_api import TigerApi
from hist_market_data import hist_bar_const as hbc

class TestTigerApi(unittest.TestCase):

    @patch('hist_market_data.REST_api.tiger_api.QuoteClient')
    def test_get_hist_bars(self, MockQuoteClient):
        # Arrange
        mock_client_instance = MockQuoteClient.return_value
        mock_get_bars = mock_client_instance.get_bars

        # Create a sample DataFrame to be returned by the mock
        mock_data = pd.DataFrame({
            'time': [1672531200000, 1672531260000],
            'open': [100, 101],
            'high': [102, 103],
            'low': [99, 100],
            'close': [101, 102],
            'volume': [1000, 1200]
        })
        mock_get_bars.return_value = mock_data

        tiger_api = TigerApi()

        # Act
        hist_data = tiger_api.get_hist_bars('AAPL', hbc.INTERVAL_1MINUTE)

        # Assert
        self.assertIsNotNone(hist_data)
        data_dict = hist_data.to_dict()

        self.assertEqual(data_dict[hbc.OHLCV_TIMESTAMP], [1672531200000, 1672531260000])
        self.assertEqual(data_dict[hbc.OHLCV_OPEN], [100, 101])
        self.assertEqual(data_dict[hbc.OHLCV_HIGH], [102, 103])
        self.assertEqual(data_dict[hbc.OHLCV_LOW], [99, 100])
        self.assertEqual(data_dict[hbc.OHLCV_CLOSE], [101, 102])
        self.assertEqual(data_dict[hbc.OHLCV_VOLUME], [1000, 1200])

        mock_get_bars.assert_called_once_with(symbols=['AAPL'], period='1min', begin_time=None, end_time=None)

if __name__ == '__main__':
    unittest.main()
