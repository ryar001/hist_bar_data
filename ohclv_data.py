import numpy as np
import pandas as pd
from . import hist_bar_const as hbc

class OhclvData:
    """
    A class to hold and process OHLCV data.
    """
    def __init__(self, parsed_data):
        if not isinstance(parsed_data, dict):
            raise TypeError("parsed_data must be a dictionary.")

        self.timestamps = np.array(parsed_data.get(hbc.OHLCV_TIMESTAMP, []), dtype=np.int64)
        self.open = np.array(parsed_data.get(hbc.OHLCV_OPEN, []), dtype=np.float64)
        self.high = np.array(parsed_data.get(hbc.OHLCV_HIGH, []), dtype=np.float64)
        self.low = np.array(parsed_data.get(hbc.OHLCV_LOW, []), dtype=np.float64)
        self.close = np.array(parsed_data.get(hbc.OHLCV_CLOSE, []), dtype=np.float64)
        self.volume = np.array(parsed_data.get(hbc.OHLCV_VOLUME, []), dtype=np.float64)

        self._validate_data()

    def _validate_data(self):
        lengths = [len(self.timestamps), len(self.open), len(self.high), len(self.low), len(self.close), len(self.volume)]
        if len(set(lengths)) > 1:
            raise ValueError("All OHLCV arrays must have the same length.")

    def to_df(self, ascending: bool = True) -> pd.DataFrame:
        """
        Converts the OHLCV data to a pandas DataFrame.

        Args:
            ascending (bool): Whether to sort the DataFrame by timestamp in ascending order.

        Returns:
            pd.DataFrame: The OHLCV data as a DataFrame.
        """
        df = pd.DataFrame({
            hbc.OHLCV_TIMESTAMP: self.timestamps,
            hbc.OHLCV_OPEN: self.open,
            hbc.OHLCV_HIGH: self.high,
            hbc.OHLCV_LOW: self.low,
            hbc.OHLCV_CLOSE: self.close,
            hbc.OHLCV_VOLUME: self.volume
        })
        df[hbc.OHLCV_TIMESTAMP] = pd.to_datetime(df[hbc.OHLCV_TIMESTAMP], unit='ms')
        df = df.set_index(hbc.OHLCV_TIMESTAMP)
        df = df.sort_index(ascending=ascending)
        return df

    def refreq(self, new_interval: str) -> 'OhclvData':
        """
        Resamples the OHLCV data to a new interval.

        Args:
            new_interval (str): The new interval to resample to (e.g., '15T', '1H').

        Returns:
            OhclvData: A new OhclvData object with the resampled data.
        """
        df = self.to_df()
        
        resampling_rules = {
            hbc.OHLCV_OPEN: 'first',
            hbc.OHLCV_HIGH: 'max',
            hbc.OHLCV_LOW: 'min',
            hbc.OHLCV_CLOSE: 'last',
            hbc.OHLCV_VOLUME: 'sum'
        }

        resampled_df = df.resample(new_interval).agg(resampling_rules).dropna()

        resampled_data = {
            hbc.OHLCV_TIMESTAMP: resampled_df.index.astype(np.int64) // 10**6,
            hbc.OHLCV_OPEN: resampled_df[hbc.OHLCV_OPEN].values,
            hbc.OHLCV_HIGH: resampled_df[hbc.OHLCV_HIGH].values,
            hbc.OHLCV_LOW: resampled_df[hbc.OHLCV_LOW].values,
            hbc.OHLCV_CLOSE: resampled_df[hbc.OHLCV_CLOSE].values,
            hbc.OHLCV_VOLUME: resampled_df[hbc.OHLCV_VOLUME].values
        }

        return OhclvData(resampled_data)
