# backend/trading/indicators/cci.py
import numpy as np
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('cci_logs').get_logger()

"""
Calculate the Commodity Channel Index (CCI) for a given DataFrame. The CCI is calculated using the typical price, which is the average of the high, low, and close prices. The CCI measures the difference between the typical price and its simple moving average, relative to the mean deviation.
"""

class CCI:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the CCI class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Commodity Channel Index (CCI) for a given DataFrame.

        :param df: DataFrame with 'high', 'low', and 'close' prices.
        :param period: Lookback period for CCI calculation.
        :return: DataFrame with the CCI values.
        """
        # Ensure the necessary columns exist
        if any(col not in df.columns for col in ['high', 'low', 'close']):
            logger.error("DataFrame must contain 'high', 'low', and 'close' columns.")
            raise KeyError("DataFrame must contain 'high', 'low', and 'close' columns.")

        # Ensure there is enough data for the calculation
        if len(df) < period:
            logger.warning("Insufficient data for CCI calculation.")
            return df

        # Calculate the typical price
        df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3

        # Calculate the rolling mean of the typical price
        df['tp_sma'] = df['typical_price'].rolling(window=period).mean()

        # Manually calculate the mean absolute deviation (MAD)
        def mean_deviation(series):
            mean_value = series.mean()
            return np.mean(np.abs(series - mean_value))

        # Apply the mean deviation function to the rolling window
        df['mean_deviation'] = df['typical_price'].rolling(window=period).apply(mean_deviation, raw=False)

        # Calculate the CCI
        df['cci'] = (df['typical_price'] - df['tp_sma']) / (0.015 * df['mean_deviation'])

        logger.info(f"CCI calculation for period={period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the CCI results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'CCI').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated CCI values.
        :param period: Period for which the CCI was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = 'cci'
        # Insert CCI results row by row
        for _, row in result_df.iterrows():
            param_value = row['cci']
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted CCI results for {instrument} into SQLite.")
