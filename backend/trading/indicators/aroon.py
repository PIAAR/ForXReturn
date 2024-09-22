# backend/trading/indicators/aroon.py
import numpy as np
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('aroon_logs').get_logger()

"""
    Calculate the Aroon Indicator for a given DataFrame. The Aroon Indicator is a trend-following indicator that measures the time between highs and lows over a given period.
"""

class Aroon:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the Aroon class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=25):
        """
        Calculate the Aroon Up and Aroon Down indicators for a given DataFrame.

        :param df: DataFrame with 'high' and 'low' prices.
        :param period: Lookback period for Aroon calculation.
        :return: DataFrame with the Aroon Up and Aroon Down values.
        """
        # Ensure the necessary columns exist
        if any(col not in df.columns for col in ['high', 'low']):
            logger.error("DataFrame must contain 'high' and 'low' columns.")
            raise KeyError("DataFrame must contain 'high' and 'low' columns.")

        if len(df) < period:
            logger.warning("Insufficient data for Aroon calculation.")
            return df

        # Calculate Aroon Up and Aroon Down
        df['aroon_up'] = df['high'].rolling(window=period).apply(lambda x: (x.argmax() + 1) / period * 100, raw=True)
        df['aroon_down'] = df['low'].rolling(window=period).apply(lambda x: (x.argmin() + 1) / period * 100, raw=True)

        logger.info(f"Aroon calculation for period={period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the Aroon results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'Aroon').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated Aroon values.
        :param period: Period for which the Aroon was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        # Insert Aroon Up and Aroon Down results row by row
        for _, row in result_df.iterrows():
            self.db_handler.add_indicator_results(indicator_id, timestamp, 'aroon_up', row['aroon_up'])
            self.db_handler.add_indicator_results(indicator_id, timestamp, 'aroon_down', row['aroon_down'])
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted Aroon results for {instrument} into SQLite.")
