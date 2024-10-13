# backend/trading/indicators/atr.py
import numpy as np
from logs.log_manager import LogManager
from backend.data.repositories._sqlite_db import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('atr_logs').get_logger()

class ATR:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the ATR class with a SQLiteDB handler.

        :parameter db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Average True Range (ATR) for a given DataFrame.

        :parameter df: DataFrame with 'high', 'low', and 'close' prices.
        :parameter period: Lookback period for ATR calculation.
        :return: DataFrame with the ATR values.
        """
        # Ensure the necessary columns exist
        if any(col not in df.columns for col in ['high', 'low', 'close']):
            logger.error("DataFrame must contain 'high', 'low', and 'close' columns.")
            raise KeyError("DataFrame must contain 'high', 'low', and 'close' columns.")

        if len(df) < period:
            logger.warning("Insufficient data for ATR calculation.")
            return df

        # Calculate True Range (TR)
        df['high-low'] = df['high'] - df['low']
        df['high-close'] = (df['high'] - df['close'].shift()).abs()
        df['low-close'] = (df['low'] - df['close'].shift()).abs()
        df['true_range'] = df[['high-low', 'high-close', 'low-close']].max(axis=1)

        # Calculate the ATR using exponential moving average
        df['atr'] = df['true_range'].rolling(window=period).mean()

        logger.info(f"ATR calculation for period={period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the ATR results into the SQLite database.

        :parameter indicator_name: The name of the indicator (e.g., 'ATR').
        :parameter instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :parameter result_df: DataFrame containing the calculated ATR values.
        :parameter period: Period for which the ATR was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        parameter_name = 'atr'
        # Insert ATR results row by row
        for _, row in result_df.iterrows():
            parameter_value = row['atr']
            self.db_handler.add_indicator_results(indicator_id, timestamp, parameter_name, parameter_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted ATR results for {instrument} into SQLite.")
