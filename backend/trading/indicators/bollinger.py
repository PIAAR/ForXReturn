# backend/trading/indicators/bollinger.py
import pandas as pd
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('bollinger_logs').get_logger()

class BollingerBands:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the BollingerBands class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=20, std=2):
        """
        Calculate the Bollinger Bands for a given DataFrame.

        :param df: DataFrame with 'close' prices.
        :param period: Lookback period for the moving average.
        :param std: Number of standard deviations for the bands.
        :return: DataFrame with the middle, upper, and lower Bollinger Bands.
        """
        # Ensure the necessary 'close' column exists
        if 'close' not in df.columns:
            logger.error("DataFrame must contain 'close' column.")
            raise KeyError("DataFrame must contain 'close' column.")

        # Ensure there is enough data to calculate the Bollinger Bands
        if len(df) < period:
            logger.warning("Insufficient data for Bollinger Bands calculation.")
            return df

        # Calculate the middle band (simple moving average)
        df[f'middle_{period}'] = df['close'].rolling(window=period).mean()

        # Calculate the standard deviation over the rolling window
        df['std'] = df['close'].rolling(window=period).std()

        # Calculate the upper and lower bands
        df[f'upper_{period}'] = df[f'middle_{period}'] + (df['std'] * std)
        df[f'lower_{period}'] = df[f'middle_{period}'] - (df['std'] * std)

        # Log the completion of the calculation
        logger.info(f"Bollinger Bands calculation for period={period}, std={std} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period, std):
        """
        Insert the Bollinger Bands results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'Bollinger Bands').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated Bollinger Bands values.
        :param period: Period for the moving average.
        :param std_dev: Standard deviation for the bands.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        # Insert Bollinger Bands results row by row
        for _, row in result_df.iterrows():
            self.db_handler.add_indicator_results(indicator_id, timestamp, 'upper_band', row['upper_band'])
            self.db_handler.add_indicator_results(indicator_id, timestamp, 'lower_band', row['lower_band'])
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period, 'std_dev': std})

        logger.info(f"Inserted Bollinger Bands results for {instrument} into SQLite.")
