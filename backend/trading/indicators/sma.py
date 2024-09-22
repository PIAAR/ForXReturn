# backend/trading/indicators/sma.py
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('sma_logs').get_logger()

class SMA:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the SMA class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Simple Moving Average (SMA) for a given DataFrame.

        :param df: DataFrame with 'close' prices.
        :param period: Lookback period for SMA calculation.
        :return: DataFrame with the SMA values.
        """
        # Ensure the necessary 'close' column exists
        if 'close' not in df.columns:
            logger.error("DataFrame must contain 'close' column.")
            raise KeyError("DataFrame must contain 'close' column.")
        
        # Ensure the DataFrame has enough rows to calculate the SMA
        if len(df) < period:
            logger.warning("Insufficient data for SMA calculation.")
            return df

        # Calculate the SMA and assign it to a new column
        df[f'sma_{period}'] = df['close'].rolling(window=period).mean()

        logger.info(f"SMA calculation for period {period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the SMA results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'SMA').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated SMA values.
        :param period: Period for which the SMA was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = f'sma_{period}'
        # Insert SMA results row by row
        for _, row in result_df.iterrows():
            param_value = row[param_name]
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted SMA results for {instrument} into SQLite.")
