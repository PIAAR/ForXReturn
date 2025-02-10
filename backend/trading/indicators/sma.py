# backend/trading/indicators/sma.py
from logs.log_manager import LogManager
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from datetime import datetime

# Configure loggers
logger = LogManager('sma_logs').get_logger()

class SMA:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the SMA class with a SQLiteDBHandler handler.

        :parameter db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDBHandler(db_name=db_name)

    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Simple Moving Average (SMA) for a given DataFrame.

        :parameter df: DataFrame with 'close' prices.
        :parameter period: Lookback period for SMA calculation.
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
        df['sma'] = df['close'].rolling(window=period).mean()
        df['period'] = period  # Store the period in a separate column

        logger.info(f"SMA calculation for period {period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the SMA results into the SQLite database.

        :parameter indicator_name: The name of the indicator (e.g., 'SMA').
        :parameter instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :parameter result_df: DataFrame containing the calculated SMA values.
        :parameter period: Period for which the SMA was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        parameter_name = indicator_name.lower()
        # Insert SMA results row by row
        for _, row in result_df.iterrows():
            parameter_value = row[parameter_name]
            self.db_handler.add_indicator_results(indicator_id, timestamp, parameter_name, parameter_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted SMA results for {instrument} into SQLite.")
