# backend/trading/indicators/vwap.py
from logs.log_manager import LogManager
from backend.data.repositories._sqlite_db import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('vwap_logs').get_logger()

class VWAP:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the VWAP class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df):
        """
        Calculate the Volume Weighted Average Price (VWAP) for a given DataFrame.
        
        :param df: DataFrame with 'high', 'low', 'close', and 'volume' columns.
        :return: DataFrame with VWAP values.
        """
        # Ensure the necessary columns exist
        if any(
            col not in df.columns for col in ['high', 'low', 'close', 'volume']
        ):
            logger.error("DataFrame must contain 'high', 'low', 'close', and 'volume' columns.")
            raise KeyError("DataFrame must contain 'high', 'low', 'close', and 'volume' columns.")

        # Calculate the typical price (average of high, low, and close)
        df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3

        # Calculate the VWAP
        df['vwap'] = (df['typical_price'] * df['volume']).cumsum() / df['volume'].cumsum()

        logger.info("VWAP calculation completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df):
        """
        Insert the VWAP results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'VWAP').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated VWAP values.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = 'vwap'
        # Insert VWAP results row by row
        for _, row in result_df.iterrows():
            param_value = row['vwap']
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)

        logger.info(f"Inserted VWAP results for {instrument} into SQLite.")
