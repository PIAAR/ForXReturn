# backend/trading/indicators/williams_r.py
from logs.log_manager import LogManager
from backend.data.repositories._sqlite_db import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('williams_r_logs').get_logger()

class WilliamsR:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the Williams %R class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

        """
        Calculate the Williams %R for a given DataFrame.

        :param df: DataFrame with 'high', 'low', 'close' prices.
        :param period: Period for Williams %R calculation.
        :return: DataFrame with Williams %R values.
        """
    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Williams %R for the given DataFrame.
        
        :param df: DataFrame with 'high', 'low', and 'close' prices.
        :param period: The lookback period for Williams %R calculation.
        :return: DataFrame with the Williams %R values.
        """
        # Ensure the necessary columns exist
        if any(col not in df.columns for col in ['high', 'low', 'close']):
            logger.error("DataFrame must contain 'high', 'low', and 'close' columns.")
            raise KeyError("DataFrame must contain 'high', 'low', and 'close' columns.")

        if len(df) < period:
            logger.warning("Insufficient data for Williams %R calculation.")
            return df

        # Calculate the Williams %R
        high_max = df['high'].rolling(window=period).max()
        low_min = df['low'].rolling(window=period).min()
        df['williams_r'] = (high_max - df['close']) / (high_max - low_min) * -100

        logger.info("Williams %R calculation completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the Williams %R results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'Williams %R').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated Williams %R values.
        :param period: Period for which the Williams %R was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = 'williams_r'
        # Insert Williams %R results row by row
        for _, row in result_df.iterrows():
            param_value = row[param_name]
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted Williams %R results for {instrument} into SQLite.")
