# backend/trading/indicators/obv.py
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('obv_logs').get_logger()

"""
Calculate the On-Balance Volume (OBV) for a given DataFrame. The OBV is the running total of positive and negative volume. A buy signal is generated when the OBV crosses above its moving average, and a sell signal is generated when the OBV crosses below its moving average.
"""

class OBV:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the OBV class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df):
        """
        Calculate the On-Balance Volume (OBV) for a given DataFrame.

        :param df: DataFrame with 'close' and 'volume' columns.
        :return: DataFrame with the OBV values.
        """
        # Ensure the necessary 'close' and 'volume' columns exist
        if any(col not in df.columns for col in ['close', 'volume']):
            logger.error("DataFrame must contain 'close' and 'volume' columns.")
            raise KeyError("DataFrame must contain 'close' and 'volume' columns.")

        # Initialize the OBV column
        df['obv'] = 0

        # Calculate the OBV values based on price changes and volume
        for i in range(1, len(df)):
            if df['close'][i] > df['close'][i - 1]:
                df['obv'][i] = df['obv'][i - 1] + df['volume'][i]
            elif df['close'][i] < df['close'][i - 1]:
                df['obv'][i] = df['obv'][i - 1] - df['volume'][i]
            else:
                df['obv'][i] = df['obv'][i - 1]

        logger.info("OBV calculation completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df):
        """
        Insert the OBV results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'OBV').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated OBV values.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = 'obv'
        # Insert OBV results row by row
        for _, row in result_df.iterrows():
            param_value = row['obv']
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)

        logger.info(f"Inserted OBV results for {instrument} into SQLite.")
