# backend/trading/indicators/ma_crossover.py
import numpy as np
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('ma_logs').get_logger()

class MACrossover:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the MACrossover class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, fast_period=12, slow_period=26):
        """
        Calculate the Moving Average Crossover for a given DataFrame.

        :param df: DataFrame with 'close' prices.
        :param fast_period: Lookback period for the fast moving average.
        :param slow_period: Lookback period for the slow moving average.
        :return: DataFrame with the fast and slow moving averages and the crossover signal.
        """
        # Ensure the necessary 'close' column exists
        if 'close' not in df.columns:
            logger.error("DataFrame must contain 'close' column.")
            raise KeyError("DataFrame must contain 'close' column.")
        
        # Ensure the DataFrame has enough rows to calculate the moving averages
        if len(df) < slow_period:
            logger.warning("Insufficient data for moving average calculation.")
            return df

        # Calculate the fast and slow moving averages
        df['fast_ma'] = df['close'].rolling(window=fast_period).mean()
        df['slow_ma'] = df['close'].rolling(window=slow_period).mean()

        # Generate the crossover signal
        df['crossover'] = df['fast_ma'] - df['slow_ma']
        df['crossover_signal'] = df['crossover'].apply(lambda x: 1 if x > 0 else -1)

        logger.info("Moving Average Crossover calculation completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, fast_period, slow_period):
        """
        Insert the MA Crossover results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'MACrossover').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated MA crossover values.
        :param fast_period: Period for the fast moving average.
        :param slow_period: Period for the slow moving average.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = 'ma_crossover'
        # Insert MA Crossover results row by row
        for _, row in result_df.iterrows():
            param_value = row['crossover_signal']
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'fast_period': fast_period, 'slow_period': slow_period})

        logger.info(f"Inserted MA Crossover results for {instrument} into SQLite.")
