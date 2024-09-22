# backend/trading/indicators/stoch.py
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('stochastic_logs').get_logger()

class StochasticOscillator:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the Stochastic Oscillator class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Stochastic Oscillator for a given DataFrame.
        
        :param df: DataFrame with 'high', 'low', and 'close' prices.
        :param period: Lookback period for Stochastic Oscillator calculation.
        :return: DataFrame with the Stochastic Oscillator values.
        """
        # Ensure the necessary columns exist
        if any(col not in df.columns for col in ['high', 'low', 'close']):
            logger.error("DataFrame must contain 'high', 'low', and 'close' columns.")
            raise KeyError("DataFrame must contain 'high', 'low', and 'close' columns.")

        if len(df) < period:
            logger.warning("Insufficient data for Stochastic Oscillator calculation.")
            return df

        # Calculate the rolling highest high and lowest low over the period
        df['highest_high'] = df['high'].rolling(window=period).max()
        df['lowest_low'] = df['low'].rolling(window=period).min()

        # Calculate the %K (Stochastic Oscillator value)
        df['stoch'] = 100 * ((df['close'] - df['lowest_low']) / (df['highest_high'] - df['lowest_low']))

        # Calculate the %D (signal line, typically a 3-period SMA of %K)
        df['stoch_signal'] = df['stoch'].rolling(window=3).mean()

        logger.info("Stochastic Oscillator calculation completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the Stochastic Oscillator results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'Stochastic Oscillator').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated Stochastic Oscillator values.
        :param period: Period for which the Stochastic Oscillator was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = 'stoch'
        # Insert Stochastic Oscillator results row by row
        for _, row in result_df.iterrows():
            param_value = row[param_name]
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted Stochastic Oscillator results for {instrument} into SQLite.")
