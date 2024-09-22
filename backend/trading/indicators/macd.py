# backend/trading/indicators/macd.py
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime
        
logger = LogManager('macd_logs').get_logger()

class MACD:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the MACD class with a SQLiteDB handler.
        
        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, short_period=12, long_period=26, signal_period=9):
        """
        Calculate the Moving Average Convergence Divergence (MACD) for a given DataFrame.

        :param df: DataFrame with 'close' prices.
        :param short_period: Lookback period for the short-term EMA.
        :param long_period: Lookback period for the long-term EMA.
        :param signal_period: Lookback period for the signal line.
        :return: DataFrame with the MACD, signal, and histogram values.
        """
        # Ensure the necessary 'close' column exists
        if 'close' not in df.columns:
            logger.error("DataFrame must contain 'close' column.")
            raise KeyError("DataFrame must contain 'close' column.")

        # Ensure there is enough data for the calculation
        if len(df) < long_period:
            logger.warning("Insufficient data for MACD calculation.")
            return df

        # Calculate the short and long EMA
        df['ema_short'] = df['close'].ewm(span=short_period, adjust=False).mean()
        df['ema_long'] = df['close'].ewm(span=long_period, adjust=False).mean()

        # Calculate MACD and signal line
        df['macd'] = df['ema_short'] - df['ema_long']
        df['signal'] = df['macd'].ewm(span=signal_period, adjust=False).mean()

        # Calculate the MACD histogram
        df['histogram'] = df['macd'] - df['signal']

        logger.info(f"MACD calculation for short_period={short_period}, long_period={long_period}, signal_period={signal_period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, short_period, long_period, signal_period):
        """
        Insert the MACD results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'MACD').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated MACD values.
        :param short_period: Short period for MACD calculation.
        :param long_period: Long period for MACD calculation.
        :param signal_period: Signal period for MACD calculation.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        # Insert MACD, signal, and histogram results row by row
        for _, row in result_df.iterrows():
            macd_value = row['macd']
            signal_value = row['signal']
            histogram_value = row['histogram']
            self.db_handler.add_indicator_results(indicator_id, timestamp, 'macd', macd_value)
            self.db_handler.add_indicator_results(indicator_id, timestamp, 'signal', signal_value)
            self.db_handler.add_indicator_results(indicator_id, timestamp, 'histogram', histogram_value)

            # Insert the indicator parameters
            self.db_handler.add_indicator_parameters(indicator_id, {
                'short_period': short_period,
                'long_period': long_period,
                'signal_period': signal_period
            })

        self.logger.info(f"Inserted MACD results for {instrument} into SQLite.")

# Example usage:
# macd_calculator = MACD(db_name="indicators.db")
# df = pd.DataFrame({'close': [some_price_data]})
# result_df = macd_calculator.calculate(df)
# macd_calculator.insert_results_to_db("MACD", "EUR_USD", result_df, 12, 26, 9)
