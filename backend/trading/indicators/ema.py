from logs.log_manager import LogManager
from backend.data.repositories._sqlite_db import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('ema_logs').get_logger()

class EMA:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the EMA class with a SQLiteDB handler.

        :parameter db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Exponential Moving Average (EMA) for a given DataFrame.

        :parameter df: DataFrame with 'close' prices.
        :parameter period: Lookback period for EMA calculation.
        :return: DataFrame with the EMA values.
        """
        # Ensure the necessary 'close' column exists
        if 'close' not in df.columns:
            logger.error("DataFrame must contain 'close' column.")
            raise KeyError("DataFrame must contain 'close' column.")

        # Ensure there is enough data to calculate the EMA
        if len(df) < period:
            logger.warning("Insufficient data for EMA calculation.")
            return df

        # Calculate the EMA and assign it to a dynamically named column
        df['ema'] = df['close'].ewm(span=period, adjust=False).mean()
        df['period'] = period  # Store the period in a separate column  
        
        logger.info(f"EMA calculation for period={period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the EMA results into the SQLite database.

        :parameter indicator_name: The name of the indicator (e.g., 'EMA').
        :parameter instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :parameter result_df: DataFrame containing the calculated EMA values.
        :parameter period: Period for which the EMA was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        parameter_name = indicator_name.lower()
        # Insert EMA results row by row
        for _, row in result_df.iterrows():
            parameter_value = row[parameter_name]
            self.db_handler.add_indicator_results(indicator_id, timestamp, parameter_name, parameter_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted EMA results for {instrument} into SQLite.")
