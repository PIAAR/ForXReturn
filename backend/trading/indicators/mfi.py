# backend/trading/indicators/mfi.py
from logs.log_manager import LogManager
from backend.data.repositories._sqlite_db import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager('mfi_logs').get_logger()

"""
    Calculate the Money Flow Index (MFI) for a given DataFrame. The MFI is a momentum indicator that measures the inflow and outflow of money into a security.
"""

class MFI:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the MFI class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Money Flow Index (MFI) for a given DataFrame.

        :param df: DataFrame with 'high', 'low', 'close', and 'volume' columns.
        :param period: Lookback period for MFI calculation.
        :return: DataFrame with the MFI values.
        """
        # Ensure the necessary columns exist
        if any(
            col not in df.columns for col in ['high', 'low', 'close', 'volume']
        ):
            logger.error("DataFrame must contain 'high', 'low', 'close', and 'volume' columns.")
            raise KeyError("DataFrame must contain 'high', 'low', 'close', and 'volume' columns.")

        if len(df) < period:
            logger.warning("Insufficient data for MFI calculation.")
            return df

        # Calculate typical price
        df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3

        # Calculate raw money flow
        df['money_flow'] = df['typical_price'] * df['volume']

        # Calculate the shifted typical price
        df['previous_typical_price'] = df['typical_price'].shift(1)

        # Positive and negative money flow
        df['money_flow_positive'] = df.apply(lambda row: row['money_flow'] if row['typical_price'] > row['previous_typical_price'] else 0, axis=1)
        df['money_flow_negative'] = df.apply(lambda row: row['money_flow'] if row['typical_price'] < row['previous_typical_price'] else 0, axis=1)

        # Rolling sums of positive and negative money flow
        positive_flow = df['money_flow_positive'].rolling(window=period).sum()
        negative_flow = df['money_flow_negative'].rolling(window=period).sum()

        # Money Flow Ratio (MFR)
        mfr = positive_flow / negative_flow

        # Money Flow Index (MFI)
        df['mfi'] = 100 - (100 / (1 + mfr))

        logger.info(f"MFI calculation for period {period} completed.")
        return df


    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the MFI results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'MFI').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated MFI values.
        :param period: Period for which the MFI was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        param_name = 'mfi'
        # Insert MFI results row by row
        for _, row in result_df.iterrows():
            param_value = row['mfi']
            self.db_handler.add_indicator_results(indicator_id, timestamp, param_name, param_value)
            self.db_handler.add_indicator_parameters(indicator_id, {'period': period})

        logger.info(f"Inserted MFI results for {instrument} into SQLite.")
