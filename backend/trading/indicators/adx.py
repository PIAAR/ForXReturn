# backend/trading/indicators/adx.py
import pandas as pd
import numpy as np
from logs.log_manager import LogManager
from data.repositories.sqlite3 import SQLiteDB
from datetime import datetime

# Configure loggers
logger = LogManager("adx_logs").get_logger()


class ADX:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the ADX class with a SQLiteDB handler.

        :param db_name: The name of the SQLite database file.
        """
        self.db_handler = SQLiteDB(db_name=db_name)

    import pandas as pd
import numpy as np
from logs.log_manager import LogManager

logger = LogManager('adx_logs').get_logger()

class ADX:
    @staticmethod
    def calculate(df, period=14):
        """
        Calculate the Average Directional Index (ADX) for a given DataFrame.

        :param df: DataFrame with 'high', 'low', and 'close' prices.
        :param period: Lookback period for ADX calculation.
        :return: DataFrame with the ADX values.
        """
        # Ensure the necessary columns exist
        if any(col not in df.columns for col in ['high', 'low', 'close']):
            logger.error("DataFrame must contain 'high', 'low', and 'close' columns.")
            raise KeyError("DataFrame must contain 'high', 'low', and 'close' columns.")

        if len(df) < period:
            logger.warning("Insufficient data for ADX calculation.")
            return df

        # Calculate True Range (TR)
        df['high-low'] = df['high'] - df['low']
        df['high-close'] = (df['high'] - df['close'].shift()).abs()
        df['low-close'] = (df['low'] - df['close'].shift()).abs()
        df['tr'] = df[['high-low', 'high-close', 'low-close']].max(axis=1)

        # Calculate the Directional Movement (DM)
        df['plus_dm'] = np.where((df['high'] - df['high'].shift()) > (df['low'].shift() - df['low']), df['high'] - df['high'].shift(), 0)
        df['minus_dm'] = np.where((df['low'].shift() - df['low']) > (df['high'] - df['high'].shift()), df['low'].shift() - df['low'], 0)

        # Smooth the values with a rolling average
        df['tr_smooth'] = df['tr'].rolling(window=period).sum()
        df['plus_dm_smooth'] = df['plus_dm'].rolling(window=period).sum()
        df['minus_dm_smooth'] = df['minus_dm'].rolling(window=period).sum()

        # Calculate the Directional Indicators (DI)
        df['plus_di'] = 100 * (df['plus_dm_smooth'] / df['tr_smooth'])
        df['minus_di'] = 100 * (df['minus_dm_smooth'] / df['tr_smooth'])

        # Calculate the Directional Index (DX)
        df['dx'] = 100 * (abs(df['plus_di'] - df['minus_di']) / (df['plus_di'] + df['minus_di']))

        # Calculate the ADX by smoothing the DX values
        df['adx'] = df['dx'].rolling(window=period).mean()

        # Fill NaN values in the 'adx' column
        df['adx'].fillna(0, inplace=True)

        logger.info(f"ADX calculation for period={period} completed.")
        return df

    def insert_results_to_db(self, indicator_name, instrument, result_df, period):
        """
        Insert the ADX results into the SQLite database.

        :param indicator_name: The name of the indicator (e.g., 'ADX').
        :param instrument: The instrument for which the calculation was made (e.g., 'EUR_USD').
        :param result_df: DataFrame containing the calculated ADX values.
        :param period: Period for which the ADX was calculated.
        """
        indicator_id = self.db_handler.get_indicator_id(indicator_name)
        timestamp = datetime.now().isoformat()

        # Insert ADX, plus_di, and minus_di results row by row
        for _, row in result_df.iterrows():
            self.db_handler.add_indicator_results(
                indicator_id, timestamp, "adx", row["adx"]
            )
            self.db_handler.add_indicator_results(
                indicator_id, timestamp, "plus_di", row["plus_di"]
            )
            self.db_handler.add_indicator_results(
                indicator_id, timestamp, "minus_di", row["minus_di"]
            )
            self.db_handler.add_indicator_parameters(indicator_id, {"period": period})

        logger.info(f"Inserted ADX results for {instrument} into SQLite.")
