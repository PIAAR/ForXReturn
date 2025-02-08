from datetime import datetime

import numpy as np
import pandas as pd

from backend.data.repositories._sqlite_db import SQLiteDB
from backend.data.repositories.mongo import MongoDBHandler
from backend.logs.log_manager import LogManager
from backend.trading.indicators.sma import SMA

# Initialize the LogManager
logger = LogManager('backtester_logs').get_logger()

class Backtester:
    def __init__(self, initial_balance=10000):
        self.balance = initial_balance
        self.positions = []
        self.trades = []
        self.results = []
        self.data = None
        
        # Initialize MongoDB and SQLite handlers
        self.mongo_handler = MongoDBHandler(db_name="forex_data")
        self.db_handler = SQLiteDB(db_name="instruments.db") 
        
    def load_data(self, instrument, granularity="D", source="mongo"):
        """
        Load historical data from the specified source (MongoDB/SQLite).
        """
        if source == "mongo":
            # MongoDB logic here (example)
            self.data = self.load_from_mongo(instrument, granularity)
        elif source == "sqlite":
            # SQLite logic here (example)
            self.data = self.load_from_sqlite(instrument, granularity)
        else:
            raise ValueError(f"Unsupported data source: {source}")

    def load_from_mongo(self, instrument, granularity):
        """
        Load historical data from MongoDB based on the instrument and granularity.
        """
        try:
            # Collection name format based on instrument and granularity
            collection_name = f"{instrument.lower()}_{granularity.lower()}_data"

            # Fetch data from MongoDB
            data = self.mongo_handler.read({}, collection_name=collection_name)
            
            if not data:
                raise ValueError(f"No data found for {instrument} with granularity {granularity} in MongoDB.")
            
            # Convert data to DataFrame
            df = pd.DataFrame(data)
            
            # Extract the relevant fields and convert timestamp to pandas datetime
            df['timestamp'] = pd.to_datetime(df['time'])
            df.set_index('timestamp', inplace=True)
            
            # Handle missing 'mid' field safely
            if 'mid' in df.columns:
                df['open'] = df['mid'].apply(lambda x: float(x.get('o', 0)))
                df['high'] = df['mid'].apply(lambda x: float(x.get('h', 0)))
                df['low'] = df['mid'].apply(lambda x: float(x.get('l', 0)))
                df['close'] = df['mid'].apply(lambda x: float(x.get('c', 0)))
                
            # Handle missing volume safely
            df['volume'] = df.get('volume', 0).astype(float)
            
            # Drop unnecessary columns (e.g., 'mid', '_id')
            df.drop(columns=['mid', '_id'], errors='ignore')
            
            return df

        except Exception as e:
            logger.error(f"Error loading data from MongoDB: {e}")
            raise

    def transfer_mongo_to_sqlite(self, instrument, granularity):
        """
        Fetch historical data from MongoDB and store it in SQLite.
        """
        try:
            collection_name = f"{instrument.lower()}_{granularity.lower()}_data"
            mongo_data = self.mongo_handler.read({}, collection_name=collection_name)

            if not mongo_data:
                logger.error(f"❌ No data found in MongoDB for {instrument} - {granularity}.")
                return

            instrument_id = self.db_handler.get_instrument_id(instrument)
            if not instrument_id:
                logger.error(f"❌ Instrument {instrument} not found in SQLite. Ensure it's added first.")
                return

            records = []
            records.extend(
                (
                    instrument_id,
                    granularity,
                    record['time'],
                    float(record['mid'].get('o', 0)),
                    float(record['mid'].get('h', 0)),
                    float(record['mid'].get('l', 0)),
                    float(record['mid'].get('c', 0)),
                    float(record.get('volume', 0)),
                )
                for record in mongo_data
            )
            query = """
                INSERT INTO historical_data (instrument_id, granularity, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.db_handler.execute_many(query, records)

            logger.info(f"✅ Successfully transferred {len(records)} records for {instrument} - {granularity} to SQLite.")

        except Exception as e:
            logger.error(f"❌ Error transferring data from MongoDB to SQLite: {e}")

    def load_from_sqlite(self, instrument, granularity, retry=False):
        """
        Load historical data from SQLite database for a given instrument and granularity.
        If no data exists, fetch from MongoDB and store it in SQLite.
        """
        instrument = instrument.upper().replace("/", "_")  # Ensure formatting
        instrument_id = self.db_handler.get_instrument_id(instrument)

        if instrument_id is None:
            logger.error(f"❌ Instrument {instrument} not found in database. Verify entry in SQLite.")
            raise ValueError(f"Instrument {instrument} not found in database.")

        query = """
            SELECT timestamp, open, high, low, close, volume FROM historical_data 
            WHERE instrument_id = ? AND granularity = ?
            ORDER BY timestamp ASC
        """
        parameters = (instrument_id, granularity)
        result = self.db_handler.fetch_records_with_query(query, parameters)

        if not result:
            if not retry:  # Prevent infinite recursion
                logger.warning(f"⚠️ No data in SQLite for {instrument} - {granularity}. Fetching from MongoDB...")
                self.transfer_mongo_to_sqlite(instrument, granularity)
                return self.load_from_sqlite(instrument, granularity, retry=True)  # Retry once
            else:
                logger.error(f"❌ Data retrieval failed even after fetching from MongoDB.")
                raise ValueError(f"No data found for instrument {instrument} with granularity {granularity}.")

        # Convert to DataFrame
        self.data = pd.DataFrame(result, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.data.set_index('timestamp', inplace=True)

        return self.data

    def apply_indicator(self, indicator_func, *args, **kwargs):
        """
        Apply a given indicator function to the historical data.
        """
        if self.data is not None:
            self.data = indicator_func(self.data, *args, **kwargs)
        else:
            raise ValueError("Historical data is not loaded. Load data before applying indicators.")

    def simulate_trades(self, buy_signal, sell_signal):
        """
        Execute buy/sell logic based on signals and simulate trades.
        """
        if self.data is None:
            raise ValueError("Data is not available for trading. Please load data first.")

        in_position = False
        buy_price = 0
        
        for index, row in self.data.iterrows():
            if buy_signal(row) and not in_position:
                # Execute buy
                in_position = True
                buy_price = row['close']
                self.positions.append((index, buy_price))
                logger.info(f"BUY at {buy_price} on {index}")
                print(f"BUY at {row['close']} on {index}")

            elif sell_signal(row) and in_position:
                # Execute sell
                in_position = False
                sell_price = row['close']
                profit = sell_price - buy_price
                self.trades.append(profit)
                self.balance += profit
                logger.info(f"SELL at {sell_price} on {index}, profit: {profit:.2f}")
                print(f"SELL at {sell_price} on {index}, profit: {profit}")
                
        logger.info(f"Final Balance: {self.balance:.2f}")

    def calculate_performance(self):
        """
        Calculate performance metrics: total return, win rate, Sharpe ratio, drawdown.
        """
        if not self.trades:
            return {"total_return": 0, "win_rate": 0, "sharpe_ratio": 0, "max_drawdown": 0}

        total_return = sum(self.trades)
        win_rate = len([t for t in self.trades if t > 0]) / len(self.trades) if self.trades else 0

        # Sharpe Ratio Calculation
        returns_series = np.array(self.trades)
        sharpe_ratio = np.mean(returns_series) / np.std(returns_series) if np.std(returns_series) != 0 else 0

        # Max Drawdown Calculation
        cumulative_returns = np.cumsum(returns_series)
        peak = np.maximum.accumulate(cumulative_returns)
        drawdowns = (peak - cumulative_returns) / peak
        max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0

        logger.info(f"Total Return: {total_return:.2f}")
        logger.info(f"Win Rate: {win_rate:.2%}")
        logger.info(f"Sharpe Ratio: {sharpe_ratio:.2f}")
        logger.info(f"Max Drawdown: {max_drawdown:.2%}")

        return {
            "total_return": total_return,
            "win_rate": win_rate,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "trades": self.trades,
        }

    def get_trade_results(self):
        """
        Return the final trading results for evaluation.
        """
        return self.results[-1] if self.results else {}

# Example usage
if __name__ == "__main__":
    backtester = Backtester()

    # Load historical data
    backtester.load_data("EUR_USD", granularity="D", source="sqlite")

    # Apply a simple moving average indicator
    backtester.apply_indicator(SMA.calculate, period=20)

    # Define buy/sell signals
    def buy_signal(row):
        return row['close'] > row['sma_20']

    def sell_signal(row):
        return row['close'] < row['sma_20']

    # Simulate trades
    backtester.simulate_trades(buy_signal, sell_signal)

    # Calculate performance
    backtester.calculate_performance()
