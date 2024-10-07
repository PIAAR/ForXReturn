import pandas as pd
from datetime import datetime
from backend.trading.indicators.sma import SMA
from backend.data.repositories._sqlite_db import SQLiteDB
from backend.logs.log_manager import LogManager

# Initialize the LogManager
logger = LogManager('backtester_logs')

class Backtester:
    def __init__(self, initial_balance=10000):
        self.balance = initial_balance
        self.positions = []
        self.trades = []
        self.results = []
        self.data = None
        self.db_handler = SQLiteDB(db_name="optimizer.db")  # Assuming we're using the optimizer database for testing

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
            
            # Ensure all required fields are present
            df['open'] = df['mid'].apply(lambda x: float(x['o']))
            df['high'] = df['mid'].apply(lambda x: float(x['h']))
            df['low'] = df['mid'].apply(lambda x: float(x['l']))
            df['close'] = df['mid'].apply(lambda x: float(x['c']))
            df['volume'] = df['volume'].astype(float)  # Convert volume to float if needed
            
            # Drop unnecessary columns (e.g., 'mid', '_id')
            df.drop(columns=['mid', '_id'], inplace=True)
            
            return df

        except Exception as e:
            logger.error(f"Error loading data from MongoDB: {e}")
            raise

    def load_from_sqlite(self, instrument, granularity):
        """
        Load historical data from SQLite database for a given instrument and granularity.
        """
        instrument_id = self.db_handler.get_instrument_id(instrument)
        if not instrument_id:
            raise ValueError(f"Instrument {instrument} not found in database.")

        query = """
            SELECT * FROM historical_data 
            WHERE instrument_id = ? AND granularity = ?
            ORDER BY timestamp ASC
        """
        params = (instrument_id, granularity)
        if not (result := self.db_handler.fetch_records_with_query(query, params)):
            raise ValueError(f"No data found for instrument {instrument} with granularity {granularity}.")
        self.data = pd.DataFrame(result)
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
        for index, row in self.data.iterrows():
            if buy_signal(row) and not in_position:
                # Execute buy
                in_position = True
                self.positions.append((index, row['close']))
                print(f"BUY at {row['close']} on {index}")

            if sell_signal(row) and in_position:
                # Execute sell
                in_position = False
                buy_price = self.positions.pop()[1]
                sell_price = row['close']
                profit = sell_price - buy_price
                self.trades.append(profit)
                print(f"SELL at {sell_price} on {index}, profit: {profit}")
                self.balance += profit

    def calculate_performance(self):
        """
        Calculate performance metrics such as total return, win rate, etc.
        """
        if not self.trades:
            return {"total_return": 0, "win_rate": 0, "trades": []}

        total_return = sum(self.trades)
        win_rate = len([t for t in self.trades if t > 0]) / len(self.trades) if self.trades else 0
        print(f"Total Return: {total_return}")
        print(f"Win Rate: {win_rate}")

        # Store results for future analysis
        self.results.append({
            "total_return": total_return,
            "win_rate": win_rate,
            "trades": self.trades,
        })
        return self.results[-1]

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
