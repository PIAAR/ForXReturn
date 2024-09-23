# backend/trading/optimizers/backtester.py
import pandas as pd
from datetime import datetime
from trading.indicators.sma import SMA

class Backtester:
    def __init__(self, initial_balance=10000):
        self.balance = initial_balance
        self.positions = []
        self.trades = []
        self.results = []

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
        # Example MongoDB data loading logic
        pass

    def load_from_sqlite(self, instrument, granularity):
        # Example SQLite data loading logic
        pass

    def apply_indicator(self, indicator_func, *args, **kwargs):
        """
        Apply a given indicator function to the historical data.
        """
        self.data = indicator_func(self.data, *args, **kwargs)

    def simulate_trades(self, buy_signal, sell_signal):
        """
        Execute buy/sell logic based on signals and simulate trades.
        """
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
