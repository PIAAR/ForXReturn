import unittest
import sqlite3
import pandas as pd
from backtesting import Backtest, Strategy
from backend.trading.optimizers.optimizer import Optimizer  # Assuming the optimizer is here
from backend.trading.indicators.sma import SMA  # Assuming you have an SMA strategy

class MockStrategy(Strategy):
    """
    A mock trading strategy to simulate backtesting with SMA.
    """
    def init(self):
        # Apply the SMA indicator to the Close price
        self.sma = self.I(SMA.calculate, self.data.Close, 20)

    def next(self):
        # Trading logic based on SMA
        if self.data.Close[-1] > self.sma[-1]:
            self.buy()
        elif self.data.Close[-1] < self.sma[-1]:
            self.sell()

class TestOptimizerBacktest(unittest.TestCase):
    def setUp(self):
        # Set up an in-memory SQLite database for testing
        self.db_path = ':memory:'
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        # Create indicator parameters table in the temp DB
        self.cursor.execute('''
            CREATE TABLE indicator_parameters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL
            )
        ''')
        self.conn.commit()

        # Insert some mock parameters for testing
        self.cursor.execute("INSERT INTO indicator_parameters (indicator_id, key, value) VALUES (1, 'SMA', '20')")
        self.conn.commit()

        # Sample data for backtesting (just a simple trend for testing purposes)
        self.data = pd.DataFrame({
            'Open': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            'High': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'Low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
            'Close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            'Volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900]
        })

    def tearDown(self):
        # Close the connection after the tests
        self.conn.close()

    def test_backtest_and_optimize(self):
        """
        Tests the integration of the optimizer with backtesting.
        """
        # Run the backtest using Backtesting.py with the mock strategy
        bt = Backtest(self.data, MockStrategy, cash=10000, commission=0.002)
        stats = bt.run()

        # Check if backtest returns meaningful stats
        self.assertIn('Sharpe Ratio', stats)
        self.assertGreater(stats['Sharpe Ratio'], 0)

        # Now, let's run the optimizer and ensure it updates the parameters
        optimizer = Optimizer(self.db_path)
        
        # Simulate running the optimization
        optimizer.run_optimization(params={'SMA': 20})

        # Check if the parameters in the database were updated
        self.cursor.execute("SELECT value FROM indicator_parameters WHERE key='SMA'")
        updated_value = self.cursor.fetchone()

        # Debugging print to check the fetched value
        print(f"Updated SMA Value in DB: {updated_value}")

        # Ensure the value was changed by the optimizer
        self.assertIsNotNone(updated_value, "Optimizer did not update the SMA value in the database")
        self.assertNotEqual(updated_value[0], '20')  # Ensure the value was updated

if __name__ == '__main__':
    unittest.main()
