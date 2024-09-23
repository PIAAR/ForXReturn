# tests/unit/test_backtester.py

import unittest
import pandas as pd
import numpy as np
from datetime import datetime
from trading.indicators.sma import SMA
from trading.optimizers.backtester import Backtester

class TestBacktester(unittest.TestCase):
    def setUp(self):
        # Create an instance of Backtester
        self.backtester = Backtester()
        
        # Generate random closing prices for testing
        np.random.seed(42)
        random_prices = np.random.normal(loc=100, scale=5, size=100)

        # Create a DataFrame with the random prices
        self.sample_data = pd.DataFrame({
            'date': pd.date_range(start='2021-01-01', periods=100, freq='D'),
            'close': random_prices
        })
        self.sample_data.set_index('date', inplace=True)
        
        # Assign the sample data to backtester
        self.backtester.data = self.sample_data.copy()

    def test_sma_calculation(self):
        # Test whether SMA calculation adds the correct column
        self.backtester.apply_indicator(SMA.calculate, period=15)
        
        # Check if the SMA column is created correctly
        self.assertIn('sma_15', self.backtester.data.columns, "SMA column was not added to data.")
        
        # Check that the length of the resulting data matches the original
        self.assertEqual(len(self.backtester.data), len(self.sample_data), "Length of data changed after SMA calculation.")
        
    def test_backtesting_logic(self):
        # Apply the SMA indicator with a larger period
        self.backtester.apply_indicator(SMA.calculate, period=60)

        # Define buy/sell signals based on SMA
        def buy_signal(row):
            return row['close'] > row['sma_60']

        def sell_signal(row):
            return row['close'] < row['sma_60']

        # Simulate trades
        self.backtester.simulate_trades(buy_signal, sell_signal)

        # Calculate performance
        performance = self.backtester.calculate_performance()

        # Assertions for performance metrics
        self.assertIsNotNone(performance)
        self.assertNotEqual(performance['total_return'], 0, "Total return should not be 0.")
        self.assertGreaterEqual(performance['win_rate'], 0)
        self.assertLessEqual(performance['win_rate'], 1)
        
    def test_insufficient_data_for_sma(self):
        # Test with insufficient data for SMA calculation
        insufficient_data = self.sample_data.iloc[:2]  # Only 2 rows
        self.backtester.data = insufficient_data

        # Apply the SMA indicator (should log a warning and not apply SMA)
        self.backtester.apply_indicator(SMA.calculate, period=15)
        
        # Ensure SMA column was not added
        self.assertNotIn('sma_15', self.backtester.data.columns, "SMA should not be calculated with insufficient data.")
        
    def test_no_trades_scenario(self):
        # Reset data and apply the SMA indicator
        self.backtester.data = self.sample_data.copy()
        self.backtester.apply_indicator(SMA.calculate, period=15)

        # Define signals that ensure no trades are made
        def buy_signal(row):
            return False  # No buy signals

        def sell_signal(row):
            return False  # No sell signals

        # Simulate trades
        self.backtester.simulate_trades(buy_signal, sell_signal)

        # Assert no trades were made
        self.assertEqual(len(self.backtester.trades), 0, "There should be no trades executed.")
        
        # Calculate performance
        performance = self.backtester.calculate_performance()

        # Ensure that total return and win rate are zero since no trades were made
        self.assertEqual(performance['total_return'], 0, "Total return should be 0 when no trades are made.")
        self.assertEqual(performance['win_rate'], 0, "Win rate should be 0 when no trades are made.")
        
    def test_all_trades_profitable(self):
        # Reset data and apply the SMA indicator
        self.backtester.data = self.sample_data.copy()
        self.backtester.apply_indicator(SMA.calculate, period=15)

        # Define buy/sell signals that always make profitable trades
        def buy_signal(row):
            return row['close'] < row['sma_15']  # Buy when price is below SMA (for profit)

        def sell_signal(row):
            return row['close'] > row['sma_15']  # Sell when price is above SMA (for profit)

        # Simulate trades
        self.backtester.simulate_trades(buy_signal, sell_signal)

        # Ensure trades were made
        self.assertGreater(len(self.backtester.trades), 0, "There should be trades executed.")

        # Calculate performance
        performance = self.backtester.calculate_performance()

        # Assert that total return is positive and win rate is 1 (all trades profitable)
        self.assertGreater(performance['total_return'], 0, "Total return should be positive.")
        self.assertEqual(performance['win_rate'], 1, "Win rate should be 1 when all trades are profitable.")
        
    def test_all_trades_loss(self):
        # Reset data and apply the SMA indicator
        self.backtester.data = self.sample_data.copy()
        self.backtester.apply_indicator(SMA.calculate, period=15)

        # Define buy/sell signals that always make losing trades
        def buy_signal(row):
            return row['close'] > row['sma_15']  # Buy when price is below SMA (for a loss scenario)

        def sell_signal(row):
            return row['close'] < row['sma_15']  # Sell when price is above SMA (for a loss scenario)

        # Simulate trades
        self.backtester.simulate_trades(buy_signal, sell_signal)

        # Ensure some trades were made
        self.assertGreater(len(self.backtester.trades), 0, "There should be trades executed.")

        # Calculate performance
        performance = self.backtester.calculate_performance()

        # Assert that total return is negative and win rate is 0 (all trades lost)
        self.assertLess(performance['total_return'], 0, "Total return should be negative.")
        self.assertEqual(performance['win_rate'], 0, "Win rate should be 0 when all trades result in loss.")
        
if __name__ == '__main__':
    unittest.main()
