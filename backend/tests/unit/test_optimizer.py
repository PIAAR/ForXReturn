import unittest
import sqlite3
import pandas as pd
import numpy as np
from logs.log_manager import LogManager
from trading.optimizers.optimizer import Optimizer
from trading.optimizers.backtester import Backtester
from trading.indicators.sma import SMA
from trading.indicators.ema import EMA
from trading.indicators.rsi import RSI

# Configure loggers
logger = LogManager('test_optimizer_logs').get_logger()

class TestOptimizer(unittest.TestCase):
    def setUp(self):
        # Create an instance of Backtester for use with the Optimizer
        self.backtester = Backtester()

        # Create random data for testing
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

        # Create an instance of Optimizer with a mock database
        self.optimizer = Optimizer(self.backtester)
        self.optimizer.db_handler = self.create_mock_db()

    def create_mock_db(self):
        """
        Creates a mock in-memory SQLite database with tables for indicators and parameters.
        """
        # In-memory SQLite database for testing
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()

        # Create mock tables
        cursor.execute('''
            CREATE TABLE indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                type TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE indicator_parameters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL
            )
        ''')
        conn.commit()

        # Insert a mock indicator
        cursor.execute("INSERT INTO indicators (name, type) VALUES ('SMA', 'Moving Average')")
        cursor.execute("INSERT INTO indicators (name, type) VALUES ('EMA', 'Exponential Moving Average')")
        cursor.execute("INSERT INTO indicators (name, type) VALUES ('RSI', 'Relative Strength Index')")
        conn.commit()

        return conn
    
    def test_optimize_sma(self):
        """
        Test that the optimizer can optimize the SMA indicator and update parameters in the database.
        """
        # Define parameter combinations to test
        sma_param_combinations = [{'period': 10}, {'period': 20}, {'period': 30}]

        # Optimize the SMA parameters
        best_result, best_params = self.optimizer.optimize_parameters("EUR_USD", SMA.calculate, sma_param_combinations)

        # Ensure that best_result is not None
        self.assertIsNotNone(best_result, "Optimizer should return a result for SMA.")

        # Check if the parameters in the database were updated
        cursor = self.optimizer.db_handler.cursor()

        # Fetch optimized parameters from the database
        cursor.execute("SELECT value FROM indicator_parameters WHERE key='period'")
        optimized_period = cursor.fetchone()
        
        if optimized_period is None:
            logger.error("No optimized period found in the database.")
        else:
            optimized_period = optimized_period[0]
            # Validate that the optimized period matches the expected best params
            print(f"Optimized period found in DB: {optimized_period}")
            self.assertEqual(int(optimized_period), best_params['period'], "The optimized period in the DB should match the best parameters found.")
            self.assertIsNotNone(optimized_period, "The optimized period should be in the database.")
            self.assertEqual(int(optimized_period), best_params['period'], "The optimized period in the DB should match the best parameters found.")

    def test_optimize_ema(self):
        """
        Test that the optimizer can optimize the EMA indicator and update parameters in the database.
        """
        # Define EMA parameter combinations
        ema_param_combinations = [{'period': 10}, {'period': 20}, {'period': 30}]

        # Run the optimization for the EMA indicator
        best_result, best_params = self.optimizer.optimize_parameters("EUR_USD", EMA.calculate, ema_param_combinations)

        # Assert the best result is not None and parameters were optimized
        self.assertIsNotNone(best_result, "Optimizer should return a result for EMA.")
        self.assertIsNotNone(best_params, "Optimizer should return optimized parameters for EMA.")

        # Verify the best parameters are saved in the mock database
        cursor = self.optimizer.db_handler.cursor()
        cursor.execute("SELECT value FROM indicator_parameters WHERE key='period'")
        optimized_period = cursor.fetchone()
        self.assertEqual(int(optimized_period), best_params['period'])

    def test_optimize_rsi(self):
        """
        Test that the optimizer can optimize the RSI indicator and update parameters in the database.
        """
        # Define RSI parameter combinations
        rsi_param_combinations = [{'period': 14}, {'period': 20}]

        # Run the optimization for the RSI indicator
        best_result, best_params = self.optimizer.optimize_parameters("EUR_USD", RSI.calculate, rsi_param_combinations)

        # Assert the best result is not None and parameters were optimized
        self.assertIsNotNone(best_result, "Optimizer should return a result for RSI.")
        self.assertIsNotNone(best_params, "Optimizer should return optimized parameters for RSI.")

        # Verify the best parameters are saved in the mock database
        cursor = self.optimizer.db_handler.cursor()
        cursor.execute("SELECT value FROM indicator_parameters WHERE key='period'")
        optimized_period = cursor.fetchone()
        self.assertEqual(int(optimized_period), best_params['period'])

if __name__ == '__main__':
    unittest.main()
