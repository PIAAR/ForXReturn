import unittest
import os
from unittest.mock import patch
from backend.api.services.state_machine import StateMachine
from backend.config.indicator_config_loader import IndicatorConfigLoader
from backend.data.repositories._sqlite_db import SQLiteDB

class TestStateMachine(unittest.TestCase):
    def setUp(self):
        # Determine the correct path for the YAML file
        base_path = os.path.dirname(__file__)  # Current directory of the test file
        yaml_path = os.path.join(base_path, '../../scripts/yml/indicator_params.yml')
        # Initialize SQLiteDB connection
        self.db_connection = SQLiteDB(db_name="instruments.db")  # Use your actual database name

        # Load the YAML configuration file or mock it if not present
        if os.path.exists(yaml_path):
            self.config_loader = IndicatorConfigLoader(yaml_path)
        else:
            # If the file doesn't exist, patch the IndicatorConfigLoader to prevent the FileNotFoundError
            patcher = patch('config.indicator_config_loader.IndicatorConfigLoader')
            self.addCleanup(patcher.stop)
            MockLoader = patcher.start()
            self.config_loader = MockLoader.return_value
            self.config_loader.get_indicator_params.return_value = {'ATR': 1, 'ADX': 0}  # Mocked example config

        # Initialize the state machine with the config loader
        self.state_machine = StateMachine(self.config_loader, self.db_connection)

    @patch('backend.data.repositories._sqlite_db.SQLiteDB.fetch_records')
    @patch('backend.data.repositories._sqlite_db.SQLiteDB.execute_script')
    def test_state_machine(self, mock_execute_script, mock_fetch_records):
        # Set up the mock for fetch_records to return a state when queried
        mock_fetch_records.return_value = [{'state': 'Red'}]

        # Simulate some results (1 = favorable, 0 = not favorable)
        indicator_results_macro = {
            'ATR': 1,
            'ADX': 1,
            'Aroon': 0,
            'BollingerBands': 1
        }

        indicator_results_daily = {
            'ATR': 1,
            'ADX': 0,
            'BollingerBands': 1,
            'RSI': 1
        }

        indicator_results_micro = {
            'ATR': 1,
            'BollingerBands': 1,
            'RSI': 0,
            'Stochastic': 1
        }

        indicator_results_by_tier = {
            'macro': indicator_results_macro,
            'daily': indicator_results_daily,
            'micro': indicator_results_micro
        }

        # Market conditions for testing
        market_conditions = {
            'volatility': 5,
            'risk_level': 6
        }

        # Convert instrument name to instrument ID (if necessary in the logic)
        instrument_id = self.db_connection.get_instrument_id('EUR_USD')

        # Run the state machine to evaluate each tier
        states = self.state_machine.run_state_machine(instrument_id, indicator_results_by_tier, market_conditions)

        # Debug output for states
        print(f"States: {states}")

        # Assertions to validate the states
        self.assertIn('macro', states)
        self.assertEqual(states['macro'], 'Red')
        self.assertEqual(states['daily'], 'Red')
        self.assertEqual(states['micro'], 'Red')

        # Ensure the execute_script is called to update states
        mock_execute_script.assert_called()

if __name__ == '__main__':
    unittest.main()
