import unittest
from api.services.state_machine import StateMachine
from config.indicator_config_loader import IndicatorConfigLoader

class TestStateMachine(unittest.TestCase):
    def setUp(self):
        # Load the YAML configuration file
        self.config_loader = IndicatorConfigLoader('/Users/black_mac/Documents/GitHub/Forex/ForXReturn/backend/scripts/yml/indicator_parameters.yml')
        self.state_machine = StateMachine(self.config_loader)
    
    def test_state_machine(self):
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

        # Run the state machine to evaluate each tier
        states = self.state_machine.run_state_machine(indicator_results_by_tier)

        # Assert that the results match the expected output
        self.assertEqual(states['macro'], 'Red')
        self.assertEqual(states['daily'], 'Red')
        self.assertEqual(states['micro'], 'Red')

if __name__ == '__main__':
    unittest.main()
