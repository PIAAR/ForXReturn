import yaml
import os

class IndicatorConfigLoader:
    def __init__(self, config_path='/Users/black_mac/Documents/GitHub/Forex/ForXReturn/backend/scripts/yml/indicator_params.yml'):
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"YAML config file not found: {config_path}")
        self.config_path = config_path
        self.indicator_params = self.load_config()

    def load_config(self):
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)

    def get_indicator_params(self, indicator_name, tier):
        """
        Get parameters and weight for a specific indicator and tier (macro, daily, micro).
        """
        if indicator_name in self.indicator_params:
            indicator_data = self.indicator_params[indicator_name]
            if tier in indicator_data:
                return indicator_data[tier]
        return None
