import yaml

class IndicatorConfigLoader:
    def __init__(self, config_path):
        """
        Load the YAML configuration file.
        :param config_path: Path to the YAML config file.
        """
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML config file not found: {config_path}")

    def get_indicator_params(self, indicator_name, tier):
        """
        Returns the indicator parameters from the loaded YAML file.
        :return: Dictionary of indicator parameters.
        """
        # Navigate the YAML config to find the relevant parameters
        if indicator_name in self.config['indicators']:
            indicator_data = self.config['indicators'][indicator_name]
            if tier in indicator_data:
                return indicator_data[tier]
        
        return None
        # return self.config.get('indicators', {})
