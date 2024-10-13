import os
import importlib
from data.repositories._sqlite_db import SQLiteDB
from datetime import datetime
from data.repositories.mongo import MongoDBHandler
from logs.log_manager import LogManager
import pandas as pd
from config.indicator_config_loader import IndicatorConfigLoader  # Import the config loader
from api.services.state_machine import StateMachine  # Import the state machine

# Configure logging
logger = LogManager('indicator_controller').get_logger()

class IndicatorsController:
    def __init__(self, db_path='backend/data/repositories/databases/indicators.db', autostart=False):
        self.db = SQLiteDB(db_path)
        self.autostart = autostart
        self.indicators_dir = os.path.join(os.path.dirname(__file__), '../../trading/indicators/')
        
        # MongoDB handler to fetch monthly data
        self.mongo_handler = MongoDBHandler(db_name="forex_data")
        
        # Initialize the YAML config loader with a valid path
        config_path = os.path.join(os.path.dirname(__file__), '../../scripts/yml/indicator_params.yml')
        self.config_loader = IndicatorConfigLoader(config_path)  # Pass the config path
        if not os.path.isfile(self.config_path):
            raise FileNotFoundError(f"YAML Config file not found: {config_path}")
        # Initialize the state machine
        self.state_machine = StateMachine(self.config_loader)
        
        # Initialize the SQLite database
        schema_path = '/Users/black_mac/Documents/GitHub/Forex/ForXReturn/backend/data/models'
        if not os.path.isfile(schema_path):
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r') as schema_file:
            schema_sql = schema_file.read()

        self.db.initialize_db(schema_sql)
        print(f'Schema initialized from: {schema_path}')
    
    def get_indicator_modules(self):
        """
        Retrieve all indicator modules from the indicators directory.
        """
        return [
            f
            for f in os.listdir(self.indicators_dir)
            if f.endswith('.py') and f != '__init__.py'
        ]

    def fetch_historical_data(self, instrument, granularity="M"):
        """
        Fetch historical data from MongoDB for a given instrument and granularity (monthly data).
        :parameter instrument: The instrument to fetch (e.g., "EUR_USD").
        :parameter granularity: The time granularity (default is monthly "M").
        :return: Pandas DataFrame of historical data.
        """
        collection_name = f"{instrument.lower()}_{granularity.lower()}_data"
        if data := self.mongo_handler.read(query={}, collection_name=collection_name):
            return self.process_mongo_data(data, instrument)
        logger.warning(f"No data found for {instrument} in MongoDB.")
        return None

    def process_mongo_data(self, data, instrument):
        """
        Process MongoDB data to extract relevant fields like 'open', 'high', 'low', 'close', 'volume'.
        """
        df = pd.DataFrame(data)
        
        # Extract 'o', 'h', 'l', 'c' from the 'mid' object
        df['open'] = df['mid'].apply(lambda x: float(x['o']) if 'o' in x else None)
        df['high'] = df['mid'].apply(lambda x: float(x['h']) if 'h' in x else None)
        df['low'] = df['mid'].apply(lambda x: float(x['l']) if 'l' in x else None)
        df['close'] = df['mid'].apply(lambda x: float(x['c']) if 'c' in x else None)
        
        # Handle volume and time
        df['volume'] = df.get('volume', None).astype(float)
        df['time'] = pd.to_datetime(df['time'])
        
        required_columns = ['open', 'high', 'low', 'close', 'volume', 'time']
        for col in required_columns:
            if col not in df.columns:
                raise KeyError(f"Missing required column: {col}")
        
        logger.info(f"Fetched {len(df)} rows for {instrument} from MongoDB.")
        return df

    def extract_parameters(self, indicator_name):
        """
        Get parameters from the YAML config file for the specific indicator.
        """
        return self.config_loader.get_indicator_params(indicator_name, "macro")  # Example using the "macro" tier
    
    def calculate_indicator(self, indicator_name, df, params, instrument):
        """
        Dynamically load and calculate the indicator values using fetched historical data.
        :parameter indicator_name: The name of the indicator file (without .py).
        :parameter df: Historical data as a Pandas DataFrame.
        :parameter params: Calculation parameters for the indicator.
        :parameter instrument: The financial instrument for which the calculation is made (e.g., EUR_USD)
        """
        try:
            module_name = f'trading.indicators.{indicator_name[:-3]}'  # Remove the .py extension
            indicator_module = importlib.import_module(module_name)
            
            # Dynamically load the class and instantiate it
            class_name = indicator_name[:-3].upper()
            if hasattr(indicator_module, class_name):
                indicator_class = getattr(indicator_module, class_name)
                indicator_instance = indicator_class()

                # Call the calculate method and store results in the DB
                result_df = indicator_instance.calculate(df, **params)
                indicator_instance.insert_results_to_db(class_name, instrument, result_df, **params)

                logger.info(f"Indicator {indicator_name} processed for {instrument}.")
            else:
                logger.warning(f"No class named '{class_name}' found in {indicator_name}.")
        except Exception as e:
            logger.error(f"Error calculating {indicator_name}: {e}")

    def run(self):
        """
        Run the process of populating indicators and evaluating states.
        """
        if self.autostart:
            logger.info("Autostart is enabled. Beginning indicator calculations...")
            indicator_modules = self.get_indicator_modules()

            for indicator_file in indicator_modules:
                major_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CHF", "USD_CAD"]
                for instrument in major_pairs:
                    df = self.fetch_historical_data(instrument, "M")
                    if df is not None:
                        params = self.extract_parameters(indicator_file)
                        self.calculate_indicator(indicator_file, df, params, instrument)
                        
                        # Assuming we have indicator results, we'll pass them into the state machine
                        # For now, simulate some results to show how it would work
                        indicator_results_macro = {
                            'ATR': 1,
                            'ADX': 1,
                            'Aroon': 0,
                            'BollingerBands': 1
                        }

                        # Calculate the state (Green/Red) using the state machine
                        state = self.state_machine.run_state_machine({'macro': indicator_results_macro})
                        logger.info(f"State for {instrument}: {state['macro']}")  # Example: macro state

            logger.info("All indicators processed.")
        else:
            logger.info("Autostart is disabled. No indicators will be processed.")

# Running the controller
if __name__ == "__main__":
    import sys
    autostart_flag = sys.argv[1].lower() == 'true' if len(sys.argv) > 1 else False
    controller = IndicatorsController(autostart=autostart_flag)
    controller.run()
