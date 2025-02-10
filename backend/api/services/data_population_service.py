import itertools
import os
import time
import json

import schedule
from backend.data.repositories._mongo_db import MongoDBHandler
from backend.logs.log_manager import LogManager
from backend.trading.brokers.oanda_client import OandaClient

class DataPopulationService:
    def __init__(self):
        """
        Initializes the DataPopulationService with MongoDBHandler and OandaClient.
        """
        # MongoDB handler for saving data
        self.mongo_handler = MongoDBHandler(db_name="forex_data")
        
        # Cache existing collections to reduce redundant database queries
        self.existing_collections = set(self.mongo_handler.list_collections())
        
        # Ensure the database exists before querying or inserting data
        self.mongo_handler.ensure_database_exists()

        # OANDA client for fetching forex data
        self.oanda_client = OandaClient(
            environment=os.getenv('OANDA_ENV', 'practice')  # Default to 'practice'
        )
        
        # Logger for data population service
        self.logger = LogManager('data_population_logs').get_logger()

        # Load scheduler configuration
        self.scheduler_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/settings/config.json"))

        self.scheduler_enabled = self.load_scheduler_config()

    def load_scheduler_config(self):
        """
        Load the scheduler configuration from JSON file.
        """
        try:
            if not os.path.exists(self.scheduler_config_path):
                self.logger.warning(f"Scheduler config file not found at {self.scheduler_config_path}. Using default settings.")
                return False
            
            with open(self.scheduler_config_path, 'r') as f:
                config = json.load(f)
            return config.get("scheduler_enabled", False)
        except Exception as e:
            self.logger.error(f"Error loading scheduler config: {e}")
            return False

    def ensure_collection_exists_and_populate(self, instrument, granularity="D", count=5000):
        """
        Ensure the MongoDB collection exists and populate it with historical data.

        :parameter instrument: The forex pair (e.g., "EUR_USD").
        :parameter granularity: The timeframe (e.g., "M1", "D", "H1").
        :parameter count: The number of data points to fetch.
        """
        try:
            instrument = instrument.upper()
            collection_name = f"{instrument.lower()}_{granularity.upper()}_data"
            
            if collection_name not in self.existing_collections:
                self.logger.info(f"Collection '{collection_name}' does not exist. Populating data...")
                self.mongo_handler.create_collection_with_index(collection_name, index_field="time")
                # Fetch and insert historical data
                data_inserted = self.mongo_handler.populate_historical_data(instrument, granularity, count)

                if data_inserted:
                    self.logger.info(f"Inserted {len(data_inserted)} records into {collection_name}.")
                else:
                    self.logger.warning(f"No data inserted for {instrument} ({granularity}).")

                self.existing_collections.add(collection_name)
            else:
                self.logger.info(f"Collection '{collection_name}' already exists. Skipping creation.")
        except Exception as e:
            self.logger.error(f"Error ensuring collection and populating data for {instrument}: {e}")
            raise

    def populate_all_instruments(self):
        """
        Populate historical data for all major forex instruments and multiple granularities.
        """
        self.logger.info("Populating all instruments with historical data.")

        major_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CHF", "USD_CAD"]
        granularities = ["M1", "D", "M"]

        for pair, granularity in itertools.product(major_pairs, granularities):
            try:
                self.logger.info(f"Populating {pair} ({granularity})...")

                if data_inserted := self.mongo_handler.populate_historical_data(
                    pair, granularity
                ):
                    self.logger.info(f"Successfully inserted {len(data_inserted)} records for {pair} ({granularity}).")
                else:
                    self.logger.warning(f"No data was inserted for {pair} ({granularity}).")

            except Exception as e:
                self.logger.error(f"Error populating {pair} ({granularity}): {e}")

    def update_data_every_minute(self):
        """
        Scheduler to update historical data for all instruments every minute.
        """
        if not self.scheduler_enabled:
            self.logger.info("Scheduler is disabled by config.")
            return
        
        self.logger.info("Data population scheduler started, updating every minute.")
        schedule.every(1).minute.do(self.populate_all_instruments)
        
        while True:
            schedule.run_pending()
            time.sleep(1)
