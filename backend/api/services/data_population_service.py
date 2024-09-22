# backend/api/services/data_population_service.py

import itertools
import os
import time

import schedule
from data.repositories.mongo import MongoDBHandler
from logs.log_manager import LogManager
from trading.brokers.oanda_client import OandaClient


class DataPopulationService:
    def __init__(self):
        """
        Initializes the DataPopulationService with MongoDBHandler and OandaClient.
        """
        # MongoDB handler for saving data
        self.mongo_handler = MongoDBHandler(db_name="forex_data")
        
        # Ensure the database exists before querying or inserting data
        self.mongo_handler.ensure_database_exists()

        # OANDA client for fetching forex data
        self.oanda_client = OandaClient(
            environment=os.getenv('OANDA_ENV', 'practice')  # Default to 'practice'
        )
        # Logger for data population service
        self.logger = LogManager('data_population_logs').get_logger()

    def ensure_collection_exists_and_populate(self, instrument, granularity="D", count=5000):
        """
        Ensure the MongoDB collection exists and populate it with historical data.

        :param instrument: The forex pair (e.g., "EUR_USD").
        :param granularity: The timeframe (e.g., "M1", "D", "H1").
        :param count: The number of data points to fetch.
        """
        try:
            # Ensure the instrument symbol is always lowercase
            instrument = instrument.upper()
            
            # Prepare collection name based on instrument and granularity
            collection_name = f"{instrument.lower()}_{granularity.lower()}_data"
            
            # Switch to or create the collection
            # self.mongo_handler.switch_collection(collection_name)
            
            # Fetch and store data if collection does not already exist
            if not self.mongo_handler.collection_exists(collection_name):
                self.logger.info(f"Collection '{collection_name}' does not exist. Populating data...")
                
                # Ensure the collection is created and indexed before querying or inserting data
                self.mongo_handler.create_collection_with_index(collection_name, index_field="time")
                
                # Fetch and store data after ensuring collection is created
                self.mongo_handler.populate_historical_data(instrument, granularity, count)
        
            else:
                self.logger.info(f"Collection '{collection_name}' already exists. Skipping creation.")

        except Exception as e:
            self.logger.error(f"Error ensuring collection and populating data for {instrument}: {e}")
            raise

    def populate_all_instruments(self):
        """
        Populate historical data for all major forex instruments and multiple granularities.
        """
        try:
            # List of major forex pairs and granularities to fetch
            major_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CHF", "USD_CAD"]
            granularities = ["M1", "D", "M"]

            # Fetch and store data for each pair and granularity
            for pair, granularity in itertools.product(major_pairs, granularities):
                self.mongo_handler.populate_historical_data(pair, granularity)
        
        except Exception as e:
            # Log error in case of failure in populating data for all instruments
            self.logger.error(f"Error populating all instruments: {e}")
            raise

    # def update_data_every_minute(self):
    #     """
    #     Scheduler to update historical data for all instruments every minute.
    #     """
    #     self.populate_all_instruments()  # Initial population
    #     schedule.every(1).minute.do(self.populate_all_instruments)

    #     self.logger.info("Data population scheduler started, updating every minute.")

    #     # Run the schedule indefinitely
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(1)