import os
import requests
import yfinance as yf
from datetime import datetime
from backend.data.repositories.mongo import MongoDBHandler
from backend.logs.log_manager import LogManager

class ForexDataFetcher:
    def __init__(self):
        """Initialize API keys, MongoDB connection, and endpoints."""
        self.oanda_api_key = os.getenv("OANDA_API_KEY")
        self.oanda_url = "https://api-fxtrade.oanda.com/v3/instruments/{}/candles"
        self.mongo_handler = MongoDBHandler(db_name="forex_data")
        self.logger = self.setup_logger()

    def setup_logger(self):
        """Set up a logger for the fetcher."""
        import logging
        logger = logging.getLogger("forex_data_fetcher")
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def get_latest_timestamp(self, collection_name):
        """
        Checks the latest stored timestamp in the MongoDB collection.
        
        :param collection_name: Name of the MongoDB collection.
        :return: Latest timestamp as a string or None if no data exists.
        """
        try:
            latest_entry = self.mongo_handler.db[collection_name].find_one(
                sort=[("timestamp", -1)]
            )
            if latest_entry:
                return latest_entry["timestamp"]
        except Exception as e:
            self.logger.error(f"Error fetching latest timestamp: {e}")
        return None

    def fetch_historical_data_yahoo(self, pair="EURUSD=X", period="5y", interval="1d"):
        """
        Fetches historical forex data from Yahoo Finance.
        
        :param pair: Forex pair symbol (e.g., "EURUSD=X" for EUR/USD)
        :param period: Data period ("1mo", "1y", "5y", "max")
        :param interval: Data granularity ("1d", "1h", "5m")
        :return: Pandas DataFrame with forex data
        """
        try:
            self.logger.info(f"Fetching historical data for {pair} from Yahoo Finance.")
            forex_data = yf.download(pair, period=period, interval=interval)
            forex_data.reset_index(inplace=True)
            return forex_data
        except Exception as e:
            self.logger.error(f"Failed to fetch Yahoo Finance data: {e}")
            return None

    def fetch_real_time_data_oanda(self, instrument="EUR_USD", granularity="M1", count=500):
        """
        Fetch real-time forex data from OANDA API.
        
        :param instrument: OANDA instrument symbol (e.g., "EUR_USD")
        :param granularity: "M1", "M5", "H1", "D"
        :param count: Number of historical candles to fetch
        :return: JSON response with forex data
        """
        try:
            headers = {"Authorization": f"Bearer {self.oanda_api_key}"}
            params = {"granularity": granularity, "count": count}
            response = requests.get(self.oanda_url.format(instrument), headers=headers, params=params)

            if response.status_code == 200:
                self.logger.info(f"Fetched real-time forex data for {instrument} from OANDA.")
                return response.json()
            else:
                self.logger.error(f"OANDA API Error: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to fetch OANDA data: {e}")
            return None

    def store_forex_data(self, data, collection_name):
        """
        Stores forex data in MongoDB.
        
        :param data: DataFrame containing forex data.
        :param collection_name: Name of the collection to store data.
        """
        if data is None or data.empty:
            self.logger.warning(f"No new data to insert into {collection_name}.")
            return

        try:
            records = data.to_dict("records")
            self.mongo_handler.db[collection_name].insert_many(records, ordered=False)
            self.logger.info(f"Inserted {len(records)} new records into {collection_name}.")
        except Exception as e:
            self.logger.error(f"Error inserting data into MongoDB: {e}")

    def update_forex_data(self, pair="EURUSD=X", interval="1d"):
        """
        Updates forex data by checking for missing records in MongoDB.

        :param pair: Forex pair symbol (e.g., "EURUSD=X").
        :param interval: Data granularity ("1m", "5m", "1h", "1d", "1w").
        """
        collection_name = f"{pair.lower().replace('=', '_')}_{interval}_data"
        latest_timestamp = self.get_latest_timestamp(collection_name)

        # Define valid periods for Yahoo Finance
        period_lookup = {
            "1m": "2mo",  # Max 2 months for 1-minute data
            "5m": "2mo",  # Max 2 months for 5-minute data
            "1h": "2y",  # Max 2 years for 1-hour data
            "1d": "5y",  # Up to 5 years for daily data
            "1w": "10y",  # Up to 10 years for weekly data
        }

        max_period = period_lookup.get(interval, "5y")  # Default to 5 years if missing

        if latest_timestamp:
            self.logger.info(f"Latest data timestamp for {pair} ({interval}): {latest_timestamp}")
            start_date = datetime.strptime(latest_timestamp, "%Y-%m-%dT%H:%M:%S")
            days_since_last_update = (datetime.now() - start_date).days

            # Map days to valid Yahoo Finance period formats
            if days_since_last_update <= 5:
                period = "5d"
            elif days_since_last_update <= 30:
                period = "1mo"
            elif days_since_last_update <= 60:
                period = "2mo"
            elif days_since_last_update <= 90:
                period = "3mo"
            elif days_since_last_update <= 180:
                period = "6mo"
            elif days_since_last_update <= 365:
                period = "1y"
            elif days_since_last_update <= 730:
                period = "2y"
            else:
                period = max_period  # Use the max allowed by Yahoo Finance
        else:
            self.logger.info(f"No data found for {pair} ({interval}), fetching max allowed history.")
            period = max_period  # Use the max Yahoo Finance allows

        new_data = self.fetch_historical_data_yahoo(pair=pair, period=period, interval=interval)

        if new_data is not None:
            self.store_forex_data(new_data, collection_name)
        else:
            self.logger.warning(f"No new data retrieved for {pair} ({interval}).")


# Example usage on app startup
fetcher = ForexDataFetcher()
fetcher.update_forex_data()
