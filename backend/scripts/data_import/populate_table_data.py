import itertools
from datetime import datetime, timedelta
import pandas as pd
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from backend.data.repositories._mongo_db import MongoDBHandler
from backend.trading.brokers.oanda_client import OandaClient
from backend.logs.log_manager import LogManager

from datetime import timezone
# Initialize Logger
logger = LogManager('populate_sqlite_data').get_logger()

class PopulateTableData:
    def __init__(self):
        """
        Initialize database connections for SQLite and MongoDB.
        """
        self.mongo_handler = MongoDBHandler(db_name="forex_data")
        self.sqlite_db = SQLiteDBHandler(db_name="historical_data.db")
        self.oanda_client = OandaClient()  # OANDA Client for fetching forex data
        self.indicators_db = SQLiteDBHandler("indicators.db")
        self.instruments_db = SQLiteDBHandler("instruments.db")
        self.optimizer_db = SQLiteDBHandler("optimizer.db")
        self.config_db = SQLiteDBHandler("configuration.db")
        self.user_db = SQLiteDBHandler("user.db")
        
    def ensure_instruments_exist(self):
        """
        Ensures that the required instruments exist in the SQLite database.
        """
        instruments = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD"]

        for instrument in instruments:
            existing = self.instruments_db.fetch_records("instruments", {"name": instrument})
            if not existing:
                self.instruments_db.add_record("instruments", {
                    "name": instrument,
                    "opening_time": "00:00:00",
                    "closing_time": "23:59:59"
                })
                logger.info(f"✅ Inserted instrument: {instrument}")

    def get_latest_timestamp(self, instrument, granularity):
        """
        Fetches the latest timestamp for a given instrument and granularity from SQLite.
        :param instrument: The forex pair (e.g., "EUR_USD").
        :param granularity: The timeframe (e.g., "D", "H1").
        :return: Latest timestamp or None if no data exists.
        """
        query = """
            SELECT MAX(timestamp) FROM historical_data 
            WHERE instrument_id = ? AND granularity = ?
        """
        instrument_id = self.sqlite_db.get_instrument_id(instrument)
        result = self.sqlite_db.fetch_records_with_query(query, (instrument_id, granularity))

        if result and result[0][0]:
            return datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S")  # Convert string to datetime
        else:
            # If no data exists, return one year ago
            return datetime.now(timezone.utc) - timedelta(days=365)

    def populate_sample_data(self):
        """
        Populates sample data into various SQLite databases.
        """
        logger.info("🔄 Populating sample data into SQLite tables...")

        # Initialize all databases
        databases = [
            self.indicators_db, self.instruments_db, self.sqlite_db,
            self.optimizer_db, self.config_db, self.user_db
        ]
        for db in databases:
            db.initialize_db()

        # Insert sample indicators
        indicators = [
            {"name": "RSI", "type": "momentum"},
            {"name": "ATR", "type": "volatility"},
            {"name": "BollingerBands", "type": "volatility"},
            {"name": "MACrossover", "type": "trend"},
        ]
        indicator_ids = {
            ind["name"]: self.indicators_db.add_record("indicators", ind)
            for ind in indicators
        }
        # Insert indicator parameters
        indicator_params = [
            (indicator_ids["RSI"], "period", "integer", "14"),
            (indicator_ids["ATR"], "period", "integer", "14"),
            (indicator_ids["BollingerBands"], "period", "integer", "20"),
            (indicator_ids["BollingerBands"], "std", "float", "2.0"),
            (indicator_ids["MACrossover"], "fast_period", "integer", "12"),
            (indicator_ids["MACrossover"], "slow_period", "integer", "26"),
        ]
        for param in indicator_params:
            self.indicators_db.add_record("indicator_parameters", {
                "indicator_id": param[0],
                "parameter_name": param[1],
                "parameter_type": param[2],
                "default_value": param[3]
            })

        # Insert sample instruments
        instruments = [
            {"name": "EUR_USD", "opening_time": "00:00:00", "closing_time": "23:59:59"},
            {"name": "GBP_USD", "opening_time": "00:00:00", "closing_time": "23:59:59"},
        ]
        for instrument in instruments:
            self.instruments_db.add_record("instruments", instrument)

        logger.info("✅ Sample data population completed.")

    def populate_historical_data_to_mongo(self, days=365):
        """
        Populates MongoDB with OANDA's historical forex data.
        
        :param days: Number of days of historical data to fetch.
        """
        logger.info(f"🔄 Fetching last {days} days of historical data from OANDA and storing in MongoDB...")

        forex_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "USD_CHF", "NZD_USD"]
        granularities = ["M", "W", "D", "H1", "M5", "M1", "S5", "S1"]
        """
        EUR/USD = "the fiber": This is considered the most traded currency pair globally, often referred to as "the fiber". 
        GBP/USD = "cable": Also known as "cable," it is highly correlated to the EUR/USD and is a volatile, liquid pair due to high trading volumes. 
        USD/JPY = "dollar-yen": This pair, also known as the "dollar-yen," measures the Japanese yen required to purchase one US dollar, and is a major link between the western and eastern worlds. 
        AUD/USD = A major pair due to high trading volumes and the inclusion of the US dollar, often considered a commodity pair. 
        USD/CHF = "Swissie": Commonly known as the "Swissie," this pair is popular due to the Swiss financial system's reputation as a safe haven.
        NZD/USD = A major pair due to high trading volumes and the inclusion of the US dollar, often considered a commodity pair.
        USD/CAD = Another major pair, often referred to as a commodity pair, as the Canadian dollar's value is tied to the price of oil.
        """

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        for pair, granularity in itertools.product(forex_pairs, granularities):
            logger.info(f"📥 Fetching {pair} data ({granularity}, {days} days)...")

            # Fetch data from OANDA
            oanda_data = self.oanda_client.fetch_historical_data(
                instrument=pair,
                granularity=granularity,
                start_date=start_date,
                end_date=end_date
            )

            if not oanda_data:
                logger.warning(f"⚠️ No data found for {pair} - {granularity}. Skipping...")
                continue

            # Convert to Pandas DataFrame
            df = pd.DataFrame(oanda_data)
            if "time" not in df.columns:
                # Extract 'time' from index if missing
                df["time"] = df.index.strftime("%Y-%m-%d %H:%M:%S")
            else:
                df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d %H:%M:%S")  # Ensure correct format

            df.reset_index(inplace=True)  # Convert index to columns

            # Extract OHLC and volume
            df['open'] = df['mid'].apply(lambda x: float(x.get('o', 0)))
            df['high'] = df['mid'].apply(lambda x: float(x.get('h', 0)))
            df['low'] = df['mid'].apply(lambda x: float(x.get('l', 0)))
            df['close'] = df['mid'].apply(lambda x: float(x.get('c', 0)))
            df['volume'] = df.get('volume', 0).astype(float)

            # Drop unnecessary columns
            df.drop(columns=['mid', '_id'], errors='ignore', inplace=True)

            # # Store in MongoDB
            collection_name = f"{pair.lower()}_{granularity}_data"
            self.mongo_handler.switch_collection(collection_name)  # Ensure we're working with the right collection
            
            # 🔍 Debug: Check if the collection is set
            if self.mongo_handler.collection is None:
                logger.error(f"❌ MongoDB collection '{collection_name}' was not set properly.")
                raise ValueError(f"MongoDB collection '{collection_name}' is None")
            
            logger.info(f"📂 Using MongoDB collection: {self.mongo_handler.collection.name}")
            
            # Debug: Print first few rows of DataFrame
            print("DataFrame Columns Before Inserting:", df.columns)  # Debugging
            print(df.head())

            # Ensure 'time' exists in DataFrame
            if "time" not in df.columns:
                logger.error("❌ The 'time' column is missing from the dataframe before inserting into MongoDB.")
                raise KeyError("Missing 'time' field in historical data")

            # Insert into MongoDB
            self.mongo_handler.switch_collection(collection_name)
            self.mongo_handler.long_bulk_insert(df.to_dict(orient="records"))

            # Insert into MongoDB
            self.mongo_handler.long_bulk_insert(df.to_dict(orient="records"))

            logger.info(f"✅ Inserted {len(df)} records for {pair} - {granularity} in MongoDB.")

        logger.info("🎯 Historical data population to MongoDB complete!")

    def populate_historical_data_to_sqlite(self, years=1):
        """
        Populates SQLite with M1 & M5 data for instruments currently in position.
        - Fetches missing data from MongoDB.
        - Updates with real-time data from OANDA.
        """

        logger.info("🔄 Populating SQLite with active trading data (M1, M5)...")

        # Step 1: Get active positions from OANDA API
        active_positions = self.oanda_client.get_open_positions()
        if not active_positions or "positions" not in active_positions:
            logger.warning("⚠️ No active positions found. Skipping SQLite update.")
            return

        for position in active_positions["positions"]:
            pair = position["instrument"]
            logger.info(f"📌 Active position detected: {pair}")

            for granularity in ["M1", "M5"]:
                logger.info(f"📥 Fetching {pair} data ({granularity})...")

                # Step 2: Get latest timestamp from SQLite
                latest_timestamp = self.get_latest_timestamp(pair, granularity)

                # Step 3: Backfill missing data from MongoDB
                collection_name = f"{pair.lower()}_{granularity}_data"
                query = {"time": {"$gt": latest_timestamp.strftime("%Y-%m-%d %H:%M:%S")}}
                if mongo_data := self.mongo_handler.read(query, collection_name):
                    df = pd.DataFrame(mongo_data)
                    df["timestamp"] = pd.to_datetime(df["time"])
                    df.set_index("timestamp", inplace=True)

                    # Extract OHLCV
                    df["open"] = df["mid"].apply(lambda x: float(x.get("o", 0)))
                    df["high"] = df["mid"].apply(lambda x: float(x.get("h", 0)))
                    df["low"] = df["mid"].apply(lambda x: float(x.get("l", 0)))
                    df["close"] = df["mid"].apply(lambda x: float(x.get("c", 0)))
                    df["volume"] = df.get("volume", 0).astype(float)

                    # Drop unnecessary columns
                    df.drop(columns=["mid", "_id", "time"], errors="ignore", inplace=True)

                    # Insert MongoDB data into SQLite
                    self.sqlite_db.bulk_insert("historical_data", df.to_dict(orient="records"))
                    logger.info(f"✅ Backfilled {len(df)} records from MongoDB for {pair} - {granularity}.")

                if oanda_data := self.oanda_client.fetch_historical_data(
                    instrument=pair, granularity=granularity, count=10
                ):
                    df = pd.DataFrame(oanda_data)
                    df["timestamp"] = pd.to_datetime(df["time"])
                    df.set_index("timestamp", inplace=True)

                    df["open"] = df["mid"].apply(lambda x: float(x.get("o", 0)))
                    df["high"] = df["mid"].apply(lambda x: float(x.get("h", 0)))
                    df["low"] = df["mid"].apply(lambda x: float(x.get("l", 0)))
                    df["close"] = df["mid"].apply(lambda x: float(x.get("c", 0)))
                    df["volume"] = df.get("volume", 0).astype(float)

                    df.drop(columns=["mid", "_id", "time"], errors="ignore", inplace=True)

                    # Insert OANDA real-time data into SQLite
                    self.sqlite_db.bulk_insert("historical_data", df.to_dict(orient="records"))
                    logger.info(f"✅ Updated {len(df)} records from OANDA for {pair} - {granularity}.")

        logger.info("🎯 SQLite population complete.")

    def run(self):
        """
        Runs both sample data and historical data population.
        """
        # self.populate_sample_data()
        self.populate_historical_data_to_mongo()
        # self.populate_historical_data_to_sqlite()


# Run when script is executed
if __name__ == "__main__":
    populator = PopulateTableData()
    populator.run()
