# backend/data/repositories/mongo.py
import os
from datetime import datetime, timedelta

import yfinance as yf
from datetime import timezone
from pymongo import MongoClient, errors

from backend.config.secrets import defs
from backend.logs.log_manager import LogManager
from backend.trading.brokers.oanda_client import OandaClient

# Initialize the logger
logger = LogManager('mongo_connection_logs').get_logger()

class MongoDBHandler:
    _client = None

    def __init__(self, db_name, collection_name=None):
        """
        Initializes the MongoDBHandler with a connection to the specified database and optionally a collection.

        :parameter db_name: The name of the database to connect to.
        :parameter collection_name: The name of the collection to interact with (optional).
        """
        self.mongo_url = os.getenv('MONGO_URL', defs.MONGO_URI)
        self.client = MongoDBHandler._get_mongo_client(self.mongo_url)
        self.oanda_client = OandaClient()
        self.db_name = db_name
        self.db = self.client[db_name]
        self.collection = self.db[collection_name] if collection_name else None

    @staticmethod
    def _get_mongo_client(mongo_url):
        """
        Establishes a reusable connection to MongoDB using the MongoDB URL.
        Returns a MongoClient instance if not already connected.
        """
        if MongoDBHandler._client is None:
            try:
                client = MongoClient(mongo_url, serverSelectionTimeoutMS=500)  # 5 seconds timeout
                # Check if the server is available
                client.admin.command('ping')
                logger.info(f"Connected to MongoDB at {mongo_url}")
                MongoDBHandler._client = client
            except errors.ServerSelectionTimeoutError as err:
                logger.error(f"Failed to connect to MongoDB: {err}")
                raise
        return MongoDBHandler._client

    def list_collections(self):
            """
            Lists all collections in the database.
            :return: A list of collection names.
            """
            try:
                collections = self.db.list_collection_names()
                logger.info(f"Retrieved collections: {collections}")
                return collections
            except errors.PyMongoError as err:
                logger.error(f"Failed to list collections: {err}")
                raise

    def collection_exists(self, collection_name):
        """
        Checks if a collection exists in the database.
        
        :parameter collection_name: The name of the collection to check.
        :return: True if the collection exists, False otherwise.
        """
        return collection_name in self.db.list_collection_names()

    def switch_collection(self, collection_name):
        """
        Switches to a different collection within the same database.
        Creates the collection if it doesn't exist.
        
        :parameter collection_name: The name of the collection to switch to.
        """
        if self.collection_exists(collection_name) is None:
            self.db.create_collection(collection_name)
            logger.info(f"Created new collection: {collection_name}")
        self.collection = self.db[collection_name]
        logger.info(f"Switched to collection: {collection_name}")

    def short_bulk_insert(self, documents):
        """
        Inserts multiple documents into the current collection.
        :parameter documents: A list of documents to insert.
        """
        try:
            if documents:
                self.collection.insert_many(documents, ordered=False)  # Perform unordered insert to improve speed
                logger.info(f"Inserted {len(documents)} documents into {self.collection.name}")
            else:
                logger.warning(f"No documents to insert into {self.collection.name}")
        except errors.PyMongoError as err:
            logger.error(f"Short Bulk insert failed: {err}")
            raise

    def long_bulk_insert(self, documents):
        """
        Inserts multiple documents into the current collection, printing each document inserted.
        :parameter documents: A list of documents to insert.
        """
        try:
            if documents:
                for doc in documents:
                    self.collection.insert_one(doc)
                    # Print each document inserted to the console
                    print(f"Inserted document with time: {doc['time']} into {self.collection.name}")
                    logger.info(f"Inserted document with time: {doc['time']} into {self.collection.name}")
            else:
                logger.warning(f"No documents to insert into {self.collection.name}")
        except errors.PyMongoError as err:
            logger.error(f"Long Bulk insert failed: {err}")
            raise

    def create_collection(self, collection_name):
        """
        Creates a new collection within the database.

        :parameter collection_name: The name of the collection to create.
        :return: The created collection object.
        """
        try:
            collection = self.db.create_collection(collection_name)
            logger.info(f"Created collection: {collection_name}")
            return collection
        except errors.CollectionInvalid as err:
            logger.error(f"Failed to create collection '{collection_name}': {err}")
            raise

    def drop_collection(self, collection_name):
        """
        Drops a collection from the database.

        :parameter collection_name: The name of the collection to drop.
        """
        try:
            self.db.drop_collection(collection_name)
            logger.info(f"Dropped collection: {collection_name}")
        except errors.PyMongoError as err:
            logger.error(f"Failed to drop collection '{collection_name}': {err}")
            raise

    def create_database(self, db_name):
        """
        Creates a new database. In MongoDB, databases are created on demand by using them.

        :parameter db_name: The name of the database to create.
        :return: The created database object.
        """
        try:
            db = self.client[db_name]
            # To actually create the database, we need to insert a collection or create a collection
            db.create_collection("initial_collection")
            logger.info(f"Created database: {db_name}")
            return db
        except errors.PyMongoError as err:
            logger.error(f"Failed to create database '{db_name}': {err}")
            raise

    def drop_database(self, db_name):
        """
        Drops a database.

        :parameter db_name: The name of the database to drop.
        """
        try:
            self.client.drop_database(db_name)
            logger.info(f"Dropped database: {db_name}")
        except errors.PyMongoError as err:
            logger.error(f"Failed to drop database '{db_name}': {err}")
            raise

    def create(self, document):
        """
        Inserts a single document into the collection.

        :parameter document: A dictionary representing the document to insert.
        :return: The inserted ID of the document.
        """
        try:
            result = self.collection.insert_one(document)
            logger.info(f"Document inserted with ID: {result.inserted_id}")
            return result.inserted_id
        except errors.DuplicateKeyError as err:
            logger.warning(f"Duplicate document error: {err}")
            return None
        except errors.PyMongoError as err:
            logger.error(f"Failed to insert document: {err}")
            raise

    def read(self, query=None, collection_name=None):
        """
        Reads documents from the collection.

        :parameter query: A dictionary representing the query to match documents.
        :parameter collection_name: The name of the collection to read from.
        :return: A list of matched documents.
        """
        if not collection_name:
            raise ValueError("Collection name must be specified.")

        collection = self.db[collection_name]
        try:
            documents = collection.find(query or {})
            results = list(documents)
            logger.info(f"Found {len(results)} documents in {collection_name}.")
            return results
        except errors.PyMongoError as err:
            logger.error(f"Failed to read documents from {collection_name}: {err}")
            raise

    def update(self, query, update_values):
        """
        Updates documents in the collection based on a query.

        :parameter query: A dictionary representing the query to match documents.
        :parameter update_values: A dictionary representing the update values.
        :return: The count of documents updated.
        """
        try:
            result = self.collection.update_many(query, {'$set': update_values})
            logger.info(f"Updated {result.modified_count} documents matching query: {query}")
            return result.modified_count
        except errors.PyMongoError as err:
            logger.error(f"Failed to update documents: {err}")
            raise

    def delete(self, query):
        """
        Deletes documents from the collection based on a query.

        :parameter query: A dictionary representing the query to match documents.
        :return: The count of documents deleted.
        """
        try:
            result = self.collection.delete_many(query)
            logger.info(f"Deleted {result.deleted_count} documents matching query: {query}")
            return result.deleted_count
        except errors.PyMongoError as err:
            logger.error(f"Failed to delete documents: {err}")
            raise

    def close(self):
        """
        Closes the MongoDB connection.
        """
        if MongoDBHandler._client:
            try:
                MongoDBHandler._client.close()
                MongoDBHandler._client = None
                logger.info("MongoDB connection closed.")
            except errors.PyMongoError as err:
                logger.error(f"Failed to close MongoDB connection: {err}")
                raise

    def ensure_database_exists(self):
        """
        Ensures that the database exists by attempting to switch to it.
        """
        try:
            self.db = self.client[self.db_name]
            logger.info(f"Using database: {self.db_name}")
        except errors.PyMongoError as err:
            logger.error(f"Failed to create or switch to database '{self.db_name}': {err}")
            raise

    def ensure_collection_exists_and_populate(self, instrument, granularity="D", count=500):
        """
        Ensure the MongoDB collection exists and populate it with historical data.

        :param instrument: The forex pair (e.g., "EUR_USD").
        :param granularity: The timeframe (e.g., "M1", "D", "H1").
        :param count: The number of data points to fetch.
        """
        try:
            collection_name = f"{instrument.lower()}_{granularity.lower()}_data"

            # Ensure collection exists and create it if necessary
            self.create_collection_with_index(collection_name, index_field="time")

            # Check if the latest data exists before fetching new data
            latest_record = self.db[collection_name].find_one(sort=[("time", -1)])
            latest_time = latest_record["time"] if latest_record else None

            if latest_time:
                logger.info(f"📌 Latest stored data for {instrument} ({granularity}): {latest_time}")
                start_time = datetime.fromisoformat(latest_time[:-1])  # Remove trailing 'Z' if present
            else:
                start_time = datetime.utcnow() - timedelta(days=30)  # Default to last 30 days if no data

            logger.info(f"Latest record found in {collection_name} at {latest_time}. Fetching newer data...")

            # Fetch **only newer data** from OANDA
            new_data = self.oanda_client.fetch_historical_data(instrument, granularity, count, start_time=start_time)
            # Ensure collection is set before inserting
            self.switch_collection(collection_name)

            if new_data:
                self.switch_collection(collection_name)
                self.short_bulk_insert(new_data)
                logger.info(f"✅ Inserted {len(new_data)} new records for {instrument} in {granularity}.")
            else:
                logger.info(f"⚠️ No new data available for {instrument} in {granularity}.")

        except Exception as e:
            logger.error(f"❌ Error ensuring collection and populating data for {instrument}: {e}")
            raise

    def create_collection_with_index(self, collection_name, index_field="time"):
        """
        Creates a new collection with an index on a specific field if it doesn't already exist.
        
        :param collection_name: The name of the collection to create.
        :param index_field: The field to index (default is 'time').
        """
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(collection_name)
            self.collection = self.db[collection_name]
            self.collection.create_index([(index_field, 1)], unique=True)
            logger.info(f"Created collection '{collection_name}' with index on '{index_field}'")
            
    def populate_historical_data(self, instrument, granularity="D", count=5000):
        """
        Fetch and store historical data for a given forex instrument.
        :parameter instrument: The forex pair (e.g., "EUR_USD").
        :parameter granularity: The timeframe (e.g., "M1", "D", "H1").
        :parameter count: The number of data points to fetch.
        """
        try:
            # Ensure the instrument symbol is in uppercase, as expected by the OANDA API
            instrument = instrument.upper()

            # Ensure collection exists and set it
            collection_name = f"{instrument.lower()}_{granularity.lower()}_data"
            # Ensure collection exists before querying and switch to it
            self.create_collection_with_index(collection_name, index_field="time")
            self.switch_collection(collection_name)  # Ensure collection is set

            print(f"Collection set to: {self.collection.name}")  # Debugging

            # Fetch historical data from OANDA
            data = self.oanda_client.fetch_historical_data(instrument, granularity, count)
            print(f"Fetched data: {data}")  # Debugging

            # Check if data is returned and proceed
            if not data:
                self.logger.warning(f"No data received for {instrument} with granularity {granularity}.")
                return

            # Ensure collection is set before read operation or create new collection
            if self.collection is None:
                raise ValueError(f"MongoDB collection for {collection_name} is not set.")

            if new_data := list(data):
                print(f"New data to insert: {new_data}")  # Debugging
                self.short_bulk_insert(new_data)
                logger.info(f"Inserted {len(new_data)} new data points for {instrument} in {granularity} timeframe.")
            else:
                logger.info(f"No new data to insert for {instrument} in {granularity} timeframe.")

        except Exception as e:
            # Log any error during the population process
            logger.error(f"Error populating data for {instrument}: {e}")
            raise

    def populate_sqlite_from_mongo(self, sqlite_db, instrument, granularity):
        """
        Populate data from MongoDB to SQLite for backtesting.

        :param sqlite_db: The SQLite database object to insert data into.
        :param instrument: The forex pair (e.g., 'EUR_USD').
        :param granularity: The timeframe (e.g., 'D', 'M1', 'H1').
        """
        collection_name = f"{instrument.lower()}_{granularity.lower()}_data"
        data = self.read({}, collection_name=collection_name)

        if not data:
            logger.warning(f"⚠️ No data found in MongoDB for {instrument} with {granularity}. Fetching...")
            self.ensure_collection_exists_and_populate(instrument, granularity, count=500)
            data = self.read({}, collection_name=collection_name)

        instrument_id = sqlite_db.get_instrument_id(instrument)
        if instrument_id is None:
            logger.error(f"❌ Instrument {instrument} not found in SQLite. Skipping...")
            return

        if records := [
            {
                "instrument_id": instrument_id,
                "granularity": granularity,
                "timestamp": record.get("time"),
                "open": float(record.get("mid", {}).get("o", 0)),
                "high": float(record.get("mid", {}).get("h", 0)),
                "low": float(record.get("mid", {}).get("l", 0)),
                "close": float(record.get("mid", {}).get("c", 0)),
                "volume": record.get("volume", 0),
            }
            for record in data
        ]:
            sqlite_db.bulk_insert("historical_data", records)
            logger.info(f"✅ Inserted {len(records)} records for {instrument} into SQLite.")

    def switch_collection(self, collection_name):
        """
        Switches to a different collection within the same database.
        """
        self.collection = self.db[collection_name]
        logger.info(f"📂 Switched to collection: {collection_name}")

    def short_bulk_insert(self, documents):
        """
        Inserts multiple documents into the current collection while avoiding duplicates.
        """
        try:
            if documents:
                self.collection.insert_many(documents, ordered=False)
                logger.info(f"✅ Inserted {len(documents)} documents into {self.collection.name}")
            else:
                logger.warning(f"⚠️ No documents to insert into {self.collection.name}")
        except errors.BulkWriteError as err:
            logger.error(f"❌ Short Bulk insert failed: {err}")
            raise

    def read(self, query=None, collection_name=None):
        """
        Reads documents from the collection.

        :param query: A dictionary representing the query to match documents.
        :param collection_name: The name of the collection to read from.
        :return: A list of matched documents.
        """
        if not collection_name:
            raise ValueError("❌ Collection name must be specified.")

        collection = self.db[collection_name]
        try:
            documents = collection.find(query or {})
            results = list(documents)
            logger.info(f"📊 Found {len(results)} documents in {collection_name}.")
            return results
        except errors.PyMongoError as err:
            logger.error(f"❌ Failed to read documents from {collection_name}: {err}")
            raise
        
    def fetch_yfinance_data(self, instrument, granularity="1h", days=30):
        """
        Fetches recent Forex data from Yahoo Finance and inserts it into MongoDB.
        """
        logger.info(f"📥 Fetching {instrument} data from Yahoo Finance ({days} days, {granularity})...")

        # Format instrument for Yahoo Finance
        yf_symbol = f"{instrument}=X"

        # Define the timeframe
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        # Fetch data from Yahoo Finance
        try:
            df = yf.download(yf_symbol, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), interval=granularity)

            if df.empty:
                logger.warning(f"⚠️ No data found for {instrument}.")
                return

            # Convert data to MongoDB format
            df.reset_index(inplace=True)
            df.rename(columns={"Datetime": "time", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
            df["time"] = df["time"].astype(str)  # Ensure time is stored as string

            # Insert into MongoDB
            collection_name = f"{instrument.lower()}_{granularity}_data"
            self.db[collection_name].insert_many(df.to_dict("records"))

            logger.info(f"✅ Inserted {len(df)} records into {collection_name}.")

        except Exception as e:
            logger.error(f"❌ Error fetching {instrument} from Yahoo Finance: {e}")

    def ensure_latest_data(self, instrument, granularity="1h", days=30):
        """
        Ensures the latest data is in MongoDB. If missing, fetches from Yahoo Finance.
        """
        collection_name = f"{instrument.lower()}_{granularity}_data"

        if latest_entry := self.db[collection_name].find_one(sort=[("time", -1)]):
            latest_date = datetime.strptime(latest_entry["time"], "%Y-%m-%d %H:%M:%S")
            if latest_date >= datetime.now(timezone.utc) - timedelta(days=1):
                logger.info(f"🔄 {instrument} is up-to-date in MongoDB.")
                return

        # If missing recent data, fetch from Yahoo Finance
        self.fetch_yfinance_data(instrument, granularity, days)