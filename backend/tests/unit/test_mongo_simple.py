import os
from pymongo import MongoClient, errors
from backend.data.repositories.mongo import MongoDBHandler
from backend.trading.brokers.oanda_client import OandaClient
from backend.logs.log_manager import LogManager
from backend.config.secrets import defs  # Ensure this exists

# Replace with your MongoDB URI
MONGO_URI = os.getenv('MONGO_URL', defs.MONGO_URI)

logger = LogManager('mongo_connection_logs').get_logger()

class MongoDBHandler:
    _client = None  # Class-level variable to maintain a single MongoClient instance

    def __init__(self, db_name, collection_name=None):
        """
        Initializes the MongoDBHandler with a connection to the specified database and optionally a collection.

        :parameter db_name: The name of the database to connect to.
        :parameter collection_name: The name of the collection to interact with (optional).
        """
        self.mongo_url = MONGO_URI
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
                client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)  # 5 seconds timeout
                # Check if the server is available
                client.admin.command('ping')
                logger.info(f"Connected to MongoDB at {mongo_url}")
                MongoDBHandler._client = client
            except errors.ServerSelectionTimeoutError as err:
                logger.error(f"Failed to connect to MongoDB: {err}")
                raise
        return MongoDBHandler._client


def test_mongo_connection():
    """
    Tests the MongoDB connection by pinging the server.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client.admin  # Test against the "admin" database
        server_status = db.command("serverStatus")  # Ping the server

        print("✅ Connection successful!")
        print(f"MongoDB Version: {server_status['version']}")
    except Exception as e:
        print("❌ Connection failed:", e)

if __name__ == "__main__":
    test_mongo_connection()
