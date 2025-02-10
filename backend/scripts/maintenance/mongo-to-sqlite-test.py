import sys
import os

# Ensure that the backend directory is added to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from backend.data.repositories._mongo_db import MongoDBHandler
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from backend.logs.log_manager import LogManager  # Import the logger

# Initialize the logger
logger = LogManager('mongo_to_sqlite_logs').get_logger()

# Initialize MongoDB and SQLiteDBHandler handlers
mongo_handler = MongoDBHandler(db_name="forex_db", collection_name="eur_usd_d_data")
sqlite_db = SQLiteDBHandler(db_name="historical_data.db")

# Log the start of the migration
logger.info("Starting data migration from MongoDB to SQLite")

# Populate SQLite with MongoDB historical data for EUR/USD, daily granularity
mongo_handler.populate_sqlite_from_mongo(
    sqlite_db=sqlite_db,
    collection_name="eur_usd_d_data",
    instrument="EUR_USD",
    granularity="D"
)

logger.info("Data migration from MongoDB to SQLite completed.")
print("Data migration from MongoDB to SQLite completed.")
