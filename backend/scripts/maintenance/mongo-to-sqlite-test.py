# backend/scripts/maintenance/mongo-to-sqlite-test.py
import sys
import os
# Ensure that the backend directory is added to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from data.repositories.mongo import MongoDBHandler
from data.repositories._sqlite_db import SQLiteDB

# Initialize MongoDB and SQLiteDB handlers
mongo_handler = MongoDBHandler(db_name="forex_db", collection_name="eur_usd_daily_data")
sqlite_db = SQLiteDB(db_name="historical_data.db")

# Populate SQLite with MongoDB historical data for EUR/USD, daily granularity
mongo_handler.populate_sqlite_from_mongo(
    sqlite_db=sqlite_db,
    collection_name="eur_usd_daily_data",
    instrument="EUR_USD",
    granularity="D"
)

print("Data migration from MongoDB to SQLite completed.")
