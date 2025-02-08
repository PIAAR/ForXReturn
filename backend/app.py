import itertools
import threading
import subprocess
import logging
import os
from backend.api.routes.routes import create_app
from backend.api.controllers.forex_data_controller import ForexDataFetcher
from backend.scripts.setup.setup_database import PopulateSQLTables
from backend.data.repositories._sqlite_db import SQLiteDB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_startup")
print("Starting app. Please wait... ")

'''
Flask is a web application framework written in Python. It is based on the Werkzeug WSGI toolkit and Jinja2 template engine. 
Flask is called a micro framework because it does not require particular tools or libraries. It has no database abstraction layer and form validation. 
This file should create and run your Flask application. You need to import the create_app function from routes.py and use it to initialize the Flask app.
'''

def check_table_exists(db_name, table_name):
    """
    Check if a specific table exists in the SQLite database.
    """
    try:
        db = SQLiteDB(db_name)
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        result = db.fetch_records_with_query(query)
        return bool(result)
    except Exception as e:
        logger.error(f"Error checking if table '{table_name}' exists in {db_name}: {e}")
        return False

def run_database_setup():
    """
    Runs the database setup script before starting the application, 
    but only if the necessary tables are missing.
    """
    try:
        db_setup = PopulateSQLTables()

        # Check if the necessary tables exist
        instruments_exist = check_table_exists("instruments.db", "instruments")
        indicators_exist = check_table_exists("indicators.db", "indicators")

        if not instruments_exist or not indicators_exist:
            logger.info("🔄 Running database setup since required tables are missing...")
            print("Running database setup...")
            db_setup.setup()
            logger.info("✅ Database setup completed successfully.")
            print("Database setup completed successfully.")
        else:
            logger.info("✅ Database already set up. Skipping setup process.")
    except Exception as e:
        logger.error(f"❌ Error running database setup: {e}")

def run_fetcher_on_startup():
    """
    Runs the forex data fetcher at app startup.
    Ensures that only missing data is fetched.
    """
    try:
        logger.info("🔄 Starting Forex Data Fetcher...")
        print("Starting Forex Data Fetcher...")

        fetcher = ForexDataFetcher()
        forex_pairs = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X"]
        granularities = ["1m", "5m", "1h", "1d"]  # Supported intervals

        for pair, granularity in itertools.product(forex_pairs, granularities):
            try:
                logger.info(f"📥 Fetching {pair} data for {granularity} granularity...")
                print(f"📥 Fetching {pair} data for {granularity} granularity...")
                fetcher.update_forex_data(pair=pair, interval=granularity)
            except Exception as e:
                logger.error(f"❌ Error fetching {pair} ({granularity}): {e}")

        logger.info("✅ Forex data ingestion completed successfully.")
        print("Forex data ingestion completed successfully.")
    except Exception as e:
        logger.error(f"❌ Critical error in Forex Data Fetcher: {e}")

# Step 1: Run database setup before starting the app
run_database_setup()

# Step 2: Start Forex Data Fetcher in a separate thread
fetcher_thread = threading.Thread(target=run_fetcher_on_startup, daemon=True)
fetcher_thread.start()

# Step 3: Create and start the Flask application
app = create_app()

if __name__ == '__main__':
    logger.info("🚀 Starting Flask application...")
    print("Starting Flask application...")
    app.run(debug=True)
