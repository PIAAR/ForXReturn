# backend/scripts/setup/setup-database.py
# This script is used to initialize the databases and populate them with sample data.
# The sample data includes instruments, indicators, configuration, users, and optimization parameters.
import os
import sys
from backend.logs.log_manager import LogManager  # Import the LogManager class
from backend.data.repositories._sqlite_db import SQLiteDB  # Use the existing SQLiteDB class

# Import your population scripts
from backend.scripts.data_import.populate_sample_sqlite_data import populate_sample_data
from backend.scripts.data_import.populate_instruments import populate_instruments
from backend.scripts.data_import.populate_indicators import populate_indicators

# Configure logging
logger = LogManager('database_setup').get_logger()

def initialize_db(db_name):
    """
    Initialize the database using the existing SQLiteDB class.
    """
    try:
        db = SQLiteDB(db_name)
        db.initialize_db()
        logger.info(f"Database '{db_name}' initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database '{db_name}': {e}")
        print(f"An error occurred while initializing '{db_name}': {e}")
        sys.exit(1)

def populate_data():
    """
    Run the population scripts after databases are initialized.
    """
    try:
        logger_data_while_populating()
    except Exception as e:
        logger.error(f"Error populating data: {e}")
        print(f"An error occurred while populating data: {e}")
        sys.exit(1)

def logger_data_while_populating():
    logger.info("Populating sample data...")
    # populate_sample_data()  # Call to populate_sample_sqlite_data
    logger.info("Populating instruments...")
    populate_instruments()  # Call to populate_instruments
    logger.info("Populating indicators...")
    populate_indicators()  # Call to populate_indicators
    logger.info("Data population completed successfully.")

def main():
    """
    Main function to initialize all or specific databases and populate them.
    """
    # List of all database names
    databases = [
        'indicators.db',
        'instruments.db',
        historical_data.db,
        'optimizer.db',
        'user.db',
        'configuration.db'
    ]

    if len(sys.argv) > 1:
        db_name = sys.argv[1]
        if db_name in databases:
            initialize_db(db_name)
        else:
            print(f"Database '{db_name}' is not recognized.")
            sys.exit(1)
    else:
        # Initialize all databases
        for db_name in databases:
            print(f"Initializing database '{db_name}'...")
            initialize_db(db_name)

        # Populate data after initializing databases
        populate_data()

if __name__ == '__main__':
    main()
