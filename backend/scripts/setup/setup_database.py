# backend/scripts/setup/setup_database.py
import os
import sys
from backend.logs.log_manager import LogManager  # Import the LogManager class
from backend.data.repositories._sqlite_db import SQLiteDB  # Use the existing SQLiteDB class

# Import newly refactored population classes
from backend.scripts.data_import.populate_table_data import PopulateTableData
from backend.scripts.data_import.populate_instruments import PopulateInstrumentData
from backend.scripts.data_import.populate_indicators import PopulateIndicatorData

# Configure logging
logger = LogManager('database_setup').get_logger()


class PopulateSQLTables:
    """
    A class responsible for initializing and populating SQLite databases.
    """

    def __init__(self):
        # List of all databases to be initialized
        self.databases = [
            "indicators.db",
            "instruments.db",
            "historical_data.db",
            "optimizer.db",
            "user.db",
            "configuration.db",
        ]

    def initialize_db(self, db_name):
        """
        Initialize the database using the existing SQLiteDB class.
        """
        try:
            db = SQLiteDB(db_name)
            db.initialize_db()
            logger.info(f"✅ Database '{db_name}' initialized successfully.")
        except Exception as e:
            logger.error(f"❌ Error initializing database '{db_name}': {e}")
            sys.exit(1)

    def populate_data(self):
        """
        Run the population scripts after databases are initialized.
        """
        try:
            logger.info("📥 Starting data population...")

            # Populate tables, instruments, and indicators
            PopulateInstrumentData().run()
            PopulateIndicatorData().run()
            PopulateTableData().run_all()

            logger.info("✅ Data population completed successfully.")
        except Exception as e:
            logger.error(f"❌ Error populating data: {e}")
            sys.exit(1)

    def setup(self, specific_db=None):
        """
        Runs the full database setup process. Can initialize a single database or all databases.
        """
        if specific_db:
            if specific_db in self.databases:
                logger.info(f"🔄 Initializing database '{specific_db}'...")
                self.initialize_db(specific_db)
            else:
                logger.error(f"❌ Database '{specific_db}' is not recognized.")
                sys.exit(1)
        else:
            # Initialize all databases
            for db_name in self.databases:
                print(f"🔄 Initializing database '{db_name}'...")
                self.initialize_db(db_name)

            # Populate data after initializing databases
            self.populate_data()


# Running from command-line
if __name__ == "__main__":
    db_setup = PopulateSQLTables()

    if len(sys.argv) > 1:
        db_setup.setup(specific_db=sys.argv[1])
    else:
        db_setup.setup()
