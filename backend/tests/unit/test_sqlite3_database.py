import unittest
from backend.data.repositories._sqlite_db import SQLiteDB
from backend.logs.log_manager import LogManager

# Configure logging
log_manager = LogManager('test_sqlite3_database')
logger = log_manager.get_logger()

class TestSQLiteDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the test class with an in-memory database."""
        logger.info("Setting up the in-memory database for testing.")
        cls.db = SQLiteDB(':memory:')  # Use in-memory database for testing

        # Schema SQL for testing
        schema_sql = """
        CREATE TABLE IF NOT EXISTS indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS indicator_parameters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_id INTEGER NOT NULL,
            parameter_name TEXT NOT NULL,
            parameter_value REAL NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            FOREIGN KEY (indicator_id) REFERENCES indicators (id) ON DELETE CASCADE
        );
        """
        logger.info("Initializing the database schema.")
        cls.db.initialize_db(schema_sql)

        logger.info("Attempting to add parameters for BollingerBands (ID 4).")

        # Add parameters for BollingerBands (ID = 4)
        cls.db.add_indicator_parameters(4, {
            "macro_period": 20,
            "upper_band": 2.0,
            "lower_band": -2.0
        })
        logger.info("Parameters for BollingerBands (ID 4) have been added.")

    @classmethod
    def tearDownClass(cls):
        """Close the database connection after all tests."""
        logger.info("Tearing down the test class and closing the database connection.")
        cls.db.close_connection()

    def test_get_bollinger_band_parameters(self):
        """Test retrieving parameters for BollingerBands indicator (ID = 4)."""
        logger.info("Retrieving parameters for BollingerBands indicator (ID 4).")

        # Get the BollingerBands parameters
        parameters = self.db.get_indicator_parameters(4)

        logger.info(f"Retrieved parameters for BollingerBands: {parameters}")

        # Assert that the parameters match the expected values
        logger.info("Starting assertion for 'macro_period'.")
        if "macro_period" not in parameters:
            logger.error("Expected 'macro_period' to be in parameters but it's missing.")
        self.assertIn("macro_period", parameters, "Expected 'macro_period' to be in parameters.")
        self.assertEqual(float(parameters["macro_period"]), 20.0, "Expected 'macro_period' to be 20.")
        logger.info("Assertion for 'macro_period' passed.")
        
        logger.info("Starting assertion for 'upper_band'.")
        if "upper_band" not in parameters:
            logger.error("Expected 'upper_band' to be in parameters but it's missing.")
        self.assertIn("upper_band", parameters, "Expected 'upper_band' to be in parameters.")
        self.assertEqual(float(parameters["upper_band"]), 2.0, "Expected 'upper_band' to be 2.0.")
        logger.info("Assertion for 'upper_band' passed.")
        
        logger.info("Starting assertion for 'lower_band'.")
        if "lower_band" not in parameters:
            logger.error("Expected 'lower_band' to be in parameters but it's missing.")
        self.assertIn("lower_band", parameters, "Expected 'lower_band' to be in parameters.")
        self.assertEqual(float(parameters["lower_band"]), -2.0, "Expected 'lower_band' to be -2.0.")
        logger.info("Assertion for 'lower_band' passed.")

        logger.info("All assertions for BollingerBands parameters passed.")

if __name__ == '__main__':
    unittest.main()
