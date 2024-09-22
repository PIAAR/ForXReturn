import unittest
from data.repositories.sqlite3 import SQLiteDB

class TestSQLiteDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the test class with an in-memory database."""
        cls.db = SQLiteDB(':memory:')  # Use in-memory database for testing

        # Schema SQL for testing
        schema_sql = """
        CREATE TABLE IF NOT EXISTS indicators (
            id INTEGER PRIMARY KEY,
            name TEXT,
            type TEXT
        );
        
        CREATE TABLE IF NOT EXISTS indicator_parameters (
            indicator_id INTEGER,
            key TEXT,
            value REAL,
            PRIMARY KEY (indicator_id, key),
            FOREIGN KEY (indicator_id) REFERENCES indicators (id)
        );
        
        CREATE TABLE IF NOT EXISTS indicator_results (
            indicator_id INTEGER,
            time TEXT,
            key TEXT,
            value REAL,
            FOREIGN KEY (indicator_id) REFERENCES indicators (id)
        );
        """
        cls.db.initialize_db(schema_sql)

        # Add a sample indicator to the database for testing
        cls.db.add_indicator("ATR", "volatility")

    @classmethod
    def tearDownClass(cls):
        """Close the database connection after all tests."""
        cls.db.close_connection()

    def test_add_indicator_results(self):
        """Test adding an indicator result."""
        self.db.add_indicator_results(1, "2024-01-01T00:00:00Z", "atr_value", 1.23)
        results = self.db.get_recent_indicator_results(1)
        self.assertGreaterEqual(len(results), 1)
        self.assertEqual(results[0][1], "2024-01-01T00:00:00Z")  # Check the time value
        self.assertEqual(results[0][3], 1.23)  # Check the ATR value

    def test_add_indicator_parameters(self):
        """Test adding parameters to an indicator."""
        self.db.add_indicator_parameters(1, {"period": 14, "multiplier": 1.5})
        params = self.db.get_indicator_parameters(1)
        self.assertEqual(params["period"], 14)
        self.assertEqual(params["multiplier"], 1.5)

    def test_update_indicator_parameters(self):
        """Test updating an indicator parameter."""
        self.db.update_indicator_parameters(1, {"period": 20, "multiplier": 2.0})
        params = self.db.get_indicator_parameters(1)
        self.assertEqual(params["period"], 20)
        self.assertEqual(params["multiplier"], 2.0)

    def test_add_bollinger_band_parameters(self):
        """Test adding parameters for Bollinger Bands indicator."""
        self.db.add_indicator("Bollinger Bands", "volatility")
        self.db.add_indicator_parameters(2, {"period": 20, "upper_band": 2.0, "lower_band": -2.0})
        params = self.db.get_indicator_parameters(2)
        self.assertEqual(params["period"], 20)
        self.assertEqual(params["upper_band"], 2.0)
        self.assertEqual(params["lower_band"], -2.0)

if __name__ == '__main__':
    unittest.main()
