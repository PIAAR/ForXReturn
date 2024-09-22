import os
import sqlite3
from logs.log_manager import LogManager  # Import the LogManager class

# Configure logging
logger = LogManager('sqlite_db_logs').get_logger()

class SQLiteDB:
    def __init__(self, db_name="indicators.db"):
        """
        Initialize the SQLiteDB class with the provided database name.

        :param db_name: The name of the SQLite database file (default: "indicators.db").
        """
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../databases", db_name)
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self._connect_db()
        logger.info(f"Database initialized at {self.db_path}")

    def _connect_db(self):
        """
        Establish a connection to the SQLite database.
        If the connection is already open, return the current connection.
        """
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connected to the database: {self.db_path}")
        return self.conn

    def close_connection(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")

    def execute_script(self, schema_sql):
        """
        Execute a SQL schema script to set up the database structure.

        :param schema_sql: SQL schema script to be executed.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.executescript(schema_sql)
            self.conn.commit()
            logger.info("SQL schema script executed successfully.")
        except sqlite3.Error as e:
            logger.error(f"Error executing schema script: {e}")
        finally:
            self.close_connection()

    def initialize_db(self, schema_sql=None):
        """
        Initialize the SQLite database with a schema.

        :param schema_sql: Optional SQL schema string. If None, the schema will be loaded from a file.
        """
        if not schema_sql:
            schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../repositories/schema.sql')
            if not os.path.isfile(schema_path):
                logger.error(f"Schema file not found: {schema_path}")
                raise FileNotFoundError(f"Schema file not found: {schema_path}")

            with open(schema_path, 'r') as f:
                schema_sql = f.read()
                logger.info(f"Schema loaded from {schema_path}")

        self.execute_script(schema_sql)

    def add_indicator(self, indicator_name, indicator_type):
        """
        Adds a new indicator to the indicators table.
        :param indicator_name: The name of the indicator.
        :param indicator_type: The type of the indicator (e.g., 'momentum').
        :return: The id of the added indicator.
        """
        try:
            self.conn = sqlite3.connect(db_path)
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO indicators (name, type) VALUES (?, ?)",
                (indicator_name, indicator_type)
            )
            self.conn.commit()
            indicator_id = cursor.lastrowid
            self.logger.info(f"Added indicator: {indicator_name} with ID {indicator_id}")
            return indicator_id
        except sqlite3.Error as e:
            self.logger.error(f"Error adding indicator: {e}")
            return None
        finally:
            self.conn.close()

    def add_indicator_results(self, indicator_id, timestamp, param_name, param_value):
        """
        Insert the calculated indicator results into the 'indicator_results' table.

        :param indicator_id: ID of the indicator.
        :param timestamp: Time when the result was calculated.
        :param param_name: The name of the parameter (e.g., 'upper_band').
        :param param_value: The value of the parameter.
        """
        query = """
        INSERT INTO indicator_results (indicator_id, time, key, value)
        VALUES (?, ?, ?, ?)
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.execute(query, (indicator_id, timestamp, param_name, param_value))
            self.conn.commit()
            logger.info(f"Inserted result for indicator ID {indicator_id} at {timestamp}")
        except sqlite3.Error as e:
            logger.error(f"Error inserting indicator results: {e}")
        finally:
            self.close_connection()

    def get_indicator_id(self, indicator_name):
        """
        Retrieve the ID of an indicator from the 'indicators' table. 
        If the indicator does not exist, insert it and return the new ID.

        :param indicator_name: Name of the indicator (e.g., 'RSI', 'MACD').
        :return: The ID of the indicator.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM indicators WHERE name = ?", (indicator_name,))
            if row := cursor.fetchone():
                logger.info(f"Found indicator ID {row[0]} for {indicator_name}")
                return row[0]
            cursor.execute("INSERT INTO indicators (name, type) VALUES (?, ?)", (indicator_name, "momentum"))
            self.conn.commit()
            logger.info(f"Inserted new indicator {indicator_name} with ID {cursor.lastrowid}")
            return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error retrieving or inserting indicator ID: {e}")
            return None

    def add_indicator_parameters(self, indicator_id, parameters):
        """
        Add parameters for a given indicator.

        :param indicator_id: ID of the indicator.
        :param parameters: Dictionary of key-value pairs representing parameters.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            for key, value in parameters.items():
                cursor.execute(
                    "INSERT INTO indicator_parameters (indicator_id, key, value) VALUES (?, ?, ?)",
                    (indicator_id, key, value),
                )
            self.conn.commit()
            logger.info(f"Added parameters for indicator ID {indicator_id}")
        except sqlite3.Error as e:
            logger.error(f"Error adding indicator parameters: {e}")

    def get_indicator_parameters(self, indicator_id):
        """
        Get parameters for a given indicator.

        :param indicator_id: ID of the indicator.
        :return: Dictionary of parameters.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT key, value FROM indicator_parameters WHERE indicator_id = ?",
                (indicator_id,),
            )
            params = cursor.fetchall()
            logger.info(f"Fetched parameters for indicator ID {indicator_id}")
            return dict(params)
        except sqlite3.Error as e:
            logger.error(f"Error fetching indicator parameters: {e}")
            return {}

    def update_indicator_parameters(self, indicator_id, parameters):
        """
        Update parameters for a given indicator.

        :param indicator_id: ID of the indicator.
        :param parameters: Dictionary of key-value pairs representing updated parameters.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            for key, value in parameters.items():
                cursor.execute(
                    "REPLACE INTO indicator_parameters (indicator_id, key, value) VALUES (?, ?, ?)",
                    (indicator_id, key, value),
                )
            self.conn.commit()
            logger.info(f"Updated parameters for indicator ID {indicator_id}")
        except sqlite3.Error as e:
            logger.error(f"Error updating indicator parameters: {e}")

    def get_recent_indicator_results(self, indicator_id, limit=500):
        """
        Fetch the most recent indicator results.

        :param indicator_id: ID of the indicator.
        :param limit: Number of recent results to fetch (default: 500).
        :return: List of results.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT * FROM indicator_results WHERE indicator_id = ? ORDER BY time DESC LIMIT ?",
                (indicator_id, limit),
            )
            results = cursor.fetchall()
            logger.info(f"Fetched {len(results)} recent results for indicator ID {indicator_id}")
            return results
        except sqlite3.Error as e:
            logger.error(f"Error fetching indicator results: {e}")
            return []

    def delete_old_results(self, indicator_id, keep_limit=500):
        """
        Delete older indicator results, keeping only the most recent entries.

        :param indicator_id: ID of the indicator.
        :param keep_limit: Number of recent entries to keep.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.execute(
                """
                DELETE FROM indicator_results 
                WHERE id NOT IN (
                    SELECT id FROM indicator_results 
                    WHERE indicator_id = ? 
                    ORDER BY time DESC 
                    LIMIT ?
                )""",
                (indicator_id, keep_limit),
            )
            self.conn.commit()
            logger.info(f"Deleted old results for indicator ID {indicator_id}, keeping the most recent {keep_limit}")
        except sqlite3.Error as e:
            logger.error(f"Error deleting old indicator results: {e}")


# Example usage
if __name__ == "__main__":
    db = SQLiteDB("indicators.db")

    # Example operations for demonstration purposes
    # Add indicator, parameters, and results
    db.add_indicator_parameters(1, {"period": 20, "upper_band": 2.0, "lower_band": -2.0})
    db.add_indicator_results(1, "2024-09-08T14:00:00Z", {"upper_band": 1.25, "lower_band": 1.20})
    
    # Fetch and print recent results
    results = db.get_recent_indicator_results(1)
    print(results)

    # Close the connection
    db.close_connection()
