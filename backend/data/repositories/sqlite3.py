import os
import sqlite3
from logs.log_manager import LogManager  # Import the LogManager class

# Configure logging
logger = LogManager('sqlite_db_logs').get_logger()

class SQLiteDB:
    def __init__(self, db_name="indicators.db"):
        if db_name != ":memory:":
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, db_name)
        else:
            self.db_path = db_name
        self.conn = None
        logger.info(f"Database initialized at {self.db_path}")

    def _connect_db(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connected to the database: {self.db_path}")
        return self.conn

    def close_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")

    def execute_script(self, schema_sql):
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
        if not schema_sql:
            schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
            if not os.path.isfile(schema_path):
                logger.error(f"Schema file not found: {schema_path}")
                raise FileNotFoundError(f"Schema file not found: {schema_path}")

            with open(schema_path, 'r') as f:
                schema_sql = f.read()
                logger.info(f"Schema loaded from {schema_path}")

        self.execute_script(schema_sql)

    def add_indicator(self, indicator_name, indicator_type):
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO indicators (name, type) VALUES (?, ?)",
                (indicator_name, indicator_type)
            )
            self.conn.commit()
            indicator_id = cursor.lastrowid
            logger.info(f"Added indicator: {indicator_name} with ID {indicator_id}")
            return indicator_id
        except sqlite3.Error as e:
            logger.error(f"Error adding indicator: {e}")
            return None
        finally:
            self.close_connection()

    def add_indicator_results(self, indicator_id, timestamp, param_name, param_value):
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

    def add_indicator_parameters(self, indicator_id):
        """
        Retrieve parameters for a given indicator.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT key, value FROM indicator_parameters WHERE indicator_id = ?",
                (indicator_id,)
            )
            params = {row[0]: row[1] for row in cursor.fetchall()}
            logger.info(f"Fetched parameters for indicator ID {indicator_id}")
            return params
        except sqlite3.Error as e:
            logger.error(f"Error fetching indicator parameters: {e}")
            return {}

    def update_indicator_parameters(self, indicator_id, parameters):
        """
        Update parameters for a given indicator.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            for key, value in parameters.items():
                cursor.execute(
                    "UPDATE indicator_parameters SET value = ? WHERE indicator_id = ? AND key = ?",
                    (value, indicator_id, key)
                )
            self.conn.commit()
            logger.info(f"Updated parameters for indicator ID {indicator_id}")
        except sqlite3.Error as e:
            logger.error(f"Error updating indicator parameters: {e}")

