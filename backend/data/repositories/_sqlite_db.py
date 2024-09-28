import os
import sqlite3
from backend.logs.log_manager import LogManager  # Import the LogManager class

# Configure logging
logger = LogManager('sqlite_db_logs').get_logger()

class SQLiteDB:
    def __init__(self, db_name):
        """
        Initializes the SQLiteDB class with the specified database.
        :param db_name: The name of the database (e.g., indicators.db, configuration.db).
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(base_dir, "databases", db_name)
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

    def load_schema(self):
        """
        Load the schema dynamically based on the database name.
        """
        schema_map = {
            'indicators.db': 'schema_indicators.sql',
            'instruments.db': 'schema_instruments.sql',
            'optimizer.db': 'schema_optimizer.sql',
            'configuration.db': 'schema_configuration.sql',
            'user.db': 'schema_user.sql'
        }

        # Get the corresponding schema file
        schema_file = schema_map.get(os.path.basename(self.db_path))

        if not schema_file:
            logger.error(f"No schema file defined for {self.db_path}")
            raise FileNotFoundError(f"No schema file defined for {self.db_path}")
        
        # Construct the path to the schema file in the models directory
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data/models/', schema_file)

        # Check if the schema file exists
        if not os.path.isfile(schema_path):
            logger.error(f"Schema file not found: {schema_path}")
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        # Load the schema file
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
            logger.info(f"Schema loaded from {schema_path}")
            return schema_sql

    def execute_script(self, schema_sql):
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.executescript(schema_sql)
            self.conn.commit()
            logger.info("SQL schema script executed successfully.")
        except Exception as e:
            logger.error(f"Error executing schema script: {e}")
        finally:
            self.close_connection()

    def initialize_db(self):
        """
        Initialize the database using the appropriate schema.
        """
        if schema_sql := self.load_schema():
            self.execute_script(schema_sql)

    def add_record(self, table_name, data):
        """
        Insert a record dynamically into the specified table.
        :param table_name: The name of the table.
        :param data: A dictionary with column names as keys and corresponding values.
        """
        try:
            return self.add_record_to_the_database(data, table_name)
        except Exception as e:
            logger.error(f"Error inserting record into {table_name}: {e}")
            return None
        finally:
            self.close_connection()

    def add_record_to_the_database(self, data, table_name):
        self._connect_db()
        cursor = self.conn.cursor()

        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, list(data.values()))
        self.conn.commit()

        logger.info(f"Record added to {table_name}")
        return cursor.lastrowid

    def update_record(self, table_name, data, where_clause):
        """
        Update records dynamically in the specified table.
        :param table_name: The name of the table.
        :param data: A dictionary of column names and values to be updated.
        :param where_clause: A dictionary for the WHERE clause to specify which records to update.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            set_clause = ', '.join([f"{key} = ?" for key in data])
            where_conditions = ' AND '.join([f"{key} = ?" for key in where_clause])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_conditions}"

            cursor.execute(query, list(data.values()) + list(where_clause.values()))
            self.conn.commit()

            logger.info(f"Record(s) updated in {table_name}")
        except Exception as e:
            logger.error(f"Error updating record in {table_name}: {e}")
        finally:
            self.close_connection()

    def fetch_records(self, table_name, where_clause=None):
        """
        Retrieve records dynamically from the specified table.
        :param table_name: The name of the table.
        :param where_clause: A dictionary for the WHERE clause to filter results (optional).
        :return: Fetched records or an empty list.
        """
        try:
            return self.fetch_from_the_database(table_name, where_clause)
        except Exception as e:
            logger.error(f"Error fetching records from {table_name}: {e}")
            return []
        finally:
            self.close_connection()

    def fetch_from_the_database(self, table_name, where_clause):
        self._connect_db()
        cursor = self.conn.cursor()

        query = f"SELECT * FROM {table_name}"
        if where_clause:
            where_conditions = ' AND '.join([f"{key} = ?" for key in where_clause])
            query += f" WHERE {where_conditions}"
            cursor.execute(query, list(where_clause.values()))
        else:
            cursor.execute(query)

        results = cursor.fetchall()
        logger.info(f"Fetched records from {table_name}")
        return results

    def delete_records(self, table_name, where_clause):
        """
        Delete records dynamically from the specified table.
        :param table_name: The name of the table.
        :param where_clause: A dictionary for the WHERE clause to specify which records to delete.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            where_conditions = ' AND '.join([f"{key} = ?" for key in where_clause])
            query = f"DELETE FROM {table_name} WHERE {where_conditions}"
            cursor.execute(query, list(where_clause.values()))
            self.conn.commit()

            logger.info(f"Record(s) deleted from {table_name}")
        except Exception as e:
            logger.error(f"Error deleting record(s) from {table_name}: {e}")
        finally:
            self.close_connection()

if __name__ == "__main__":
    # Connect to indicators.db and perform operations
    indicators_db = SQLiteDB("indicators.db")
    indicators_db.initialize_db()  # Initialize the database if needed
    # Insert a record into the indicators table
    # Example: Add the RSI indicator
    indicators_db.add_record("indicators", {"name": "RSI", "type": "momentum"})
    # ===========================================================================================================#

    # Connect to the instruments table and perform operations
    instruments_db = SQLiteDB("instruments.db")
    instruments_db.initialize_db()  # Initialize the database if needed
    # Insert a record into the instruments table
    # Example: Add the EUR_USD instrument with opening and closing dates
    instruments_db.add_record("instruments", {"name": "EUR_USD", "opening_time": "00:00:00", "closing_time": "23:59:59"})
    # ===========================================================================================================#

    # Connect to configuration.db and perform operations
    config_db = SQLiteDB("configuration.db")
    config_db.initialize_db()  # Initialize the database if needed
    # Insert a profile record into the profiles table
    config_db.add_record("profiles", {"profile_name": "Default", "last_update": "2024-01-01"})
    # ===========================================================================================================#
    
    # Connect to optimizer.db and perform operations
    optimizer_db = SQLiteDB("optimizer.db")
    optimizer_db.initialize_db()  # Initialize the database if needed

    if instrument_records := instruments_db.fetch_records(
        "instruments", {"name": "EUR_USD"}
    ):
        instrument_id = instrument_records[0][0]
        print(f"Instrument ID for EUR_USD: {instrument_id}")
    else:
        print("Error: EUR_USD not found in the instruments table.")
        exit(1)  # or handle this case as needed

    if indicator_records := indicators_db.fetch_records(
        "indicators", {"name": "RSI"}
    ):
        indicator_id = indicator_records[0][0]
        print(f"Indicator ID for RSI: {indicator_id}")
    else:
        print("Error: RSI not found in the indicators table.")
        exit(1)  # or handle this case as needed

    # Insert optimized parameters for the RSI indicator on EUR_USD
    optimizer_db.add_record("optimized_parameters", {"instrument_id": instrument_id, "indicator_id": indicator_id, "parameter_name": "ATR", "parameter_value": 0.02, "timestamp": "2023-09-25 12:00:00"})

    # Insert optimization performance results (assuming the optimization ID is known)
    optimizer_db.add_record("optimization_results", {"optimization_id": 1, "sharpe_ratio": 1.5, "total_return": 25.3, "max_drawdown": -5.6, "win_rate": 60, "timestamp": "2023-09-25 12:30:00"})
    # ===========================================================================================================#
    # Connect to user.db and perform operations
    user_db = SQLiteDB("user.db")
    user_db.initialize_db()  # Initialize the database if needed

    # Insert a session record into the session table
    user_db.add_record("session", {"user_id": 1, "session_token": "abc123"})
