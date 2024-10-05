import os
import sqlite3
from logs.log_manager import LogManager  # Import the LogManager class

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
        if not schema_sql or not isinstance(schema_sql, str):
            logger.error("Invalid SQL script provided.")
            return
    
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            cursor.executescript(schema_sql)
            self.conn.commit()
            logger.info("SQL schema script executed successfully.")
        except Exception as e:
            logger.error(f"Error executing schema script: {e}")
        finally:
            logger.info(f"Closing the connection for self: {self}")  # Check what `self` is here
            self.close_connection()

    def initialize_db(self):
        """
        Initialize the database using the appropriate schema.
        """
        if schema_sql := self.load_schema():
            self.execute_script(schema_sql)

    def get_instrument_id(self, instrument_name):
        """
        Retrieve the instrument_id for the given instrument name.
        If the instrument doesn't exist, it will be inserted.
        """
        query = "SELECT id FROM instruments WHERE name = ?"
        
        # Execute the query to find the instrument_id by name
        self._connect_db()
        cursor = self.conn.cursor()
        cursor.execute(query, (instrument_name,))
        result = cursor.fetchone()
        
        # If no result, insert the instrument
        if not result:
            instrument_id = self.add_record_to_the_database({"name": instrument_name}, "instruments")
            logger.info(f"Inserted new instrument: {instrument_name} with ID {instrument_id}")
            return instrument_id
        return result[0]  # Return the existing instrument_id

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
        parameters = []
        if where_clause:
            where_conditions = ' AND '.join([f"{key} = ?" for key in where_clause])
            query += f" WHERE {where_conditions}"
            parameters = list(where_clause.values())
            
        cursor.execute(query, parameters)
        # print(f"Executing query: {query}")  # Debugging line
        # print(f"With parameters: {parameters}")  # Debugging line

        results = cursor.fetchall()
        # Convert the sqlite3.Row objects into dictionaries for easy access
        # records = [dict(row) for row in results]

        logger.info(f"Fetched {len(results)} records from {table_name}")
        return results

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
        except Except