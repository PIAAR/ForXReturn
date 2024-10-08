import os
import datetime
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
            'historical_data.db': 'schema_historical_data.sql',
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
            self.close_connection()

    def initialize_db(self, schema_sql=None):
        """
        Initialize the database using the appropriate schema or provided schema_sql.
        :param schema_sql: The SQL script for creating the necessary tables (optional).
        """
        if schema_sql:
            self.execute_script(schema_sql)
        else:
            # Load the default schema for the current database
            if schema_sql := self.load_schema():
                self.execute_script(schema_sql)

    def get_instrument_id(self, instrument_name):
        """
        Retrieve the instrument ID from the `instruments` table based on the instrument name.
        :param instrument_name: The name of the instrument (e.g., 'EUR_USD').
        :return: Instrument ID if found, otherwise None.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = "SELECT id FROM instruments WHERE name = ?"
            cursor.execute(query, (instrument_name,))
            result = cursor.fetchone()

            if result:
                return result[0]
            else:
                logger.error(f"Instrument {instrument_name} not found.")
                return None
        except Exception as e:
            logger.error(f"Error fetching instrument ID: {e}")
            return None
        finally:
            self.close_connection()

    def get_indicator_id(self, indicator_name):
        """
        Retrieve the indicator ID from the `indicators` table based on the indicator name.
        :param indicator_name: The name of the indicator (e.g., 'SMA').
        :return: Indicator ID if found, otherwise None.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = "SELECT id FROM indicators WHERE name = ?"
            cursor.execute(query, (indicator_name,))
            result = cursor.fetchone()

            if result:
                return result[0]
            else:
                logger.error(f"Indicator {indicator_name} not found.")
                return None
        except Exception as e:
            logger.error(f"Error fetching indicator ID: {e}")
            return None
        finally:
            self.close_connection()

    def get_indicator_parameters(self, indicator_id):
        """
        Retrieve the parameters for a given indicator from the database.

        :param indicator_id: The ID of the indicator to retrieve parameters for.
        :return: A dictionary of parameter names and their values.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                SELECT parameter_name, parameter_value
                FROM indicator_parameters
                WHERE indicator_id = ?
            """
            cursor.execute(query, (indicator_id,))
            rows = cursor.fetchall()

            # Convert the result into a dictionary
            parameters = {row[0]: row[1] for row in rows}

            logger.info(f"Retrieved parameters for indicator ID {indicator_id}.")
            return parameters
        except Exception as e:
            logger.error(f"Error retrieving indicator parameters: {e}")
            return {}
        finally:
            self.close_connection()

    def get_indicator_results(self, indicator_id):
        """
        Retrieve the results for a given indicator from the database.

        :param indicator_id: The ID of the indicator to retrieve results for.
        :return: A list of dictionaries containing result data.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                SELECT timestamp, parameter_name, result_value
                FROM indicator_results
                WHERE indicator_id = ?
            """
            cursor.execute(query, (indicator_id,))
            rows = cursor.fetchall()

            # Convert the result into a list of dictionaries
            results = [{"timestamp": row[0], "parameter_name": row[1], "result_value": row[2]} for row in rows]

            logger.info(f"Retrieved results for indicator ID {indicator_id}.")
            return results
        except Exception as e:
            logger.error(f"Error retrieving indicator results: {e}")
            return []
        finally:
            self.close_connection()

    def add_indicator(self, name, indicator_type):
        """
        Add a new indicator to the indicators table.
        :param name: Name of the indicator (e.g., 'SMA').
        :param indicator_type: The type of indicator (e.g., 'volatility', 'trend', etc.).
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                INSERT INTO indicators (name, type, created_at)
                VALUES (?, ?, ?)
            """
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(query, (name, indicator_type, timestamp))
            self.conn.commit()

            logger.info(f"Indicator '{name}' of type '{indicator_type}' added to the database.")
        except Exception as e:
            logger.error(f"Error adding indicator: {e}")
        finally:
            self.close_connection()

    def add_indicator_parameters(self, indicator_id, params):
            """
            Add or update indicator parameters in the database.
            :param indicator_id: The ID of the indicator.
            :param params: A dictionary of parameters to be added or updated (e.g., {'period': 14, 'multiplier': 1.5}).
            """
            try:
                self._connect_db()
                cursor = self.conn.cursor()

                # Insert or update each parameter for the indicator
                query = """
                    INSERT INTO indicator_parameters (indicator_id, parameter_name, parameter_value, last_updated)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(indicator_id, parameter_name) DO UPDATE SET
                    parameter_value = excluded.parameter_value,
                    last_updated = excluded.last_updated
                """
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for param_name, param_value in params.items():
                    cursor.execute(query, (indicator_id, param_name, param_value, timestamp))

                self.conn.commit()
                logger.info(f"Parameters for indicator ID {indicator_id} updated: {params}")
            except Exception as e:
                logger.error(f"Error updating indicator parameters: {e}")
            finally:
                self.close_connection()

    def add_indicator_results(self, indicator_id, timestamp, result_name, result_value):
        """
        Insert indicator results into the indicator_results table.
        :param indicator_id: The ID of the indicator (from the indicators table).
        :param timestamp: The timestamp for when the result was calculated.
        :param result_name: The name of the result (e.g., 'atr_value').
        :param result_value: The value of the result (e.g., 1.23).
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                INSERT INTO indicator_results (indicator_id, timestamp, result_name, result_value)
                VALUES (?, ?, ?, ?)
            """

            cursor.execute(query, (indicator_id, timestamp, result_name, result_value))
            self.conn.commit()
            logger.info(f"Indicator result added for indicator ID {indicator_id} at {timestamp}")
        except Exception as e:
            logger.error(f"Error adding indicator result: {e}")
        finally:
            self.close_connection()

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

    def add_optimized_params(self, instrument_id, indicator_id, params):
        """
        Insert optimized parameters into the optimized_parameters table.
        :param instrument_id: The instrument ID (from instruments table).
        :param indicator_id: The indicator ID (from indicators table).
        :param params: A dictionary of the optimized parameters.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                    INSERT INTO optimized_parameters (instrument_id, indicator_id, parameter_name, parameter_value, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                    """
            for param_name, param_value in params.items():
                timestamp = datetime.datetime.now().isoformat()
                cursor.execute(query, (instrument_id, indicator_id, param_name, param_value, timestamp))

            self.conn.commit()
            logger.info("Optimized parameters added to the database.")
        except Exception as e:
            logger.error(f"Failed to insert optimized parameters: {e}")
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
        parameters = []
        if where_clause:
            where_conditions = ' AND '.join([f"{key} = ?" for key in where_clause])
            query += f" WHERE {where_conditions}"
            parameters = list(where_clause.values())

        cursor.execute(query, parameters)

        results = cursor.fetchall()
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
        except Exception as e:
            logger.error(f"Error updating record in {table_name}: {e}")
        finally:
            self.close_connection()

    def update_indicator_parameters(self, indicator_id, parameters):
        """
        Update indicator parameters in the database for a given indicator.
        
        :param indicator_id: The ID of the indicator to update parameters for.
        :param parameters: A dictionary of parameter names and values to update.
        """
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                    UPDATE indicator_parameters
                    SET parameter_value = ?
                    WHERE indicator_id = ? AND parameter_name = ?
                """
            for param_name, param_value in parameters.items():
                cursor.execute(query, (param_value, indicator_id, param_name))

            self.conn.commit()
            logger.info(f"Updated indicator parameters for indicator ID {indicator_id}.")
        except Exception as e:
            logger.error(f"Error updating indicator parameters: {e}")
        finally:
            self.close_connection()


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
