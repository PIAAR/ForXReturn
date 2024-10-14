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
        :parameter db_name: The name of the database (e.g., indicators.db, configuration.db).
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

        schema_file = schema_map.get(os.path.basename(self.db_path))

        if not schema_file:
            logger.error(f"No schema file defined for {self.db_path}")
            raise FileNotFoundError(f"No schema file defined for {self.db_path}")
        
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data/models/', schema_file)

        if not os.path.isfile(schema_path):
            logger.error(f"Schema file not found: {schema_path}")
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

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
        if schema_sql:
            self.execute_script(schema_sql)
        else:
            if schema_sql := self.load_schema():
                self.execute_script(schema_sql)

    def get_instrument_id(self, instrument_name):
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
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = "SELECT id FROM indicators WHERE name = ?"
            cursor.execute(query, (indicator_name,))
            result = cursor.fetchone()

            if result:
                logger.info(f"Indicator '{indicator_name}' found with ID: {result[0]}")
                return result[0]
            else:
                logger.error(f"Indicator '{indicator_name}' not found in the database.")
                return None
        except Exception as e:
            logger.error(f"Error fetching indicator ID for '{indicator_name}': {e}")
            return None
        finally:
            self.close_connection()

    def get_indicator_parameters(self, indicator_id):
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

            parameters = {row[0]: row[1] for row in rows}
            logger.info(f"Retrieved parameters for indicator ID {indicator_id}: {parameters}")
            return parameters
        except Exception as e:
            logger.error(f"Error retrieving indicator parameters: {e}")
            return {}
        finally:
            self.close_connection()

    def get_indicator_results(self, indicator_id):
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                SELECT timestamp, parameter_name, parameter_value 
                FROM instrument_indicator_results 
                WHERE indicator_id = ?
            """
            cursor.execute(query, (indicator_id,))
            rows = cursor.fetchall()

            results = [{"timestamp": row[0], "parameter_name": row[1], "result_value": row[2]} for row in rows]

            logger.info(f"Retrieved results for indicator ID {indicator_id}.")
            return results
        except Exception as e:
            logger.error(f"Error retrieving indicator results: {e}")
            return []
        finally:
            self.close_connection()

    def add_indicator(self, name, indicator_type):
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                INSERT INTO indicators (name, type)
                VALUES (?, ?)
            """
            logger.info(f"Attempting to insert indicator '{name}' of type '{indicator_type}'.")
            
            cursor.execute(query, (name, indicator_type))
            self.conn.commit()

            logger.info(f"Indicator '{name}' of type '{indicator_type}' added to the database.")
        except Exception as e:
            logger.error(f"Error adding indicator '{name}': {e}")
        finally:
            self.close_connection()

    def add_indicator_parameters(self, indicator_id, parameters):
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                INSERT INTO indicator_parameters (indicator_id, parameter_name, parameter_value, last_update)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(indicator_id, parameter_name) DO UPDATE SET
                parameter_value = excluded.parameter_value,
                last_update = excluded.last_update
            """
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for parameter_name, parameter_value in parameters.items():
                logger.info(f"Adding parameter '{parameter_name}' with value {parameter_value} for indicator ID {indicator_id}.")
                cursor.execute(query, (indicator_id, parameter_name, parameter_value, timestamp))

            self.conn.commit()
            logger.info(f"Parameters for indicator ID {indicator_id} updated: {parameters}")
        except Exception as e:
            logger.error(f"Error updating indicator parameters for indicator ID {indicator_id}: {e}")
        finally:
            self.close_connection()

    def add_indicator_results(self, indicator_id, timestamp, parameter_name, parameter_value):
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            parameter_id = self.get_parameter_id(indicator_id, parameter_name)
            if parameter_id is None:
                raise ValueError(f"Parameter {parameter_name} not found for indicator {indicator_id}.")

            query = """
                INSERT INTO instrument_indicator_results 
                (instrument_id, indicator_id, parameter_id, parameter_name, parameter_value, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, (None, indicator_id, parameter_id, parameter_name, parameter_value, timestamp))  # instrument_id to be added later
            self.conn.commit()
            logger.info(f"Indicator result added for indicator ID {indicator_id} at {timestamp}")
        except Exception as e:
            logger.error(f"Error adding indicator result: {e}")
        finally:
            self.close_connection()

    def get_parameter_id(self, indicator_id, parameter_name):
        try:
            self._connect_db()
            cursor = self.conn.cursor()
            query = """
                SELECT id
                FROM indicator_parameters
                WHERE indicator_id = ? AND parameter_name = ?
            """
            cursor.execute(query, (indicator_id, parameter_name))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                logger.error(f"Parameter {parameter_name} not found for indicator {indicator_id}.")
                return None
        except Exception as e:
            logger.error(f"Error fetching parameter ID for {parameter_name}: {e}")
            return None
        finally:
            self.close_connection()

    def add_record(self, table_name, data):
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

    def add_optimized_parameters(self, instrument_id, indicator_id, parameters):
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                    INSERT INTO optimized_parameters (instrument_id, indicator_id, parameter_name, parameter_value, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                    """
            for parameter_name, parameter_value in parameters.items():
                timestamp = datetime.datetime.now().isoformat()
                cursor.execute(query, (instrument_id, indicator_id, parameter_name, parameter_value, timestamp))

            self.conn.commit()
            logger.info("Optimized parameters added to the database.")
        except Exception as e:
            logger.error(f"Failed to insert optimized parameters: {e}")
        finally:
            self.close_connection()

    def fetch_records(self, table_name, where_clause=None):
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
        try:
            self._connect_db()
            cursor = self.conn.cursor()

            query = """
                    UPDATE indicator_parameters
                    SET parameter_value = ?, last_update = ?
                    WHERE indicator_id = ? AND parameter_name = ?
                """
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for parameter_name, parameter_value in parameters.items():
                cursor.execute(query, (parameter_value, timestamp, indicator_id, parameter_name))

            self.conn.commit()
            logger.info(f"Updated indicator parameters for indicator ID {indicator_id}.")
        except Exception as e:
            logger.error(f"Error updating indicator parameters: {e}")
        finally:
            self.close_connection()

    def delete_records(self, table_name, where_clause):
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
