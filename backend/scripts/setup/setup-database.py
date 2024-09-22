import os
import sys
from data.repositories.sqlite3 import initialize_db
from logs.log_manager import LogManager  # Import the LogManager class

# Configure logging
logger = LogManager('database_setup').get_logger()

import os
import sqlite3

def connect_db(db_name=None):
    """Establish a connection to the SQLite database."""
    return sqlite3.connect(db_name)

def connect_commit_close(database_path, schema_sql):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.executescript(schema_sql)
    conn.commit()
    conn.close()

def initialize_db(db_type='sqlite', db_name=None):
    """
    Initializes the database based on the provided type and file name.

    :param db_type: Type of the database to create (e.g., 'sqlite').
    :param db_name: The name of the database file.
    """
    # Define the directory for databases
    databases_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data/repositories/databases')

    # Ensure the directory exists
    if not os.path.exists(databases_dir):
        os.makedirs(databases_dir)

    # Define the path for the database file
    database_path = os.path.join(databases_dir, db_name)

    # Define the path for the schema file
    schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data/repositories/schema.sql')

    if not os.path.isfile(schema_path):
        print(f"Schema file not found: {schema_path}")
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    if db_type != 'sqlite':
        raise NotImplementedError(f"Database type '{db_type}' is not supported.")
    
    connect_commit_close(database_path, schema_sql)
    print(f"SQLite database '{db_name}' initialized successfully at '{database_path}'.")

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python setup-database.py <db_type> <db_name>")
        sys.exit(1)
    
    db_type = sys.argv[1]
    db_name = sys.argv[2]
    
    initialize_db(db_type, db_name)


def main(db_name='indicators.db', db_type='sqlite'):
    """
    Main function to initialize the specified database.

    :param db_name: The name of the database file to be created.
    :param db_type: The type of database to initialize (currently supports 'sqlite').
    """
    try:
        logger.info("Starting database initialization...")
        initialize_db(db_type=db_type, db_name=db_name)
        logger.info(f"Database '{db_name}' initialized successfully.")
    except Exception as e:
        logger.error(f"An error occurred while initializing the database: {e}")
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    
    # Default database types and names
    databases = {
        'indicators': 'indicators.db',
        'optimizer': 'optimizer.db',
        'user': 'user.db',
        'configuration': 'configuration.db'
    }

    db_type = sys.argv[1] if len(sys.argv) > 1 else 'sqlite'
    db_name = sys.argv[2] if len(sys.argv) > 2 else None

    # Validate the database type
    if db_type not in ['sqlite']:
        logger.error(f"Unsupported database type: {db_type}")
        print(f"Unsupported database type: {db_type}")
        sys.exit(1)

    # Initialize specified database or all databases
    if db_name is None:
        for db_file in databases.values():
            logger.info(f"Initializing '{db_name}' database...")
            print(f"Initializing database '{db_file}'...")
            main(db_name=db_file, db_type=db_type)
    elif db_name in databases.values():
        main(db_name=db_name, db_type=db_type)
    else:
        logger.error(f"Database name '{db_name}' is not recognized.")
        print(f"Database name '{db_name}' is not recognized.")
        sys.exit(1)
