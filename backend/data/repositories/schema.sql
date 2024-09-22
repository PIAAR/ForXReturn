-- schema.sql

-- Table for storing basic indicator information
CREATE TABLE IF NOT EXISTS indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL
);

-- Table for storing indicator parameters
CREATE TABLE IF NOT EXISTS indicator_parameters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY(indicator_id) REFERENCES indicators(id)
);

-- New table for storing indicator results per instrument and month
CREATE TABLE IF NOT EXISTS instrument_indicator_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    parameter_name TEXT NOT NULL,
    parameter_value REAL NOT NULL,
    timestamp TEXT NOT NULL
);
