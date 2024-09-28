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
    FOREIGN KEY(indicator_id) REFERENCES indicators(id) ON DELETE CASCADE
);

-- Table for storing indicator results per instrument and timestamp
CREATE TABLE IF NOT EXISTS instrument_indicator_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,  -- Foreign key to instruments table
    indicator_id INTEGER NOT NULL,   -- Foreign key to indicators table
    parameter_name TEXT NOT NULL,
    parameter_value REAL NOT NULL,
    timestamp TEXT NOT NULL,
    FOREIGN KEY(instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
    FOREIGN KEY(indicator_id) REFERENCES indicators(id) ON DELETE CASCADE
);
