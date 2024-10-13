-- Table for storing basic indicator information
CREATE TABLE IF NOT EXISTS indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,  -- Name of the indicator (e.g., "RSI")
    type TEXT NOT NULL   -- Type of the indicator (e.g., "momentum", "volatility")
);

-- Table for storing indicator parameter definitions
CREATE TABLE IF NOT EXISTS indicator_parameters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id INTEGER NOT NULL,  -- Foreign key to indicators table
    parameter_name TEXT NOT NULL,       -- Name of the parameter (e.g., "period", "multiplier")
    parameter_type TEXT NOT NULL,       -- Type of the parameter (e.g., "integer", "float", "boolean")
    default_value TEXT,             -- Default value for the parameter (optional)
    FOREIGN KEY(indicator_id) REFERENCES indicators(id) ON DELETE CASCADE
);

-- Table for storing indicator results per instrument and timestamp
CREATE TABLE IF NOT EXISTS instrument_indicator_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,     -- Foreign key to instruments table
    indicator_id INTEGER NOT NULL,      -- Foreign key to indicators table
    parameter_id INTEGER NOT NULL,          -- Foreign key to indicator_parameters table
    parameter_name TEXT, -- Foreign key to indicator_parameters table
    parameter_value REAL NOT NULL,          -- Value of the parameter (calculated or used)
    timestamp TEXT NOT NULL,            -- When the result was recorded
    FOREIGN KEY(instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
    FOREIGN KEY(indicator_id) REFERENCES indicators(id) ON DELETE CASCADE,
    FOREIGN KEY(parameter_id) REFERENCES indicator_parameters(id) ON DELETE CASCADE
);

-- Create index for instrument lookups in indicator results
CREATE INDEX idx_instrument_indicator_instrument_id ON instrument_indicator_results (instrument_id);
