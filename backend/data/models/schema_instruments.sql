-- Table for storing instrument information
CREATE TABLE IF NOT EXISTS instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    opening_time TIME NOT NULL,
    closing_time TIME NOT NULL
);

-- Create index for quick lookup by instrument name
CREATE INDEX idx_instrument_name ON instruments (name);

-- Table for storing the states of instruments across different timeframes
CREATE TABLE IF NOT EXISTS instrument_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,  -- Foreign key to instruments table
    timeframe TEXT NOT NULL,         -- Timeframe (e.g., 'macro', 'daily', 'minute')
    state TEXT NOT NULL,             -- State (e.g., 'GREEN', 'YELLOW', 'RED')
    last_updated TEXT NOT NULL,      -- Timestamp of the last state update
    FOREIGN KEY(instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
    UNIQUE(instrument_id, timeframe) -- Ensure uniqueness per instrument and timeframe
);

