-- Table for storing historical data
CREATE TABLE IF NOT EXISTS historical_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,  -- Foreign key to instruments
    instrument TEXT NOT NULL, -- Instrument information
    granularity TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    open REAL,        -- Open price
    high REAL,        -- High price
    low REAL,         -- Low price
    close REAL,       -- Close price
    volume INTEGER,   -- Volume (optional)
    FOREIGN KEY (instrument_id) REFERENCES instruments(id)
);

-- Create the index only if it doesn't exist
CREATE INDEX IF NOT EXISTS idx_historical_instrument_id ON historical_data (instrument_id);

