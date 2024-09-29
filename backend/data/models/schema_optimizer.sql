-- Table for storing optimized parameters for indicators
CREATE TABLE IF NOT EXISTS optimized_parameters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,  -- Foreign key to instruments table
    indicator_id INTEGER NOT NULL,   -- Foreign key to indicators table
    parameter_name TEXT NOT NULL,    -- Name of the optimized parameter (e.g., period, threshold)
    parameter_value REAL NOT NULL,   -- Optimized value for the parameter
    timestamp TEXT NOT NULL,         -- Timestamp of when the optimization occurred
    FOREIGN KEY(instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
    FOREIGN KEY(indicator_id) REFERENCES indicators(id) ON DELETE CASCADE
);

-- Table for storing optimization performance results
CREATE TABLE IF NOT EXISTS optimization_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    optimization_id INTEGER NOT NULL,  -- Foreign key to the optimized_parameters table
    instrument_id INTEGER NOT NULL,    -- Foreign key to the instruments table (for easier lookup)
    sharpe_ratio REAL,                 -- Sharpe ratio of the optimized strategy
    total_return REAL,                 -- Total return percentage
    max_drawdown REAL,                 -- Maximum drawdown observed
    win_rate REAL,                     -- Win rate percentage
    profit_loss REAL,                  -- Profit or loss value
    total_trades INTEGER,              -- Total number of trades taken in this optimization
    timestamp TEXT NOT NULL,           -- Timestamp of when the result was recorded
    FOREIGN KEY(optimization_id) REFERENCES optimized_parameters(id) ON DELETE CASCADE,
    FOREIGN KEY(instrument_id) REFERENCES instruments(id) ON DELETE CASCADE
);
