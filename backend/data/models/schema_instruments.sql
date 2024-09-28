-- Table for storing instrument information
CREATE TABLE IF NOT EXISTS instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    opening_time TIME NOT NULL,
    closing_time TIME NOT NULL
);
