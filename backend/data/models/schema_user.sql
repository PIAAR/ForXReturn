-- Table for storing user information
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,          -- Username must be unique
    password TEXT NOT NULL,                 -- Hashed password
    email TEXT NOT NULL UNIQUE,             -- Email must be unique
    is_verified INTEGER DEFAULT 0,          -- Email verification status (0 = unverified, 1 = verified)
    role TEXT DEFAULT 'user',               -- User role (e.g., 'user', 'admin', 'moderator')
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,  -- Timestamp for when the user was created
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP  -- Timestamp for when the user data was last updated
);

-- Table for managing user sessions
CREATE TABLE IF NOT EXISTS session (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,                -- Foreign key to the users table
    session_token TEXT NOT NULL,             -- Unique session token
    ip_address TEXT,                         -- IP address of the user session
    user_agent TEXT,                         -- User agent string from the browser
    last_login TEXT,                         -- Timestamp for the last time the user logged in
    created_at TEXT DEFAULT CURRENT_TIMESTAMP, -- Timestamp for when the session was created
    expires_at TEXT,                         -- Optional: Expiration timestamp for the session
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table for storing password reset tokens
CREATE TABLE IF NOT EXISTS password_resets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,                -- Foreign key to the users table
    reset_token TEXT NOT NULL,               -- Unique reset token
    expires_at TEXT NOT NULL,                -- Expiration timestamp for the reset token
    used INTEGER DEFAULT 0,                  -- Whether the token has been used (0 = unused, 1 = used)
    created_at TEXT DEFAULT CURRENT_TIMESTAMP, -- Timestamp for when the reset token was created
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table for tracking email verification tokens
CREATE TABLE IF NOT EXISTS email_verification_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,                -- Foreign key to the users table
    verification_token TEXT NOT NULL,        -- Unique email verification token
    expires_at TEXT NOT NULL,                -- Expiration timestamp for the verification token
    used INTEGER DEFAULT 0,                  -- Whether the token has been used (0 = unused, 1 = used)
    created_at TEXT DEFAULT CURRENT_TIMESTAMP, -- Timestamp for when the token was created
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table for managing user roles and permissions (optional)
CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_name TEXT NOT NULL UNIQUE,          -- Name of the role (e.g., 'user', 'admin', 'moderator')
    description TEXT                         -- Optional description of the role
);

-- Table for assigning roles to users (many-to-many relationship)
CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL,                -- Foreign key to the users table
    role_id INTEGER NOT NULL,                -- Foreign key to the roles table
    PRIMARY KEY(user_id, role_id),           -- Composite primary key
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(role_id) REFERENCES roles(id) ON DELETE CASCADE
);
