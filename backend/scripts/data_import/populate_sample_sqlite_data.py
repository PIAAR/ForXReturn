# backend/scripts/data_import/populate_sample_sqlite_data.py
# Used to populate sample data into the SQLite databases.

from backend.data.repositories._sqlite_db import SQLiteDB

def populate_sample_data():
    # Connect to the indicators database
    indicators_db = SQLiteDB("indicators.db")
    # Connect to the instruments database
    instruments_db = SQLiteDB("instruments.db")
    # Connect to the optimizer database
    optimizer_db = SQLiteDB("optimizer.db")
    # Connect to the configuration database
    config_db = SQLiteDB("configuration.db")
    # Connect to the user database
    user_db = SQLiteDB("user.db")
    # Connect to the historical data database
    historical_data_db = SQLiteDB("historical_data.db")

    # Initialize the databases  
    indicators_db.initialize_db()
    instruments_db.initialize_db()
    historical_data_db.initialize_db()
    optimizer_db.initialize_db()
    config_db.initialize_db()
    user_db.initialize_db()

    # Insert sample indicators
    rsi_id = indicators_db.add_record("indicators", {"name": "RSI", "type": "momentum"})
    atr_id = indicators_db.add_record("indicators", {"name": "ATR", "type": "volatility"})
    bb_id = indicators_db.add_record("indicators", {"name": "BollingerBands", "type": "volatility"})
    ma_crossover_id = indicators_db.add_record("indicators", {"name": "MACrossover", "type": "trend"})

    # Insert parameters for RSI, ATR, Bollinger Bands, and MA Crossover indicators
    indicators_db.add_record("indicator_parameters", {
        "indicator_id": rsi_id,
        "parameter_name": "period",
        "parameter_type": "integer",
        "default_value": "14"
    })
    indicators_db.add_record("indicator_parameters", {
        "indicator_id": atr_id,
        "parameter_name": "period",
        "parameter_type": "integer",
        "default_value": "14"
    })
    indicators_db.add_record("indicator_parameters", {
        "indicator_id": bb_id,
        "parameter_name": "period",
        "parameter_type": "integer",
        "default_value": "20"
    })
    indicators_db.add_record("indicator_parameters", {
        "indicator_id": bb_id,
        "parameter_name": "std",
        "parameter_type": "float",
        "default_value": "2.0"
    })
    indicators_db.add_record("indicator_parameters", {
        "indicator_id": ma_crossover_id,
        "parameter_name": "fast_period",
        "parameter_type": "integer",
        "default_value": "12"
    })
    indicators_db.add_record("indicator_parameters", {
        "indicator_id": ma_crossover_id,
        "parameter_name": "slow_period",
        "parameter_type": "integer",
        "default_value": "26"
    })

    # Insert a sample instrument
    eur_usd_id = instruments_db.add_record("instruments", {"name": "EUR_USD", "opening_time": "00:00:00", "closing_time": "23:59:59"})
    gbp_usd_id = instruments_db.add_record("instruments", {"name": "GBP_USD", "opening_time": "00:00:00", "closing_time": "23:59:59"})

    # Insert sample optimization parameters and results
    optimizer_db.add_record("optimized_parameters", {
        "instrument_id": eur_usd_id,
        "indicator_id": rsi_id,
        "parameter_name": "period",
        "parameter_value": 14,
        "timestamp": "2023-09-30 12:00:00"
    })

    optimizer_db.add_record("optimization_results", {
        "instrument_id": eur_usd_id,  # Ensure this field is included
        "optimization_id": 1,
        "sharpe_ratio": 1.5,
        "total_return": 25.3,
        "max_drawdown": -5.6,
        "win_rate": 60,
        "timestamp": "2023-09-30 12:30:00"
    })

    # Insert sample indicator results for EUR_USD using RSI, Bollinger Bands, and MA Crossover
    indicators_db.add_record("instrument_indicator_results", {
        "instrument_id": eur_usd_id,
        "indicator_id": rsi_id,
        "parameter_name": "RSI",
        "parameter_value": 60,
        "timestamp": "2023-09-30 12:45:00"
    })
    indicators_db.add_record("instrument_indicator_results", {
        "instrument_id": eur_usd_id,
        "indicator_id": bb_id,
        "parameter_name": "BollingerBands",
        "parameter_value": 1.5,
        "timestamp": "2023-09-30 12:45:00"
    })
    indicators_db.add_record("instrument_indicator_results", {
        "instrument_id": eur_usd_id,
        "indicator_id": ma_crossover_id,
        "parameter_name": "MACrossover",
        "parameter_value": 1,
        "timestamp": "2023-09-30 12:45:00"
    })

    # Insert sample profile into configuration
    config_db.add_record("profiles", {
        "profile_name": "Default",
        "last_update": "2024-01-01"
    })

    # Insert sample settings into the configuration
    config_db.add_record("settings", {
        "setting_name": "trade_frequency",
        "setting_value": "high"
    })

    # Insert sample session into user database
    user_db.add_record("session", {
        "user_id": 1,
        "session_token": "abc123",
        "last_login": "2024-01-01 10:00:00"
    })

    # Insert sample user
    user_db.add_record("users", {
        "username": "test_user",
        "email": "test_user@example.com",
        "password": "securepassword",
        "created_at": "2023-09-30 10:00:00"
    })

    print("Sample data populated successfully.")

if __name__ == "__main__":
    populate_sample_data()
