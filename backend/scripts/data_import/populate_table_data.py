from datetime import datetime, timedelta
import pandas as pd
from backend.data.repositories._sqlite_db import SQLiteDB
from backend.data.repositories.mongo import MongoDBHandler
from backend.logs.log_manager import LogManager

# Initialize Logger
logger = LogManager('populate_sqlite_data').get_logger()

class PopulateTableData:
    def __init__(self):
        """
        Initialize database connections for SQLite and MongoDB.
        """
        self.mongo_handler = MongoDBHandler(db_name="forex_data")
        self.sqlite_db = SQLiteDB(db_name="historical_data.db")
        self.indicators_db = SQLiteDB("indicators.db")
        self.instruments_db = SQLiteDB("instruments.db")
        self.optimizer_db = SQLiteDB("optimizer.db")
        self.config_db = SQLiteDB("configuration.db")
        self.user_db = SQLiteDB("user.db")

    def populate_sample_data(self):
        """
        Populates sample data into various SQLite databases.
        """
        logger.info("🔄 Populating sample data into SQLite tables...")

        # Initialize all databases
        self.indicators_db.initialize_db()
        self.instruments_db.initialize_db()
        self.sqlite_db.initialize_db()
        self.optimizer_db.initialize_db()
        self.config_db.initialize_db()
        self.user_db.initialize_db()

        # Insert sample indicators
        rsi_id = self.indicators_db.add_record("indicators", {"name": "RSI", "type": "momentum"})
        atr_id = self.indicators_db.add_record("indicators", {"name": "ATR", "type": "volatility"})
        bb_id = self.indicators_db.add_record("indicators", {"name": "BollingerBands", "type": "volatility"})
        ma_crossover_id = self.indicators_db.add_record("indicators", {"name": "MACrossover", "type": "trend"})

        # Insert parameters for indicators
        indicator_params = [
            (rsi_id, "period", "integer", "14"),
            (atr_id, "period", "integer", "14"),
            (bb_id, "period", "integer", "20"),
            (bb_id, "std", "float", "2.0"),
            (ma_crossover_id, "fast_period", "integer", "12"),
            (ma_crossover_id, "slow_period", "integer", "26"),
        ]
        for param in indicator_params:
            self.indicators_db.add_record("indicator_parameters", {
                "indicator_id": param[0],
                "parameter_name": param[1],
                "parameter_type": param[2],
                "default_value": param[3]
            })

        # Insert sample instruments
        eur_usd_id = self.instruments_db.add_record("instruments", {
            "name": "EUR_USD", "opening_time": "00:00:00", "closing_time": "23:59:59"
        })
        gbp_usd_id = self.instruments_db.add_record("instruments", {
            "name": "GBP_USD", "opening_time": "00:00:00", "closing_time": "23:59:59"
        })

        logger.info("✅ Sample data population completed.")

    def populate_historical_data(self, days=5):
        """
        Populates historical_data.db with the most recent X days of data from MongoDB.

        :param days: Number of days of recent data to fetch.
        """
        logger.info(f"🔄 Fetching last {days} days of historical data from MongoDB...")

        # Define instruments and granularities
        forex_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD"]
        granularities = ["1h", "1d"]

        # Calculate the date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)

        for pair in forex_pairs:
            for granularity in granularities:
                collection_name = f"{pair.lower()}_{granularity}_data"

                # Fetch MongoDB Data (filter last X days)
                query = {"time": {"$gte": start_date.isoformat()}}
                mongo_data = self.mongo_handler.read(query, collection_name)

                if not mongo_data:
                    logger.warning(f"⚠️ No recent data found for {pair} - {granularity}. Skipping...")
                    continue

                # Convert MongoDB data to Pandas DataFrame
                df = pd.DataFrame(mongo_data)
                df['timestamp'] = pd.to_datetime(df['time'])
                df.set_index('timestamp', inplace=True)

                # Extract fields
                df['open'] = df['mid'].apply(lambda x: float(x.get('o', 0)))
                df['high'] = df['mid'].apply(lambda x: float(x.get('h', 0)))
                df['low'] = df['mid'].apply(lambda x: float(x.get('l', 0)))
                df['close'] = df['mid'].apply(lambda x: float(x.get('c', 0)))
                df['volume'] = df.get('volume', 0).astype(float)

                # Drop unnecessary columns
                df.drop(columns=['mid', '_id'], errors='ignore', inplace=True)

                # Retrieve Instrument ID from SQLite
                instrument_id = self.sqlite_db.get_instrument_id(pair)
                if instrument_id is None:
                    logger.error(f"❌ Instrument {pair} not found in SQLite. Skipping...")
                    continue

                # Insert Data into SQLite
                for _, row in df.iterrows():
                    record = {
                        "instrument_id": instrument_id,
                        "timestamp": row.name,
                        "open": row['open'],
                        "high": row['high'],
                        "low": row['low'],
                        "close": row['close'],
                        "volume": row['volume'],
                        "granularity": granularity,
                    }
                    self.sqlite_db.add_record("historical_data", record)

                logger.info(f"✅ Inserted {len(df)} records for {pair} - {granularity}.")

        logger.info("🎯 Historical data population complete!")

    def run_all(self):
        """
        Runs both sample data and historical data population.
        """
        self.populate_sample_data()
        self.populate_historical_data()


# Run when script is executed
if __name__ == "__main__":
    populator = PopulateTableData()
    populator.run_all()
