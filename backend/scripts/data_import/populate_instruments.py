import os
import yaml
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from backend.logs.log_manager import LogManager

# Initialize Logger
logger = LogManager('populate_instruments').get_logger()


class PopulateInstrumentData:
    def __init__(self, db_name="instruments.db", yaml_path=None):
        """
        Initializes the database connection and loads instruments from the YAML file.
        """
        self.db = SQLiteDBHandler(db_name)
        self.db.initialize_db()

        # Determine YAML file path
        self.yaml_path = yaml_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            "../../scripts/yml/instruments.yml"
        )

        # Load instruments from YAML
        self.instruments = self.load_instruments_from_yaml()

    def load_instruments_from_yaml(self):
        """
        Load the list of instruments from the YAML file.
        """
        try:
            with open(self.yaml_path, "r") as file:
                data = yaml.safe_load(file)
            
            instruments = data.get("instruments", [])
            if not instruments:
                logger.warning("⚠️ No instruments found in YAML. Check the file content.")

            logger.info(f"✅ Loaded {len(instruments)} instruments from YAML.")
            return instruments

        except FileNotFoundError:
            logger.error(f"❌ YAML file not found at: {self.yaml_path}")
            return []
        except yaml.YAMLError as e:
            logger.error(f"❌ Error parsing YAML file: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Unexpected error loading instruments from YAML: {e}")
            return []

    def compare_and_insert_instruments(self):
        """
        Compare instruments from the YAML file with existing records in the database.
        If the instrument exists, update it if necessary. Otherwise, insert a new record.
        """
        if not self.instruments:
            logger.warning("⚠️ No instruments to process. Skipping population.")
            return

        for instrument in self.instruments:
            try:
                existing_record = self.db.fetch_records(
                    "instruments", {"name": instrument['name']}
                )

                if existing_record:
                    # Extract existing data
                    existing_opening_time, existing_closing_time = existing_record[0][2:4]

                    # Check if times need updating
                    if (
                        existing_opening_time != instrument["opening_time"]
                        or existing_closing_time != instrument["closing_time"]
                    ):
                        logger.info(f"🔄 Updating times for {instrument['name']}.")
                        self.db.update_record(
                            "instruments",
                            {
                                "opening_time": instrument["opening_time"],
                                "closing_time": instrument["closing_time"],
                            },
                            {"name": instrument["name"]},
                        )
                    else:
                        logger.info(f"✅ Instrument {instrument['name']} already exists with the same times. Skipping.")
                else:
                    # Insert new instrument
                    logger.info(f"📥 Inserting new instrument: {instrument['name']}.")
                    self.db.add_record(
                        "instruments",
                        {
                            "name": instrument["name"],
                            "opening_time": instrument["opening_time"],
                            "closing_time": instrument["closing_time"],
                        },
                    )
            except Exception as e:
                logger.error(f"❌ Error processing instrument {instrument['name']}: {e}")

    def run(self):
        """
        Runs the instrument population process.
        """
        self.compare_and_insert_instruments()
        logger.info("🎯 Instrument data population complete!")


# Run when script is executed
if __name__ == "__main__":
    populator = PopulateInstrumentData()
    populator.run()
