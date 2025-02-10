import os
import yaml
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from backend.logs.log_manager import LogManager

# Initialize Logger
logger = LogManager('populate_indicators').get_logger()


class PopulateIndicatorData:
    def __init__(self, db_name="indicators.db", yaml_path=None):
        """
        Initializes the database connection and loads indicators from the YAML file.
        """
        self.db = SQLiteDBHandler(db_name)
        self.db.initialize_db()

        # Determine YAML file path
        self.yaml_path = yaml_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            "../../scripts/yml/indicator_params.yml"
        )

        # Load indicators from YAML
        self.indicators = self.load_indicators_from_yaml()

    def load_indicators_from_yaml(self):
        """
        Load the list of indicators from the YAML file.
        """
        try:
            with open(self.yaml_path, "r") as file:
                data = yaml.safe_load(file)
            logger.info(f"✅ Loaded {len(data.get('indicators', {}))} indicators from YAML.")
            return data.get('indicators', {})
        except Exception as e:
            logger.error(f"❌ Error loading indicators from YAML: {e}")
            return {}

    def check_existing_indicator(self, indicator_name):
        """
        Check if an indicator already exists in the database.
        """
        existing_record = self.db.fetch_records("indicators", {"name": indicator_name})
        return existing_record[0][0] if existing_record else None

    def populate_indicators(self):
        """
        Populate indicators and their parameters into the indicators database.
        """
        if not self.indicators:
            logger.warning("⚠️ No indicators found in YAML. Skipping population.")
            return

        for indicator_name, indicator_data in self.indicators.items():
            try:
                # Check if indicator already exists
                existing_id = self.check_existing_indicator(indicator_name)

                if existing_id:
                    logger.info(f"⚠️ Indicator {indicator_name} already exists. Skipping insert.")
                    indicator_id = existing_id
                else:
                    # Insert new indicator
                    indicator_type = indicator_data.get('type', "unknown")
                    indicator_id = self.db.add_record("indicators", {
                        "name": indicator_name,
                        "type": indicator_type
                    })
                    logger.info(f"📥 Inserted Indicator: {indicator_name} (Type: {indicator_type})")

                # Insert indicator parameters
                self.populate_parameters(indicator_id, indicator_name, indicator_data)
            except Exception as e:
                logger.error(f"❌ Error inserting {indicator_name}: {e}")

        logger.info("🎯 Indicator data population complete!")

    def populate_parameters(self, indicator_id, indicator_name, indicator_data):
        """
        Insert indicator parameters into the database.
        """
        for timeframe, parameters_data in indicator_data.items():
            if timeframe == "type":
                continue  # Skip the 'type' key

            # Insert weight
            weight = parameters_data.get('weight', 1)
            self.db.add_record("indicator_parameters", {
                "indicator_id": indicator_id,
                "parameter_name": f"{timeframe}_weight",
                "parameter_type": "float",
                "default_value": str(weight)
            })

            # Insert other parameters
            for parameter_name, parameter_value in parameters_data.items():
                if parameter_name != 'weight':
                    parameter_type = "float" if isinstance(parameter_value, (int, float)) else "text"
                    self.db.add_record("indicator_parameters", {
                        "indicator_id": indicator_id,
                        "parameter_name": f"{timeframe}_{parameter_name}",
                        "parameter_type": parameter_type,
                        "default_value": str(parameter_value)
                    })

        logger.info(f"✅ Parameters added for {indicator_name}")

    def run(self):
        """
        Runs the indicator population process.
        """
        self.populate_indicators()


# Run when script is executed
if __name__ == "__main__":
    populator = PopulateIndicatorData()
    populator.run()
