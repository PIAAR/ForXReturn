import os
import yaml
from backend.data.repositories._sqlite_db import SQLiteDB
from backend.logs.log_manager import LogManager

# Initialize Logger
logger = LogManager('populate_indicators').get_logger()


class PopulateIndicatorData:
    def __init__(self, db_name="indicators.db", yaml_path=None):
        """
        Initializes the database connection and loads indicators from the YAML file.
        """
        self.db = SQLiteDB(db_name)
        self.db.initialize_db()

        # Determine YAML file path
        if yaml_path is None:
            self.yaml_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 
                "../../scripts/yml/indicator_params.yml"
            )
        else:
            self.yaml_path = yaml_path

        # Load indicators from YAML
        self.indicators = self.load_indicators_from_yaml()

    def load_indicators_from_yaml(self):
        """
        Load the list of indicators from the YAML file.
        """
        try:
            with open(self.yaml_path, "r") as file:
                data = yaml.safe_load(file)
            logger.info(f"✅ Loaded {len(data['indicators'])} indicators from YAML.")
            return data['indicators']
        except Exception as e:
            logger.error(f"❌ Error loading indicators from YAML: {e}")
            return {}

    def populate_indicators(self):
        """
        Populate indicators and their parameters into the indicators database.
        """
        for indicator_name, indicator_data in self.indicators.items():
            try:
                # Get the indicator type
                indicator_type = indicator_data.get('type', None)

                # Insert the indicator into the indicators table
                indicator_id = self.db.add_record("indicators", {
                    "name": indicator_name,
                    "type": indicator_type
                })
                logger.info(f"📥 Inserted Indicator: {indicator_name} (Type: {indicator_type})")

                # Insert indicator parameters for each timeframe (macro, daily, micro)
                for timeframe, parameters_data in indicator_data.items():
                    if timeframe == "type":
                        continue  # Skip the type key

                    # Get weight for the current timeframe
                    weight = parameters_data.get('weight', 1)

                    # Insert weight into the indicator_parameters table
                    self.db.add_record("indicator_parameters", {
                        "indicator_id": indicator_id,
                        "parameter_name": f"{timeframe}_weight",
                        "parameter_type": "float",
                        "default_value": str(weight)
                    })

                    # Insert other parameters into the indicator_parameters table
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

            except Exception as e:
                logger.error(f"❌ Error inserting {indicator_name}: {e}")

        logger.info("🎯 Indicator data population complete!")

    def run(self):
        """
        Runs the indicator population process.
        """
        self.populate_indicators()


# Run when script is executed
if __name__ == "__main__":
    populator = PopulateIndicatorData()
    populator.run()
