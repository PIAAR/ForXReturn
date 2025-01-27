import os
import yaml
from backend.data.repositories._sqlite_db import SQLiteDB

def load_indicators_from_yaml(yaml_file):
    """
    Load the list of indicators from a YAML file.
    """
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    return data['indicators']

def populate_indicators():
    """
    Populate indicators and their parameters into the indicators database from a YAML file.
    """
    db = SQLiteDB("indicators.db")
    db.initialize_db()

    # Path to the YAML file
    yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../scripts/yml/indicator_params.yml')
    
    # Load indicators from YAML
    indicators = load_indicators_from_yaml(yaml_file)

    # Populate each indicator into the database
    for indicator_name, indicator_data in indicators.items():
        # Get the indicator type
        indicator_type = indicator_data.get('type', None)
        
        # Insert the indicator into the indicators table
        indicator_id = db.add_record("indicators", {
            "name": indicator_name,
            "type": indicator_type
        })

        # Insert indicator parameters for each timeframe (macro, daily, micro)
        for timeframe, parameters_data in indicator_data.items():
            # Skip if the key is 'type'
            if timeframe == "type":
                continue

            # Get the weight for the current timeframe
            weight = parameters_data.get('weight', 1)

            # Insert the weight as a parameter in the indicator_parameters table
            db.add_record("indicator_parameters", {
                "indicator_id": indicator_id,
                "parameter_name": f"{timeframe}_weight",
                "parameter_type": "float",  # Assuming weight is a float
                "default_value": str(weight)
            })

            # Insert other parameters into the indicator_parameters table
            for parameter_name, parameter_value in parameters_data.items():
                if parameter_name != 'weight':  # Skip weight since it's handled separately
                    parameter_type = "float" if isinstance(parameter_value, (int, float)) else "text"
                    db.add_record("indicator_parameters", {
                        "indicator_id": indicator_id,
                        "parameter_name": f"{timeframe}_{parameter_name}",  # Prefix parameter with timeframe
                        "parameter_type": parameter_type,
                        "default_value": str(parameter_value)  # Convert the value to a string
                    })

    print("Indicators and parameters populated successfully.")

if __name__ == "__main__":
    populate_indicators()
