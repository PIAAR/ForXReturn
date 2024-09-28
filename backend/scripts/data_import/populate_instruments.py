import os
import yaml
from backend.data.repositories._sqlite_db import SQLiteDB


def load_instruments_from_yaml(yaml_file):
    """
    Load the list of instruments from a YAML file.
    """
    with open(yaml_file, "r") as file:
        data = yaml.safe_load(file)
    return data["instruments"]


def compare_and_insert_instruments(db, instruments):
    """
    Compare instruments from the YAML file with existing records in the database.
    If the instrument exists, skip or update it. Otherwise, insert a new record.
    """
    for instrument in instruments:
        if existing_record := db.fetch_records(
            "instruments", {"name": instrument['name']}
        ):
            # Get the existing opening and closing times from the database
            existing_opening_time = existing_record[0][2]
            existing_closing_time = existing_record[0][3]

            # Check if the times are the same or different
            if (
                existing_opening_time != instrument["opening_time"]
                or existing_closing_time != instrument["closing_time"]
            ):
                print(f"Updating times for {instrument['name']}")
                db.update_record(
                    "instruments",
                    {
                        "opening_time": instrument["opening_time"],
                        "closing_time": instrument["closing_time"],
                    },
                    {"name": instrument["name"]},
                )
            else:
                print(
                    f"Instrument {instrument['name']} already exists with the same times. Skipping."
                )
        else:
            # Insert the new instrument
            print(f"Inserting new instrument {instrument['name']}")
            db.add_record(
                "instruments",
                {
                    "name": instrument["name"],
                    "opening_time": instrument["opening_time"],
                    "closing_time": instrument["closing_time"],
                },
            )


def populate_instruments():
    """
    Populate instruments into the instruments database from a YAML file.
    """
    db = SQLiteDB("instruments.db")
    db.initialize_db()

    # Path to the YAML file
    yaml_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../../scripts/yml/instruments.yml"
    )

    # Load instruments from YAML
    instruments = load_instruments_from_yaml(yaml_file)

    # Compare and insert instruments into the database
    compare_and_insert_instruments(db, instruments)


if __name__ == "__main__":
    populate_instruments()
