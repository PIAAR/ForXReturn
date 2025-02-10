from datetime import datetime
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from backend.api.services.state_machine import StateMachine
from config.indicator_config_loader import IndicatorConfigLoader

def evaluate_and_store_states():
    # Initialize database connections
    instruments_db = SQLiteDBHandler("instruments.db")
    state_machine_db = SQLiteDBHandler("instrument_states.db")

    # Fetch all instruments
    instruments = instruments_db.fetch_records("instruments")

    # Initialize the StateMachine
    config_loader = IndicatorConfigLoader()
    state_machine = StateMachine(config_loader, state_machine_db)

    # Iterate over each instrument and evaluate the state
    for instrument in instruments:
        instrument_name = instrument[1]  # Assuming Name is the second column

        # Example market conditions, these would be derived from real data
        market_conditions = {
            'volatility': 5,  # Sample volatility value
            'risk_level': 6   # Sample risk level value
        }

        # Retrieve historical data for each indicator (to be fetched based on real-time or historical data)
        indicator_results_by_tier = {
            'macro': {'RSI': 1, 'ATR': 0, 'BollingerBands': 1},
            'daily': {'RSI': 1, 'ATR': 1, 'BollingerBands': 0},
            'minute': {'RSI': 0, 'ATR': 1, 'BollingerBands': 1}
        }

        # Run the state machine for the instrument (use instrument_name, which is converted to instrument_id internally)
        states = state_machine.run_state_machine(instrument_name, indicator_results_by_tier, market_conditions)

        # Store the state results in the database
        timestamp = datetime.now().isoformat()
        for tier, state in states.items():
            # Get instrument_id from instrument_name inside the state machine
            instrument_id = instruments_db.get_instrument_id(instrument_name)
            state_machine.update_state_in_db(instrument_id, tier, state)

        print(f"States for {instrument_name}: {states}")

    print(f"State evaluation complete for all instruments at {timestamp}.")

if __name__ == "__main__":
    evaluate_and_store_states()
