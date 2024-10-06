# backend/api/routes/routes.py
from datetime import datetime

from backend.api.services.data_population_service import DataPopulationService
from backend.api.services.state_machine import StateMachine
from backend.api.services.trading_services import TradingService
from backend.config.indicator_config_loader import IndicatorConfigLoader
from backend.data.repositories._sqlite_db import SQLiteDB
from flask import Blueprint, Flask, jsonify, request
from backend.logs.log_manager import LogManager

'''
Creates the Flask app and registers the blueprints. Defines the API routes.
'''

# Initialize the LogManager
logger = LogManager('api_routes')

# Define blueprints for different functionalities
main = Blueprint('main', __name__)
trading_bp = Blueprint('trading', __name__)
data_population_bp = Blueprint('data_population', __name__)

# Create a database connection for indicators.db
indicator_db_connection = SQLiteDB("indicators.db")._connect_db()
instrument_db_connection = SQLiteDB("instruments.db")._connect_db()

# Initialize services with db connections
trading_service = TradingService(indicator_db_connection)
data_population_service = DataPopulationService()
config_loader = IndicatorConfigLoader()
state_machine = StateMachine(config_loader, instrument_db_connection)

# -------------------- Main Blueprint --------------------
@main.route("/", methods=['GET'])
def main_route():
    """
    Root route for the application.
    """
    return jsonify({'message': 'Welcome to the trading API!'}), 200

from flask import jsonify
from backend.data.repositories._sqlite_db import SQLiteDB

@main.route("/system-status", methods=['GET'])
def system_status():
    instruments_db = SQLiteDB("instruments.db")  # Now using instruments.db

    # Fetch all instruments
    instruments = instruments_db.fetch_records("instruments")

    response_data = []

    # Fetch the current state for each instrument
    for instrument in instruments:
        instrument_id = instrument[0]
        instrument_name = instrument[1]

        # Fetch the states for each timeframe
        states = {
            'macro': get_state(instrument_id, 'macro', instruments_db),
            'daily': get_state(instrument_id, 'daily', instruments_db),
            'minute': get_state(instrument_id, 'minute', instruments_db),
        }

        # Build the response
        response_data.append({
            'instrument_id': instrument_id,
            'instrument_name': instrument_name,
            'state': states,
            'timestamp': datetime.now().isoformat()
        })

    return jsonify(response_data), 200

def get_state(instrument_id, timeframe, db):
    """
    Retrieve the state for a specific instrument and timeframe from the database.
    """
    
    if result := db.fetch_records(
        "instrument_states",
        {"instrument_id": instrument_id, "timeframe": timeframe},
    ):
        return result[0][0]  # Return the first state's value
    return "UNKNOWN"  # Default to UNKNOWN if no state found

# -------------------- Trading Routes --------------------
@trading_bp.route('/start', methods=['POST'])
def start_trading():
    """
    Starts the trading process by calling the `start_trading` method of the trading service.
    """
    try:
        trading_service.start_trading()
        return jsonify({'status': 'Trading started'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/stop', methods=['POST'])
def stop_trading():
    """
    Stops the trading process by calling the `stop_trading` method of the trading service.
    """
    try:
        trading_service.stop_trading()
        return jsonify({'status': 'Trading stopped'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/status', methods=['GET'])
def get_status():
    """
    Retrieves the current status of the trading process.
    """
    try:
        status = trading_service.get_status()
        open_orders = trading_service.get_orders()
        positions = trading_service.get_positions()
        return jsonify({
                        'status': status,
                        'open_orders': open_orders,
                        'positions': positions
                        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/place_order', methods=['POST'])
def place_order():
    """
    Places a new order by calling the `place_order` method in the TradingService.
    """
    try:
        order_data = request.json
        order_response = trading_service.place_order(order_data)
        return jsonify(order_response), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/positions', methods=['GET'])
def get_positions():
    """
    Retrieves current positions.
    """
    positions = trading_service.get_positions()
    return jsonify(positions), 200

@trading_bp.route('/performance', methods=['GET'])
def performance():
    """
    Retrieves trading performance metrics.
    """
    
    # Create a database connection
    indicator_db_connection = SQLiteDB("indicators.db")._connect_db()
    # Initialize TradingService with indicator_db_connection
    trading_service = TradingService(indicator_db_connection)

    try:
        performance_data = trading_service.get_performance()  # This method should use indicator_db_connection
        return jsonify(performance_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@trading_bp.route('/evaluate_state', methods=['POST'])
def evaluate_state():
    """
    Evaluate the state for an instrument across multiple timeframes (monthly, daily, minute).
    """
    data = request.json
    instrument_id = data.get('instrument_id')
    indicator_results_by_tier = data.get('indicator_results_by_tier', {})
    market_conditions = data.get('market_conditions', {})

    # Example structure of indicator_results_by_tier:
    # {
    #     'macro': {'RSI': 1, 'ATR': 0, 'BollingerBands': 1},
    #     'daily': {'RSI': 1, 'ATR': 1, 'BollingerBands': 0},
    #     'micro': {'RSI': 0, 'ATR': 1, 'BollingerBands': 1}
    # }
    
    # Ensure instrument_id and market_conditions are provided
    if not instrument_id or not market_conditions:
        return jsonify({"error": "Missing required parameters: instrument_id or market_conditions"}), 400
    
    # Run the state machine and get the states
    state = state_machine.run_state_machine(instrument_id, indicator_results_by_tier, market_conditions)

    return jsonify({"state": state}), 200

# -------------------- Data Population Routes --------------------
@data_population_bp.route('/populate_data', methods=['POST'])
def populate_data():
    """
    Populate data for all instruments.
    """
    try:
        data_population_service.populate_all_instruments()
        return jsonify({"message": "Data population started"}), 200
    except Exception as e:
        logger.error(f"Error populating data: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------- Create the Flask App --------------------
def create_app():
    app = Flask(__name__)
    
    # Register blueprints
    app.register_blueprint(main, url_prefix='/')
    app.register_blueprint(trading_bp, url_prefix='/api/trading')
    app.register_blueprint(data_population_bp, url_prefix='/api/data')

    # Optionally, print the URL rules to verify routing
    for endpoint in app.url_map.iter_rules():
        print(endpoint)
    
    return app
