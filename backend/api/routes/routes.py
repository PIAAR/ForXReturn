from datetime import datetime
import os
from backend.api.services.data_population_service import DataPopulationService
from backend.api.services.state_machine import StateMachine
from backend.api.services.trading_services import TradingService
from backend.config.indicator_config_loader import IndicatorConfigLoader
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from backend.data.repositories._mongo_db import MongoDBHandler
from flask import Blueprint, Flask, jsonify, request
from backend.logs.log_manager import LogManager

'''
Creates the Flask app and registers the blueprints. Defines the API routes.
'''

# Initialize the LogManager
logger = LogManager('api_routes').get_logger()

# Define blueprints for different functionalities
main = Blueprint('main', __name__)
trading_bp = Blueprint('trading', __name__)
data_population_bp = Blueprint('data_population', __name__)

# Create a database connection for indicators.db
indicator_db_connection = SQLiteDBHandler("indicators.db")._connect_db()
instrument_db_connection = SQLiteDBHandler("instruments.db")._connect_db()

# Initialize services with db connections
trading_service = TradingService(indicator_db_connection)
data_population_service = DataPopulationService()

# Initialize services with config loader
config_path = os.path.join(os.path.dirname(__file__), '../../scripts/yml/indicator_params.yml')
config_loader = IndicatorConfigLoader(config_path)
state_machine = StateMachine(config_loader, instrument_db_connection)

# -------------------- Main Blueprint --------------------
@main.route("/", methods=['GET'])
def main_route():
    """
    Root route for the application.
    """
    logger.info("Root route accessed.")
    return jsonify({'message': 'Welcome to the trading API!'}), 200

@main.route("/system-status", methods=['GET'])
def system_status():
    """
    Fetches the system status, including:
    - Instrument states from SQLite
    - Data freshness check for MongoDB (backtest data)
    - Data freshness check for SQLite (active trades)
    - Alerts for missing or out-of-sync data
    """
    logger.info("Fetching system status.")
    try:
        instruments_db = SQLiteDBHandler("instruments.db")
        mongo_handler = MongoDBHandler(db_name="forex_data")
        sqlite_db = SQLiteDBHandler("historical_data.db")

        instruments = instruments_db.fetch_records("instruments")

        response_data = []
        alerts = []  # Stores missing or outdated data issues

        for instrument in instruments:
            instrument_id = instrument[0]
            instrument_name = instrument[1]

            states = {
                'macro': get_state(instrument_id, 'macro', instruments_db),
                'daily': get_state(instrument_id, 'daily', instruments_db),
                'minute': get_state(instrument_id, 'minute', instruments_db),
            }

            # Step 1: Check MongoDB Data Health
            mongo_collection = f"{instrument_name.lower()}_D_data"
            if mongo_latest_record := mongo_handler.read(
                {}, collection_name=mongo_collection
            ):
                latest_mongo_time = max(record["time"] for record in mongo_latest_record)
                latest_mongo_dt = datetime.strptime(latest_mongo_time, "%Y-%m-%d %H:%M:%S")
            else:
                latest_mongo_dt = None

            # Step 2: Check SQLite Data Health
            sqlite_latest_time = sqlite_db.fetch_records_with_query(
                "SELECT MAX(timestamp) FROM historical_data WHERE instrument_id = ? AND granularity = ?",
                (instrument_id, "D")
            )
            latest_sqlite_dt = sqlite_latest_time[0][0] if sqlite_latest_time and sqlite_latest_time[0][0] else None

            # Step 3: Determine if data is missing or out-of-sync
            now = datetime.now()

            if not latest_mongo_dt:
                alerts.append(f"⚠️ No data in MongoDB for {instrument_name} (Daily)")
            elif (now - latest_mongo_dt).days > 1:
                alerts.append(f"⚠️ MongoDB data outdated for {instrument_name} (Last updated: {latest_mongo_dt})")

            if not latest_sqlite_dt:
                alerts.append(f"⚠️ No data in SQLite for {instrument_name} (Daily)")
            elif (now - datetime.strptime(latest_sqlite_dt, "%Y-%m-%d %H:%M:%S")).days > 1:
                alerts.append(f"⚠️ SQLite data outdated for {instrument_name} (Last updated: {latest_sqlite_dt})")

            response_data.append({
                'instrument_id': instrument_id,
                'instrument_name': instrument_name,
                'state': states,
                'mongo_last_update': latest_mongo_dt.strftime("%Y-%m-%d %H:%M:%S") if latest_mongo_dt else "Missing",
                'sqlite_last_update': latest_sqlite_dt or "Missing",
                'timestamp': now.isoformat()
            })

        logger.info("System status fetched successfully.")
        return jsonify({'status': response_data, 'alerts': alerts}), 200

    except Exception as e:
        logger.error(f"Error fetching system status: {e}")
        return jsonify({'error': str(e)}), 500


def get_state(instrument_id, timeframe, db):
    """
    Retrieve the state for a specific instrument and timeframe from the database.
    """
    logger.debug(f"Fetching state for instrument_id={instrument_id}, timeframe={timeframe}.")
    try:
        if result := db.fetch_records("instrument_states", {"instrument_id": instrument_id, "timeframe": timeframe}):
            return result[0][0]  # Return the first state's value
        return "UNKNOWN"
    except Exception as e:
        logger.error(f"Error fetching state for instrument_id={instrument_id}, timeframe={timeframe}: {e}")
        return "UNKNOWN"

# -------------------- Trading Routes --------------------
@trading_bp.route('/start', methods=['POST'])
def start_trading():
    """
    Starts the trading process by calling the `start_trading` method of the trading service.
    """
    logger.info("Start trading endpoint accessed.")
    try:
        trading_service.start_trading()
        logger.info("Trading started successfully.")
        return jsonify({'status': 'Trading started'}), 200
    except Exception as e:
        logger.error(f"Error starting trading: {e}")
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/stop', methods=['POST'])
def stop_trading():
    """
    Stops the trading process by calling the `stop_trading` method of the trading service.
    """
    logger.info("Stop trading endpoint accessed.")
    try:
        trading_service.stop_trading()
        logger.info("Trading stopped successfully.")
        return jsonify({'status': 'Trading stopped'}), 200
    except Exception as e:
        logger.error(f"Error stopping trading: {e}")
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/status', methods=['GET'])
def get_status():
    """
    Retrieves the current status of the trading process.
    """
    logger.info("Get trading status endpoint accessed.")
    try:
        status = trading_service.get_status()
        open_orders = trading_service.get_orders()
        positions = trading_service.get_positions()
        logger.info("Trading status fetched successfully.")
        return jsonify({'status': status, 'open_orders': open_orders, 'positions': positions}), 200
    except Exception as e:
        logger.error(f"Error fetching trading status: {e}")
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/place_order', methods=['POST'])
def place_order():
    """
    Places a new order by calling the `place_order` method in the TradingService.
    """
    logger.info("Place order endpoint accessed.")
    try:
        order_data = request.json
        logger.debug(f"Order data received: {order_data}")
        order_response = trading_service.place_order(order_data)
        logger.info("Order placed successfully.")
        return jsonify(order_response), 201
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/positions', methods=['GET'])
def get_positions():
    """
    Retrieves current positions.
    """
    logger.info("Get positions endpoint accessed.")
    try:
        positions = trading_service.get_positions()
        logger.info("Positions fetched successfully.")
        return jsonify(positions), 200
    except Exception as e:
        logger.error(f"Error fetching positions: {e}")
        return jsonify({'error': str(e)}), 500

@trading_bp.route('/performance', methods=['GET'])
def performance():
    """
    Retrieves trading performance metrics.
    """
    logger.info("Get performance endpoint accessed.")
    try:
        performance_data = trading_service.get_performance()
        logger.info("Performance metrics fetched successfully.")
        return jsonify(performance_data), 200
    except Exception as e:
        logger.error(f"Error fetching performance metrics: {e}")
        return jsonify({"error": str(e)}), 500

@trading_bp.route('/evaluate_state', methods=['POST'])
def evaluate_state():
    """
    Evaluate the state for an instrument across multiple timeframes (macro, daily, minute).
    """
    logger.info("Evaluate state endpoint accessed.")
    try:
        data = request.json
        logger.debug(f"Evaluation data received: {data}")

        instrument_id = data.get('instrument_id')
        indicator_results_by_tier = data.get('indicator_results_by_tier', {})
        market_conditions = data.get('market_conditions', {})

        if not instrument_id or not market_conditions:
            logger.error("Missing required parameters: instrument_id or market_conditions")
            return jsonify({"error": "Missing required parameters: instrument_id or market_conditions"}), 400

        state = state_machine.run_state_machine(instrument_id, indicator_results_by_tier, market_conditions)
        logger.info("State evaluated successfully.")
        return jsonify({"state": state}), 200
    except Exception as e:
        logger.error(f"Error evaluating state: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------- Data Population Routes --------------------
@data_population_bp.route('/populate_data', methods=['GET, POST'])
def populate_data():
    """
    Populate data for all instruments.
    """
    logger.info("Populate data endpoint accessed.")
    try:
        data_population_service.populate_all_instruments()
        logger.info("Data population started successfully.")
        return jsonify({"message": "Data population started"}), 200
    except Exception as e:
        logger.error(f"Error populating data: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------- Create the Flask App --------------------
def create_app():
    """
    Creates and configures the Flask application with registered blueprints.
    """
    app = Flask(__name__)
    app.register_blueprint(main, url_prefix='/')
    app.register_blueprint(trading_bp, url_prefix='/api/trading')
    app.register_blueprint(data_population_bp, url_prefix='/api/data')

    logger.info("Flask application created and blueprints registered.")

    return app
