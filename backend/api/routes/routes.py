# backend/api/routes/routes.py
from flask import Blueprint, Flask, jsonify, request
from logs.log_manager import LogManager
from api.services.trading_services import TradingService as trading_service
from api.services.data_population_service import DataPopulationService

'''
Creates the Flask app and registers the blueprints. Defines the API routes.
'''

# Initialize the LogManager
logger = LogManager('api_routes')

# Define the main and API blueprints
main = Blueprint('main', __name__)
bp = Blueprint('api', __name__)
dp = Blueprint('data_population', __name__)

# Initialize services
data_population_service = DataPopulationService()
# Ensure all historical data is populated on startup
data_population_service.populate_all_instruments()
# Schedule the updates every minute
# data_population_service.update_data_every_minute()

# Define your routes for the main blueprint
@main.route("/", methods=['GET'])
def main_route():
    """
    Root route for the application.
    """
    return jsonify({'message': 'Welcome to the trading API!'}), 200

# Status endpoint
@bp.route('/status', methods=['GET'])
def status():
    """
    Returns a status message indicating that the API is active.
    """
    return jsonify({'status': 'active'}), 200

@bp.route('/order', methods=['POST'])
def place_order():
    """
    Places a new order by calling the TradingService's place_trade method.
    The order data is expected to be in the JSON body of the request.

    :return: A JSON response containing the details of the placed order or an error message.
    """
    try:
        # Extract trade data from the incoming request
        trade_data = request.json

        # Call the TradingService's place_trade method
        oanda_response = trading_service.place_trade(trade_data)

        if oanda_response is not None:
            logger.info(f"Trade placed successfully: {oanda_response}")
            return jsonify(oanda_response), 201
        else:
            logger.error("Failed to place trade.")
            return jsonify({'error': 'Failed to place trade.'}), 500

    except Exception as e:
        logger.error(f"Error processing trade: {e}")
        return jsonify({'error': str(e)}), 500

# Start trading endpoint
@bp.route('/start', methods=['POST'])
def start():
    """
    Starts trading by calling the start_trading function in the controller.
    """
    return jsonify({'message': 'Trading started'}), 200

# Stop trading endpoint
@bp.route('/stop', methods=['POST'])
def stop():
    """
    Stops trading by calling the stop_trading function in the controller.
    """
    return jsonify({'message': 'Trading stopped'}), 200

# Configure settings endpoint
@bp.route('/settings', methods=['GET', 'POST'])
def settings():
    """
    Retrieves or updates settings based on the request method.
    """
    if request.method == 'GET':
        settings_data = {}  # Replace with actual settings retrieval logic
        return jsonify(settings_data), 200
    elif request.method == 'POST':
        new_settings = request.json
        # Add logic to update settings
        return jsonify({'message': f'Settings updated to {new_settings}' }), 200

# Trading performance endpoint
@bp.route('/performance', methods=['GET'])
def performance():
    """
    Retrieves trading performance metrics.
    """
    performance_data = {}  # Replace with actual performance retrieval logic
    return jsonify(performance_data), 200

# Trade history endpoint
@bp.route('/trade-history', methods=['GET'])
def trade_history():
    """
    Retrieves trade history.
    """
    trade_history_data = []  # Replace with actual trade history retrieval logic
    return jsonify(trade_history_data), 200

# Data population route
@dp.route('/populate_data', methods=['POST'])
def populate_data():
    try:
        data_population_service.populate_all_instruments()
        return jsonify({"message": "Data population started"}), 200
    except Exception as e:
        logger.error(f"Error populating data: {e}")
        return jsonify({"error": str(e)}), 500

# Define the create_app function
"""
Initializes the Flask app and registers the blueprints.
"""
def create_app():
    app = Flask(__name__)
    # Register the blueprints
    app.register_blueprint(main, url_prefix='/')
    app.register_blueprint(bp, url_prefix='/api')
    app.register_blueprint(dp, url_prefix='/api/data-population')
    
    # Optionally, print the URL rules to verify routing
    for endpoint in app.url_map.iter_rules():
        print(endpoint)
    
    return app