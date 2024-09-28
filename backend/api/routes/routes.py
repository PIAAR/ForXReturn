# backend/api/routes/routes.py
from flask import Blueprint, Flask, jsonify, request
from logs.log_manager import LogManager
from api.services.trading_services import TradingService
from api.services.data_population_service import DataPopulationService

'''
Creates the Flask app and registers the blueprints. Defines the API routes.
'''

# Initialize the LogManager
logger = LogManager('api_routes')

# Define blueprints for different functionalities
main = Blueprint('main', __name__)
trading_bp = Blueprint('trading', __name__)
data_population_bp = Blueprint('data_population', __name__)

# Initialize services
trading_service = TradingService()
data_population_service = DataPopulationService()

# -------------------- Main Blueprint --------------------
@main.route("/", methods=['GET'])
def main_route():
    """
    Root route for the application.
    """
    return jsonify({'message': 'Welcome to the trading API!'}), 200

# -------------------- Trading Routes --------------------
@trading_bp.route('/start', methods=['POST'])
def start_trading():
    """
    Starts the trading process.
    """
    trading_service.start_trading()
    return jsonify({'status': 'Trading started'}), 200

@trading_bp.route('/stop', methods=['POST'])
def stop_trading():
    """
    Stops the trading process.
    """
    trading_service.stop_trading()
    return jsonify({'status': 'Trading stopped'}), 200

@trading_bp.route('/status', methods=['GET'])
def get_status():
    """
    Retrieves the current status of trading.
    """
    status = trading_service.get_status()
    return jsonify(status), 200

@trading_bp.route('/order', methods=['POST'])
def place_order():
    """
    Places a new trade order.
    """
    trade_data = request.json
    response = trading_service.place_trade(trade_data)
    if response:
        return jsonify(response), 201
    return jsonify({'error': 'Failed to place order'}), 500

@trading_bp.route('/orders', methods=['GET'])
def get_orders():
    """
    Retrieves open orders.
    """
    orders = trading_service.get_orders()
    return jsonify(orders), 200

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
    performance_data = trading_service.get_performance()
    return jsonify(performance_data), 200

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
