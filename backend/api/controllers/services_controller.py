# backend/api/controllers/services_controller.py
from flask import Blueprint, jsonify, request
from trading.brokers.oanda_client import OandaClient
from services.trading_services import TradingService as trading_service
from api.services.data_population_service import DataPopulationService as data_population_service
import os

'''
Handles requests and interacts with services.
The controllers here are responsible for handling incoming HTTP requests, interacting with the service layer, and returning responses. They act as intermediaries between the request and the business logic.
'''

# Blueprints
bp = Blueprint('api', __name__)
dp = Blueprint('data_population', __name__)

@bp.route('/start', methods=['POST'])
def start_trading():
    """
    Starts the trading process by calling the `start_trading` method of the trading service.

    :return: A JSON response indicating that trading has started.
    """
    trading_service.start_trading()
    return jsonify({'status': 'Trading started'}), 200

@bp.route('/stop', methods=['POST'])
def stop_trading():
    """
    Stops the trading process by calling the `stop_trading` method of the trading service.

    :return: A JSON response indicating that trading has stopped.
    """
    trading_service.stop_trading()
    return jsonify({'status': 'Trading stopped'}), 200

@bp.route('/status', methods=['GET'])
def get_status():
    """
    Retrieves the current status of the trading process by calling the `get_status` method of the trading service.

    :return: A JSON response containing the status of the trading process.
    """
    status = trading_service.get_status()
    return jsonify(status), 200

@bp.route('/account', methods=['GET'])
def get_account():
    """
    Retrieves account details from OANDA by calling the `get_account` method of the OandaClient.

    :return: A JSON response containing the account details from OANDA.
    """
    account_info = trading_service.get_account()
    return jsonify(account_info), 200

@bp.route('/order', methods=['POST'])
def place_order():
    """
    Places a new order with OANDA by calling the `place_order` method of the OandaClient.
    The order data is expected to be in the JSON body of the request.

    :return: A JSON response containing the details of the placed order.
    """
    order_data = request.json
    response = OandaClient.place_order(order_data)
    return jsonify(response), 201

@bp.route('/orders', methods=['GET'])
def get_orders():
    """
    Retrieves a list of all open orders from OANDA by calling the `get_orders` method of the OandaClient.

    :return: A JSON response containing the list of open orders.
    """
    orders = OandaClient.get_orders()
    return jsonify(orders), 200

@bp.route('/positions', methods=['GET'])
def get_positions():
    """
    Retrieves current positions from OANDA by calling the `get_positions` method of the OandaClient.

    :return: A JSON response containing the current positions.
    """
    positions = OandaClient.get_positions()
    return jsonify(positions), 200

@bp.route('/candles', methods=['GET'])
def get_candles():
    """
    Retrieves historical candle data for a specific instrument from OANDA by calling the `get_candles` method of the OandaClient.
    The `instrument`, `granularity`, and `count` parameters are expected to be provided as query parameters.

    :return: A JSON response containing the historical candle data.
    """
    instrument = request.args.get('instrument')
    granularity = request.args.get('granularity', 'M1')
    count = int(request.args.get('count', 100))
    candles = OandaClient.get_candles(instrument, granularity, count)
    return jsonify(candles), 200

@bp.route('/populate_data', methods=['POST'])
def populate_data():
    """
    Endpoint to trigger data population for all instruments.
    """
    try:
        data_population_service.populate_all_instruments()
        return jsonify({"message": "Data population started"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
