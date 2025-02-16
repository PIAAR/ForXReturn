import requests
import datetime
import pytz
from backend.config.secrets import defs
from backend.logs.log_manager import LogManager

# Initialize the logger
logger = LogManager('oanda_client_logs').get_logger()

class OandaClient:
    def __init__(self, environment='practice'):
        """
        Initializes the OandaClient with the provided environment.

        :parameter environment: The trading environment to use ('live' or 'practice').
        """
        self.environment = environment
        self.base_url = defs.OANDA_URL_D if environment == 'practice' else defs.OANDA_URL_L
        self.headers = defs.SECURE_HEADER
        self.account_id = defs.ACCOUNT_ID

    ## ================================================
    ## ✅ ACCOUNT FUNCTIONS
    ## ================================================
    
    def get_account_summary(self):  # sourcery skip: class-extract-method
        """
        Retrieves account details including balance, margin, and open trades.
        :return: A dictionary containing account summary.
        """
        url = f'{self.base_url}/accounts/{self.account_id}/summary'
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to retrieve account summary: {e}")
            return {}

    def get_account(self):  # sourcery skip: raise-from-previous-error
        """
        Retrieves full account details from OANDA.

        :return: A dictionary containing account details.
        :raises Exception: If request fails (e.g., invalid account).
        """
        url = f'{self.base_url}/accounts/{self.account_id}'
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                raise Exception(f"Invalid account: {self.account_id} - {response.status_code} {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to retrieve account details: {e}")
            raise Exception(f"Invalid account: {self.account_id}")  # ✅ Ensure exception is raised

    ## ================================================
    ## ✅ GET ORDERS FUNCTION (ADDED)
    ## ================================================

    def get_orders(self):
        """
        Retrieves a list of all open and pending orders.
        :return: A dictionary containing open orders.
        """
        url = f"{self.base_url}/accounts/{self.account_id}/orders"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            orders = response.json()
            
            if "orders" not in orders:
                orders["orders"] = []

            logger.info(f"📋 Retrieved {len(orders.get('orders', []))} open/pending orders.")
            return orders
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to retrieve orders: {e}")
            return {"orders": []}

    def get_account_transactions(self, last_trans_id=None):
        """
        Retrieves a list of recent account transactions.
        :param last_trans_id: If provided, fetches transactions since that ID.
        :return: A dictionary containing transactions.
        """
        url = f"{self.base_url}/accounts/{self.account_id}/transactions"
        if last_trans_id:
            url += f"?sinceTransactionID={last_trans_id}"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to retrieve transactions: {e}")
            return None

    ## ================================================
    ## ✅ HISTORICAL MARKET DATA FUNCTIONS
    ## ================================================

    import requests
import datetime
import pytz
from backend.config.secrets import defs
from backend.logs.log_manager import LogManager

# Initialize the logger
logger = LogManager('oanda_client_logs').get_logger()

class OandaClient:
    def __init__(self, environment='practice'):
        """
        Initializes the OandaClient with the provided environment.

        :parameter environment: The trading environment to use ('live' or 'practice').
        """
        self.environment = environment
        self.base_url = defs.OANDA_URL_D if environment == 'practice' else defs.OANDA_URL_L
        self.headers = defs.SECURE_HEADER
        self.account_id = defs.ACCOUNT_ID

    def fetch_historical_data(self, instrument, granularity, start_date=None, end_date=None, count=None):
        # sourcery skip: remove-unreachable-code
        """
        Retrieves historical candle data.

        :param instrument: The instrument to retrieve data for (e.g., 'EUR_USD').
        :param granularity: The granularity of the candle data (e.g., 'M1', 'D', 'H1').
        :param start_date: The starting date for historical data (datetime object, UTC-aware).
        :param end_date: The end date for historical data (datetime object, UTC-aware).
        :param count: The number of candles to retrieve (default: None).
        :return: A list of historical candles.
        """
        logger.info(f"📊 Fetching {instrument} data from OANDA ({granularity})...")

        # Convert dates to UTC format required by OANDA API
        parameters = {"granularity": granularity.upper()}

        if start_date:
            start_date = start_date.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            parameters["from"] = start_date

        if end_date:
            end_date = end_date.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            parameters["to"] = end_date

        # Use count only if start_date and end_date are not provided
        if not start_date and not end_date and count:
            parameters["count"] = count

        url = f'{self.base_url}/instruments/{instrument}/candles'

        try:
            response = requests.get(url, headers=self.headers, params=parameters)
            response.raise_for_status()
            data = response.json()
            
            # Debug: Print raw API response
            print("RAW RESPONSE:", data)  # Debugging

            if 'candles' in data:
                # Extract 'time' from each candle
                formatted_data = [
                    {
                        "time": candle["time"],  # ✅ Ensure 'time' exists
                        "mid": candle["mid"],    # ✅ Keep the OHLC data
                        "volume": candle.get("volume", 0)  # ✅ Handle missing 'volume' field
                    }
                    for candle in data["candles"] if candle["complete"]  # ✅ Only take completed candles
                ]
                
                # Debug: Print formatted data
                print("FORMATTED DATA:", formatted_data[:5])  # Debugging, show first 5 entries

                logger.info(f"✅ Retrieved {len(formatted_data)} candles for {instrument}")
                return formatted_data
            else:
                logger.warning(f"⚠️ No candles found in response: {data}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching {instrument} data: {e}")
            return []

    ## ================================================
    ## ✅ MARKET ORDER & TRADE MANAGEMENT FUNCTIONS
    ## ================================================

    def place_order(self, instrument, units, stop_loss=None, take_profit=None):
        """
        Places a market order to buy or sell an instrument.
        Alias for `place_market_order()` to match test expectations.
        """
        return self.place_market_order(instrument, units, stop_loss, take_profit)

    def place_limit_order(self, instrument, units, price, stop_loss=None, take_profit=None):
        """
        Places a limit order for an instrument at a specific price.

        :param instrument: The trading pair (e.g., "EUR_USD").
        :param units: Positive for buy, negative for sell.
        :param price: The price at which to execute the order.
        :param stop_loss: Stop loss price (optional).
        :param take_profit: Take profit price (optional).
        :return: A dictionary with the trade execution response.
        """
        url = f"{self.base_url}/accounts/{self.account_id}/orders"

        order_data = {
            "order": {
                "instrument": instrument,
                "units": str(units),
                "price": str(price),
                "type": "LIMIT",
                "timeInForce": "GTC"  # Good 'Til Canceled
            }
        }

        # Add stop loss and take profit if provided
        if stop_loss:
            order_data["order"]["stopLossOnFill"] = {"price": str(stop_loss)}
        if take_profit:
            order_data["order"]["takeProfitOnFill"] = {"price": str(take_profit)}

        try:
            response = requests.post(url, json=order_data, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to place limit order: {e}")
            return None

    def place_market_order(self, instrument, units, stop_loss=None, take_profit=None):
        """
        Places a market order to buy or sell an instrument.
        
        :param instrument: The trading pair (e.g., "EUR_USD").
        :param units: Positive for buy, negative for sell.
        :param stop_loss: Stop loss price (optional).
        :param take_profit: Take profit price (optional).
        :return: A dictionary with the trade execution response.
        """
        url = f"{self.base_url}/accounts/{self.account_id}/orders"

        order_data = {
            "order": {
                "instrument": instrument,
                "units": str(units),
                "type": "MARKET",
                "timeInForce": "FOK"  # Fill-or-Kill
            }
        }

        # Add stop loss and take profit if provided
        if stop_loss:
            order_data["order"]["stopLossOnFill"] = {"price": str(stop_loss)}
        if take_profit:
            order_data["order"]["takeProfitOnFill"] = {"price": str(take_profit)}

        try:
            response = requests.post(url, json=order_data, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to place market order: {e}")
            return None

    def close_trade(self, trade_id):
        """
        Closes an open trade by its ID.

        :param trade_id: The ID of the trade to close.
        :return: The response from OANDA.
        """
        url = f"{self.base_url}/accounts/{self.account_id}/trades/{trade_id}/close"

        try:
            response = requests.put(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to close trade {trade_id}: {e}")
            return None

    ## ================================================
    ## ✅ POSITION & ORDER MONITORING FUNCTIONS
    ## ================================================

    def get_positions(self):
        """
        Retrieves all open positions (Alias for `get_open_positions`).
        :return: A dictionary containing open positions.
        """
        return self.get_open_positions()  # Alias to match test expectations

    def get_open_positions(self):  # sourcery skip: extract-method
        """
        Retrieves all open positions.

        :return: A list of open positions.
        """
        url = f"{self.base_url}/accounts/{self.account_id}/positions"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            positions =  response.json()
        
            if "positions" not in positions:
                    positions["positions"] = []

            logger.info(f"📋 Retrieved {len(positions.get('positions', []))} open positions.")
            return positions
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to retrieve open positions: {e}")
            return {"positions": []}

    def get_open_trades(self):
        """
        Retrieves all open trades.

        :return: A list of open trades.
        """
        url = f"{self.base_url}/accounts/{self.account_id}/openTrades"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to retrieve open trades: {e}")
            return None

    ## ================================================
    ## ✅ STREAMING PRICES (IF NEEDED)
    ## ================================================

    def stream_prices(self, instruments):
        """
        Streams live prices for the given instruments.

        :param instruments: A comma-separated string of instrument names (e.g., "EUR_USD,USD_JPY").
        """
        url = f"{self.base_url}/accounts/{self.account_id}/pricing?instruments={instruments}"

        try:
            response = requests.get(url, headers=self.headers, stream=True)
            response.raise_for_status()

            for line in response.iter_lines():
                if line:
                    logger.info(f"📡 Received price update: {line.decode('utf-8')}")

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to stream prices: {e}")

