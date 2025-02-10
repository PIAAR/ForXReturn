import requests
import datetime
import oandapyV20
import oandapyV20.endpoints.instruments as instruments
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

    def get_account(self):
        """
        Retrieves account details from OANDA.

        :return: A dictionary containing account details.
        """
        url = f'{self.base_url}/accounts/{self.account_id}'
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve account details: {e}")
            raise

    def place_order(self, order_data):
        """
        Places a new order with OANDA.

        :parameter order_data: A dictionary containing order details.
        :return: A dictionary containing the response from OANDA.
        """
        url = f'{self.base_url}/accounts/{self.account_id}/orders'
        try:
            response = requests.post(url, json=order_data, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to place order: {e}")
            raise

    def get_orders(self):
        """
        Retrieves a list of open orders from OANDA.

        :return: A dictionary containing the list of open orders.
        """
        url = f'{self.base_url}/accounts/{self.account_id}/orders'
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve orders: {e}")
            raise

    def get_positions(self):
        """
        Retrieves current positions from OANDA.

        :return: A dictionary containing current positions.
        """
        url = f'{self.base_url}/accounts/{self.account_id}/positions'
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve positions: {e}")
            raise

    def fetch_historical_data(self, instrument, granularity, start_date=None, end_date=None, count=500):
        """
        Retrieves historical candle data in smaller batches to comply with OANDA's API limits.

        :parameter instrument: The instrument to retrieve candle data for (e.g., 'EUR_USD').
        :parameter granularity: The granularity of the candle data (e.g., 'M1', 'D', 'H1').
        :parameter start_date: The starting date for historical data (datetime object, UTC-aware).
        :parameter days: The number of days per batch (default=30).
        :parameter count: The number of candles per request (default=5000).
        :return: A list of all historical candles.
        """
        logger.info(f"📊 Fetching {instrument} data from OANDA ({granularity})...")

        url = f'{self.base_url}/instruments/{instrument}/candles'
        
        # Apply date filtering if provided
        # The code snippet `if start_date:` and `if end_date:` is checking if the `start_date` and `end_date`
        # parameters are provided when calling the `fetch_historical_data` method in the `OandaClient` class.
        # If they are provided, the `fetch_historical_data` method sets the `from` and `to` parameters in the `parameters` dictionary.
        # ✅ Define parameters at the beginning
        
        # Ensure date filtering is valid
        parameters = {"granularity": granularity.upper()}
        
        # Convert now to an offset-aware UTC datetime
        now = datetime.datetime.now().replace(tzinfo=pytz.UTC)  # ✅ Convert to UTC-aware datetime
        
        # Validate `start_date` and `end_date`
        if start_date:
            if not isinstance(start_date, datetime.datetime):
                raise ValueError("start_date must be a datetime object")

            # Convert to UTC-aware datetime if naive
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=pytz.UTC)  # ✅ Ensure UTC-awareness
            
            start_date = min(start_date, now)  # ✅ Ensure start_date is not in the future
            parameters["from"] = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")  # ✅ Correct Format

        if end_date:
            if not isinstance(end_date, datetime.datetime):
                raise ValueError("end_date must be a datetime object")

            # Convert to UTC-aware datetime if naive
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=pytz.UTC)  # ✅ Ensure UTC-awareness
            
            end_date = min(end_date, now)  # ✅ Ensure end_date is not in the future
            parameters["to"] = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")  # ✅ Correct Format

        # Apply `count` only if `start_date` is NOT provided
        if "from" not in parameters:
            parameters["count"] = count  # ✅ Only use `count` if no date range is set

        try:
            response = requests.get(url, headers=self.headers, params=parameters)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            if 'candles' in data:
                logger.info(f"Successfully fetched {len(data['candles'])} candles for {instrument}")
                return data['candles']
            else:
                logger.error(f"Unexpected response format: {data}")
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch historical data for {instrument}: {e}")
            raise
