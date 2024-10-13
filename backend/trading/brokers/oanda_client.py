import requests
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

    def fetch_historical_data(self, instrument, granularity='M1', count=500):
        """
        Retrieves historical candle data for a specific instrument from OANDA.

        :parameter instrument: The instrument to retrieve candle data for (e.g., 'EUR_USD').
        :parameter granularity: The granularity of the candle data (e.g., 'M1', 'D').
        :parameter count: The number of candles to retrieve.
        :return: A list of candle data.
        """
        url = f'{self.base_url}/instruments/{instrument}/candles'
        params = {
            'granularity': granularity,
            'count': count
        }
        try:
            response = requests.get(url, headers=self.headers, params=params)
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
