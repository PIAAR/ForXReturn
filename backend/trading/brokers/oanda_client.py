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

    def fetch_historical_data(self, instrument, granularity, count=5000):
        """
        Retrieves historical candle data using max count instead of date ranges.

        :parameter instrument: The instrument to retrieve candle data for (e.g., 'EUR_USD').
        :parameter granularity: The granularity of the candle data (e.g., 'M1', 'D', 'H1').
        :parameter count: The number of candles to retrieve (default: 5000, max for OANDA).
        :return: A list of all historical candles.
        """
        logger.info(f"📊 Fetching {instrument} data from OANDA ({granularity}) with {count} candles...")

        parameters = {
            "granularity": granularity.upper(),
            "count": count  # Max data in one request
        }

        url = f'{self.base_url}/instruments/{instrument}/candles'

        try:
            response = requests.get(url, headers=self.headers, params=parameters)
            response.raise_for_status()
            data = response.json()

            if 'candles' in data:
                logger.info(f"✅ Retrieved {len(data['candles'])} candles for {instrument}")
                return data['candles']
            else:
                logger.warning(f"⚠️ No candles found in response: {data}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching {instrument} data: {e}")
            return []
