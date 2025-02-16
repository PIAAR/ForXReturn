import unittest
import datetime
import pytz
from backend.trading.brokers.oanda_client import OandaClient
from backend.config.secrets import defs

class TestOandaClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Set up OandaClient once for all tests.
        """
        cls.oanda = OandaClient(environment="practice")  # Use OANDA practice environment

    def test_fetch_historical_data(self):
        """
        Test fetching historical forex data from OANDA.
        """
        instrument = "EUR_USD"
        granularity = "D"
        count = 500

        candles = self.oanda.fetch_historical_data(instrument, granularity, count=count)
        
        self.assertIsInstance(candles, list, "Data should be a list of candles")
        self.assertGreater(len(candles), 0, "No historical data retrieved")
        self.assertIn('mid', candles[0], "Expected 'mid' price data in response")
        self.assertIn('time', candles[0], "Expected 'time' field in response")

    def test_fetch_historical_data_invalid_symbol(self):
        """
        Test fetching data for an invalid instrument name.
        """
        invalid_instrument = "INVALID_SYMBOL"
        granularity = "D"

        candles = self.oanda.fetch_historical_data(invalid_instrument, granularity, count=100)
        
        self.assertEqual(len(candles), 0, "Invalid instrument should return no data")

    def test_place_market_order(self):
        """
        Test placing a market order.
        """
        order_data = {
            "order": {
                "instrument": "EUR_USD",
                "units": "100",  # Buy 100 units
                "type": "MARKET",
                "timeInForce": "FOK",  # Fill or Kill
                "positionFill": "DEFAULT",
                "stop_loss": 1.0950,       # Optional Stop Loss
                "take_profit": 1.1050      # Optional Take Profit
            }
        }
        
        response = self.oanda.place_market_order(order_data["units"],
            stop_loss=order_data["stop_loss"], take_profit=order_data["take_profit"]
        )
        self.assertIsInstance(response, dict, "Response should be a dictionary.")
        self.assertIn("orderFillTransaction", response, "Response should contain 'orderFillTransaction' key.")
    
    def test_place_limit_order(self):
        """
        Test placing a limit order.
        """
        order_data = {
            "order": {
                "instrument": "EUR_USD",
                "units": "100",  # Buy 100 units
                "type": "LIMIT",
                "timeInForce": "GTC",  # Good Till Canceled
                "price": "1.2000",
                "positionFill": "DEFAULT",
                "stop_loss": 1.0950,       # Optional Stop Loss
                "take_profit": 1.1050      # Optional Take Profit
            }
        }

        response = self.oanda.place_limit_order(
            order_data["instrument"], order_data["units"],
            order_data["price"], stop_loss=order_data["stop_loss"], take_profit=order_data["take_profit"]
        )
        self.assertIsInstance(response, dict, "Response should be a dictionary.")
        self.assertIn("orderCreateTransaction", response, "Response should contain 'orderCreateTransaction' key.")
    
    def test_place_invalid_order(self):
        """
        Test placing an invalid order.
        """
        invalid_order_data = {
            "order": {
                "instrument": "EUR_USD",
                "units": "-99999999999",  # Invalid order size
                "type": "MARKET",
                "timeInForce": "FOK",
                "positionFill": "DEFAULT"
            }
        }

        with self.assertRaises(Exception):
            self.oanda.place_market_order(invalid_order_data)

    def test_get_orders(self):
        """
        Test retrieving active orders.
        """
        orders = self.oanda.get_orders()

        self.assertIsInstance(orders, dict, "Orders should be returned as a dictionary")
        self.assertIn("orders", orders, "Response should contain 'orders' key")

    def test_get_positions(self):
        """
        Test retrieving open positions.
        """
        positions = self.oanda.get_open_positions()

        self.assertIsInstance(positions, dict, "Positions should be returned as a dictionary")
        self.assertIn("positions", positions, "Response should contain 'positions' key")

    def test_get_invalid_account(self):
        """
        Test fetching account details with an invalid account ID.
        """
        invalid_client = OandaClient(environment="practice")
        invalid_client.account_id = "INVALID_ACCOUNT"  # ✅ Force invalid account

        with self.assertRaises(Exception) as context:
            invalid_client.get_account()

        # ✅ Verify exception message contains "Invalid account"
        self.assertIn("Invalid account", str(context.exception), "Exception message should indicate invalid account")

    def test_get_valid_account(self):
        """
        Test fetching valid account details.
        """
        self.oanda.account_id = defs.ACCOUNT_ID  # Reset to valid account

        account_info = self.oanda.get_account()
        
        self.assertIn("account", account_info, "Expected 'account' key in response")

if __name__ == '__main__':
    unittest.main()
