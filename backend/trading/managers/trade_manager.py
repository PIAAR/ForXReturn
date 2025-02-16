from backend.trading.strategies import strategy
from backend.trading.optimizers import optimizer
from backend.trading.brokers.oanda_client import OandaClient

class TradeManager:
    """
    Manages trading activities, including initialization, termination, and status tracking.
    """
    is_trading_active = False 

    @classmethod
    def initialize(cls):
        """
        Initializes the trading manager by setting up strategies and optimizers.
        """
        if not cls.is_trading_active:
            strategy.setup_strategy()  # Setup trading strategy
            optimizer.setup_optimizer()  # Setup trading optimizer
            cls.is_trading_active = True
            print("✅ Trading manager initialized.")

    @classmethod
    def terminate(cls):
        """
        Terminates the trading manager and cleans up resources.
        """
        if cls.is_trading_active:
            strategy.teardown_strategy()  # Teardown trading strategy
            optimizer.teardown_optimizer()  # Teardown trading optimizer
            cls.is_trading_active = False
            print("❌ Trading manager terminated.")

    @classmethod
    def is_running(cls):
        """
        Checks if the trading manager is currently running.

        :return: True if trading is active, False otherwise.
        """
        return cls.is_trading_active

    @classmethod
    def place_trade(cls, instrument, units, stop_loss=None, take_profit=None):
        """
        Places a market trade via OANDA.
        """
        if not cls.is_trading_active:
            print("❌ TradeManager is not running. Start trading first.")
            return None

        oanda = OandaClient()
        return oanda.place_market_order(instrument, units, stop_loss, take_profit)
    
# Example usage
if __name__ == "__main__":
    
    # Initialize trading
    TradeManager.initialize()
    
    # Check status
    print("Trading Active:", TradeManager.is_running())

    # Terminate trading
    TradeManager.terminate()

    # Check status again
    print("Trading Active:", TradeManager.is_running())
