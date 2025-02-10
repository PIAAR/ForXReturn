from backend.trading.strategies import strategy
from backend.trading.optimizers import optimizer

class TradeManager:
    """
    Manages trading activities, including initialization, termination, and status tracking.
    """

    def __init__(self):
        """
        Initializes the TradeManager instance.
        """
        self.is_trading_active = False

    def initialize(self):
        """
        Initializes the trading manager by setting up strategies and optimizers.
        """
        if not self.is_trading_active:
            strategy.setup_strategy()  # Setup trading strategy
            optimizer.setup_optimizer()  # Setup trading optimizer
            self.is_trading_active = True
            print("✅ Trading manager initialized.")

    def terminate(self):
        """
        Terminates the trading manager and cleans up resources.
        """
        if self.is_trading_active:
            strategy.teardown_strategy()  # Teardown trading strategy
            optimizer.teardown_optimizer()  # Teardown trading optimizer
            self.is_trading_active = False
            print("❌ Trading manager terminated.")

    def is_running(self):
        """
        Checks if the trading manager is currently running.

        :return: True if trading is active, False otherwise.
        """
        return self.is_trading_active


# Example usage
if __name__ == "__main__":
    trade_manager = TradeManager()
    
    # Initialize trading
    trade_manager.initialize()
    
    # Check status
    print("Trading Active:", trade_manager.is_running())

    # Terminate trading
    trade_manager.terminate()

    # Check status again
    print("Trading Active:", trade_manager.is_running())
