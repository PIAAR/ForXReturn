from trading.strategies import strategy
from trading.optimizers import optimizer

'''
This module contains the logic for managing trading.
Manages trading activities.
'''

# Global state for managing trading
is_trading_active = False

def initialize():
    """
    Initializes the trading manager by setting up strategies and optimizers.
    """
    global is_trading_active
    if not is_trading_active:
        strategy.setup_strategy()  # Setup trading strategy
        optimizer.setup_optimizer()  # Setup trading optimizer
        is_trading_active = True
        print("Trading manager initialized.")

def terminate():
    """
    Terminates the trading manager and cleans up resources.
    """
    global is_trading_active
    if is_trading_active:
        strategy.teardown_strategy()  # Teardown trading strategy
        optimizer.teardown_optimizer()  # Teardown trading optimizer
        is_trading_active = False
        print("Trading manager terminated.")

def is_running():
    """
    Checks if the trading manager is currently running.

    :return: True if trading is active, False otherwise.
    """
    return is_trading_active
