import itertools
from sqlite3 import SQLiteDB
from trading.optimizers.backtester import Backtester
from trading.indicators.sma import SMA

class Optimizer:
    def __init__(self, backtester):
        self.backtester = backtester
        self.db_handler = SQLiteDB(db_name="optimizer.db")

    def optimize_parameters(self, instrument, param_combinations):
        """
        Run backtests with different parameter combinations and store the best-performing one.
        """
        best_result = None
        best_params = None

        for params in param_combinations:
            print(f"Testing params: {params}")
            self.backtester.apply_indicator(SMA.calculate, period=params['period'])
            self.backtester.simulate_trades(buy_signal, sell_signal)
            result = self.backtester.calculate_performance()

            if best_result is None or result["total_return"] > best_result["total_return"]:
                best_result = result
                best_params = params

        print(f"Best result: {best_result} with params: {best_params}")
        self.store_optimized_params(instrument, best_params)

    def store_optimized_params(self, instrument, params):
        """
        Store the best-performing parameters in the SQLite database.
        """
        self.db_handler.add_optimized_params(instrument, params)

# Example usage
if __name__ == "__main__":
    backtester = Backtester()

    # Load historical data
    backtester.load_data("EUR_USD", granularity="D", source="sqlite")

    optimizer = Optimizer(backtester)

    # Define parameter combinations to test
    param_combinations = [{'period': 10}, {'period': 20}, {'period': 30}]

    # Optimize the parameters
    optimizer.optimize_parameters("EUR_USD", param_combinations)
