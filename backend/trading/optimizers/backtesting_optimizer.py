import sqlite3
from backtesting import Backtest, Strategy  # Assuming Backtesting.py is used
from backtesting.lib import crossover
from threading import Thread

class Backtester:
    def __init__(self, db_path='backend/data/repositories/databases/optimizer.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def backtest_indicator(self, indicator_strategy, data):
        """
        Backtests a given strategy using Backtesting.py and returns the performance metrics.
        :parameter indicator_strategy: The strategy class to be backtested (inherits from Backtesting.py's Strategy).
        :parameter data: DataFrame containing historical data for backtesting.
        :return: Results of the backtest including metrics like Sharpe ratio, total return, etc.
        """
        bt = Backtest(data, indicator_strategy, cash=10000, commission=0.002)
        return bt.run()

    def fetch_indicator_params(self, indicator_name):
        """
        Fetch the parameters for a given indicator from the SQLite database.
        :parameter indicator_name: The name of the indicator.
        :return: Dictionary of parameters for the indicator.
        """
        self.cursor.execute("SELECT key, value FROM indicator_parameters WHERE indicator_id = (SELECT id FROM indicators WHERE name=?)", (indicator_name,))
        return dict(self.cursor.fetchall())

    def update_parameters_in_db(self, indicator_name, optimized_params):
        """
        Update the optimized parameters for a given indicator in the SQLite database.
        :parameter indicator_name: The name of the indicator.
        :parameter optimized_params: Dictionary of optimized parameters.
        """
        for key, value in optimized_params.items():
            self.cursor.execute(
                "UPDATE indicator_parameters SET value = ? WHERE key = ? AND indicator_id = (SELECT id FROM indicators WHERE name=?)",
                (value, key, indicator_name)
            )
        self.conn.commit()

    def run_backtest_and_optimize(self, indicator_name, strategy_class, data):
        """
        Runs a backtest on a strategy, optimizes its parameters, and updates the database.
        :parameter indicator_name: The name of the indicator (e.g., 'SMA').
        :parameter strategy_class: The strategy class for the indicator.
        :parameter data: Data for the backtest.
        """
        # 1. Fetch current parameters from DB
        params = self.fetch_indicator_params(indicator_name)

        # 2. Run the backtest
        print(f"Running backtest for {indicator_name}...")
        stats = self.backtest_indicator(strategy_class, data)

        # 3. Optimize the parameters based on the backtest results
        optimized_params = self.optimize_parameters(params, stats)

        # 4. Update the database with the optimized parameters
        self.update_parameters_in_db(indicator_name, optimized_params)

        print(f"Optimization complete for {indicator_name}. Parameters updated in DB.")

    def optimize_parameters(self, params, stats):
        """
        Optimizes parameters based on backtest results (e.g., Sharpe ratio).
        :parameter params: The original parameters.
        :parameter stats: Backtest statistics (e.g., Sharpe ratio, total return).
        :return: Optimized parameters.
        """
        return {
            parameter: (
                float(value) * 1.1
                if stats['Sharpe Ratio'] > 1
                else float(value) * 0.9
            )
            for parameter, value in params.items()
        }


# Multithreading to run backtest and optimization in parallel
def run_backtest_optimizer_in_thread(indicator_name, strategy_class, data):
    backtester = Backtester()
    optimizer_thread = Thread(target=backtester.run_backtest_and_optimize, args=(indicator_name, strategy_class, data))
    optimizer_thread.start()

# Example usage
if __name__ == "__main__":
    from backtesting.test import GOOG
    from trading.indicators.sma import SMA

    run_backtest_optimizer_in_thread('SMA', SMA, GOOG)