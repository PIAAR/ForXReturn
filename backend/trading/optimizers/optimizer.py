from datetime import datetime
from backend.logs.log_manager import LogManager
from backend.data.repositories._sqlite_db import SQLiteDBHandler
from backend.trading.optimizers.backtester import Backtester
from backend.trading.indicators.sma import SMA
from backend.trading.indicators.ema import EMA
from backend.trading.indicators.rsi import RSI  # Assuming you have this implemented

# Configure logging
logger = LogManager('optimizer_logs').get_logger()

class Optimizer:
    def __init__(self, backtester):
        """
        Initialize the optimizer with a backtester and SQLiteDBHandler handler.
        """
        self.backtester = backtester
        self.db_handler = SQLiteDBHandler(db_name="optimizer.db")
        logger.info("Optimizer initialized.")

    def optimize_parameters(self, instrument, indicator_func, param_combinations):
        """
        Run backtests with different parameter combinations for a given indicator and store the best-performing one.
        """
        best_result = None
        best_parameters = None

        # Get the correct column name based on the indicator function
        if indicator_func == SMA.calculate:
            indicator_name = 'sma'
        elif indicator_func == EMA.calculate:
            indicator_name = 'ema'
        elif indicator_func == RSI.calculate:
            indicator_name = 'rsi'
        else:
            logger.error("Unknown indicator function.")
            return {}, {}
        
        # Get the IDs for the instrument and the indicator
        instrument_id = self.db_handler.get_instrument_id(instrument)
        indicator_id = self.db_handler.get_indicator_id(indicator_name)

        if not instrument_id or not indicator_id:
            logger.error("Missing instrument or indicator ID, cannot continue optimization.")
            return {}, {}

        for parameters in param_combinations:
            try:
                logger.info(f"Testing parameters: {parameters}")

                # Apply the indicator with the current parameter set
                self.backtester.apply_indicator(indicator_func, **parameters)

                # Since we're using a fixed column name (e.g., 'sma', 'ema', 'rsi'), reference that directly
                indicator_column = indicator_name  # No need to add a period-specific suffix

                if indicator_column not in self.backtester.data.columns:
                    logger.error(f"Column {indicator_column} not found in data. Skipping...")
                    continue

                # Define buy/sell signals based on the calculated indicator
                def buy_signal(row):
                    return row['close'] > row[indicator_column]

                def sell_signal(row):
                    return row['close'] < row[indicator_column]

                # Simulate trades and calculate performance
                self.backtester.simulate_trades(buy_signal, sell_signal)
                result = self.backtester.calculate_performance()

                # Update best result and parameters
                if best_result is None or result["total_return"] > best_result["total_return"]:
                    best_result = result
                    best_parameters = parameters

            except Exception as e:
                logger.error(f"Error during optimization: {e}")

        # Ensure we return the best result and parameters
        if best_result and best_parameters:
            logger.info(f"Best result: {best_result} with parameters: {best_parameters}")
            self.store_optimized_parameters(instrument_id, indicator_id, best_parameters)
            return best_result, best_parameters
        else:
            logger.error("No valid result was found during optimization.")
            return {}, {}

    def save_optimized_parameters(self, instrument_id, indicator_id, parameters):
        """
        Save optimized parameters to the SQLite database.
        
        :parameter instrument_id: The ID of the financial instrument.
        :parameter indicator_id: The ID of the indicator.
        :parameter parameters: Dictionary of the optimized parameters.
        """
        timestamp = datetime.now().isoformat()

        if not indicator_id:
            logger.error("Indicator ID not found in the database.")
            return

        # Insert the parameters into the database
        for parameter_name, parameter_value in parameters.items():
            try:
                self.db_handler.add_optimized_parameters(instrument_id, indicator_id, {parameter_name: parameter_value})
                logger.info(f"Optimized parameters for indicator {indicator_id} on instrument {instrument_id} saved at {timestamp}.")
            except Exception as e:
                logger.error(f"Failed to save optimized parameters for indicator {indicator_id}: {e}")

    def store_optimized_parameters(self, instrument_id, indicator_id, parameters):
        """
        Store the best-performing parameters in the SQLite database.
        
        :parameter instrument_id: The ID of the financial instrument.
        :parameter indicator_id: The ID of the indicator.
        :parameter parameters: Dictionary of optimized parameters.
        """
        try:
            self.save_optimized_parameters(instrument_id, indicator_id, parameters)
            logger.info(f"Optimized parameters for instrument {instrument_id} and indicator {indicator_id} stored in the database.")
        except Exception as e:
            logger.error(f"Error storing optimized parameters for instrument {instrument_id} and indicator {indicator_id}: {e}")

    def run_optimization(self, indicator_func, data, **kwargs):
        """
        Run optimization for a specific indicator and save results.
        :parameter indicator_func: The indicator function to optimize.
        :parameter data: The historical data for backtesting.
        :parameter kwargs: Optimization parameters to be tested.
        """
        # Run the optimization (this can be extended for more complex optimizations)
        optimized_parameters = indicator_func(data, **kwargs)
        
        # Save optimized parameters
        self.save_optimized_parameters(indicator_func.__name__, kwargs)

        return optimized_parameters

# Example usage
if __name__ == "__main__":
    backtester = Backtester()

    # Load historical data
    backtester.load_data("EUR_USD", granularity="D", source="sqlite")

    optimizer = Optimizer(backtester)

    # Define parameter combinations for different indicators
    sma_param_combinations = [{'period': 10}, {'period': 20}, {'period': 30}]
    rsi_param_combinations = [{'period': 14}, {'period': 20}]
    ema_param_combinations = [{'period': 10}, {'period': 20}, {'period': 30}]

    # Optimize SMA parameters
    optimizer.optimize_parameters("EUR_USD", SMA.calculate, sma_param_combinations)

    # Optimize RSI parameters
    optimizer.optimize_parameters("EUR_USD", RSI.calculate, rsi_param_combinations)

    # Optimize EMA parameters
    optimizer.optimize_parameters("EUR_USD", EMA.calculate, ema_param_combinations)
