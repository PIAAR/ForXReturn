# backend/api/controllers/multithreading_controller.py
import concurrent.futures
from trading.optimizers.backtester import Backtester
from trading.optimizers.optimizer import Optimizer
from api.services.data_population_service import DataPopulationService
from api.services.state_machine import StateMachine
from logs.log_manager import LogManager

# Initialize logging
logger = LogManager('multithreading_controller')

class MultithreadingController:
    def __init__(self, max_workers=5):
        """
        Initializes the multithreading controller with a thread pool.
        :param max_workers: Maximum number of worker threads to run concurrently.
        """
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.backtester = Backtester()
        self.optimizer = Optimizer(self.backtester)
        self.state_machine = StateMachine()
        self.data_population_service = DataPopulationService()

    def run_parallel_optimization(self, param_sets):
        """
        Run multiple parameter optimizations in parallel using ThreadPoolExecutor.
        """
        logger.info("Running parallel optimization")
        futures = [self.executor.submit(self.optimizer.optimize_parameters, "EUR_USD", param_set) for param_set in param_sets]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
        return results

    def run_backtest(self, strategy_name, instrument, timeframe):
        """
        Schedules a backtest to run in a separate thread.
        """
        logger.info(f"Running backtest for {strategy_name} on {instrument}")
        future = self.executor.submit(self.backtester.run, strategy_name, instrument, timeframe)
        return future.result()

    def update_historical_data(self):
        """
        Schedules a data population task to run in a separate thread.
        """
        logger.info("Updating historical data")
        future = self.executor.submit(self.data_population_service.populate_all_instruments)
        return future.result()

    def run_state_machine(self, data):
        """
        Schedules the state machine to process the provided data in a separate thread.
        """
        logger.info("Running state machine")
        future = self.executor.submit(self.state_machine.run_state_machine, data)
        return future.result()

    def shutdown(self):
        """
        Shuts down the thread pool executor, allowing all running tasks to complete.
        """
        logger.info("Shutting down the multithreading controller...")
        self.executor.shutdown(wait=True)


# Example usage
if __name__ == "__main__":
    controller = MultithreadingController(max_workers=10)

    # Run parallel optimization
    param_sets = [{'period': 10}, {'period': 20}, {'period': 30}]
    optimization_results = controller.run_parallel_optimization(param_sets)
    print(f"Optimization Results: {optimization_results}")

    # Run a backtest
    backtest_result = controller.run_backtest('RSI', 'EUR_USD', '1H')
    print(f"Backtest Result: {backtest_result}")

    # Update historical data
    controller.update_historical_data()

    # Shutdown when done
    controller.shutdown()
