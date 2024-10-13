# backend/api/controllers/multithreading_controller.py

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from api.services.trading_services import TradingService
from api.services.data_population_service import DataPopulationService
from trading.optimizers.backtester import Backtester
from trading.optimizers.optimizer import Optimizer
from api.services.state_machine import StateMachine
from logs.log_manager import LogManager

# Initialize logging
logger = LogManager('multithreading_controller')

class MultithreadingController:
    def __init__(self, max_workers=5):
        """
        Initializes the multithreading controller with a thread pool.
        :parameter max_workers: Maximum number of worker threads to run concurrently.
        """
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.lock = Lock()  # For thread-safe access to shared resources
        
        # Initialize services and other components
        self.trading_service = TradingService()
        self.data_population_service = DataPopulationService()
        self.backtester = Backtester()
        self.optimizer = Optimizer()
        self.state_machine = StateMachine()

        # Keep track of running futures
        self.futures = []

    def _run_task(self, task, *args, **kwargs):
        """
        Wrapper to run a task within a thread and log errors.
        """
        try:
            task_name = task.__name__
            logger.info(f"Starting task: {task_name}")
            result = task(*args, **kwargs)
            logger.info(f"Task {task_name} completed with result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error in task {task.__name__}: {e}")
            return None

    def run_backtest(self, strategy_name, instrument, timeframe):
        """
        Schedules a backtest to run in a separate thread.
        :parameter strategy_name: Name of the trading strategy.
        :parameter instrument: Forex instrument (e.g., 'EUR_USD').
        :parameter timeframe: Timeframe to run the backtest on.
        """
        future = self.executor.submit(self._run_task, self.backtester.run, strategy_name, instrument, timeframe)
        self.futures.append(future)

    def run_optimizer(self, instrument, parameters):
        """
        Schedules an optimization task to run in a separate thread.
        :parameter instrument: The instrument being optimized.
        :parameter parameters: Optimization parameters.
        """
        future = self.executor.submit(self._run_task, self.optimizer.optimize, instrument, parameters)
        self.futures.append(future)

    def update_historical_data(self):
        """
        Schedules a data population task to run in a separate thread.
        """
        future = self.executor.submit(self._run_task, self.data_population_service.populate_all_instruments)
        self.futures.append(future)

    def run_state_machine(self, data):
        """
        Schedules the state machine to process the provided data in a separate thread.
        """
        future = self.executor.submit(self._run_task, self.state_machine.run_state_machine, data)
        self.futures.append(future)

    def monitor_tasks(self):
        """
        Monitor the progress of the tasks and handle completion of futures.
        """
        logger.info("Monitoring tasks...")
        for future in as_completed(self.futures):
            try:
                result = future.result()
                logger.info(f"Task completed with result: {result}")
            except Exception as e:
                logger.error(f"Error during task execution: {e}")
        
        # Clean up finished tasks
        self.futures = [f for f in self.futures if not f.done()]
    
    def shutdown(self):
        """
        Shuts down the thread pool executor, allowing all running tasks to complete.
        """
        logger.info("Shutting down the multithreading controller...")
        self.executor.shutdown(wait=True)

# Example usage of the multithreading controller
if __name__ == "__main__":
    controller = MultithreadingController(max_workers=10)
    
    # Schedule various tasks
    controller.run_backtest('RSI', 'EUR_USD', '1H')
    controller.run_optimizer('EUR_USD', {'ATR': 14, 'EMA': 200})
    controller.update_historical_data()

    # Monitor and wait for tasks to complete
    controller.monitor_tasks()

    # Shutdown when done
    controller.shutdown()
