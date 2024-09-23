import concurrent.futures
from trading.optimizers.backtester import Backtester
from trading.optimizers.optimizer import Optimizer

class MultithreadedOptimizer:
    def __init__(self, optimizer):
        self.optimizer = optimizer

    def run_parallel_optimization(self, param_sets, max_workers=4):
        """
        Run multiple parameter optimizations in parallel using ThreadPoolExecutor.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.optimizer.optimize_parameters, "EUR_USD", param_set) for param_set in param_sets]
            results = [f.result() for f in futures]
        return results

# Example usage
if __name__ == "__main__":
    backtester = Backtester()
    backtester.load_data("EUR_USD", granularity="D", source="sqlite")
    
    optimizer = Optimizer(backtester)
    multithreaded_optimizer = MultithreadedOptimizer(optimizer)

    # Define multiple parameter sets to test
    param_sets = [{'period': 10}, {'period': 20}, {'period': 30}]

    # Run optimization in parallel
    multithreaded_optimizer.run_parallel_optimization(param_sets)
