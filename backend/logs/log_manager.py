import os
import logging

class LogManager:
    def __init__(self, area=None):
        """
        Initializes the LogManager with a specific logging area.
        
        :param area: The area for which to create a logger (e.g., 'oanda_logs', 'controller_logs').
        """
        self.area = area
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """
        Sets up the logger for the specified area.
        
        :return: Configured logger instance.
        """
        # Ensure log directory exists
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Set up logging
        logger = logging.getLogger(self.area)
        logger.setLevel(logging.DEBUG)  # Adjust the logging level as needed
        
        # Create file handler for logging
        log_file = os.path.join(log_dir, f'{self.area}.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Adjust the file logging level as needed
        
        # Create a console handler for logging
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # Adjust the console logging level as needed
        
        # Define the log format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def get_logger(self):
        """
        Returns the configured logger instance.
        
        :return: Logger instance.
        """
        return self.logger

# Example usage:
if __name__ == '__main__':
    # Create loggers for different areas
    oanda_logger = LogManager('oanda_logs').get_logger()
    controller_logger = LogManager('controller_logs').get_logger()
    system_logger = LogManager('system_logs').get_logger()

    # Example log messages
    oanda_logger.info("OANDA logger initialized.")
    controller_logger.error("Controller encountered an error.")
    system_logger.debug("System log message.")
