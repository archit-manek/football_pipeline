import logging
import os

def setup_logger(log_path, logger_name):
    """
    Setup a logger to write to a file.

    Args:
        log_path (str): The path to the log file.
        logger_name (str): The name of the logger.

    Returns:
        logging.Logger: The logger.
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Remove all handlers associated with the logger (prevents duplicates)
    logger.handlers.clear()

    # Add file handler
    fh = logging.FileHandler(log_path, mode="w")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

class NullLogger:
    """
    A logger that does nothing. Useful for when you want to suppress logging.
    """
    def info(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
    def debug(self, *args, **kwargs): pass
    def exception(self, *args, **kwargs): pass
