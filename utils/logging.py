import logging
import os

def setup_logger(log_path, logger_name):
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