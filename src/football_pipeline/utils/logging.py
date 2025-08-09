import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logger(log_path, logger_name, console_level=logging.INFO, file_level=logging.DEBUG):
    """
    Setup a logger with both file and console handlers following best practices.
    
    - Detailed logs to files with timestamps
    - Concise progress/info to terminal
    - Replaces log files on each run for clean logs
    
    Args:
        log_path (str|Path): The path to the log file.
        logger_name (str): The name of the logger.
        console_level: Logging level for console output (default: INFO)
        file_level: Logging level for file output (default: DEBUG)
        
    Returns:
        logging.Logger: The configured logger.
    """
    # Ensure log directory exists
    log_path = Path(log_path)
    os.makedirs(log_path.parent, exist_ok=True)
    
    # Add timestamp to log filename but remove old timestamped files first
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_with_timestamp = log_path.parent / f"{log_path.stem}_{timestamp}.log"
    
    # Remove existing log files with the same base name to prevent accumulation
    if log_path.parent.exists():
        for existing_log in log_path.parent.glob(f"{log_path.stem}_*.log"):
            try:
                existing_log.unlink()
            except OSError:
                pass  # Ignore if file is in use or doesn't exist
    
    # Get or create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # Set to lowest level, handlers will filter
    
    # Remove existing handlers to prevent duplicates
    logger.handlers.clear()
    
    # File handler - detailed logs with timestamps
    file_formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler = logging.FileHandler(log_file_with_timestamp, mode="w", encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Console handler - concise progress info
    console_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Log the setup info
    logger.info(f"Logger '{logger_name}' initialized")
    logger.debug(f"Log file: {log_file_with_timestamp}")
    logger.debug(f"Console level: {logging.getLevelName(console_level)}, File level: {logging.getLevelName(file_level)}")
    
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
