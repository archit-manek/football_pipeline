"""Configuration management for the football pipeline."""

# Import the main functions for convenience
from .loader import (
    load_config,
    get_processing_config,
    get_logging_config,
    get_directories_config
)

__all__ = [
    'load_config',
    'get_processing_config',
    'get_logging_config', 
    'get_directories_config'
]
