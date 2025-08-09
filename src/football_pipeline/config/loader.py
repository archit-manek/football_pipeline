"""Configuration loader for the football pipeline."""

import yaml
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: Path | None = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, uses default config.
        
    Returns:
        Dict containing configuration settings
    """
    if config_path is None:
        config_path = Path(__file__).parent / "pipeline_config.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def get_processing_config() -> Dict[str, Any]:
    """Get processing configuration settings."""
    config = load_config()
    return config.get('processing', {})


def get_logging_config() -> Dict[str, Any]:
    """Get logging configuration settings."""
    config = load_config()
    return config.get('logging', {})





def get_directories_config() -> Dict[str, Any]:
    """Get directories configuration settings."""
    config = load_config()
    return config.get('directories', {})



