"""Simple constants for the football pipeline."""

from pathlib import Path

def find_project_root() -> Path:
    """Find project root by looking for pyproject.toml."""
    current = Path(__file__).parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    raise FileNotFoundError("Could not find project root")

PROJECT_ROOT = find_project_root()
SUPPORTED_SOURCES = ["open_data", "j1_league"]

# Simple directory structure
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"