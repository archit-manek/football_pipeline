from pathlib import Path


def is_source_newer(source_path: Path, output_path: Path) -> bool:
    """
    Check if source file is newer than output file.
    
    Args:
        source_path (Path): Path to the source file
        output_path (Path): Path to the output file
        
    Returns:
        bool: True if source is newer than output or output doesn't exist, False otherwise
    """
    if not output_path.exists():
        return True
    return source_path.stat().st_mtime > output_path.stat().st_mtime