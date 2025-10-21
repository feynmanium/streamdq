"""I/O utilities for VTKE."""

import json
import shutil
from pathlib import Path
from typing import Any, Dict


def ensure_dir(path: Path) -> Path:
    """Ensure directory exists.

    Args:
        path: Directory path

    Returns:
        The directory path
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, data: Dict[str, Any], indent: int = 2) -> None:
    """Write data to JSON file.

    Args:
        path: Output file path
        data: Data to write
        indent: JSON indentation level
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


def read_json(path: Path) -> Dict[str, Any]:
    """Read JSON file.

    Args:
        path: Input file path

    Returns:
        Parsed JSON data
    """
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_text(path: Path, text: str) -> None:
    """Write text to file.

    Args:
        path: Output file path
        text: Text to write
    """
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)


def read_text(path: Path, encoding: str = 'utf-8', fallback_encoding: str = 'latin-1') -> str:
    """Read text file with encoding fallback.

    Args:
        path: Input file path
        encoding: Primary encoding to try
        fallback_encoding: Fallback encoding if primary fails

    Returns:
        File contents
    """
    try:
        with open(path, 'r', encoding=encoding) as f:
            return f.read()
    except UnicodeDecodeError:
        with open(path, 'r', encoding=fallback_encoding) as f:
            return f.read()


def get_disk_space(path: Path) -> tuple[int, int, int]:
    """Get disk space information.

    Args:
        path: Path to check

    Returns:
        Tuple of (total, used, free) in bytes
    """
    stat = shutil.disk_usage(path)
    return stat.total, stat.used, stat.free


def get_file_size(path: Path) -> int:
    """Get file size in bytes.

    Args:
        path: File path

    Returns:
        File size in bytes
    """
    return path.stat().st_size if path.exists() else 0
