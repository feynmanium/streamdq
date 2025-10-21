"""Preflight checks for VTKE."""

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any


def check_python_version() -> Dict[str, Any]:
    """Check Python version.

    Returns:
        Dict with status and version info
    """
    version = sys.version_info
    is_ok = version >= (3, 10)

    return {
        "status": "ok" if is_ok else "warn",
        "version": f"{version.major}.{version.minor}.{version.micro}",
        "message": "Python 3.10+ required" if not is_ok else "OK"
    }


def check_tesseract() -> Dict[str, Any]:
    """Check if Tesseract is available.

    Returns:
        Dict with status and version info
    """
    tesseract_path = shutil.which("tesseract")

    if not tesseract_path:
        return {
            "status": "missing",
            "version": None,
            "message": "Tesseract not found. Install with: brew install tesseract (macOS)"
        }

    try:
        result = subprocess.run(
            ["tesseract", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        version_line = result.stdout.split('\n')[0]
        version = version_line.split()[1] if len(version_line.split()) > 1 else "unknown"

        return {
            "status": "ok",
            "version": version,
            "path": tesseract_path,
            "message": "OK"
        }
    except Exception as e:
        return {
            "status": "error",
            "version": None,
            "message": f"Error checking Tesseract: {e}"
        }


def check_ffmpeg() -> Dict[str, Any]:
    """Check if ffmpeg is available.

    Returns:
        Dict with status and version info
    """
    ffmpeg_path = shutil.which("ffmpeg")

    if not ffmpeg_path:
        return {
            "status": "missing",
            "version": None,
            "message": "ffmpeg not found. Install with: brew install ffmpeg (macOS)"
        }

    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        version_line = result.stdout.split('\n')[0]
        version = version_line.split()[2] if len(version_line.split()) > 2 else "unknown"

        return {
            "status": "ok",
            "version": version,
            "path": ffmpeg_path,
            "message": "OK"
        }
    except Exception as e:
        return {
            "status": "error",
            "version": None,
            "message": f"Error checking ffmpeg: {e}"
        }


def check_opencv() -> Dict[str, Any]:
    """Check if OpenCV is available.

    Returns:
        Dict with status and version info
    """
    try:
        import cv2
        return {
            "status": "ok",
            "version": cv2.__version__,
            "message": "OK"
        }
    except ImportError:
        return {
            "status": "missing",
            "version": None,
            "message": "OpenCV not found. Install with: pip install opencv-python"
        }


def check_disk_space(path: Path, required_gb: float = 1.0) -> Dict[str, Any]:
    """Check available disk space.

    Args:
        path: Path to check
        required_gb: Required space in GB

    Returns:
        Dict with status and space info
    """
    try:
        stat = shutil.disk_usage(path)
        free_gb = stat.free / (1024**3)
        is_ok = free_gb >= required_gb

        return {
            "status": "ok" if is_ok else "warn",
            "free_gb": round(free_gb, 2),
            "required_gb": required_gb,
            "message": f"{free_gb:.1f} GB available" if is_ok else f"Low disk space: {free_gb:.1f} GB < {required_gb} GB"
        }
    except Exception as e:
        return {
            "status": "error",
            "free_gb": 0,
            "message": f"Error checking disk space: {e}"
        }


def check_path_writable(path: Path) -> Dict[str, Any]:
    """Check if path is writable.

    Args:
        path: Path to check

    Returns:
        Dict with status and info
    """
    try:
        # Create parent if it doesn't exist
        path.parent.mkdir(parents=True, exist_ok=True)

        # Try to create a temporary file
        test_file = path.parent / ".vtke_write_test"
        test_file.touch()
        test_file.unlink()

        return {
            "status": "ok",
            "message": "Writable"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Not writable: {e}"
        }


def run_preflight_checks(output_path: Path) -> Dict[str, Dict[str, Any]]:
    """Run all preflight checks.

    Args:
        output_path: Planned output directory

    Returns:
        Dict of check results
    """
    return {
        "python": check_python_version(),
        "tesseract": check_tesseract(),
        "ffmpeg": check_ffmpeg(),
        "opencv": check_opencv(),
        "disk_space": check_disk_space(output_path),
        "writable": check_path_writable(output_path)
    }
