"""OCR backend interface and implementations."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import cv2
import numpy as np


class OCRBackend(ABC):
    """Abstract base class for OCR backends."""

    @abstractmethod
    def recognize_text(self, image: np.ndarray, language_hint: Optional[str] = None) -> str:
        """Recognize text from image.

        Args:
            image: Image as numpy array (BGR format)
            language_hint: Optional language hint (e.g., 'eng', 'eng+fra')

        Returns:
            Recognized text
        """
        pass


class TesseractBackend(OCRBackend):
    """Tesseract OCR backend."""

    def __init__(self):
        try:
            import pytesseract
            self.pytesseract = pytesseract
            self.available = True
        except ImportError:
            self.available = False
            raise ImportError("pytesseract not available. Install with: pip install pytesseract")

    def recognize_text(self, image: np.ndarray, language_hint: Optional[str] = None) -> str:
        """Recognize text using Tesseract.

        Args:
            image: Image as numpy array (BGR format)
            language_hint: Language code (e.g., 'eng', 'eng+fra')

        Returns:
            Recognized text
        """
        if not self.available:
            return ""

        # Convert BGR to RGB for Tesseract
        rgb_image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

        config = '--psm 6'  # Assume uniform block of text
        if language_hint:
            text = self.pytesseract.image_to_string(rgb_image, lang=language_hint, config=config)
        else:
            text = self.pytesseract.image_to_string(rgb_image, config=config)

        return text.strip()


class NullBackend(OCRBackend):
    """Null OCR backend (for testing without OCR)."""

    def recognize_text(self, image: np.ndarray, language_hint: Optional[str] = None) -> str:
        """Return empty string (no OCR)."""
        return ""


def get_ocr_backend(engine: str = "tesseract") -> OCRBackend:
    """Get OCR backend by name.

    Args:
        engine: Backend name ('tesseract', 'none')

    Returns:
        OCR backend instance

    Raises:
        ValueError: If engine is unknown
    """
    if engine == "tesseract":
        try:
            return TesseractBackend()
        except ImportError:
            return NullBackend()
    elif engine == "none":
        return NullBackend()
    else:
        raise ValueError(f"Unknown OCR engine: {engine}")
