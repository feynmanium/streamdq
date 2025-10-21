"""Image asset hygiene stage for VTKE."""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Optional

from PIL import Image

from vtke.utils.io import write_json
from vtke.utils.logging import Logger


@dataclass
class AssetSettings:
    """Asset hygiene settings."""
    mode: str = "detect"  # detect, rename, convert
    dry_run: bool = True


@dataclass
class AssetReport:
    """Report for a single asset."""
    path: str
    real_type: Optional[str]
    extension: str
    mislabeled: bool
    action: str
    output_path: Optional[str] = None
    error: Optional[str] = None


class AssetsProcessor:
    """Process image assets for hygiene issues."""

    def __init__(
        self,
        assets_dir: Path,
        settings: AssetSettings,
        logger: Optional[Logger] = None
    ):
        self.assets_dir = Path(assets_dir)
        self.settings = settings
        self.logger = logger or Logger()
        self.reports: List[AssetReport] = []

    def _detect_image_type(self, path: Path) -> Optional[str]:
        """Detect actual image type by reading header.

        Args:
            path: Image file path

        Returns:
            Image format (e.g., 'JPEG', 'PNG') or None if cannot determine
        """
        try:
            with Image.open(path) as img:
                return img.format
        except Exception as e:
            self.logger.warn(f"Cannot detect type for {path}: {e}", stage="assets")
            return None

    def _is_mislabeled(self, path: Path, real_type: Optional[str]) -> bool:
        """Check if file is mislabeled.

        Args:
            path: File path
            real_type: Detected image type

        Returns:
            True if mislabeled
        """
        if not real_type:
            return False

        extension = path.suffix.lower()
        real_type_lower = real_type.lower()

        # Common mismatches
        if extension == '.png' and real_type_lower == 'jpeg':
            return True
        if extension in ['.jpg', '.jpeg'] and real_type_lower == 'png':
            return True

        return False

    def _rename_file(self, path: Path, real_type: str) -> Optional[Path]:
        """Rename file to match actual type.

        Args:
            path: Original file path
            real_type: Detected image type

        Returns:
            New path if renamed, None otherwise
        """
        if self.settings.dry_run:
            return None

        # Determine new extension
        if real_type.lower() == 'jpeg':
            new_ext = '.jpg'
        else:
            new_ext = f'.{real_type.lower()}'

        new_path = path.with_suffix(new_ext)

        try:
            path.rename(new_path)
            return new_path
        except Exception as e:
            self.logger.error(f"Failed to rename {path} to {new_path}: {e}", stage="assets")
            return None

    def _convert_file(self, path: Path, target_type: str) -> Optional[Path]:
        """Convert file to match extension.

        Args:
            path: Original file path
            target_type: Target image type

        Returns:
            New path if converted, None otherwise
        """
        if self.settings.dry_run:
            return None

        # Create output path with _converted suffix
        stem = path.stem
        new_path = path.parent / f"{stem}_converted{path.suffix}"

        try:
            with Image.open(path) as img:
                # Convert RGBA to RGB for JPEG
                if target_type.upper() == 'JPEG' and img.mode == 'RGBA':
                    rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                    rgb_img.paste(img, mask=img.split()[3])
                    rgb_img.save(new_path, 'JPEG', quality=95)
                else:
                    img.save(new_path, target_type.upper())

            return new_path
        except Exception as e:
            self.logger.error(f"Failed to convert {path}: {e}", stage="assets")
            return None

    def _process_file(self, path: Path) -> AssetReport:
        """Process a single file.

        Args:
            path: File path

        Returns:
            Asset report
        """
        extension = path.suffix.lower()

        # Detect actual type
        real_type = self._detect_image_type(path)

        # Check if mislabeled
        mislabeled = self._is_mislabeled(path, real_type)

        if not mislabeled:
            return AssetReport(
                path=str(path),
                real_type=real_type,
                extension=extension,
                mislabeled=False,
                action="OK"
            )

        # Handle based on mode
        action = self.settings.mode.upper()
        output_path = None
        error = None

        if self.settings.mode == "rename" and not self.settings.dry_run:
            output_path = self._rename_file(path, real_type)
            if output_path:
                action = "RENAMED"
            else:
                action = "RENAME_FAILED"
                error = "Failed to rename"

        elif self.settings.mode == "convert" and not self.settings.dry_run:
            # Convert to match extension
            target_ext = extension[1:]  # Remove dot
            output_path = self._convert_file(path, target_ext.upper())
            if output_path:
                action = "CONVERTED"
            else:
                action = "CONVERT_FAILED"
                error = "Failed to convert"

        return AssetReport(
            path=str(path),
            real_type=real_type,
            extension=extension,
            mislabeled=True,
            action=action if not self.settings.dry_run else f"WOULD_{action}",
            output_path=str(output_path) if output_path else None,
            error=error
        )

    def run(self) -> Dict[str, Any]:
        """Run asset hygiene processing.

        Returns:
            Dict with report data
        """
        self.logger.info(f"Scanning assets in: {self.assets_dir}", stage="assets")
        self.logger.info(f"Mode: {self.settings.mode}, Dry run: {self.settings.dry_run}", stage="assets")

        # Find image files
        image_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.bmp'}
        image_files = [
            f for f in self.assets_dir.rglob('*')
            if f.is_file() and f.suffix.lower() in image_extensions
        ]

        self.logger.info(f"Found {len(image_files)} image files", stage="assets")

        # Process each file
        for path in image_files:
            report = self._process_file(path)
            self.reports.append(report)

        # Summary stats
        total = len(self.reports)
        mislabeled = sum(1 for r in self.reports if r.mislabeled)
        renamed = sum(1 for r in self.reports if r.action == "RENAMED")
        converted = sum(1 for r in self.reports if r.action == "CONVERTED")

        self.logger.info(
            f"Total: {total}, Mislabeled: {mislabeled}, Renamed: {renamed}, Converted: {converted}",
            stage="assets"
        )

        return {
            "total": total,
            "mislabeled": mislabeled,
            "renamed": renamed,
            "converted": converted,
            "reports": [
                {
                    "path": r.path,
                    "real_type": r.real_type,
                    "extension": r.extension,
                    "mislabeled": r.mislabeled,
                    "action": r.action,
                    "output_path": r.output_path,
                    "error": r.error
                }
                for r in self.reports
            ]
        }
