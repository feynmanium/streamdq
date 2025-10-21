"""Video processing stage for VTKE."""

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Dict, Any, List

import cv2
import numpy as np

from vtke.stages.ocr import OCRBackend, get_ocr_backend
from vtke.utils.io import ensure_dir, write_json, write_text
from vtke.utils.logging import Logger
from vtke.utils.text import (
    normalize_text,
    text_to_ngrams,
    jaccard_similarity,
    find_overlap_and_novel_tail,
    collapse_blank_lines
)


@dataclass
class VideoSettings:
    """Video processing settings."""
    extraction_fps: float = 6.0
    sharpness_min: float = 40.0
    min_text_length: int = 50
    jaccard_k: int = 20
    overlap_threshold: float = 0.65
    min_unique_lines: int = 2
    save_frames: bool = False
    ocr_engine: str = "tesseract"
    language_hint: Optional[str] = None
    max_frames: Optional[int] = None
    blackframe_threshold: float = 20.0


@dataclass
class FrameInfo:
    """Information about a processed frame."""
    frame_num: int
    sharpness: float
    action: str  # FIRST, ADDED, DUPLICATE, QUALITY, NO_NOVEL
    novel_lines: int = 0
    chars: int = 0


class VideoProcessor:
    """Process video to extract text."""

    def __init__(
        self,
        video_path: Path,
        output_dir: Path,
        settings: VideoSettings,
        logger: Optional[Logger] = None,
        progress_callback: Optional[Callable[[float, str], None]] = None
    ):
        self.video_path = Path(video_path)
        self.output_dir = Path(output_dir)
        self.settings = settings
        self.logger = logger or Logger()
        self.progress_callback = progress_callback

        # Initialize OCR backend
        self.ocr_backend = get_ocr_backend(settings.ocr_engine)

        # Stats
        self.stats = {
            "processed": 0,
            "skipped_blackframe": 0,
            "skipped_quality": 0,
            "skipped_duplicate": 0,
            "skipped_no_novel": 0
        }

        self.frame_infos: List[FrameInfo] = []
        self.accumulated_text: List[str] = []
        self.prev_text = ""

    def _report_progress(self, progress: float, message: str) -> None:
        """Report progress via callback if available."""
        if self.progress_callback:
            self.progress_callback(progress, message)

    def _calculate_sharpness(self, frame: np.ndarray) -> float:
        """Calculate frame sharpness using Laplacian variance.

        Args:
            frame: Input frame

        Returns:
            Sharpness score
        """
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        laplacian = cv2.Laplacian(gray, cv2.CV_64F)
        return float(laplacian.var())

    def _is_blackframe(self, frame: np.ndarray) -> bool:
        """Check if frame is mostly black.

        Args:
            frame: Input frame

        Returns:
            True if blackframe
        """
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        mean_luma = float(np.mean(gray))
        return mean_luma < self.settings.blackframe_threshold

    def _ocr_frame(self, frame: np.ndarray) -> str:
        """Run OCR on frame.

        Args:
            frame: Input frame

        Returns:
            Recognized text
        """
        return self.ocr_backend.recognize_text(frame, self.settings.language_hint)

    def _should_add_text(self, current_text: str) -> tuple[bool, str, int]:
        """Determine if current text should be added.

        Args:
            current_text: Text from current frame

        Returns:
            Tuple of (should_add, text_to_add, novel_line_count)
        """
        if not self.prev_text:
            # First frame with text
            return True, current_text, len(current_text.splitlines())

        # Normalize both texts
        norm_prev = normalize_text(self.prev_text)
        norm_curr = normalize_text(current_text)

        # Calculate Jaccard similarity
        prev_ngrams = text_to_ngrams(norm_prev, self.settings.jaccard_k)
        curr_ngrams = text_to_ngrams(norm_curr, self.settings.jaccard_k)
        similarity = jaccard_similarity(prev_ngrams, curr_ngrams)

        # If below threshold, treat as completely new
        if similarity < self.settings.overlap_threshold:
            return True, current_text, len(current_text.splitlines())

        # Find overlap and extract novel tail
        novel_tail, novel_lines = find_overlap_and_novel_tail(
            self.prev_text,
            current_text,
            self.settings.min_unique_lines
        )

        if novel_lines >= self.settings.min_unique_lines:
            return True, novel_tail, novel_lines

        return False, "", 0

    def run(self) -> Dict[str, Path]:
        """Run video processing.

        Returns:
            Dict of output file paths
        """
        start_time = datetime.now()

        self.logger.info(f"Processing video: {self.video_path}", stage="video")

        # Open video
        cap = cv2.VideoCapture(str(self.video_path))
        if not cap.isOpened():
            raise RuntimeError(f"Cannot open video: {self.video_path}")

        # Get video properties
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        duration_sec = total_frames / fps if fps > 0 else 0

        video_info = {
            "path": str(self.video_path),
            "fps": fps,
            "total_frames": total_frames,
            "width": width,
            "height": height,
            "duration_sec": duration_sec
        }

        self.logger.info(
            f"Video info: {width}x{height}, {fps:.2f} FPS, {total_frames} frames, {duration_sec:.1f}s",
            stage="video"
        )

        # Calculate frame interval for extraction
        frame_interval = max(1, int(fps / self.settings.extraction_fps))
        expected_frames = total_frames // frame_interval

        if self.settings.max_frames:
            expected_frames = min(expected_frames, self.settings.max_frames)

        self.logger.info(
            f"Extracting at {self.settings.extraction_fps} FPS (every {frame_interval} frames), "
            f"expect ~{expected_frames} frames",
            stage="video"
        )

        # Create output directories
        frames_dir = ensure_dir(self.output_dir / "frames") if self.settings.save_frames else None
        transcriptions_dir = ensure_dir(self.output_dir / "transcriptions")
        output_dir = ensure_dir(self.output_dir / "output")

        # Process frames
        frame_num = 0
        extracted_count = 0

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            # Check max frames limit
            if self.settings.max_frames and extracted_count >= self.settings.max_frames:
                break

            # Skip frames based on extraction FPS
            if frame_num % frame_interval != 0:
                frame_num += 1
                continue

            extracted_count += 1
            progress = extracted_count / expected_frames if expected_frames > 0 else 0
            self._report_progress(progress, f"Processing frame {extracted_count}/{expected_frames}")

            # Check for blackframe
            if self._is_blackframe(frame):
                self.stats["skipped_blackframe"] += 1
                frame_num += 1
                continue

            # Calculate sharpness
            sharpness = self._calculate_sharpness(frame)

            if sharpness < self.settings.sharpness_min:
                self.stats["skipped_quality"] += 1
                self.frame_infos.append(FrameInfo(
                    frame_num=frame_num,
                    sharpness=sharpness,
                    action="QUALITY"
                ))
                frame_num += 1
                continue

            # Save frame if requested
            if self.settings.save_frames and frames_dir:
                frame_path = frames_dir / f"frame_{frame_num:06d}.png"
                cv2.imwrite(str(frame_path), frame)

            # Run OCR
            text = self._ocr_frame(frame)

            # Save per-frame transcription
            trans_path = transcriptions_dir / f"frame_{frame_num:06d}.txt"
            write_text(trans_path, text)

            # Check minimum text length
            if len(text) < self.settings.min_text_length:
                self.stats["skipped_quality"] += 1
                self.frame_infos.append(FrameInfo(
                    frame_num=frame_num,
                    sharpness=sharpness,
                    action="QUALITY"
                ))
                frame_num += 1
                continue

            # Determine if we should add this text
            should_add, text_to_add, novel_lines = self._should_add_text(text)

            if should_add:
                if not self.prev_text:
                    action = "FIRST"
                else:
                    action = "ADDED"

                self.accumulated_text.append(text_to_add)
                self.prev_text = text
                self.stats["processed"] += 1

                self.frame_infos.append(FrameInfo(
                    frame_num=frame_num,
                    sharpness=sharpness,
                    action=action,
                    novel_lines=novel_lines,
                    chars=len(text_to_add)
                ))
            else:
                if novel_lines > 0:
                    action = "NO_NOVEL"
                    self.stats["skipped_no_novel"] += 1
                else:
                    action = "DUPLICATE"
                    self.stats["skipped_duplicate"] += 1

                self.frame_infos.append(FrameInfo(
                    frame_num=frame_num,
                    sharpness=sharpness,
                    action=action
                ))

            frame_num += 1

        cap.release()

        # Combine and clean up text
        final_text = "\n".join(self.accumulated_text)
        final_text = collapse_blank_lines(final_text)

        # Write outputs
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")

        text_path = output_dir / f"extracted_text_{timestamp}.txt"
        write_text(text_path, final_text)

        # Create metadata
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        metadata = {
            "video": video_info,
            "run": {
                "timestamp": timestamp,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_sec": duration,
                "extraction_fps": self.settings.extraction_fps,
                "out_dir": str(self.output_dir)
            },
            "stats": self.stats,
            "settings": asdict(self.settings),
            "frames": [asdict(fi) for fi in self.frame_infos]
        }

        metadata_path = output_dir / f"metadata_{timestamp}.json"
        write_json(metadata_path, metadata)

        # Create summary
        total_chars = len(final_text)
        total_words = len(final_text.split())
        total_lines = len(final_text.splitlines())

        summary = f"""Video Processing Summary
{'=' * 50}

Video: {self.video_path.name}
Duration: {duration_sec:.1f}s
Resolution: {width}x{height}
Total Frames: {total_frames}

Extraction Settings:
- FPS: {self.settings.extraction_fps}
- Sharpness Min: {self.settings.sharpness_min}
- Min Text Length: {self.settings.min_text_length}
- Jaccard k: {self.settings.jaccard_k}
- Overlap Threshold: {self.settings.overlap_threshold}
- Min Unique Lines: {self.settings.min_unique_lines}

Processing Stats:
- Processed: {self.stats['processed']}
- Skipped (blackframe): {self.stats['skipped_blackframe']}
- Skipped (quality): {self.stats['skipped_quality']}
- Skipped (duplicate): {self.stats['skipped_duplicate']}
- Skipped (no novel): {self.stats['skipped_no_novel']}

Output:
- Characters: {total_chars:,}
- Words: {total_words:,}
- Lines: {total_lines:,}

Processing Time: {duration:.1f}s

Output Files:
- Text: {text_path.name}
- Metadata: {metadata_path.name}
"""

        summary_path = output_dir / f"summary_{timestamp}.txt"
        write_text(summary_path, summary)

        self.logger.info(f"Completed in {duration:.1f}s", stage="video")
        self.logger.info(f"Processed: {self.stats['processed']}, Skipped: {sum(v for k, v in self.stats.items() if k.startswith('skipped'))}", stage="video")

        return {
            "text": text_path,
            "metadata": metadata_path,
            "summary": summary_path
        }
