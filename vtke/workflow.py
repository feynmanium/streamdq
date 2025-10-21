"""Workflow orchestration for VTKE."""

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable, Dict, Any, List

from vtke.stages.assets import AssetsProcessor, AssetSettings
from vtke.stages.mdx import MarkdownExtractor, MDXSettings
from vtke.stages.video import VideoProcessor, VideoSettings
from vtke.utils.io import ensure_dir, write_json
from vtke.utils.logging import Logger


@dataclass
class WorkflowSettings:
    """Complete workflow settings."""
    # Video settings
    video: VideoSettings
    # Asset settings
    assets: AssetSettings
    # MDX settings
    mdx: MDXSettings


class WorkflowEngine:
    """Orchestrate VTKE workflow."""

    def __init__(
        self,
        settings: WorkflowSettings,
        output_root: Path,
        logger: Optional[Logger] = None,
        progress_callback: Optional[Callable[[str, float, str], None]] = None
    ):
        self.settings = settings
        self.output_root = Path(output_root)
        self.logger = logger or Logger()
        self.progress_callback = progress_callback

        # Create run directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = ensure_dir(self.output_root / f"run_{timestamp}")

        self.results = {}

    def _report_progress(self, stage: str, progress: float, message: str) -> None:
        """Report progress via callback if available."""
        if self.progress_callback:
            self.progress_callback(stage, progress, message)

    def run_video_stage(self, video_path: Path) -> Dict[str, Path]:
        """Run video processing stage.

        Args:
            video_path: Path to video file

        Returns:
            Dict of output paths
        """
        self.logger.info("Starting video processing stage", stage="workflow")

        def video_progress(progress: float, message: str):
            self._report_progress("video", progress, message)

        processor = VideoProcessor(
            video_path=video_path,
            output_dir=self.run_dir,
            settings=self.settings.video,
            logger=self.logger,
            progress_callback=video_progress
        )

        return processor.run()

    def run_assets_stage(self, assets_dir: Path) -> Dict[str, Any]:
        """Run asset hygiene stage.

        Args:
            assets_dir: Directory containing assets

        Returns:
            Asset report
        """
        self.logger.info("Starting asset hygiene stage", stage="workflow")
        self._report_progress("assets", 0.0, "Scanning assets")

        processor = AssetsProcessor(
            assets_dir=assets_dir,
            settings=self.settings.assets,
            logger=self.logger
        )

        result = processor.run()

        self._report_progress("assets", 1.0, "Assets processing complete")

        # Write report
        output_dir = ensure_dir(self.run_dir / "output")
        report_path = output_dir / "assets_report.json"
        write_json(report_path, result)

        result["report_path"] = report_path

        return result

    def run_mdx_stage(self, file_paths: List[Path], output_format: str = "csv") -> Optional[Path]:
        """Run markdown extraction stage.

        Args:
            file_paths: List of markdown files
            output_format: Output format ('csv' or 'parquet')

        Returns:
            Path to output file
        """
        self.logger.info("Starting markdown extraction stage", stage="workflow")
        self._report_progress("mdx", 0.0, "Extracting code blocks")

        extractor = MarkdownExtractor(
            settings=self.settings.mdx,
            logger=self.logger
        )

        df = extractor.extract_from_files(file_paths)

        if df.empty:
            self.logger.warn("No code blocks extracted", stage="workflow")
            return None

        # Write output
        output_dir = ensure_dir(self.run_dir / "output")

        if output_format == "parquet":
            output_path = output_dir / "blocks.parquet"
            df.to_parquet(output_path, index=False)
        else:
            output_path = output_dir / "blocks.csv"
            df.to_csv(output_path, index=False)

        self._report_progress("mdx", 1.0, f"Extracted {len(df)} blocks")

        return output_path

    def run(
        self,
        video_path: Optional[Path] = None,
        assets_dir: Optional[Path] = None,
        mdx_files: Optional[List[Path]] = None,
        mdx_format: str = "csv"
    ) -> Dict[str, Any]:
        """Run complete workflow.

        Args:
            video_path: Optional video file path
            assets_dir: Optional assets directory
            mdx_files: Optional list of markdown files
            mdx_format: Output format for MDX stage

        Returns:
            Dict of results
        """
        start_time = datetime.now()

        self.logger.info(f"Starting workflow in: {self.run_dir}", stage="workflow")

        # Run stages based on inputs
        if video_path:
            self.results["video"] = self.run_video_stage(video_path)

        if assets_dir:
            self.results["assets"] = self.run_assets_stage(assets_dir)

        if mdx_files:
            mdx_output = self.run_mdx_stage(mdx_files, mdx_format)
            if mdx_output:
                self.results["mdx"] = {"output_path": mdx_output}

        # Create workflow summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        summary = {
            "run_dir": str(self.run_dir),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_sec": duration,
            "stages_run": list(self.results.keys()),
            "settings": {
                "video": asdict(self.settings.video),
                "assets": asdict(self.settings.assets),
                "mdx": asdict(self.settings.mdx)
            },
            "results": {}
        }

        # Add stage results (convert Path objects to strings)
        for stage, result in self.results.items():
            if isinstance(result, dict):
                summary["results"][stage] = {
                    k: str(v) if isinstance(v, Path) else v
                    for k, v in result.items()
                }
            else:
                summary["results"][stage] = str(result) if isinstance(result, Path) else result

        # Write workflow summary
        summary_path = self.run_dir / "workflow_summary.json"
        write_json(summary_path, summary)

        self.logger.info(f"Workflow complete in {duration:.1f}s", stage="workflow")

        return {
            "run_dir": self.run_dir,
            "summary_path": summary_path,
            "duration": duration,
            **self.results
        }
