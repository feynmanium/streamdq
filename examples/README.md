# VTKE Configuration Examples

This directory contains sample configuration files for different use cases.

## Available Presets

### `config_accurate.yaml`
- **Use case**: Production documentation, important recordings
- **Settings**: Balanced FPS (6), strong deduplication
- **Trade-off**: Slower processing, highest quality output

### `config_fast.yaml`
- **Use case**: Quick scans, low-priority content
- **Settings**: Lower FPS (3), aggressive filtering
- **Trade-off**: Faster processing, may miss some content

### `config_debug.yaml`
- **Use case**: Troubleshooting, algorithm tuning
- **Settings**: Saves frames, relaxed thresholds, limited frames
- **Trade-off**: Large output, detailed artifacts for analysis

## Usage

These configuration files demonstrate the available settings. The Streamlit GUI provides
these presets interactively, but you can use these YAML files as reference for
programmatic usage:

```python
import yaml
from pathlib import Path
from vtke.stages.video import VideoSettings
from vtke.stages.assets import AssetSettings
from vtke.stages.mdx import MDXSettings
from vtke.workflow import WorkflowEngine, WorkflowSettings

# Load configuration
with open('examples/config_accurate.yaml') as f:
    config = yaml.safe_load(f)

# Create settings objects
video_settings = VideoSettings(**config['video'])
assets_settings = AssetSettings(**config['assets'])
mdx_settings = MDXSettings(**config['mdx'])

workflow_settings = WorkflowSettings(
    video=video_settings,
    assets=assets_settings,
    mdx=mdx_settings
)

# Run workflow
engine = WorkflowEngine(
    settings=workflow_settings,
    output_root=Path.home() / "VTKE_Runs"
)

results = engine.run(
    video_path=Path("path/to/video.mov")
)
```

## Customization

You can customize any setting. Key parameters:

### Video Processing
- `extraction_fps`: Frames per second to sample (1-15)
- `sharpness_min`: Blur threshold (0-200, lower = more permissive)
- `min_text_length`: Minimum OCR output length in characters
- `jaccard_k`: N-gram size for similarity (5-50)
- `overlap_threshold`: Similarity threshold (0.3-0.9)
- `min_unique_lines`: Required novel lines to add text (0-10)
- `save_frames`: Keep extracted frame images (true/false)
- `ocr_engine`: OCR backend ("tesseract" or "none")
- `language_hint`: Language code for OCR (e.g., "eng", "eng+fra")
- `max_frames`: Limit processing (null = no limit)

### Asset Hygiene
- `mode`: Processing mode ("detect", "rename", or "convert")
- `dry_run`: Preview changes without modifying files (true/false)

### Markdown/Code Extraction
- `include_lang`: Languages to include (empty = all)
- `exclude_lang`: Languages to exclude
- `min_block_lines`: Minimum lines per block
- `max_block_chars`: Maximum characters per block
