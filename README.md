# VTKE - Video to Text Knowledge Extractor

**Version:** 1.0.0
**Local-first tool for extracting text from videos, sanitizing image assets, and extracting code from markdown.**

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

---

## Overview

VTKE is a comprehensive toolkit for knowledge extraction from various media formats. It provides:

1. **Video → Text**: Extract and deduplicate text from screen recording videos
2. **Asset Hygiene**: Detect and fix mislabeled image files
3. **Code Extraction**: Parse and extract code blocks from markdown files

All processing happens **locally** - no data leaves your machine.

---

## Features

### Video Processing
- Smart Frame Extraction with configurable FPS-based sampling
- Quality Filtering with automatic blackframe and blur detection
- OCR via Tesseract with language hints
- Intelligent Deduplication using N-gram Jaccard similarity + overlap detection
- Incremental Stitching to avoid duplicates

### Asset Hygiene
- Magic Header Detection to identify actual file types
- Safe Operations with rename or convert modes
- Dry Run Mode to preview changes
- Comprehensive JSON Reports

### Markdown/Code Extraction
- Extract from fenced (```) and indented code blocks
- Language Filtering (include/exclude)
- Hash-based Deduplication
- Export to CSV or Parquet

---

## Installation

### Prerequisites

**Required:**
- Python 3.10 or higher

**Optional:**
- Tesseract OCR (for video text extraction)
- ffmpeg (for advanced video probing)

### macOS/Linux Installation

```bash
# Install Tesseract and ffmpeg (optional)
brew install tesseract ffmpeg  # macOS
# OR
sudo apt-get install tesseract-ocr ffmpeg  # Debian/Ubuntu

# Install Python dependencies
pip install -r requirements.txt
```

---

## Quick Start

### Launch the GUI

```bash
streamlit run app.py
```

Then open your browser to `http://localhost:8501`

### Workflow

1. Go to **Preflight Checks** to verify dependencies
2. Navigate to **Configure & Run**
3. Select your input type (Video, Assets, or Markdown)
4. Configure settings or use a preset
5. Click **Start Processing**
6. View results in the **Results** tab

---

## Usage

### Video Processing

**Output Files** (saved to `~/VTKE_Runs/run_<timestamp>/`):

```
run_20251021_143022/
├── frames/                    # Optional: extracted frames
├── transcriptions/            # Per-frame OCR outputs
└── output/
    ├── extracted_text_*.txt   # Final combined text
    ├── metadata_*.json        # Processing metadata
    └── summary_*.txt          # Human-readable summary
```

**Deduplication Algorithm:**

1. **Jaccard Pre-filter**: Computes n-grams, checks similarity
2. **Overlap Detection**: Extracts only novel content using difflib

### Asset Hygiene

Detects files with mismatched extensions (e.g., `.png` files that are actually JPEGs).

**Modes:**
- `detect`: Report only
- `rename`: Fix extensions
- `convert`: Create corrected copies

### Markdown/Code Extraction

Extracts code blocks and exports to CSV/Parquet with schema:

| Column | Description |
|--------|-------------|
| file | Source file path |
| language | Programming language |
| code | Code content |
| start_line | Start line (1-based) |
| end_line | End line |
| block_index | Block number |
| code_hash | SHA256 hash |

---

## Configuration

### Presets

- **Accurate**: Best quality, slower (FPS: 6, strong dedup)
- **Fast**: Quick scans (FPS: 3, aggressive filtering)
- **Debug**: Saves frames, relaxed thresholds

### Advanced Settings

All settings customizable via GUI or code:

```python
from vtke.stages.video import VideoSettings

settings = VideoSettings(
    extraction_fps=6.0,
    sharpness_min=40.0,
    min_text_length=50,
    jaccard_k=20,
    overlap_threshold=0.65,
    min_unique_lines=2,
    save_frames=False,
    ocr_engine="tesseract",
    language_hint="eng"
)
```

---

## Architecture

### Project Structure

```
streamdq/
├── app.py              # Streamlit GUI
├── vtke/
│   ├── workflow.py     # Orchestration
│   ├── stages/         # Processing stages
│   │   ├── video.py
│   │   ├── assets.py
│   │   ├── mdx.py
│   │   └── ocr.py
│   └── utils/          # Utilities
└── tests/              # Unit tests
```

### Design Principles

- **Modularity**: Independent, composable stages
- **Extensibility**: Plugin architecture
- **Observability**: Structured logging
- **Safety**: Non-destructive defaults
- **Testability**: Clear interfaces

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=vtke --cov-report=html
```

---

## Troubleshooting

### Tesseract Not Found
```bash
brew install tesseract  # macOS
sudo apt-get install tesseract-ocr  # Linux
```

### Low OCR Quality
1. Increase `extraction_fps`
2. Lower `sharpness_min`
3. Try different `language_hint`
4. Enable `save_frames` to inspect quality

### Too Many Duplicates
1. Increase `overlap_threshold` (0.70-0.75)
2. Increase `min_unique_lines` (3-5)

### Missing Content
1. Increase `extraction_fps` (8-10)
2. Lower quality thresholds
3. Check blackframe detection

---

## Development

```bash
# Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run tests
pytest

# Format code
black .

# Lint
ruff check .
```

---

## FAQ

**Q: Does VTKE send data to the cloud?**
A: No. All processing is local.

**Q: What video formats are supported?**
A: Any format supported by OpenCV (MP4, MOV, AVI, MKV, etc.)

**Q: Can I use VTKE without Tesseract?**
A: Yes, but OCR will be disabled.

**Q: Where are outputs saved?**
A: `~/VTKE_Runs/run_<timestamp>/`

---

## License

MIT License

---

Built with Python, Streamlit, OpenCV, Tesseract, and Pandas.