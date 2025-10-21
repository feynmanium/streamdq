"""VTKE Streamlit GUI Application."""

import glob
from pathlib import Path
from typing import Optional

import streamlit as st

from vtke.stages.assets import AssetSettings
from vtke.stages.mdx import MDXSettings
from vtke.stages.video import VideoSettings
from vtke.utils.logging import Logger
from vtke.utils.preflight import run_preflight_checks
from vtke.workflow import WorkflowEngine, WorkflowSettings


# Page configuration
st.set_page_config(
    page_title="VTKE - Video to Text Knowledge Extractor",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Session state initialization
if 'run_state' not in st.session_state:
    st.session_state.run_state = 'idle'  # idle, running, completed, failed

if 'results' not in st.session_state:
    st.session_state.results = None

if 'logger' not in st.session_state:
    st.session_state.logger = Logger(verbose=False)


# Presets
PRESETS = {
    "Accurate": {
        "extraction_fps": 6.0,
        "sharpness_min": 40.0,
        "min_text_length": 50,
        "jaccard_k": 20,
        "overlap_threshold": 0.65,
        "min_unique_lines": 2,
        "save_frames": False
    },
    "Fast": {
        "extraction_fps": 3.0,
        "sharpness_min": 30.0,
        "min_text_length": 80,
        "jaccard_k": 15,
        "overlap_threshold": 0.70,
        "min_unique_lines": 3,
        "save_frames": False
    },
    "Debug": {
        "extraction_fps": 4.0,
        "sharpness_min": 25.0,
        "min_text_length": 30,
        "jaccard_k": 20,
        "overlap_threshold": 0.60,
        "min_unique_lines": 1,
        "save_frames": True
    }
}


def render_preflight_panel():
    """Render preflight checks panel."""
    st.header("🔍 System Preflight Checks")

    output_root = Path.home() / "VTKE_Runs"

    with st.spinner("Running preflight checks..."):
        checks = run_preflight_checks(output_root)

    # Display checks
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Dependencies")

        # Python
        py_check = checks["python"]
        if py_check["status"] == "ok":
            st.success(f"✅ Python {py_check['version']}")
        else:
            st.warning(f"⚠️ Python {py_check['version']} - {py_check['message']}")

        # OpenCV
        cv_check = checks["opencv"]
        if cv_check["status"] == "ok":
            st.success(f"✅ OpenCV {cv_check['version']}")
        else:
            st.error(f"❌ OpenCV - {cv_check['message']}")

        # Tesseract
        tess_check = checks["tesseract"]
        if tess_check["status"] == "ok":
            st.success(f"✅ Tesseract {tess_check['version']}")
        elif tess_check["status"] == "missing":
            st.info(f"ℹ️ Tesseract not found - OCR will be disabled\n\n{tess_check['message']}")
        else:
            st.warning(f"⚠️ Tesseract - {tess_check['message']}")

        # ffmpeg
        ff_check = checks["ffmpeg"]
        if ff_check["status"] == "ok":
            st.success(f"✅ ffmpeg {ff_check['version']}")
        elif ff_check["status"] == "missing":
            st.info(f"ℹ️ ffmpeg not found (optional)\n\n{ff_check['message']}")
        else:
            st.warning(f"⚠️ ffmpeg - {ff_check['message']}")

    with col2:
        st.subheader("Storage")

        # Disk space
        disk_check = checks["disk_space"]
        if disk_check["status"] == "ok":
            st.success(f"✅ {disk_check['free_gb']} GB available")
        else:
            st.warning(f"⚠️ {disk_check['message']}")

        # Writable
        write_check = checks["writable"]
        if write_check["status"] == "ok":
            st.success(f"✅ Output directory writable")
            st.info(f"📁 Output: {output_root}")
        else:
            st.error(f"❌ {write_check['message']}")

    return checks


def render_wizard():
    """Render the configuration wizard."""
    st.header("🛠️ Configuration Wizard")

    # Tabs for different input types
    tab1, tab2, tab3, tab4 = st.tabs(["📹 Video", "🖼️ Assets", "📝 Markdown/Code", "⚙️ Settings"])

    # Tab 1: Video input
    with tab1:
        st.subheader("Video Processing")

        video_enabled = st.checkbox("Enable video processing", value=True, key="video_enabled")

        video_path = None
        if video_enabled:
            video_path_str = st.text_input(
                "Video file path",
                placeholder="/path/to/video.mov",
                help="Path to video file (MP4, MOV, etc.)",
                key="video_path"
            )

            if video_path_str:
                video_path = Path(video_path_str)
                if not video_path.exists():
                    st.error(f"File not found: {video_path}")
                    video_path = None
                elif not video_path.is_file():
                    st.error(f"Not a file: {video_path}")
                    video_path = None

    # Tab 2: Assets input
    with tab2:
        st.subheader("Image Asset Hygiene")

        assets_enabled = st.checkbox("Enable asset hygiene", value=False, key="assets_enabled")

        assets_dir = None
        if assets_enabled:
            assets_dir_str = st.text_input(
                "Assets directory",
                placeholder="/path/to/images",
                help="Directory containing image files",
                key="assets_dir"
            )

            if assets_dir_str:
                assets_dir = Path(assets_dir_str)
                if not assets_dir.exists():
                    st.error(f"Directory not found: {assets_dir}")
                    assets_dir = None
                elif not assets_dir.is_dir():
                    st.error(f"Not a directory: {assets_dir}")
                    assets_dir = None

            assets_mode = st.selectbox(
                "Processing mode",
                ["detect", "rename", "convert"],
                help="detect: report only, rename: fix extensions, convert: create corrected copies",
                key="assets_mode"
            )

            assets_dry_run = st.checkbox(
                "Dry run (no changes)",
                value=True,
                help="Preview changes without modifying files",
                key="assets_dry_run"
            )

    # Tab 3: Markdown/Code input
    with tab3:
        st.subheader("Markdown/Code Extraction")

        mdx_enabled = st.checkbox("Enable markdown/code extraction", value=False, key="mdx_enabled")

        mdx_files = None
        if mdx_enabled:
            mdx_pattern = st.text_input(
                "File pattern (glob)",
                placeholder="/path/to/notes/**/*.md",
                help="Glob pattern for markdown files",
                key="mdx_pattern"
            )

            if mdx_pattern:
                matched_files = glob.glob(mdx_pattern, recursive=True)
                mdx_files = [Path(f) for f in matched_files if Path(f).is_file()]
                st.info(f"Found {len(mdx_files)} files")

                if not mdx_files:
                    st.warning("No files matched the pattern")

            mdx_format = st.selectbox(
                "Output format",
                ["csv", "parquet"],
                help="Output file format",
                key="mdx_format"
            )

            mdx_include_lang = st.text_input(
                "Include languages (comma-separated)",
                placeholder="python,javascript,java",
                help="Leave empty to include all",
                key="mdx_include_lang"
            )

            mdx_exclude_lang = st.text_input(
                "Exclude languages (comma-separated)",
                placeholder="",
                help="Languages to exclude",
                key="mdx_exclude_lang"
            )

    # Tab 4: Settings
    with tab4:
        st.subheader("Processing Settings")

        # Preset selector
        preset = st.selectbox(
            "Preset",
            list(PRESETS.keys()),
            help="Pre-configured settings for different use cases",
            key="preset"
        )

        preset_values = PRESETS[preset]

        st.markdown("### Video Settings")

        col1, col2 = st.columns(2)

        with col1:
            extraction_fps = st.slider(
                "Extraction FPS",
                min_value=1.0,
                max_value=15.0,
                value=preset_values["extraction_fps"],
                step=0.5,
                help="Frames per second to extract from video",
                key="extraction_fps"
            )

            sharpness_min = st.slider(
                "Sharpness threshold",
                min_value=0.0,
                max_value=200.0,
                value=preset_values["sharpness_min"],
                step=5.0,
                help="Minimum Laplacian variance for frame quality",
                key="sharpness_min"
            )

            min_text_length = st.slider(
                "Min text length",
                min_value=0,
                max_value=300,
                value=preset_values["min_text_length"],
                step=10,
                help="Minimum characters in OCR output",
                key="min_text_length"
            )

        with col2:
            jaccard_k = st.slider(
                "Jaccard k (n-gram size)",
                min_value=5,
                max_value=50,
                value=preset_values["jaccard_k"],
                step=5,
                help="N-gram size for similarity calculation",
                key="jaccard_k"
            )

            overlap_threshold = st.slider(
                "Overlap threshold",
                min_value=0.3,
                max_value=0.9,
                value=preset_values["overlap_threshold"],
                step=0.05,
                help="Jaccard similarity threshold for overlap detection",
                key="overlap_threshold"
            )

            min_unique_lines = st.slider(
                "Min unique lines",
                min_value=0,
                max_value=10,
                value=preset_values["min_unique_lines"],
                step=1,
                help="Minimum novel lines required to add text",
                key="min_unique_lines"
            )

        save_frames = st.checkbox(
            "Save extracted frames",
            value=preset_values["save_frames"],
            help="Keep frame images (uses more disk space)",
            key="save_frames"
        )

        ocr_engine = st.selectbox(
            "OCR engine",
            ["tesseract", "none"],
            help="OCR backend (tesseract requires installation)",
            key="ocr_engine"
        )

        language_hint = st.text_input(
            "Language hint",
            placeholder="eng (optional)",
            help="Language code for OCR (e.g., eng, eng+fra)",
            key="language_hint"
        )

        max_frames = st.number_input(
            "Max frames (optional)",
            min_value=0,
            value=0,
            help="Limit number of frames (0 = no limit)",
            key="max_frames"
        )

    # Review and run
    st.markdown("---")
    st.header("▶️ Review and Run")

    # Show what will be processed
    stages = []
    if video_enabled and video_path:
        stages.append(f"✅ Video: {video_path.name}")
    if assets_enabled and assets_dir:
        stages.append(f"✅ Assets: {assets_dir.name}")
    if mdx_enabled and mdx_files:
        stages.append(f"✅ Markdown: {len(mdx_files)} files")

    if stages:
        st.success("Stages to run:")
        for stage in stages:
            st.write(stage)

        if st.button("🚀 Start Processing", type="primary", disabled=st.session_state.run_state == 'running'):
            # Build settings
            video_settings = VideoSettings(
                extraction_fps=extraction_fps,
                sharpness_min=sharpness_min,
                min_text_length=min_text_length,
                jaccard_k=jaccard_k,
                overlap_threshold=overlap_threshold,
                min_unique_lines=min_unique_lines,
                save_frames=save_frames,
                ocr_engine=ocr_engine,
                language_hint=language_hint if language_hint else None,
                max_frames=max_frames if max_frames > 0 else None
            )

            assets_settings = AssetSettings(
                mode=assets_mode if assets_enabled else "detect",
                dry_run=assets_dry_run if assets_enabled else True
            )

            mdx_include = [l.strip() for l in mdx_include_lang.split(',')] if mdx_include_lang else []
            mdx_exclude = [l.strip() for l in mdx_exclude_lang.split(',')] if mdx_exclude_lang else []

            mdx_settings = MDXSettings(
                include_lang=mdx_include,
                exclude_lang=mdx_exclude,
                min_block_lines=2,
                max_block_chars=100000
            )

            workflow_settings = WorkflowSettings(
                video=video_settings,
                assets=assets_settings,
                mdx=mdx_settings
            )

            # Run workflow
            try:
                st.session_state.run_state = 'running'
                st.session_state.logger = Logger(verbose=False)

                output_root = Path.home() / "VTKE_Runs"

                progress_container = st.container()
                status_container = st.container()

                with status_container:
                    st.info("🔄 Processing...")

                engine = WorkflowEngine(
                    settings=workflow_settings,
                    output_root=output_root,
                    logger=st.session_state.logger
                )

                results = engine.run(
                    video_path=video_path if video_enabled else None,
                    assets_dir=assets_dir if assets_enabled else None,
                    mdx_files=mdx_files if mdx_enabled else None,
                    mdx_format=mdx_format if mdx_enabled else "csv"
                )

                st.session_state.results = results
                st.session_state.run_state = 'completed'
                st.rerun()

            except Exception as e:
                st.session_state.run_state = 'failed'
                st.error(f"❌ Processing failed: {e}")
                st.exception(e)

    else:
        st.warning("⚠️ No stages enabled. Please configure at least one input source.")


def render_results():
    """Render results panel."""
    if st.session_state.results is None:
        return

    st.header("✅ Processing Complete")

    results = st.session_state.results

    # Summary
    st.success(f"✅ Completed in {results['duration']:.1f} seconds")
    st.info(f"📁 Output directory: {results['run_dir']}")

    # Results by stage
    if 'video' in results:
        st.subheader("📹 Video Processing")
        video_results = results['video']

        col1, col2, col3 = st.columns(3)

        with col1:
            if 'text' in video_results:
                st.download_button(
                    "📄 Download Text",
                    data=video_results['text'].read_text(),
                    file_name=video_results['text'].name,
                    mime="text/plain"
                )

        with col2:
            if 'summary' in video_results:
                st.download_button(
                    "📊 Download Summary",
                    data=video_results['summary'].read_text(),
                    file_name=video_results['summary'].name,
                    mime="text/plain"
                )

        with col3:
            if 'metadata' in video_results:
                st.download_button(
                    "📋 Download Metadata",
                    data=video_results['metadata'].read_text(),
                    file_name=video_results['metadata'].name,
                    mime="application/json"
                )

    if 'assets' in results:
        st.subheader("🖼️ Asset Hygiene")
        assets_results = results['assets']

        st.metric("Total files", assets_results.get('total', 0))
        st.metric("Mislabeled", assets_results.get('mislabeled', 0))

        if 'report_path' in assets_results:
            st.download_button(
                "📋 Download Report",
                data=assets_results['report_path'].read_text(),
                file_name="assets_report.json",
                mime="application/json"
            )

    if 'mdx' in results:
        st.subheader("📝 Markdown/Code Extraction")
        mdx_results = results['mdx']

        if 'output_path' in mdx_results:
            output_path = mdx_results['output_path']
            st.download_button(
                "📊 Download Code Blocks",
                data=output_path.read_bytes(),
                file_name=output_path.name,
                mime="application/octet-stream"
            )

    # Reset button
    if st.button("🔄 New Run"):
        st.session_state.run_state = 'idle'
        st.session_state.results = None
        st.rerun()


def main():
    """Main application."""
    st.title("🎬 VTKE - Video to Text Knowledge Extractor")
    st.markdown("Local-first tool for extracting text from videos, sanitizing image assets, and extracting code from markdown.")

    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("Navigation")
        page = st.radio(
            "Select page",
            ["Preflight Checks", "Configure & Run", "Results"],
            disabled=st.session_state.run_state == 'running'
        )

        st.markdown("---")
        st.markdown("### About")
        st.markdown("**Version:** 1.0.0")
        st.markdown("**Status:** " + st.session_state.run_state.upper())

    # Main content
    if page == "Preflight Checks":
        render_preflight_panel()
    elif page == "Configure & Run":
        if st.session_state.run_state == 'completed':
            st.info("✅ A run has completed. View results in the Results page or start a new run below.")
        render_wizard()
    elif page == "Results":
        if st.session_state.run_state == 'completed':
            render_results()
        else:
            st.info("ℹ️ No results yet. Complete a run first.")


if __name__ == "__main__":
    main()
