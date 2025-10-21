#!/bin/bash
# VTKE Launcher Script

echo "🎬 VTKE - Video to Text Knowledge Extractor"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check if dependencies are installed
if ! python -c "import streamlit" 2>/dev/null; then
    echo "❌ Dependencies not installed!"
    echo "Run: pip install -r requirements.txt"
    exit 1
fi

echo "✅ Environment ready"
echo ""
echo "Starting Streamlit server..."
echo "Open your browser to http://localhost:8501"
echo ""

# Launch Streamlit
streamlit run app.py
