#!/bin/bash

# Script to initialize the project environment

set -e

echo "🚀 Starting project initialization..."

# 0. Check for system dependencies
if ! command -v cmake &> /dev/null; then
    echo "⚠️ Warning: 'cmake' not found. This may cause installation of some packages (like pyarrow) to fail."
    echo "   Recommendation: Install cmake (e.g., 'brew install cmake' on macOS)."
fi

# 1. Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment (.venv)..."
    python3 -m venv .venv
else
    echo "✅ Virtual environment (.venv) already exists."
fi

# 2. Activate virtual environment and install/upgrade poetry
echo "🛠 Installing/Upgrading pip and poetry in .venv..."
source .venv/bin/activate
pip install --upgrade pip poetry

# 3. Check for pyproject.toml
if [ ! -f "pyproject.toml" ]; then
    echo "📝 Initializing poetry project..."
    poetry init --no-interaction
else
    echo "✅ pyproject.toml already exists."
fi

# 4. Generate lock file
echo "🔐 Generating lock file..."
poetry lock

# 5. Install dependencies
echo "📥 Installing dependencies..."
poetry install --no-root --all-extras

echo "✨ Initialization complete!"
echo "💡 To activate the environment, run: source .venv/bin/activate"
