#!/bin/bash

echo "Starting Google Maps Scraper Flask API with uv..."
echo

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed. Please install it first."
    echo "Install with: pip install uv"
    exit 1
fi

# Check if virtual environment exists with uv
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Creating with uv..."
    uv venv
    if [ $? -ne 0 ]; then
        echo "Setup failed. Exiting."
        exit 1
    fi
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Install/sync dependencies
echo "Installing dependencies..."
uv pip install -r requirements_web.txt
if [ $? -ne 0 ]; then
    echo "Dependency installation failed. Exiting."
    exit 1
fi

# Check if in development mode
read -p "Start in development mode? (y/n) [default: y]: " mode
if [ -z "$mode" ]; then
    mode="y"
fi

if [ "$mode" = "y" ] || [ "$mode" = "Y" ]; then
    echo "Starting in development mode..."
    echo
    echo "Flask API will be available at: http://localhost:5000"
    echo "Press Ctrl+C to stop the server"
    echo
    uv run python app.py
else
    echo "Starting in production mode with Gunicorn..."
    echo
    echo "Flask API will be available at: http://localhost:8000"
    echo "Press Ctrl+C to stop the server"
    echo
    uv run gunicorn -w 4 -b 0.0.0.0:8000 app:app
fi
