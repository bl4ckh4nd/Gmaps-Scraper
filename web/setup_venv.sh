#!/bin/bash

echo "Setting up Flask API Virtual Environment..."
echo

# Create virtual environment if it doesn't exist
if [ ! -d "flask_venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv flask_venv
    if [ $? -ne 0 ]; then
        echo "Error: Failed to create virtual environment"
        exit 1
    fi
fi

# Activate virtual environment
echo "Activating virtual environment..."
source flask_venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade pip

# Install web requirements
echo "Installing Flask API requirements..."
pip install -r requirements_web.txt

# Install core scraper requirements
echo "Installing core scraper requirements..."
pip install -r ../requirements.txt

echo
echo "Virtual environment setup complete!"
echo
echo "To activate the environment in the future, run:"
echo "  cd web"
echo "  source flask_venv/bin/activate"
echo
echo "To start the Flask API:"
echo "  python app.py"
echo