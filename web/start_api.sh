#!/bin/bash

echo "Starting Google Maps Scraper Flask API..."
echo

# Check if virtual environment exists
if [ ! -d "flask_venv" ]; then
    echo "Virtual environment not found. Running setup..."
    ./setup_venv.sh
    if [ $? -ne 0 ]; then
        echo "Setup failed. Exiting."
        exit 1
    fi
fi

# Activate virtual environment
echo "Activating virtual environment..."
source flask_venv/bin/activate

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
    python app.py
else
    echo "Starting in production mode with Gunicorn..."
    echo
    echo "Flask API will be available at: http://localhost:8000"
    echo "Press Ctrl+C to stop the server"
    echo
    gunicorn -w 4 -b 0.0.0.0:8000 app:app
fi