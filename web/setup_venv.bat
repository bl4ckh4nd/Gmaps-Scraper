@echo off
echo Setting up Flask API Virtual Environment...
echo.

REM Create virtual environment if it doesn't exist
if not exist "flask_venv" (
    echo Creating virtual environment...
    python -m venv flask_venv
    if errorlevel 1 (
        echo Error: Failed to create virtual environment
        pause
        exit /b 1
    )
)

REM Activate virtual environment
echo Activating virtual environment...
call flask_venv\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install web requirements
echo Installing Flask API requirements...
pip install -r requirements_web.txt

REM Install core scraper requirements
echo Installing core scraper requirements...
pip install -r ..\requirements.txt

echo.
echo Virtual environment setup complete!
echo.
echo To activate the environment in the future, run:
echo   cd web
echo   flask_venv\Scripts\activate
echo.
echo To start the Flask API:
echo   python app.py
echo.
pause