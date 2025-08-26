@echo off
echo Starting Google Maps Scraper Flask API...
echo.

REM Check if virtual environment exists
if not exist "flask_venv" (
    echo Virtual environment not found. Running setup...
    call setup_venv.bat
    if errorlevel 1 (
        echo Setup failed. Exiting.
        pause
        exit /b 1
    )
)

REM Activate virtual environment
echo Activating virtual environment...
call flask_venv\Scripts\activate.bat

REM Check if in development mode
set /p mode="Start in development mode? (y/n) [default: y]: "
if "%mode%"=="" set mode=y

if /i "%mode%"=="y" (
    echo Starting in development mode...
    echo.
    echo Flask API will be available at: http://localhost:5000
    echo Press Ctrl+C to stop the server
    echo.
    python app.py
) else (
    echo Starting in production mode with Waitress...
    echo.
    echo Flask API will be available at: http://localhost:8000
    echo Press Ctrl+C to stop the server
    echo.
    waitress-serve --host=0.0.0.0 --port=8000 app:app
)

pause