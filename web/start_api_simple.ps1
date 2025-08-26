# Google Maps Scraper - Flask API Startup (PowerShell - Simple Version)
# This script starts the Flask API with virtual environment activation

param(
    [switch]$Production,
    [switch]$Dev,
    [int]$Port = 0,
    [switch]$Help
)

# Set error handling
$ErrorActionPreference = "Stop"

if ($Help) {
    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Blue
    Write-Host "Google Maps Scraper - Flask API Startup" -ForegroundColor Blue
    Write-Host "================================================================" -ForegroundColor Blue
    Write-Host ""
    Write-Host "USAGE:" -ForegroundColor Green
    Write-Host "  .\start_api_simple.ps1 [OPTIONS]"
    Write-Host ""
    Write-Host "OPTIONS:" -ForegroundColor Green
    Write-Host "  -Production    Start in production mode with Waitress"
    Write-Host "  -Dev          Start in development mode (default)"
    Write-Host "  -Port <int>   Specify custom port (default: 5000 dev, 8000 prod)"
    Write-Host "  -Help         Show this help message"
    Write-Host ""
    Write-Host "EXAMPLES:" -ForegroundColor Yellow
    Write-Host "  .\start_api_simple.ps1                    # Development mode"
    Write-Host "  .\start_api_simple.ps1 -Production        # Production mode"
    Write-Host "  .\start_api_simple.ps1 -Dev -Port 3000    # Development on port 3000"
    Write-Host "  .\start_api_simple.ps1 -Production -Port 80  # Production on port 80"
    Write-Host ""
    exit 0
}

try {
    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Blue
    Write-Host "Google Maps Scraper - Flask API" -ForegroundColor Blue
    Write-Host "================================================================" -ForegroundColor Blue
    Write-Host ""
    
    # Check if running in correct directory
    if (-not (Test-Path "app.py")) {
        Write-Host "ERROR: Please run this script from the 'web' directory" -ForegroundColor Red
        Write-Host "Expected files: app.py, scraper_service.py" -ForegroundColor Yellow
        exit 1
    }
    
    # Check virtual environment
    if (-not (Test-Path "flask_venv")) {
        Write-Host "ERROR: Virtual environment 'flask_venv' not found" -ForegroundColor Red
        Write-Host "Please run setup first: .\setup_venv_simple.ps1" -ForegroundColor Yellow
        exit 1
    }
    
    if (-not (Test-Path "flask_venv\Scripts\Activate.ps1")) {
        Write-Host "ERROR: Virtual environment activation script not found" -ForegroundColor Red
        Write-Host "Please recreate the virtual environment: .\setup_venv_simple.ps1 -Force" -ForegroundColor Yellow
        exit 1
    }
    
    # Determine mode
    $isProduction = $Production
    $isDevelopment = $Dev -or (-not $Production)  # Default to development
    
    if ($Production -and $Dev) {
        Write-Host "ERROR: Cannot specify both -Production and -Dev flags" -ForegroundColor Red
        exit 1
    }
    
    # Determine port
    if ($Port -eq 0) {
        $Port = if ($isProduction) { 8000 } else { 5000 }
    }
    
    # Validate port
    if ($Port -lt 1 -or $Port -gt 65535) {
        Write-Host "ERROR: Port must be between 1 and 65535" -ForegroundColor Red
        exit 1
    }
    
    # Check if port is in use (simplified check)
    try {
        $listener = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
        $portInUse = $listener | Where-Object { $_.Port -eq $Port }
        
        if ($portInUse) {
            Write-Host "WARNING: Port $Port is already in use" -ForegroundColor Yellow
            $response = Read-Host "Continue anyway? The server may fail to start (y/n) [default: n]"
            if (-not ($response -eq "y" -or $response -eq "Y")) {
                exit 1
            }
        }
    }
    catch {
        Write-Host "WARNING: Could not check if port $Port is in use" -ForegroundColor Yellow
    }
    
    # Activate virtual environment
    Write-Host "Activating virtual environment..." -ForegroundColor Cyan
    
    # Check and set execution policy if needed
    $policy = Get-ExecutionPolicy -Scope CurrentUser
    if ($policy -eq "Restricted") {
        Write-Host "WARNING: PowerShell execution policy is Restricted" -ForegroundColor Yellow
        Write-Host "Setting execution policy to RemoteSigned for current user..." -ForegroundColor Yellow
        try {
            Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
            Write-Host "[OK] Execution policy updated" -ForegroundColor Green
        }
        catch {
            Write-Host "WARNING: Could not update execution policy. Trying process scope..." -ForegroundColor Yellow
            Set-ExecutionPolicy RemoteSigned -Scope Process -Force
        }
    }
    
    # Activate the virtual environment
    & flask_venv\Scripts\Activate.ps1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to activate virtual environment" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "[OK] Virtual environment activated" -ForegroundColor Green
    
    # Verify required files exist
    if (-not (Test-Path "app.py")) {
        Write-Host "ERROR: app.py not found" -ForegroundColor Red
        exit 1
    }
    
    if (-not (Test-Path "scraper_service.py")) {
        Write-Host "ERROR: scraper_service.py not found" -ForegroundColor Red
        exit 1
    }
    
    # Start the appropriate server
    if ($isProduction) {
        Write-Host "Starting Flask API in PRODUCTION mode with Waitress..." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Configuration:" -ForegroundColor Blue
        Write-Host "  Mode: Production" -ForegroundColor Yellow
        Write-Host "  Port: $Port" -ForegroundColor Yellow
        Write-Host "  URL:  http://localhost:$Port" -ForegroundColor Yellow
        Write-Host "  WSGI Server: Waitress" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "[OK] Production-ready server" -ForegroundColor Green
        Write-Host ""
        Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
        Write-Host ""
        
        # Set environment variables for production
        $env:FLASK_ENV = "production"
        $env:FLASK_DEBUG = "0"
        
        # Check if Waitress is installed
        try {
            & python -c "import waitress" 2>$null
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Waitress is not installed. Installing..." -ForegroundColor Yellow
                & python -m pip install waitress
            }
        }
        catch {
            Write-Host "ERROR: Could not verify Waitress installation" -ForegroundColor Red
            exit 1
        }
        
        # Start Waitress server
        & waitress-serve --host=0.0.0.0 --port=$Port app:app
    }
    else {
        Write-Host "Starting Flask API in DEVELOPMENT mode..." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Configuration:" -ForegroundColor Blue
        Write-Host "  Mode: Development" -ForegroundColor Yellow
        Write-Host "  Port: $Port" -ForegroundColor Yellow
        Write-Host "  URL:  http://localhost:$Port" -ForegroundColor Yellow
        Write-Host "  Hot Reload: Enabled" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "WARNING: This is NOT suitable for production use!" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
        Write-Host ""
        
        # Set environment variables for development
        $env:FLASK_ENV = "development"
        $env:FLASK_DEBUG = "1"
        
        # Start Flask development server
        if ($Port -ne 5000) {
            Write-Host "WARNING: Custom port requires app.py modification or environment variable support" -ForegroundColor Yellow
        }
        
        & python app.py
    }
}
catch {
    Write-Host ""
    Write-Host "ERROR: Startup failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Try running setup first: .\setup_venv_simple.ps1" -ForegroundColor Yellow
    exit 1
}
finally {
    # Cleanup
    Write-Host ""
    Write-Host "Server stopped" -ForegroundColor Yellow
}

# Optional: Ask if user wants to restart
Write-Host ""
$restart = Read-Host "Would you like to restart the server? (y/n) [default: n]"
if ($restart -eq "y" -or $restart -eq "Y") {
    Write-Host "Restarting..." -ForegroundColor Cyan
    & $MyInvocation.MyCommand.Path @PSBoundParameters
}