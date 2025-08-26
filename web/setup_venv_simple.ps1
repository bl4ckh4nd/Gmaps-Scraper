# Google Maps Scraper - Flask API Setup (PowerShell - Simple Version)
# This script sets up the virtual environment and installs all required dependencies

param(
    [switch]$Force,
    [switch]$Verbose
)

# Set error handling
$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "================================================================" -ForegroundColor Blue
Write-Host "Google Maps Scraper - Flask API Setup" -ForegroundColor Blue
Write-Host "================================================================" -ForegroundColor Blue
Write-Host ""

try {
    # Check if running in correct directory
    if (-not (Test-Path "app.py")) {
        Write-Host "ERROR: Please run this script from the 'web' directory" -ForegroundColor Red
        Write-Host "Expected files: app.py, requirements_web.txt" -ForegroundColor Yellow
        exit 1
    }

    # Check if virtual environment already exists
    if ((Test-Path "flask_venv") -and -not $Force) {
        $response = Read-Host "Virtual environment 'flask_venv' already exists. Recreate? (y/n) [default: n]"
        if ($response -eq "y" -or $response -eq "Y") {
            Write-Host "Removing existing virtual environment..." -ForegroundColor Yellow
            Remove-Item -Recurse -Force flask_venv -ErrorAction SilentlyContinue
            Write-Host "✓ Existing virtual environment removed" -ForegroundColor Green
        }
        else {
            Write-Host "Using existing virtual environment..." -ForegroundColor Yellow
        }
    }
    elseif ((Test-Path "flask_venv") -and $Force) {
        Write-Host "Force flag set. Removing existing virtual environment..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force flask_venv -ErrorAction SilentlyContinue
        Write-Host "✓ Existing virtual environment removed" -ForegroundColor Green
    }

    # Check Python installation
    Write-Host "Checking Python installation..." -ForegroundColor Cyan
    
    try {
        $pythonVersion = & python --version 2>&1
        Write-Host "✓ Found $pythonVersion" -ForegroundColor Green
        
        if ($pythonVersion -match "Python (\d+)\.(\d+)") {
            $major = [int]$matches[1]
            $minor = [int]$matches[2]
            
            if (-not ($major -eq 3 -and $minor -ge 7)) {
                Write-Host "ERROR: Python 3.7+ required, found $pythonVersion" -ForegroundColor Red
                Write-Host "Please install Python 3.7+ from https://python.org" -ForegroundColor Yellow
                exit 1
            }
        }
    }
    catch {
        Write-Host "ERROR: Python not found or not accessible" -ForegroundColor Red
        Write-Host "Please install Python 3.7+ and ensure it's in your PATH" -ForegroundColor Yellow
        exit 1
    }

    # Create virtual environment if it doesn't exist
    if (-not (Test-Path "flask_venv")) {
        Write-Host "Creating virtual environment 'flask_venv'..." -ForegroundColor Cyan
        
        & python -m venv flask_venv
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERROR: Failed to create virtual environment" -ForegroundColor Red
            exit 1
        }
        
        Write-Host "✓ Virtual environment created successfully" -ForegroundColor Green
    }

    # Activate virtual environment
    Write-Host "Activating virtual environment..." -ForegroundColor Cyan
    
    # Check and set execution policy if needed
    $policy = Get-ExecutionPolicy -Scope CurrentUser
    if ($policy -eq "Restricted") {
        Write-Host "⚠ PowerShell execution policy is Restricted" -ForegroundColor Yellow
        Write-Host "Setting execution policy to RemoteSigned for current user..." -ForegroundColor Yellow
        try {
            Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
            Write-Host "✓ Execution policy updated" -ForegroundColor Green
        }
        catch {
            Write-Host "⚠ Could not update execution policy. Trying process scope..." -ForegroundColor Yellow
            Set-ExecutionPolicy RemoteSigned -Scope Process -Force
        }
    }

    # Activate the virtual environment
    $activateScript = "flask_venv\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
        Write-Host "✓ Virtual environment activated" -ForegroundColor Green
    }
    else {
        Write-Host "ERROR: Virtual environment activation script not found" -ForegroundColor Red
        exit 1
    }

    # Upgrade pip
    Write-Host "Upgrading pip..." -ForegroundColor Cyan
    $pipOutput = & python -m pip install --upgrade pip 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Pip upgraded successfully" -ForegroundColor Green
    }
    else {
        Write-Host "⚠ Pip upgrade had issues (continuing anyway)" -ForegroundColor Yellow
        if ($Verbose) {
            Write-Host $pipOutput -ForegroundColor Yellow
        }
    }

    # Install web requirements
    Write-Host "Installing Flask API dependencies..." -ForegroundColor Cyan
    
    if (Test-Path "requirements_web.txt") {
        $webOutput = & python -m pip install -r requirements_web.txt 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Flask API dependencies installed successfully" -ForegroundColor Green
        }
        else {
            Write-Host "ERROR: Failed to install Flask API dependencies" -ForegroundColor Red
            if ($Verbose) {
                Write-Host $webOutput -ForegroundColor Red
            }
            exit 1
        }
    }
    else {
        Write-Host "⚠ requirements_web.txt not found, skipping..." -ForegroundColor Yellow
    }

    # Install core scraper requirements
    Write-Host "Installing core scraper dependencies..." -ForegroundColor Cyan
    
    if (Test-Path "..\requirements.txt") {
        $coreOutput = & python -m pip install -r ..\requirements.txt 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Core scraper dependencies installed successfully" -ForegroundColor Green
        }
        else {
            Write-Host "ERROR: Failed to install core scraper dependencies" -ForegroundColor Red
            if ($Verbose) {
                Write-Host $coreOutput -ForegroundColor Red
            }
            exit 1
        }
    }
    else {
        Write-Host "⚠ ..\requirements.txt not found, skipping..." -ForegroundColor Yellow
    }

    # Install Playwright browsers (optional)
    Write-Host "Installing Playwright browsers..." -ForegroundColor Cyan
    try {
        $playwrightOutput = & python -m playwright install chromium 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Playwright browsers installed successfully" -ForegroundColor Green
        }
        else {
            Write-Host "⚠ Playwright browser installation had issues (this may be okay)" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "⚠ Could not install Playwright browsers (continuing anyway)" -ForegroundColor Yellow
    }

    # Test the setup
    Write-Host "Testing virtual environment..." -ForegroundColor Cyan
    
    try {
        $pythonPath = & python -c "import sys; print(sys.executable)" 2>&1
        if ($pythonPath -like "*flask_venv*") {
            Write-Host "✓ Virtual environment is working correctly" -ForegroundColor Green
        }
        else {
            Write-Host "⚠ Virtual environment test inconclusive, but setup may still work" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "⚠ Could not test virtual environment, but setup may still work" -ForegroundColor Yellow
    }

    # Show completion message
    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Green
    Write-Host "Setup Complete!" -ForegroundColor Green
    Write-Host "================================================================" -ForegroundColor Green
    Write-Host ""
    
    Write-Host "The Flask API virtual environment has been set up successfully." -ForegroundColor Green
    Write-Host ""
    
    Write-Host "To activate the environment manually:" -ForegroundColor Cyan
    Write-Host "  flask_venv\Scripts\Activate.ps1" -ForegroundColor Yellow
    Write-Host ""
    
    Write-Host "To start the Flask API:" -ForegroundColor Cyan
    Write-Host "  .\start_api.ps1        # PowerShell" -ForegroundColor Yellow
    Write-Host "  start_api.bat          # Command Prompt" -ForegroundColor Yellow
    Write-Host ""
    
    Write-Host "To start manually (after activation):" -ForegroundColor Cyan
    Write-Host "  python app.py          # Development mode" -ForegroundColor Yellow
    Write-Host ""
    
    Write-Host "The web interface will be available at:" -ForegroundColor Cyan
    Write-Host "  http://localhost:5000" -ForegroundColor Yellow
    Write-Host ""

    Write-Host "✓ Setup completed successfully!" -ForegroundColor Green

}
catch {
    Write-Host ""
    Write-Host "ERROR: Setup failed: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Check the error above and try running the script again" -ForegroundColor Yellow
    exit 1
}

# Optional: Ask if user wants to start the API now
Write-Host ""
$startNow = Read-Host "Would you like to start the Flask API now? (y/n) [default: n]"
if ($startNow -eq "y" -or $startNow -eq "Y") {
    Write-Host "Starting Flask API..." -ForegroundColor Cyan
    if (Test-Path "start_api.ps1") {
        & .\start_api.ps1
    }
    elseif (Test-Path "start_api.bat") {
        & .\start_api.bat
    }
    else {
        Write-Host "Starting manually..." -ForegroundColor Yellow
        & python app.py
    }
}