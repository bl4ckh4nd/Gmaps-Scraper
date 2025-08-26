# Google Maps Scraper - Flask API Startup (PowerShell)
# This script starts the Flask API with virtual environment activation

param(
    [switch]$Production,
    [switch]$Dev,
    [int]$Port = 0,
    [switch]$Help
)

# Colors for output
$Green = @{ForegroundColor = "Green"}
$Yellow = @{ForegroundColor = "Yellow"}
$Red = @{ForegroundColor = "Red"}
$Blue = @{ForegroundColor = "Blue"}

function Write-Header {
    param([string]$Message)
    Write-Host "`n" @Blue
    Write-Host "=" * 60 @Blue
    Write-Host $Message @Blue
    Write-Host "=" * 60 @Blue
    Write-Host "`n"
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" @Green
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" @Blue
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠ $Message" @Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" @Red
}

function Show-Help {
    Write-Host "Google Maps Scraper - Flask API Startup" @Blue
    Write-Host ""
    Write-Host "USAGE:" @Green
    Write-Host "  .\start_api.ps1 [OPTIONS]"
    Write-Host ""
    Write-Host "OPTIONS:" @Green
    Write-Host "  -Production    Start in production mode with Waitress"
    Write-Host "  -Dev          Start in development mode (default)"
    Write-Host "  -Port <int>   Specify custom port (default: 5000 dev, 8000 prod)"
    Write-Host "  -Help         Show this help message"
    Write-Host ""
    Write-Host "EXAMPLES:" @Yellow
    Write-Host "  .\start_api.ps1                    # Development mode"
    Write-Host "  .\start_api.ps1 -Production        # Production mode"
    Write-Host "  .\start_api.ps1 -Dev -Port 3000    # Development on port 3000"
    Write-Host "  .\start_api.ps1 -Production -Port 80  # Production on port 80"
    Write-Host ""
}

function Test-VirtualEnvironment {
    if (-not (Test-Path "flask_venv")) {
        Write-Error "Virtual environment 'flask_venv' not found"
        Write-Info "Please run setup first: .\setup_venv.ps1"
        return $false
    }
    
    if (-not (Test-Path "flask_venv\Scripts\Activate.ps1")) {
        Write-Error "Virtual environment activation script not found"
        Write-Info "Please recreate the virtual environment: .\setup_venv.ps1 -Force"
        return $false
    }
    
    return $true
}

function Start-FlaskDevelopment {
    param([int]$PortNum = 5000)
    
    Write-Info "Starting Flask API in DEVELOPMENT mode..."
    Write-Host ""
    Write-Host "Configuration:" @Blue
    Write-Host "  Mode: Development" @Yellow
    Write-Host "  Port: $PortNum" @Yellow
    Write-Host "  URL:  http://localhost:$PortNum" @Yellow
    Write-Host "  Hot Reload: Enabled" @Yellow
    Write-Host ""
    Write-Warning "This is NOT suitable for production use!"
    Write-Host ""
    Write-Info "Press Ctrl+C to stop the server"
    Write-Host ""
    
    # Set environment variables for development
    $env:FLASK_ENV = "development"
    $env:FLASK_DEBUG = "1"
    
    # Start Flask development server
    try {
        if ($PortNum -ne 5000) {
            # If custom port, need to modify the app.py or pass via environment
            Write-Warning "Custom port requires app.py modification or environment variable support"
        }
        & python app.py
    }
    catch {
        Write-Error "Failed to start Flask development server: $($_.Exception.Message)"
        return $false
    }
}

function Start-FlaskProduction {
    param([int]$PortNum = 8000)
    
    Write-Info "Starting Flask API in PRODUCTION mode with Waitress..."
    Write-Host ""
    Write-Host "Configuration:" @Blue
    Write-Host "  Mode: Production" @Yellow
    Write-Host "  Port: $PortNum" @Yellow
    Write-Host "  URL:  http://localhost:$PortNum" @Yellow
    Write-Host "  WSGI Server: Waitress" @Yellow
    Write-Host ""
    Write-Success "Production-ready server"
    Write-Host ""
    Write-Info "Press Ctrl+C to stop the server"
    Write-Host ""
    
    # Set environment variables for production
    $env:FLASK_ENV = "production"
    $env:FLASK_DEBUG = "0"
    
    # Check if Waitress is installed
    try {
        & python -c "import waitress" 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Waitress is not installed"
            Write-Info "Installing Waitress..."
            & python -m pip install waitress
        }
    }
    catch {
        Write-Error "Could not verify Waitress installation"
        return $false
    }
    
    # Start Waitress server
    try {
        & waitress-serve --host=0.0.0.0 --port=$PortNum app:app
    }
    catch {
        Write-Error "Failed to start Waitress server: $($_.Exception.Message)"
        return $false
    }
}

function Test-Port {
    param([int]$PortNum)
    
    try {
        $listener = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
        $portInUse = $listener | Where-Object { $_.Port -eq $PortNum }
        
        if ($portInUse) {
            Write-Warning "Port $PortNum is already in use"
            $response = Read-Host "Continue anyway? The server may fail to start (y/n) [default: n]"
            return ($response -eq "y" -or $response -eq "Y")
        }
        
        return $true
    }
    catch {
        Write-Warning "Could not check if port $PortNum is in use"
        return $true  # Continue anyway
    }
}

# Main execution
try {
    # Show help if requested
    if ($Help) {
        Show-Help
        exit 0
    }
    
    Write-Header "Google Maps Scraper - Flask API"
    
    # Check if running in correct directory
    if (-not (Test-Path "app.py")) {
        Write-Error "Please run this script from the 'web' directory"
        Write-Info "Expected files: app.py, scraper_service.py"
        exit 1
    }
    
    # Check virtual environment
    if (-not (Test-VirtualEnvironment)) {
        exit 1
    }
    
    # Determine mode
    $isProduction = $Production
    $isDevelopment = $Dev -or (-not $Production)  # Default to development
    
    if ($Production -and $Dev) {
        Write-Error "Cannot specify both -Production and -Dev flags"
        Show-Help
        exit 1
    }
    
    # Determine port
    if ($Port -eq 0) {
        $Port = if ($isProduction) { 8000 } else { 5000 }
    }
    
    # Validate port
    if ($Port -lt 1 -or $Port -gt 65535) {
        Write-Error "Port must be between 1 and 65535"
        exit 1
    }
    
    # Check if port is available
    if (-not (Test-Port $Port)) {
        exit 1
    }
    
    # Activate virtual environment
    Write-Info "Activating virtual environment..."
    
    # Check and set execution policy if needed
    $policy = Get-ExecutionPolicy
    if ($policy -eq "Restricted") {
        Write-Warning "PowerShell execution policy is Restricted"
        Write-Info "Temporarily setting execution policy to RemoteSigned for this session..."
        Set-ExecutionPolicy RemoteSigned -Scope Process -Force
    }
    
    & flask_venv\Scripts\Activate.ps1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to activate virtual environment"
        exit 1
    }
    
    Write-Success "Virtual environment activated"
    
    # Verify required files exist
    if (-not (Test-Path "app.py")) {
        Write-Error "app.py not found"
        exit 1
    }
    
    if (-not (Test-Path "scraper_service.py")) {
        Write-Error "scraper_service.py not found"
        exit 1
    }
    
    # Start the appropriate server
    if ($isProduction) {
        Start-FlaskProduction -PortNum $Port
    }
    else {
        Start-FlaskDevelopment -PortNum $Port
    }
    
}
catch {
    Write-Error "Startup failed: $($_.Exception.Message)"
    Write-Info "Try running setup first: .\setup_venv.ps1"
    exit 1
}
finally {
    # Cleanup
    Write-Host ""
    Write-Info "Server stopped"
}