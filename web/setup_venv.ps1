# Google Maps Scraper - Flask API Setup (PowerShell)
# This script sets up the virtual environment and installs all required dependencies

param(
    [switch]$Force,
    [switch]$Verbose
)

# Set error handling
$ErrorActionPreference = "Stop"

# Colors for output
$Green = @{ForegroundColor = "Green"}
$Yellow = @{ForegroundColor = "Yellow"}  
$Red = @{ForegroundColor = "Red"}
$Blue = @{ForegroundColor = "Blue"}

function Write-Header {
    param([string]$Message)
    Write-Host "`n" @Blue
    Write-Host ("=" * 60) @Blue
    Write-Host $Message @Blue
    Write-Host ("=" * 60) @Blue
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

function Test-Command {
    param([string]$Command)
    
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

function Test-PythonVersion {
    try {
        $pythonVersion = & python --version 2>&1
        if ($pythonVersion -match "Python (\d+)\.(\d+)") {
            $major = [int]$matches[1]
            $minor = [int]$matches[2]
            
            if ($major -eq 3 -and $minor -ge 7) {
                Write-Success "Found $pythonVersion"
                return $true
            }
            else {
                Write-Error "Python 3.7+ required, found $pythonVersion"
                return $false
            }
        }
        else {
            Write-Error "Could not determine Python version"
            return $false
        }
    }
    catch {
        Write-Error "Python not found or not accessible"
        return $false
    }
}

function New-VirtualEnvironment {
    Write-Info "Creating virtual environment 'flask_venv'..."
    
    try {
        & python -m venv flask_venv
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Virtual environment created successfully"
            return $true
        }
        else {
            Write-Error "Failed to create virtual environment"
            return $false
        }
    }
    catch {
        Write-Error "Error creating virtual environment: $($_.Exception.Message)"
        return $false
    }
}

function Install-Requirements {
    param([string]$RequirementsFile, [string]$Description)
    
    Write-Info "Installing $Description..."
    
    if (-not (Test-Path $RequirementsFile)) {
        Write-Warning "$RequirementsFile not found, skipping..."
        return $true
    }
    
    try {
        $output = & python -m pip install -r $RequirementsFile 2>&1
        
        if ($Verbose) {
            Write-Host $output
        }
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "$Description installed successfully"
            return $true
        }
        else {
            Write-Error "Failed to install $Description"
            Write-Host $output @Red
            return $false
        }
    }
    catch {
        Write-Error "Error installing ${Description}: $($_.Exception.Message)"
        return $false
    }
}

function Install-Playwright {
    Write-Info "Installing Playwright browsers..."
    
    try {
        $output = & python -m playwright install chromium 2>&1
        
        if ($Verbose) {
            Write-Host $output
        }
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Playwright browsers installed successfully"
            return $true
        }
        else {
            Write-Warning "Playwright browser installation had issues (this may be okay)"
            if ($Verbose) {
                Write-Host $output @Yellow
            }
            return $true  # Continue anyway
        }
    }
    catch {
        Write-Warning "Could not install Playwright browsers: $($_.Exception.Message)"
        return $true  # Continue anyway
    }
}

function Test-VirtualEnvironment {
    Write-Info "Testing virtual environment..."
    
    # Check if activation script exists
    $activateScript = "flask_venv\Scripts\Activate.ps1"
    if (-not (Test-Path $activateScript)) {
        Write-Error "Virtual environment activation script not found"
        return $false
    }
    
    # Test activation
    try {
        & $activateScript
        $pythonPath = & python -c "import sys; print(sys.executable)" 2>&1
        
        if ($pythonPath -like "*flask_venv*") {
            Write-Success "Virtual environment is working correctly"
            return $true
        }
        else {
            Write-Error "Virtual environment activation failed"
            return $false
        }
    }
    catch {
        Write-Error "Could not test virtual environment: $($_.Exception.Message)"
        return $false
    }
}

function Show-Instructions {
    Write-Header "Setup Complete!"
    
    Write-Host "The Flask API virtual environment has been set up successfully." @Green
    Write-Host ""
    
    Write-Host "To activate the environment manually:" @Blue
    Write-Host "  flask_venv\Scripts\Activate.ps1" @Yellow
    Write-Host ""
    
    Write-Host "To start the Flask API:" @Blue
    Write-Host "  .\start_api.ps1        # PowerShell" @Yellow
    Write-Host "  start_api.bat          # Command Prompt" @Yellow
    Write-Host ""
    
    Write-Host "To start manually (after activation):" @Blue
    Write-Host "  python app.py          # Development mode" @Yellow
    Write-Host ""
    
    Write-Host "The web interface will be available at:" @Blue
    Write-Host "  http://localhost:5000" @Yellow
    Write-Host ""
}

# Main execution
try {
    Write-Header "Google Maps Scraper - Flask API Setup"
    
    # Check if running in correct directory
    if (-not (Test-Path "app.py")) {
        Write-Error "Please run this script from the 'web' directory"
        Write-Info "Expected files: app.py, requirements_web.txt"
        exit 1
    }
    
    # Check if virtual environment already exists
    if ((Test-Path "flask_venv") -and -not $Force) {
        $response = Read-Host "Virtual environment 'flask_venv' already exists. Recreate? (y/n) [default: n]"
        if ($response -eq "y" -or $response -eq "Y") {
            Write-Info "Removing existing virtual environment..."
            Remove-Item -Recurse -Force flask_venv
            Write-Success "Existing virtual environment removed"
        }
        else {
            Write-Info "Using existing virtual environment..."
        }
    }
    elseif ((Test-Path "flask_venv") -and $Force) {
        Write-Info "Force flag set. Removing existing virtual environment..."
        Remove-Item -Recurse -Force flask_venv
        Write-Success "Existing virtual environment removed"
    }
    
    # Check Python installation
    Write-Info "Checking Python installation..."
    if (-not (Test-Command "python")) {
        Write-Error "Python is not installed or not in PATH"
        Write-Info "Please install Python 3.7+ and ensure it's in your PATH"
        exit 1
    }
    
    if (-not (Test-PythonVersion)) {
        Write-Info "Please install Python 3.7 or higher from https://python.org"
        exit 1
    }
    
    # Create virtual environment if it doesn't exist
    if (-not (Test-Path "flask_venv")) {
        if (-not (New-VirtualEnvironment)) {
            exit 1
        }
    }
    
    # Activate virtual environment
    Write-Info "Activating virtual environment..."
    $activateScript = "flask_venv\Scripts\Activate.ps1"
    
    # Check execution policy
    $policy = Get-ExecutionPolicy
    if ($policy -eq "Restricted") {
        Write-Warning "PowerShell execution policy is Restricted"
        Write-Info "Temporarily setting execution policy to RemoteSigned for this session..."
        Set-ExecutionPolicy RemoteSigned -Scope Process -Force
    }
    
    & $activateScript
    
    # Upgrade pip
    Write-Info "Upgrading pip..."
    & python -m pip install --upgrade pip | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Pip upgraded successfully"
    }
    else {
        Write-Warning "Pip upgrade had issues (continuing anyway)"
    }
    
    # Install web requirements
    if (-not (Install-Requirements "requirements_web.txt" "Flask API dependencies")) {
        exit 1
    }
    
    # Install core scraper requirements
    if (-not (Install-Requirements "..\requirements.txt" "core scraper dependencies")) {
        exit 1
    }
    
    # Install Playwright browsers
    Install-Playwright | Out-Null
    
    # Test the setup
    if (-not (Test-VirtualEnvironment)) {
        Write-Warning "Virtual environment test failed, but setup may still work"
    }
    
    # Show final instructions
    Show-Instructions
    
    Write-Success "Setup completed successfully!"
    
}
catch {
    Write-Error "Setup failed: $($_.Exception.Message)"
    Write-Info "Check the error above and try running the script again"
    exit 1
}

# Optional: Ask if user wants to start the API now
Write-Host ""
$startNow = Read-Host "Would you like to start the Flask API now? (y/n) [default: n]"
if ($startNow -eq "y" -or $startNow -eq "Y") {
    Write-Info "Starting Flask API..."
    if (Test-Path "start_api.ps1") {
        & .\start_api.ps1
    }
    elseif (Test-Path "start_api.bat") {
        & .\start_api.bat
    }
    else {
        Write-Info "Starting manually..."
        & python app.py
    }
}