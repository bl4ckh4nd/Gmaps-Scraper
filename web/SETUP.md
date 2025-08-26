# Flask API Setup Guide

This guide will help you set up and run the Google Maps Scraper web interface on both Windows and Unix/Linux systems.

## Quick Start

### Windows

#### Option 1: PowerShell (Recommended)

1. **Open PowerShell** and navigate to the web directory:
   ```powershell
   cd web
   ```

2. **Run the setup script**:
   ```powershell
   .\setup_venv.ps1
   ```

3. **Start the Flask API**:
   ```powershell
   .\start_api.ps1
   ```

#### Option 2: Command Prompt

1. **Open Command Prompt** and navigate to the web directory:
   ```cmd
   cd web
   ```

2. **Run the setup script**:
   ```cmd
   setup_venv.bat
   ```

3. **Start the Flask API**:
   ```cmd
   start_api.bat
   ```

4. **Open your browser** and go to: http://localhost:5000

### Unix/Linux/Mac

1. **Open terminal** and navigate to the web directory:
   ```bash
   cd web
   ```

2. **Make scripts executable** (if not already):
   ```bash
   chmod +x setup_venv.sh start_api.sh
   ```

3. **Run the setup script**:
   ```bash
   ./setup_venv.sh
   ```

4. **Start the Flask API**:
   ```bash
   ./start_api.sh
   ```

5. **Open your browser** and go to: http://localhost:5000

## Manual Setup (Alternative)

If the automated scripts don't work, you can set up manually:

### 1. Create Virtual Environment

**Windows:**
```cmd
python -m venv flask_venv
flask_venv\Scripts\activate
```

**Unix/Linux/Mac:**
```bash
python3 -m venv flask_venv
source flask_venv/bin/activate
```

### 2. Install Dependencies

```bash
# Upgrade pip
python -m pip install --upgrade pip

# Install web requirements
pip install -r requirements_web.txt

# Install core scraper requirements
pip install -r ../requirements.txt
```

### 3. Start the Application

**Development Mode:**
```bash
python app.py
```

**Production Mode (Windows):**
```bash
waitress-serve --host=0.0.0.0 --port=8000 app:app
```

**Production Mode (Unix/Linux):**
```bash
gunicorn -w 4 -b 0.0.0.0:8000 app:app
```

## Troubleshooting

### Common Issues

**1. Python not found**
- Ensure Python 3.7+ is installed and in PATH
- Try `python3` instead of `python`

**2. Virtual environment creation fails**
- Install `python3-venv` on Ubuntu/Debian: `sudo apt install python3-venv`
- Ensure you have write permissions in the web directory

**3. Permission denied on scripts (Unix/Linux)**
```bash
chmod +x *.sh
```

**4. Port 5000 already in use**
- Kill the process using the port: `lsof -ti:5000 | xargs kill -9` (Unix/Linux)
- Or change the port in `app.py`

**5. Import errors**
- Ensure you're in the activated virtual environment
- Check that all dependencies installed correctly

**6. Chrome/Chromium not found**
- Update the Chrome path in `../config.yaml`
- Or install Playwright browsers: `playwright install chromium`

### Checking the Setup

**Verify virtual environment is active:**
- Command prompt should show `(flask_venv)` prefix
- `which python` should point to venv (Unix/Linux)
- `where python` should point to venv (Windows)

**Test the API:**
```bash
curl http://localhost:5000/api/health
```

Should return:
```json
{
  "status": "healthy",
  "timestamp": "...",
  "active_jobs": 0,
  "total_jobs": 0
}
```

## Development

### Activating Environment for Development

**Windows:**
```cmd
cd web
flask_venv\Scripts\activate
```

**Unix/Linux/Mac:**
```bash
cd web
source flask_venv/bin/activate
```

### Running in Debug Mode

Set environment variable and run:
```bash
export FLASK_DEBUG=1  # Unix/Linux/Mac
set FLASK_DEBUG=1     # Windows
python app.py
```

### Installing Additional Packages

With virtual environment active:
```bash
pip install package_name
pip freeze > requirements_web.txt  # Update requirements
```

## Production Deployment

For production use:

1. **Use production WSGI server** (already configured in start scripts)
2. **Set environment variables:**
   ```bash
   export FLASK_ENV=production
   export SECRET_KEY=your-secret-key-here
   ```
3. **Configure reverse proxy** (nginx/Apache)
4. **Set up SSL/HTTPS**
5. **Monitor resources** and set limits

## File Structure

```
web/
├── flask_venv/              # Virtual environment (created by setup)
├── app.py                   # Main Flask application
├── scraper_service.py       # Background job management
├── requirements_web.txt     # Web-specific dependencies
├── setup_venv.bat          # Windows setup script
├── setup_venv.sh           # Unix/Linux setup script
├── start_api.bat           # Windows start script
├── start_api.sh            # Unix/Linux start script
├── templates/              # HTML templates
├── static/                 # CSS, JS, images
└── README.md               # Detailed documentation
```

## Next Steps

After setup:

1. **Test the interface** by creating a small scraping job
2. **Review the configuration** in `../config.yaml`
3. **Check the logs** for any issues
4. **Customize settings** as needed for your use case

For detailed usage instructions, see `README.md` in this directory.