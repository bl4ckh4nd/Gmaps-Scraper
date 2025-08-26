# Google Maps Scraper - Web Interface

A Flask-based web interface for the Google Maps Scraper that allows you to start, monitor, and manage scraping jobs through a browser.

## Features

- **Web Dashboard**: Start new scraping jobs through a user-friendly form
- **Real-time Progress**: Live updates on job progress using Server-Sent Events
- **Interactive Map**: Select search areas by drawing on a map
- **Job Management**: View, cancel, and monitor active jobs
- **Results Download**: Download scraped data in CSV format
- **Job History**: View completed, failed, and cancelled jobs
- **Responsive Design**: Works on desktop, tablet, and mobile devices

## Quick Start

### 1. Install Dependencies

```bash
# Install web interface dependencies
pip install -r web/requirements_web.txt

# Make sure core dependencies are also installed
pip install -r requirements.txt
```

### 2. Start the Web Interface

```bash
# Navigate to the web directory
cd web

# Start the Flask development server
python app.py
```

The web interface will be available at: http://localhost:5000

### 3. Using the Interface

1. **Start a Job**: Fill out the form on the main page
   - Enter your search term (e.g., "restaurants in Toronto")
   - Set target number of results
   - Optionally select a geographic area on the map
   - Adjust grid size and review limits as needed

2. **Monitor Progress**: Active jobs show real-time progress bars and statistics

3. **Download Results**: Once completed, download business data and reviews

## API Endpoints

The web interface provides a REST API that can also be used programmatically:

### Job Management
- `POST /api/jobs` - Start new scraping job
- `GET /api/jobs` - List all jobs
- `GET /api/jobs/<job_id>` - Get job details
- `DELETE /api/jobs/<job_id>` - Cancel job
- `GET /api/jobs/<job_id>/stream` - Real-time progress (SSE)

### Results
- `GET /api/jobs/<job_id>/results` - Get results metadata
- `GET /api/jobs/<job_id>/download/<file_type>` - Download files
- `GET /api/jobs/<job_id>/stats` - Get detailed statistics

### System
- `GET /api/health` - Health check
- `GET /api/config` - Get configuration

## Configuration

The web interface uses the same configuration system as the command-line tool. Create or modify `config.yaml` in the parent directory to customize settings:

```yaml
browser:
  headless: true
  timeout_navigation: 60000

scraping:
  max_listings_per_cell: 120
  max_reviews_per_business: 100
  scroll_interval: 1500

files:
  result_filename: 'result.csv'
  reviews_filename: 'reviews.csv'
```

## Job Configuration Options

When starting a job through the web interface, you can configure:

- **Search Term**: What to search for (required)
- **Total Results**: Number of businesses to collect (required)
- **Search Area**: Geographic bounds (optional - draws on map)
- **Grid Size**: How to divide the search area (1x1 to 5x5)
- **Max Reviews**: Reviews to collect per business
- **Headless Mode**: Whether to run browser invisibly

## Real-time Updates

The web interface uses Server-Sent Events (SSE) to provide real-time updates:

- Progress bars update automatically
- Job status changes are reflected immediately
- Completion notifications appear automatically
- No need to refresh the page

## File Downloads

Completed jobs generate these downloadable files:

- **Business Data**: Main results with business information
- **Reviews Data**: Customer reviews and ratings
- **Log Files**: Detailed scraping logs for debugging

## Production Deployment

For production use, consider:

1. **Use a Production WSGI Server**:
   ```bash
   pip install gunicorn
   gunicorn -w 4 -b 0.0.0.0:5000 app:app
   ```

2. **Set Environment Variables**:
   ```bash
   export FLASK_ENV=production
   export SECRET_KEY=your-secret-key
   ```

3. **Configure Reverse Proxy** (nginx/Apache)

4. **Set Resource Limits**: Monitor CPU/memory usage

5. **Database Integration**: For job persistence across restarts

## Security Considerations

- Change the default `SECRET_KEY` in production
- Implement authentication if needed
- Set up HTTPS for production deployment
- Consider rate limiting for the API
- Validate all user inputs

## Troubleshooting

**Common Issues:**

1. **Port Already in Use**: Change port in `app.py` or kill existing process
2. **Import Errors**: Ensure `src` directory is in Python path
3. **Browser Issues**: Check Chrome installation path in config
4. **Memory Issues**: Reduce concurrent jobs or grid size

**Debugging:**

- Check browser console for JavaScript errors
- Monitor Flask logs for backend issues
- Use browser dev tools to inspect network requests
- Check file permissions for downloads

## Browser Compatibility

The web interface supports:
- Chrome/Chromium (recommended)
- Firefox
- Safari
- Edge

**Required Features:**
- JavaScript ES6+
- Server-Sent Events (SSE)
- Fetch API
- CSS Grid/Flexbox

## Architecture

```
Web Interface Components:
├── Flask API Backend (app.py)
├── Background Job Manager (scraper_service.py)
├── HTML Templates (templates/)
├── CSS Styles (static/css/)
├── JavaScript Frontend (static/js/)
└── Job Progress Tracking (SSE)
```

**Key Components:**

- **Flask App**: Serves web pages and API endpoints
- **ScraperManager**: Handles job queuing and execution
- **ProgressCallback**: Reports real-time progress
- **EventSource**: Streams progress updates to browser
- **Interactive Map**: Leaflet.js for area selection

## Development

To modify the web interface:

1. **Frontend Changes**: Edit files in `templates/` and `static/`
2. **API Changes**: Modify `app.py` endpoints
3. **Job Logic**: Update `scraper_service.py`
4. **Styling**: Customize `static/css/style.css`

**Development Server**:
```bash
export FLASK_DEBUG=1
python app.py
```

## Limitations

- Jobs run in memory (lost on restart)
- Single server instance (no clustering)
- Limited to local file storage
- No user authentication/authorization
- Browser resource requirements for map

## Future Enhancements

Potential improvements:
- Database persistence
- User management system
- Job scheduling/automation
- Advanced filtering and search
- Data visualization
- Export to multiple formats
- WebSocket instead of SSE
- Docker containerization