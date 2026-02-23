"""Flask API for Google Maps Scraper Web Interface."""

import json
import os
import time
from datetime import datetime
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify, render_template, Response, send_file
from flask_cors import CORS
from dataclasses import asdict
import logging
from pathlib import Path

from scraper_service import scraper_manager, JobConfig, JobStatus
from src.config import Config
from src.config.config_manager import ConfigurationManager
from src.config.migration import run_migration_if_needed
from src.services.browser_detector import BrowserDetector
from src.services.system_validation import SystemValidationService

# Configure Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-in-production'
CORS(app)  # Enable CORS for all domains

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize configuration manager
yaml_path = Path(__file__).parent.parent / "config.yaml"
db_path = Path(__file__).parent / "database" / "scraper.db"
config_manager = ConfigurationManager(yaml_path, db_path)

# Initialize services
browser_detector = BrowserDetector()
system_validator = SystemValidationService()


# Middleware for onboarding check
@app.before_request
def check_onboarding():
    """Redirect to onboarding if not completed."""
    # Skip check for certain paths
    skip_paths = [
        '/onboarding',
        '/api/onboarding/',
        '/api/system/',
        '/static/',
        '/_debug'  # Flask debug toolbar
    ]

    # Check if current path should skip onboarding check
    if any(request.path.startswith(path) for path in skip_paths):
        return None

    # Skip for all API routes (handled by endpoints themselves if needed)
    if request.path.startswith('/api/'):
        return None

    # Check onboarding status
    try:
        if not config_manager.is_onboarding_completed():
            # Redirect to onboarding page for web routes
            if not request.path.startswith('/api/'):
                from flask import redirect
                return redirect('/onboarding')
    except Exception as e:
        logger.warning(f"Could not check onboarding status: {e}")
        # Continue anyway to avoid breaking the app

    return None


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


# Utility functions
def job_status_to_dict(job_status: JobStatus) -> Dict[str, Any]:
    """Convert JobStatus to dictionary for JSON response."""
    data = asdict(job_status)
    data['elapsed_time'] = job_status.get_elapsed_time()
    data['estimated_remaining'] = job_status.get_estimated_remaining()
    return data


def validate_job_config(data: Dict[str, Any]) -> tuple[Optional[JobConfig], Optional[str]]:
    """Validate and create JobConfig from request data."""
    try:
        job_type = data.get('job_type', 'scrape')

        if job_type == 'owner_enrichment':
            csv_path = str(data.get('owner_csv_path', '')).strip()
            if not csv_path:
                return None, "owner_csv_path is required for owner enrichment jobs"

            output_path = data.get('owner_output_path')
            if output_path is not None and not isinstance(output_path, str):
                return None, "owner_output_path must be a string"

            owner_in_place = bool(data.get('owner_in_place', False))
            if owner_in_place and output_path:
                return None, "owner_in_place cannot be combined with owner_output_path"
            if owner_in_place and bool(data.get('owner_resume', False)):
                return None, "owner_in_place cannot be combined with owner_resume"

            config_overrides = data.get('config_overrides', {})
            if not isinstance(config_overrides, dict):
                return None, "config_overrides must be an object"

            job_config = JobConfig(
                job_type='owner_enrichment',
                owner_csv_path=csv_path,
                owner_output_path=output_path,
                owner_in_place=owner_in_place,
                owner_resume=bool(data.get('owner_resume', False)),
                owner_model=data.get('owner_model'),
                owner_skip_existing=bool(data.get('owner_skip_existing', True)),
                config_overrides=config_overrides,
            )
            return job_config, None

        # Scrape job validation
        search_term = data.get('search_term', '').strip()
        if not search_term:
            return None, "search_term is required"

        total_results = data.get('total_results')
        if not isinstance(total_results, int) or total_results <= 0:
            return None, "total_results must be a positive integer"
        if total_results > 10000:
            return None, "total_results cannot exceed 10000"

        bounds = data.get('bounds')
        if bounds is not None:
            if not isinstance(bounds, list) or len(bounds) != 4:
                return None, "bounds must be an array of 4 numbers [min_lat, min_lng, max_lat, max_lng]"
            try:
                bounds = tuple(float(x) for x in bounds)
            except (ValueError, TypeError):
                return None, "bounds must contain valid numbers"

            min_lat, min_lng, max_lat, max_lng = bounds
            if not (-90 <= min_lat <= 90) or not (-90 <= max_lat <= 90):
                return None, "Latitude values must be between -90 and 90"
            if not (-180 <= min_lng <= 180) or not (-180 <= max_lng <= 180):
                return None, "Longitude values must be between -180 and 180"
            if min_lat >= max_lat:
                return None, "min_lat must be less than max_lat"
            if min_lng >= max_lng:
                return None, "min_lng must be less than max_lng"

        grid_size = data.get('grid_size', 2)
        if not isinstance(grid_size, int) or not (1 <= grid_size <= 10):
            return None, "grid_size must be an integer between 1 and 10"

        max_reviews = data.get('max_reviews')
        if max_reviews is not None:
            if not isinstance(max_reviews, int) or max_reviews < 0:
                return None, "max_reviews must be a non-negative integer"

        headless = data.get('headless', True)
        if not isinstance(headless, bool):
            return None, "headless must be a boolean"

        scraping_mode = data.get('scraping_mode')
        if scraping_mode is None:
            try:
                config = Config.from_file(str(Path(__file__).parent.parent / "config.yaml"))
                scraping_mode = config.settings.scraping.default_mode
            except Exception:
                scraping_mode = 'fast'
        if scraping_mode not in ['fast', 'coverage']:
            return None, "scraping_mode must be 'fast' or 'coverage'"

        config_overrides = data.get('config_overrides', {})
        if not isinstance(config_overrides, dict):
            return None, "config_overrides must be an object"

        owner_override = config_overrides.get('owner_enrichment')
        if owner_override is not None:
            if not isinstance(owner_override, dict):
                return None, "owner_enrichment override must be an object"
            if 'max_pages' in owner_override:
                try:
                    max_pages = int(owner_override['max_pages'])
                except (TypeError, ValueError):
                    return None, "owner_enrichment.max_pages must be an integer"
                if max_pages < 1 or max_pages > 10:
                    return None, "owner_enrichment.max_pages must be between 1 and 10"

        job_config = JobConfig(
            job_type='scrape',
            search_term=search_term,
            total_results=total_results,
            bounds=bounds,
            grid_size=grid_size,
            scraping_mode=scraping_mode,
            max_reviews=max_reviews,
            headless=headless,
            config_overrides=config_overrides
        )
        return job_config, None

    except Exception as e:
        return None, f"Invalid request data: {str(e)}"


# Web routes
@app.route('/onboarding')
def onboarding_page():
    """Serve the onboarding wizard page."""
    return render_template('onboarding.html')


@app.route('/settings')
def settings_page():
    """Serve the settings page."""
    return render_template('settings.html')


@app.route('/')
def index():
    """Serve the main dashboard."""
    return render_template('index.html')


@app.route('/jobs/<job_id>')
def job_detail(job_id):
    """Serve the job detail page."""
    return render_template('job_detail.html', job_id=job_id)


@app.route('/results/<job_id>')
def results_viewer(job_id):
    """Serve the results viewer page."""
    return render_template('results.html', job_id=job_id)


# API routes
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'active_jobs': len(scraper_manager.get_active_jobs()),
        'total_jobs': len(scraper_manager.list_jobs())
    })


# System endpoints
@app.route('/api/system/status', methods=['GET'])
def system_status():
    """Get comprehensive system status including onboarding state."""
    try:
        system_settings = config_manager.get_system_settings_dict()

        # Get current effective config
        effective_config = config_manager.get_effective_config()
        chrome_path = effective_config.settings.browser.executable_path

        # Run system checks
        checks = system_validator.run_system_checks(chrome_path if system_settings.get('chrome_configured') else None)

        # Convert ValidationResult objects to dicts
        checks_dict = {k: v.to_dict() for k, v in checks.items()}

        return jsonify({
            'onboarding_completed': system_settings.get('onboarding_completed', False),
            'chrome_configured': bool(system_settings.get('chrome_path')),
            'chrome_validated': system_settings.get('chrome_validated', False),
            'chrome_version': checks_dict.get('chrome', {}).get('details', {}).get('version') if 'chrome' in checks_dict else None,
            'api_key_configured': bool(system_settings.get('openrouter_api_key')),
            'api_key_validated': system_settings.get('openrouter_validated', False),
            'owner_enrichment_enabled': system_settings.get('owner_enrichment_enabled', False),
            'system_checks': checks_dict
        })

    except Exception as e:
        logger.error(f"System status check failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/system/validate-browser', methods=['POST'])
def validate_browser():
    """Validate a Chrome browser path."""
    try:
        data = request.get_json()
        chrome_path = data.get('chrome_path', '').strip()

        if not chrome_path:
            return jsonify({'error': 'chrome_path is required'}), 400

        # Validate the path
        result = system_validator.validate_chrome(chrome_path)

        return jsonify(result.to_dict())

    except Exception as e:
        logger.error(f"Browser validation failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/system/detect-browser', methods=['GET'])
def detect_browser():
    """Auto-detect Chrome browser installations."""
    try:
        candidates = browser_detector.detect_browsers()

        # Convert to dicts
        candidates_dict = [c.to_dict() for c in candidates]

        # Get best candidate
        best = browser_detector.get_best_candidate()
        best_path = best.path if best else None

        return jsonify({
            'candidates': candidates_dict,
            'best_candidate': best_path
        })

    except Exception as e:
        logger.error(f"Browser detection failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/system/validate-api-key', methods=['POST'])
def validate_api_key():
    """Validate OpenRouter API key."""
    try:
        data = request.get_json()
        api_key = data.get('api_key', '').strip()
        model = data.get('model')

        if not api_key:
            return jsonify({'error': 'api_key is required'}), 400

        # Validate the API key
        result = system_validator.validate_openrouter_api_key(api_key, model)

        return jsonify(result.to_dict())

    except Exception as e:
        logger.error(f"API key validation failed: {e}")
        return jsonify({'error': str(e)}), 500


# Onboarding endpoints
@app.route('/api/onboarding/status', methods=['GET'])
def onboarding_status():
    """Get onboarding status."""
    try:
        system_settings = config_manager.get_system_settings_dict()

        return jsonify({
            'onboarding_completed': system_settings.get('onboarding_completed', False),
            'chrome_configured': bool(system_settings.get('chrome_path')),
            'api_key_configured': bool(system_settings.get('openrouter_api_key'))
        })

    except Exception as e:
        logger.error(f"Failed to get onboarding status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/onboarding/complete', methods=['POST'])
def complete_onboarding():
    """Mark onboarding as completed."""
    try:
        config_manager.mark_onboarding_completed()

        return jsonify({
            'success': True,
            'message': 'Onboarding completed successfully'
        })

    except Exception as e:
        logger.error(f"Failed to complete onboarding: {e}")
        return jsonify({'error': str(e)}), 500


# Settings endpoints
@app.route('/api/settings', methods=['GET'])
def get_settings():
    """Get current settings."""
    try:
        system_settings = config_manager.get_system_settings_dict()
        user_prefs = config_manager.get_user_preferences_dict()

        # Don't include encrypted API key in response
        if 'openrouter_api_key' in system_settings:
            system_settings['openrouter_api_key'] = '***' if system_settings['openrouter_api_key'] else None

        return jsonify({
            'system_settings': system_settings,
            'user_preferences': user_prefs
        })

    except Exception as e:
        logger.error(f"Failed to get settings: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/settings', methods=['PUT'])
def update_settings():
    """Update settings."""
    try:
        data = request.get_json()

        # Update Chrome path if provided
        if 'chrome_path' in data:
            chrome_path = data['chrome_path'].strip()
            validated = data.get('chrome_validated', False)
            config_manager.save_chrome_path(chrome_path, validated)

        # Update API key if provided
        if 'api_key' in data:
            api_key = data['api_key'].strip()
            model = data.get('model')
            validated = data.get('api_key_validated', False)
            config_manager.save_api_key(api_key, model, validated)

        # Update owner enrichment enabled
        if 'owner_enrichment_enabled' in data:
            enabled = bool(data['owner_enrichment_enabled'])
            config_manager.save_owner_enrichment_enabled(enabled)

        # Update user preferences
        user_pref_updates = {}
        if 'default_headless' in data:
            user_pref_updates['default_headless'] = bool(data['default_headless'])
        if 'default_grid_size' in data:
            user_pref_updates['default_grid_size'] = int(data['default_grid_size'])
        if 'default_scraping_mode' in data:
            user_pref_updates['default_scraping_mode'] = data['default_scraping_mode']

        if user_pref_updates:
            config_manager.db_repo.update_user_preferences(user_pref_updates)

        config_manager.invalidate_cache()

        return jsonify({
            'success': True,
            'message': 'Settings updated successfully'
        })

    except Exception as e:
        logger.error(f"Failed to update settings: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/settings/test', methods=['POST'])
def test_settings():
    """Test settings without saving."""
    try:
        data = request.get_json()
        results = {}

        # Test Chrome path if provided
        if 'chrome_path' in data:
            chrome_path = data['chrome_path'].strip()
            result = system_validator.validate_chrome(chrome_path)
            results['chrome'] = result.to_dict()

        # Test API key if provided
        if 'api_key' in data:
            api_key = data['api_key'].strip()
            model = data.get('model')
            result = system_validator.validate_openrouter_api_key(api_key, model)
            results['api_key'] = result.to_dict()

        return jsonify(results)

    except Exception as e:
        logger.error(f"Settings test failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs', methods=['POST'])
def start_job():
    """Start a new scraping job."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body must be JSON'}), 400
        
        # Validate job configuration
        job_config, error = validate_job_config(data)
        if error:
            return jsonify({'error': error}), 400
        
        # Start the job
        job_id = scraper_manager.start_job(job_config)

        if job_config.job_type == 'owner_enrichment':
            logger.info(f"Started owner enrichment job {job_id}: {job_config.owner_csv_path}")
        else:
            logger.info(f"Started job {job_id}: {job_config.search_term}")
        
        return jsonify({
            'job_id': job_id,
            'status': 'pending',
            'message': 'Job started successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"Error starting job: {e}")
        return jsonify({'error': 'Failed to start job'}), 500


@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    """List all jobs."""
    try:
        limit = request.args.get('limit', 50, type=int)
        status_filter = request.args.get('status')  # pending, running, completed, failed, cancelled
        
        jobs = scraper_manager.list_jobs(limit=limit)
        
        # Apply status filter if provided
        if status_filter:
            jobs = [job for job in jobs if job.status == status_filter]
        
        return jsonify({
            'jobs': [job_status_to_dict(job) for job in jobs],
            'total': len(jobs)
        })
        
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        return jsonify({'error': 'Failed to list jobs'}), 500


@app.route('/api/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get status of a specific job."""
    try:
        job = scraper_manager.get_job_status(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify(job_status_to_dict(job))
        
    except Exception as e:
        logger.error(f"Error getting job status for {job_id}: {e}")
        return jsonify({'error': 'Failed to get job status'}), 500


@app.route('/api/jobs/<job_id>', methods=['DELETE'])
def cancel_job(job_id):
    """Cancel a job."""
    try:
        success = scraper_manager.cancel_job(job_id)
        if not success:
            return jsonify({'error': 'Job not found or cannot be cancelled'}), 404
        
        logger.info(f"Cancelled job {job_id}")
        
        return jsonify({
            'message': 'Job cancelled successfully',
            'job_id': job_id
        })
        
    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
        return jsonify({'error': 'Failed to cancel job'}), 500


@app.route('/api/jobs/<job_id>/stream')
def stream_job_progress(job_id):
    """Stream real-time progress updates for a job using Server-Sent Events."""
    def generate():
        """Generate progress updates."""
        last_status = None
        
        while True:
            try:
                job = scraper_manager.get_job_status(job_id)
                if not job:
                    yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
                    break
                
                current_status = job_status_to_dict(job)
                
                # Only send update if status changed
                if current_status != last_status:
                    yield f"data: {json.dumps(current_status)}\n\n"
                    last_status = current_status
                
                # Stop streaming if job is finished
                if job.status in ['completed', 'failed', 'cancelled']:
                    break
                
                time.sleep(1)  # Update every second
                
            except Exception as e:
                logger.error(f"Error streaming progress for job {job_id}: {e}")
                yield f"data: {json.dumps({'error': 'Stream error'})}\n\n"
                break
    
    return Response(generate(), mimetype='text/event-stream',
                   headers={
                       'Cache-Control': 'no-cache',
                       'Connection': 'keep-alive',
                       'X-Accel-Buffering': 'no'
                   })


@app.route('/api/jobs/<job_id>/results', methods=['GET'])
def get_job_results(job_id):
    """Get results information for a job."""
    try:
        job = scraper_manager.get_job_status(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job.status != 'completed':
            return jsonify({'error': 'Job not completed yet'}), 400
        
        results = scraper_manager.get_job_results(job_id)
        
        # Check if files exist and get file sizes
        file_info = {}
        for file_type, file_path in results.items():
            if file_path and os.path.exists(file_path):
                stat = os.stat(file_path)
                file_info[file_type] = {
                    'path': file_path,
                    'size': stat.st_size,
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
            else:
                file_info[file_type] = None
        
        return jsonify({
            'job_id': job_id,
            'files': file_info,
            'completion_time': job.end_time
        })
        
    except Exception as e:
        logger.error(f"Error getting results for job {job_id}: {e}")
        return jsonify({'error': 'Failed to get job results'}), 500


@app.route('/api/jobs/<job_id>/download/<file_type>')
def download_job_file(job_id, file_type):
    """Download a specific file from a completed job."""
    try:
        job = scraper_manager.get_job_status(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job.status != 'completed':
            return jsonify({'error': 'Job not completed yet'}), 400
        
        results = scraper_manager.get_job_results(job_id)
        
        if file_type not in results:
            return jsonify({'error': 'Invalid file type'}), 400
        
        file_path = results[file_type]
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=f"{job_id}_{file_type}.csv"
        )
        
    except Exception as e:
        logger.error(f"Error downloading file {file_type} for job {job_id}: {e}")
        return jsonify({'error': 'Failed to download file'}), 500


@app.route('/api/jobs/<job_id>/stats')
def get_job_stats(job_id):
    """Get detailed statistics for a job."""
    try:
        job = scraper_manager.get_job_status(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        stats = {
            'job_id': job_id,
            'search_term': job.config.search_term,
            'total_target': job.config.total_results,
            'current_progress': job.progress.get('current', 0),
            'percentage': job.progress.get('percentage', 0),
            'cells_completed': job.progress.get('cells_completed', 0),
            'cells_total': job.progress.get('cells_total', 0),
            'elapsed_time': job.get_elapsed_time(),
            'estimated_remaining': job.get_estimated_remaining(),
            'status': job.status,
            'start_time': job.start_time,
            'end_time': job.end_time
        }
        
        # Add file statistics if completed
        if job.status == 'completed':
            results = scraper_manager.get_job_results(job_id)
            file_stats = {}
            
            for file_type, file_path in results.items():
                if file_path and os.path.exists(file_path):
                    stat = os.stat(file_path)
                    file_stats[file_type] = {
                        'size_bytes': stat.st_size,
                        'size_mb': round(stat.st_size / 1024 / 1024, 2)
                    }
                    
                    # Try to get row count for CSV files
                    if file_path.endswith('.csv'):
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                row_count = sum(1 for line in f) - 1  # Subtract header
                            file_stats[file_type]['row_count'] = row_count
                        except Exception:
                            pass
            
            stats['file_stats'] = file_stats
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting stats for job {job_id}: {e}")
        return jsonify({'error': 'Failed to get job stats'}), 500


@app.route('/api/config', methods=['GET'])
def get_config():
    """Get default configuration values."""
    try:
        config = Config.from_file(str(Path(__file__).parent.parent / "config.yaml"))
    except Exception:
        config = Config()

    settings = config.settings
    return jsonify({
        'default_bounds': list(settings.grid.default_bounds),
        'default_grid_size': settings.grid.default_grid_size,
        'max_results': 10000,
        'max_reviews': settings.scraping.max_reviews_per_business,
        'supported_file_types': ['business_data', 'reviews_data', 'log_file']
    })


# Cleanup task - using with_appcontext instead of deprecated before_first_request
def startup():
    """Run startup tasks."""
    logger.info("Google Maps Scraper Web Interface started")

    # Run migration if needed (existing installations)
    try:
        run_migration_if_needed(yaml_path, db_path)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        logger.warning("Manual configuration may be required")

    # Clean up old jobs on startup
    scraper_manager.cleanup_old_jobs(older_than_hours=48)

# Call startup when app is created
with app.app_context():
    startup()


if __name__ == '__main__':
    # Development server
    debug_mode = os.getenv("FLASK_DEBUG", "0").lower() in {"1", "true", "yes", "on"}
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    app.run(debug=debug_mode, host=host, port=port, threaded=True)
