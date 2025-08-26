/* Google Maps Scraper Web Interface JavaScript */

// Global variables
let map = null;
let boundsRectangle = null;
let activeJobEventSources = new Map();
let lastJobsUpdate = 0;

// Configuration
const CONFIG = {
    refreshInterval: 5000, // 5 seconds
    maxToasts: 5,
    defaultBounds: [52.4, 13.2, 52.6, 13.6], // Berlin area
    mapCenter: [52.52, 13.405],
    mapZoom: 11
};

// Initialize the application
function initializeApp() {
    console.log('Initializing Google Maps Scraper Web Interface');
    
    // Initialize map
    initializeMap();
    
    // Set up form submission
    setupFormSubmission();
    
    // Load initial data
    loadJobs();
    updateHeaderStats();
    
    // Start periodic refresh
    setInterval(() => {
        refreshActiveJobs();
        updateHeaderStats();
    }, CONFIG.refreshInterval);
    
    // Load configuration
    loadConfiguration();
}

// Map initialization
function initializeMap() {
    try {
        // Initialize Leaflet map
        map = L.map('map').setView(CONFIG.mapCenter, CONFIG.mapZoom);
        
        // Add OpenStreetMap tiles
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(map);
        
        // Drawing controls will be enabled via toggle button
        
        console.log('Map initialized successfully');
    } catch (error) {
        console.error('Error initializing map:', error);
        showToast('Error loading map', 'error');
    }
}

// Map drawing functionality
let isDrawing = false;
let isDrawingModeEnabled = false;
let startLatLng = null;

function startDrawing(e) {
    if (isDrawing || !isDrawingModeEnabled) return;
    
    isDrawing = true;
    startLatLng = e.latlng;
    
    // Remove existing rectangle
    if (boundsRectangle) {
        map.removeLayer(boundsRectangle);
        boundsRectangle = null;
        updateBoundsInfo();
    }
    
    // Change cursor
    map.getContainer().style.cursor = 'crosshair';
    
    // Add temporary event listeners
    map.on('mousemove', updateDrawing);
    map.on('mouseup', finishDrawing);
}

function updateDrawing(e) {
    if (!isDrawing || !startLatLng) return;
    
    // Remove previous rectangle
    if (boundsRectangle) {
        map.removeLayer(boundsRectangle);
    }
    
    // Create new rectangle
    const bounds = L.latLngBounds(startLatLng, e.latlng);
    boundsRectangle = L.rectangle(bounds, {
        color: '#007bff',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.2
    }).addTo(map);
}

function finishDrawing(e) {
    if (!isDrawing || !startLatLng) return;
    
    isDrawing = false;
    
    // Reset cursor
    map.getContainer().style.cursor = isDrawingModeEnabled ? 'crosshair' : '';
    
    // Remove event listeners
    map.off('mousemove', updateDrawing);
    map.off('mouseup', finishDrawing);
    
    // Create final rectangle
    if (boundsRectangle) {
        const bounds = boundsRectangle.getBounds();
        console.log('Selected bounds:', bounds);
        showToast('Search area selected', 'success');
        updateBoundsInfo();
        
        // Exit drawing mode after selection
        toggleDrawingMode(false);
    }
    
    startLatLng = null;
}

// Drawing mode toggle
function toggleDrawingMode(enable) {
    if (enable === undefined) {
        enable = !isDrawingModeEnabled;
    }
    
    isDrawingModeEnabled = enable;
    const drawButton = document.getElementById('draw-area');
    
    if (enable) {
        drawButton.classList.add('btn-success');
        drawButton.classList.remove('btn-primary');
        drawButton.innerHTML = '<i class="fas fa-times"></i> Cancel Drawing';
        map.getContainer().style.cursor = 'crosshair';
        
        // Disable map dragging when in drawing mode
        map.dragging.disable();
        map.doubleClickZoom.disable();
        
        map.on('mousedown', startDrawing);
        showToast('Click and drag to draw area', 'info');
    } else {
        drawButton.classList.add('btn-primary');
        drawButton.classList.remove('btn-success');
        drawButton.innerHTML = '<i class="fas fa-draw-polygon"></i> Draw Area';
        map.getContainer().style.cursor = '';
        
        // Re-enable map dragging
        map.dragging.enable();
        map.doubleClickZoom.enable();
        
        map.off('mousedown', startDrawing);
        
        // Cancel active drawing
        if (isDrawing) {
            isDrawing = false;
            map.off('mousemove', updateDrawing);
            map.off('mouseup', finishDrawing);
            startLatLng = null;
        }
    }
}

// Bounds management
function clearBounds() {
    if (boundsRectangle) {
        map.removeLayer(boundsRectangle);
        boundsRectangle = null;
        updateBoundsInfo();
        showToast('Search area cleared', 'info');
    }
    
    // Exit drawing mode
    toggleDrawingMode(false);
}

function useDefaultBounds() {
    clearBounds();
    
    const [minLat, minLng, maxLat, maxLng] = CONFIG.defaultBounds;
    const bounds = L.latLngBounds([minLat, minLng], [maxLat, maxLng]);
    
    boundsRectangle = L.rectangle(bounds, {
        color: '#007bff',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.2
    }).addTo(map);
    
    map.fitBounds(bounds);
    updateBoundsInfo();
    showToast('Using Berlin default bounds', 'info');
}

// Update bounds info display
function updateBoundsInfo() {
    const boundsInfo = document.getElementById('bounds-info');
    const boundsCoords = document.getElementById('bounds-coordinates');
    
    if (boundsRectangle) {
        const bounds = boundsRectangle.getBounds();
        const south = bounds.getSouth().toFixed(3);
        const north = bounds.getNorth().toFixed(3);
        const west = bounds.getWest().toFixed(3);
        const east = bounds.getEast().toFixed(3);
        
        boundsCoords.textContent = `${south}, ${west} to ${north}, ${east}`;
        boundsInfo.style.display = 'block';
    } else {
        boundsInfo.style.display = 'none';
    }
}

// Form submission
function setupFormSubmission() {
    const form = document.getElementById('scraper-form');
    form.addEventListener('submit', handleFormSubmit);
}

async function handleFormSubmit(e) {
    e.preventDefault();
    
    const formData = new FormData(e.target);
    const data = Object.fromEntries(formData.entries());
    
    // Validate form data
    const validation = validateFormData(data);
    if (!validation.valid) {
        showToast(validation.message, 'error');
        return;
    }
    
    // Prepare job configuration
    const jobConfig = prepareJobConfig(data);
    
    try {
        showLoadingOverlay('Starting scraping job...');
        
        const response = await fetch('/api/jobs', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(jobConfig)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showToast('Scraping job started successfully!', 'success');
            
            // Reset form
            e.target.reset();
            clearBounds();
            document.getElementById('grid-label').textContent = '2x2 cells';
            
            // Refresh job lists
            setTimeout(() => {
                loadJobs();
                updateHeaderStats();
            }, 1000);
            
        } else {
            throw new Error(result.error || 'Failed to start job');
        }
        
    } catch (error) {
        console.error('Error starting job:', error);
        showToast(error.message || 'Failed to start scraping job', 'error');
    } finally {
        hideLoadingOverlay();
    }
}

function validateFormData(data) {
    if (!data.search_term || data.search_term.trim().length === 0) {
        return { valid: false, message: 'Search term is required' };
    }
    
    const totalResults = parseInt(data.total_results);
    if (!totalResults || totalResults < 1 || totalResults > 10000) {
        return { valid: false, message: 'Total results must be between 1 and 10,000' };
    }
    
    const gridSize = parseInt(data.grid_size);
    if (!gridSize || gridSize < 1 || gridSize > 5) {
        return { valid: false, message: 'Grid size must be between 1 and 5' };
    }
    
    const maxReviews = data.max_reviews ? parseInt(data.max_reviews) : null;
    if (maxReviews && (maxReviews < 0 || maxReviews > 200)) {
        return { valid: false, message: 'Max reviews must be between 0 and 200' };
    }
    
    // Validate bounds area if selected
    if (boundsRectangle) {
        const bounds = boundsRectangle.getBounds();
        const latDiff = Math.abs(bounds.getNorth() - bounds.getSouth());
        const lngDiff = Math.abs(bounds.getEast() - bounds.getWest());
        
        // Minimum area check (about 1km x 1km at equator)
        if (latDiff < 0.01 || lngDiff < 0.01) {
            return { valid: false, message: 'Selected area is too small. Please draw a larger rectangle.' };
        }
        
        // Maximum area check (prevent extremely large areas)
        if (latDiff > 180 || lngDiff > 360) {
            return { valid: false, message: 'Selected area is too large. Please draw a smaller rectangle.' };
        }
    }
    
    return { valid: true };
}

function prepareJobConfig(data) {
    const config = {
        search_term: data.search_term.trim(),
        total_results: parseInt(data.total_results),
        grid_size: parseInt(data.grid_size),
        headless: data.headless === 'on'
    };
    
    // Add max reviews if specified
    if (data.max_reviews && parseInt(data.max_reviews) > 0) {
        config.max_reviews = parseInt(data.max_reviews);
    }
    
    // Add bounds if selected
    if (boundsRectangle) {
        const bounds = boundsRectangle.getBounds();
        
        // Normalize bounds to ensure min < max (fixes drawing direction issues)
        const south = bounds.getSouth();
        const north = bounds.getNorth();
        const west = bounds.getWest();
        const east = bounds.getEast();
        
        config.bounds = [
            Math.min(south, north),  // min_lat
            Math.min(west, east),    // min_lng
            Math.max(south, north),  // max_lat
            Math.max(west, east)     // max_lng
        ];
    }
    
    return config;
}

// Job management
async function loadJobs() {
    try {
        const response = await fetch('/api/jobs?limit=50');
        const data = await response.json();
        
        if (response.ok) {
            const activeJobs = data.jobs.filter(job => job.status === 'running' || job.status === 'pending');
            const completedJobs = data.jobs.filter(job => job.status !== 'running' && job.status !== 'pending');
            
            displayActiveJobs(activeJobs);
            displayCompletedJobs(completedJobs);
            
            // Setup real-time monitoring for active jobs
            setupRealTimeMonitoring(activeJobs);
            
        } else {
            throw new Error(data.error || 'Failed to load jobs');
        }
    } catch (error) {
        console.error('Error loading jobs:', error);
        showToast('Error loading jobs', 'error');
    }
}

function displayActiveJobs(jobs) {
    const container = document.getElementById('active-jobs');
    
    if (jobs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-clock"></i>
                <p>No active jobs</p>
                <small>Start a new scraping job above</small>
            </div>
        `;
        return;
    }
    
    container.innerHTML = jobs.map(job => createJobElement(job, true)).join('');
}

function displayCompletedJobs(jobs) {
    const container = document.getElementById('completed-jobs');
    
    if (jobs.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-check-circle"></i>
                <p>No completed jobs</p>
                <small>Completed jobs will appear here</small>
            </div>
        `;
        return;
    }
    
    container.innerHTML = jobs.map(job => createJobElement(job, false)).join('');
}

function createJobElement(job, isActive = false) {
    const statusClass = getStatusClass(job.status);
    const statusIcon = getStatusIcon(job.status);
    const progress = job.progress;
    const percentage = Math.round(progress.percentage);
    
    return `
        <div class="job-item" data-job-id="${job.job_id}">
            <div class="job-header">
                <h3 class="job-title">${job.config.search_term}</h3>
                <div class="status-badge ${statusClass}">
                    <i class="fas ${statusIcon}"></i>
                    ${job.status.toUpperCase()}
                </div>
            </div>
            
            <div class="job-meta">
                <div class="job-id">
                    <strong>ID:</strong> 
                    <code>${job.job_id.substring(0, 8)}...</code>
                </div>
                <div class="time-info">
                    <i class="fas fa-clock"></i>
                    <span>${job.elapsed_time}</span>
                </div>
                ${job.estimated_remaining && isActive ? `
                    <div class="time-info">
                        <i class="fas fa-hourglass-half"></i>
                        <span>~${job.estimated_remaining}</span>
                    </div>
                ` : ''}
            </div>
            
            ${isActive ? `
                <div class="job-progress">
                    <div class="progress-label">
                        <span>Progress</span>
                        <span>${progress.current} / ${progress.total} (${percentage}%)</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: ${percentage}%"></div>
                    </div>
                </div>
            ` : ''}
            
            <div class="job-stats">
                <div class="stat-item">
                    <i class="fas fa-target"></i>
                    <span class="stat-value">${progress.total.toLocaleString()}</span>
                    <span class="stat-label">Target</span>
                </div>
                <div class="stat-item">
                    <i class="fas fa-check"></i>
                    <span class="stat-value">${progress.current.toLocaleString()}</span>
                    <span class="stat-label">Collected</span>
                </div>
                <div class="stat-item">
                    <i class="fas fa-th"></i>
                    <span class="stat-value">${job.config.grid_size}x${job.config.grid_size}</span>
                    <span class="stat-label">Grid</span>
                </div>
                <div class="stat-item">
                    <i class="fas fa-route"></i>
                    <span class="stat-value">${job.config.scraping_mode || 'fast'}</span>
                    <span class="stat-label">Mode</span>
                </div>
                ${job.config.max_reviews ? `
                    <div class="stat-item">
                        <i class="fas fa-star"></i>
                        <span class="stat-value">${job.config.max_reviews}</span>
                        <span class="stat-label">Max Reviews</span>
                    </div>
                ` : ''}
            </div>
            
            ${job.progress.cell_distribution && Object.keys(job.progress.cell_distribution).length > 0 ? `
                <div class="cell-distribution">
                    <small class="distribution-summary">
                        <strong>Cell Coverage:</strong> 
                        ${job.progress.cell_distribution.cells_with_results}/${job.progress.cell_distribution.total_cells_expected} cells used
                        ${job.progress.cell_distribution.avg_per_cell > 0 ? `
                            | Avg: ${Math.round(job.progress.cell_distribution.avg_per_cell)}/cell
                            | Range: ${job.progress.cell_distribution.min_per_cell}-${job.progress.cell_distribution.max_per_cell}
                        ` : ''}
                    </small>
                </div>
            ` : ''}
            
            <div class="job-actions">
                <a href="/jobs/${job.job_id}" class="btn btn-primary btn-sm">
                    <i class="fas fa-info-circle"></i> Details
                </a>
                
                ${job.status === 'completed' ? `
                    <button onclick="downloadResults('${job.job_id}')" class="btn btn-success btn-sm">
                        <i class="fas fa-download"></i> Download
                    </button>
                ` : ''}
                
                ${(job.status === 'running' || job.status === 'pending') ? `
                    <button onclick="cancelJob('${job.job_id}')" class="btn btn-danger btn-sm">
                        <i class="fas fa-stop"></i> Cancel
                    </button>
                ` : ''}
            </div>
        </div>
    `;
}

// Real-time monitoring
function setupRealTimeMonitoring(activeJobs) {
    // Close existing event sources
    activeJobEventSources.forEach((eventSource, jobId) => {
        eventSource.close();
    });
    activeJobEventSources.clear();
    
    // Start monitoring active jobs
    activeJobs.forEach(job => {
        if (job.status === 'running' || job.status === 'pending') {
            startJobMonitoring(job.job_id);
        }
    });
}

function startJobMonitoring(jobId) {
    if (activeJobEventSources.has(jobId)) {
        return; // Already monitoring
    }
    
    const eventSource = new EventSource(`/api/jobs/${jobId}/stream`);
    
    eventSource.onmessage = function(event) {
        try {
            const job = JSON.parse(event.data);
            updateJobDisplay(job);
            
            // Stop monitoring if job finished
            if (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') {
                eventSource.close();
                activeJobEventSources.delete(jobId);
                
                // Refresh job lists after a delay
                setTimeout(() => {
                    loadJobs();
                    updateHeaderStats();
                }, 1000);
                
                // Show completion notification
                const statusMessage = job.status === 'completed' ? 'completed successfully' : 
                                    job.status === 'failed' ? 'failed' : 'was cancelled';
                showToast(`Job ${statusMessage}: ${job.config.search_term}`, 
                         job.status === 'completed' ? 'success' : 'error');
            }
            
        } catch (error) {
            console.error('Error parsing job update:', error);
        }
    };
    
    eventSource.onerror = function(event) {
        console.error('Job monitoring error for', jobId, event);
        eventSource.close();
        activeJobEventSources.delete(jobId);
    };
    
    activeJobEventSources.set(jobId, eventSource);
}

function updateJobDisplay(job) {
    const jobElement = document.querySelector(`[data-job-id="${job.job_id}"]`);
    if (!jobElement) return;
    
    // Update progress bar
    const progressBar = jobElement.querySelector('.progress-fill');
    const progressLabel = jobElement.querySelector('.progress-label span:last-child');
    
    if (progressBar && progressLabel) {
        const percentage = Math.round(job.progress.percentage);
        progressBar.style.width = `${percentage}%`;
        progressLabel.textContent = `${job.progress.current} / ${job.progress.total} (${percentage}%)`;
    }
    
    // Update stats
    const statValues = jobElement.querySelectorAll('.stat-value');
    if (statValues.length >= 2) {
        statValues[1].textContent = job.progress.current.toLocaleString(); // Collected count
    }
    
    // Update time info
    const timeSpan = jobElement.querySelector('.time-info span');
    if (timeSpan) {
        timeSpan.textContent = job.elapsed_time;
    }
    
    // Update estimated remaining time
    const estimatedSpan = jobElement.querySelector('.time-info:nth-child(3) span');
    if (estimatedSpan && job.estimated_remaining) {
        estimatedSpan.textContent = `~${job.estimated_remaining}`;
    }
}

// Job actions
async function cancelJob(jobId) {
    if (!confirm('Are you sure you want to cancel this job?')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/jobs/${jobId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showToast('Job cancelled successfully', 'success');
            
            // Close event source if monitoring
            if (activeJobEventSources.has(jobId)) {
                activeJobEventSources.get(jobId).close();
                activeJobEventSources.delete(jobId);
            }
            
            // Refresh jobs
            setTimeout(() => {
                loadJobs();
                updateHeaderStats();
            }, 500);
            
        } else {
            throw new Error(result.error || 'Failed to cancel job');
        }
        
    } catch (error) {
        console.error('Error cancelling job:', error);
        showToast(error.message || 'Failed to cancel job', 'error');
    }
}

async function downloadResults(jobId) {
    try {
        // Get job results info first
        const response = await fetch(`/api/jobs/${jobId}/results`);
        const results = await response.json();
        
        if (!response.ok) {
            throw new Error(results.error || 'Failed to get results info');
        }
        
        // Download business data
        if (results.files.business_data) {
            const businessLink = document.createElement('a');
            businessLink.href = `/api/jobs/${jobId}/download/business_data`;
            businessLink.download = `${jobId}_business_data.csv`;
            document.body.appendChild(businessLink);
            businessLink.click();
            document.body.removeChild(businessLink);
        }
        
        // Download reviews data if available
        if (results.files.reviews_data) {
            setTimeout(() => {
                const reviewsLink = document.createElement('a');
                reviewsLink.href = `/api/jobs/${jobId}/download/reviews_data`;
                reviewsLink.download = `${jobId}_reviews_data.csv`;
                document.body.appendChild(reviewsLink);
                reviewsLink.click();
                document.body.removeChild(reviewsLink);
            }, 1000);
        }
        
        showToast('Download started', 'success');
        
    } catch (error) {
        console.error('Error downloading results:', error);
        showToast(error.message || 'Failed to download results', 'error');
    }
}

// Header stats
async function updateHeaderStats() {
    try {
        const response = await fetch('/api/health');
        const health = await response.json();
        
        if (response.ok) {
            document.getElementById('active-count').textContent = `${health.active_jobs} Active`;
            document.getElementById('total-count').textContent = `${health.total_jobs} Total`;
        }
    } catch (error) {
        console.error('Error updating header stats:', error);
    }
}

// Refresh functions
async function refreshActiveJobs() {
    const activeContainer = document.getElementById('active-jobs');
    if (activeContainer.querySelector('.empty-state')) {
        // Only refresh if we're not currently showing active jobs
        loadJobs();
    }
}

async function refreshRecentJobs() {
    loadJobs();
}

// Filter jobs
function filterJobs() {
    const statusFilter = document.getElementById('status-filter').value;
    loadJobs(); // Reload with current filter
    // Note: In a full implementation, we'd pass the filter to the API
}

// Configuration loading
async function loadConfiguration() {
    try {
        const response = await fetch('/api/config');
        const config = await response.json();
        
        if (response.ok) {
            // Update default bounds if different
            CONFIG.defaultBounds = config.default_bounds;
        }
    } catch (error) {
        console.error('Error loading configuration:', error);
    }
}

// Utility functions
function getStatusClass(status) {
    const classes = {
        'pending': 'status-pending',
        'running': 'status-running',
        'completed': 'status-completed',
        'failed': 'status-failed',
        'cancelled': 'status-cancelled'
    };
    return classes[status] || 'status-unknown';
}

function getStatusIcon(status) {
    const icons = {
        'pending': 'fa-clock',
        'running': 'fa-spinner fa-spin',
        'completed': 'fa-check-circle',
        'failed': 'fa-times-circle',
        'cancelled': 'fa-ban'
    };
    return icons[status] || 'fa-question-circle';
}

function formatDateTime(dateString) {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleString();
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Loading overlay
function showLoadingOverlay(message = 'Loading...') {
    const overlay = document.getElementById('loading-overlay');
    const spinner = overlay.querySelector('.spinner p');
    if (spinner) {
        spinner.textContent = message;
    }
    overlay.classList.add('show');
}

function hideLoadingOverlay() {
    const overlay = document.getElementById('loading-overlay');
    overlay.classList.remove('show');
}

// Toast notifications
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    
    // Limit number of toasts
    const existingToasts = container.querySelectorAll('.toast');
    if (existingToasts.length >= CONFIG.maxToasts) {
        existingToasts[0].remove();
    }
    
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    
    const icon = type === 'success' ? 'fa-check' : 
                 type === 'error' ? 'fa-exclamation-triangle' : 
                 'fa-info-circle';
    
    toast.innerHTML = `
        <i class="fas ${icon}"></i>
        <span>${message}</span>
    `;
    
    container.appendChild(toast);
    
    // Show toast
    setTimeout(() => toast.classList.add('show'), 100);
    
    // Auto remove
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => {
            if (container.contains(toast)) {
                container.removeChild(toast);
            }
        }, 300);
    }, 4000);
}

// Event listeners setup
document.addEventListener('DOMContentLoaded', function() {
    // Map bounds controls
    const drawAreaBtn = document.getElementById('draw-area');
    const clearBoundsBtn = document.getElementById('clear-bounds');
    const defaultBoundsBtn = document.getElementById('use-default-bounds');
    
    if (drawAreaBtn) {
        drawAreaBtn.addEventListener('click', () => toggleDrawingMode());
    }
    
    if (clearBoundsBtn) {
        clearBoundsBtn.addEventListener('click', clearBounds);
    }
    
    if (defaultBoundsBtn) {
        defaultBoundsBtn.addEventListener('click', useDefaultBounds);
    }
    
    // Add keyboard escape to cancel drawing
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && isDrawingModeEnabled) {
            toggleDrawingMode(false);
            showToast('Drawing cancelled', 'info');
        }
    });
});

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    // Close all event sources
    activeJobEventSources.forEach((eventSource) => {
        eventSource.close();
    });
    activeJobEventSources.clear();
});

// Export functions for global access
window.refreshActiveJobs = refreshActiveJobs;
window.refreshRecentJobs = refreshRecentJobs;
window.filterJobs = filterJobs;
window.cancelJob = cancelJob;
window.downloadResults = downloadResults;