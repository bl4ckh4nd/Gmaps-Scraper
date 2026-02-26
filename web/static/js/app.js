/* Google Maps Scraper Web Interface JavaScript */

// Global variables
let map = null;
let mapInitialized = false;
let boundsRectangle = null;
let activeJobEventSources = new Map();
let lastJobsUpdate = 0;
let currentJobStatusFilter = '';

// Configuration
const CONFIG = {
    refreshInterval: 5000, // 5 seconds
    maxToasts: 5,
    defaultBounds: [52.4, 13.2, 52.6, 13.6], // Berlin area
    mapCenter: [52.52, 13.405],
    mapZoom: 11
};
const TAB_STORAGE_KEY = 'gmaps_scraper_active_tab';

function setupTabs() {
    const tabButtons = Array.from(document.querySelectorAll('.tab-button'));
    const tabPanels = Array.from(document.querySelectorAll('.tab-panel'));

    if (!tabButtons.length || !tabPanels.length) {
        return;
    }

    const availableTabs = new Set(tabButtons.map((button) => button.dataset.tab));
    let initialTab = 'dashboard';

    try {
        const storedTab = window.localStorage.getItem(TAB_STORAGE_KEY);
        if (storedTab && availableTabs.has(storedTab)) {
            initialTab = storedTab;
        }
    } catch (error) {
        console.debug('Unable to read tab preference:', error);
    }

    const activateTab = (tabName, { persist = true } = {}) => {
        if (!availableTabs.has(tabName)) {
            tabName = 'dashboard';
        }

        tabButtons.forEach((button) => {
            const isActive = button.dataset.tab === tabName;
            button.classList.toggle('active', isActive);
            button.setAttribute('aria-selected', String(isActive));
        });

        tabPanels.forEach((panel) => {
            const isActive = panel.dataset.tabPanel === tabName;
            panel.classList.toggle('active', isActive);
            panel.toggleAttribute('hidden', !isActive);
        });

        if (persist) {
            try {
                window.localStorage.setItem(TAB_STORAGE_KEY, tabName);
            } catch (error) {
                console.debug('Unable to persist tab preference:', error);
            }
        }

        if (tabName === 'dashboard') {
            initializeMapIfNeeded();
        }
    };

    tabButtons.forEach((button) => {
        button.addEventListener('click', () => activateTab(button.dataset.tab));
    });

    // Activate desired initial tab without persisting the selection twice
    activateTab(initialTab, { persist: false });
}

function initializeMapIfNeeded() {
    if (!mapInitialized) {
        initializeMap();
        return;
    }

    if (map) {
        requestAnimationFrame(() => {
            map.invalidateSize();
        });
    }
}

// Initialize the application
function initializeApp() {
    console.log('Initializing Google Maps Scraper Web Interface');

    // Set up tabs and lazy-load heavier UI
    setupTabs();

    // Eagerly initialize the map so it is ready before the user interacts
    // with any bounds controls, regardless of which tab is shown first.
    requestAnimationFrame(() => initializeMapIfNeeded());

    // Set up form submission
    setupFormSubmission();
    setupOwnerEnrichmentForm();
    setupSettingsForms();

    // Load initial data
    loadJobs();
    updateHeaderStats();
    loadSettings();

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
    if (mapInitialized && map) {
        requestAnimationFrame(() => map.invalidateSize());
        return;
    }

    try {
        // Initialize Leaflet map
        map = L.map('map').setView(CONFIG.mapCenter, CONFIG.mapZoom);
        
        // Add OpenStreetMap tiles
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(map);
        
        // Drawing controls will be enabled via toggle button
        
        console.log('Map initialized successfully');
        mapInitialized = true;
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
    if (isDrawing || !isDrawingModeEnabled || !map) return;
    
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
    if (!isDrawing || !startLatLng || !map) return;
    
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
    if (!isDrawing || !startLatLng || !map) return;
    
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
    if (!map) {
        initializeMapIfNeeded();
        if (!map) return; // map container may not exist on this page
    }

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
    if (boundsRectangle && map) {
        map.removeLayer(boundsRectangle);
        boundsRectangle = null;
        updateBoundsInfo();
        showToast('Search area cleared', 'info');
    }

    // Exit drawing mode
    toggleDrawingMode(false);
}

function useDefaultBounds() {
    if (!map) {
        initializeMapIfNeeded();
        if (!map) return;
    }

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

    if (data.owner_enrichment === 'on' && data.owner_max_pages) {
        const ownerMaxPages = parseInt(data.owner_max_pages);
        if (!ownerMaxPages || ownerMaxPages < 1 || ownerMaxPages > 10) {
            return { valid: false, message: 'Owner crawl pages must be between 1 and 10' };
        }
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
        headless: data.headless === 'on',
        scraping_mode: data.scraping_mode || 'fast'
    };

    config.job_type = 'scrape';

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

    const overrides = {};
    if (data.owner_enrichment === 'on') {
        const ownerOverrides = { enabled: true };
        if (data.owner_model && data.owner_model.trim().length > 0) {
            ownerOverrides.openrouter_default_model = data.owner_model.trim();
        }
        if (data.owner_max_pages) {
            ownerOverrides.max_pages = parseInt(data.owner_max_pages);
        }
        overrides.owner_enrichment = ownerOverrides;
    }

    if (Object.keys(overrides).length > 0) {
        config.config_overrides = overrides;
    }

    return config;
}

function setupOwnerEnrichmentForm() {
    const form = document.getElementById('owner-enrichment-form');
    if (!form) return;
    form.addEventListener('submit', handleOwnerEnrichmentSubmit);
}

async function handleOwnerEnrichmentSubmit(e) {
    e.preventDefault();

    const form = e.target;
    const formData = new FormData(form);
    const data = Object.fromEntries(formData.entries());
    data.owner_in_place = document.getElementById('owner-in-place').checked;
    data.owner_resume = document.getElementById('owner-resume').checked;
    data.owner_skip_existing = document.getElementById('owner-skip-existing').checked;

    const validation = validateOwnerEnrichmentData(data);
    if (!validation.valid) {
        showToast(validation.message, 'error');
        return;
    }

    const jobConfig = prepareOwnerEnrichmentJob(data);

    try {
        showLoadingOverlay('Starting owner enrichment job...');

        const response = await fetch('/api/jobs', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(jobConfig)
        });

        const result = await response.json();

        if (response.ok) {
            showToast('Owner enrichment job started successfully!', 'success');
            form.reset();
            setTimeout(() => {
                loadJobs();
                updateHeaderStats();
            }, 1000);
        } else {
            throw new Error(result.error || 'Failed to start owner enrichment job');
        }
    } catch (error) {
        console.error('Error starting owner enrichment job:', error);
        showToast(error.message || 'Failed to start owner enrichment job', 'error');
    } finally {
        hideLoadingOverlay();
    }
}

function validateOwnerEnrichmentData(data) {
    if (!data.owner_csv_path || data.owner_csv_path.trim().length === 0) {
        return { valid: false, message: 'Input CSV path is required' };
    }

    if (data.owner_in_place && data.owner_output_path && data.owner_output_path.trim().length > 0) {
        return { valid: false, message: 'Cannot use in-place mode and a custom output path together' };
    }
    if (data.owner_in_place && data.owner_resume) {
        return { valid: false, message: 'Cannot use in-place mode together with resume' };
    }

    return { valid: true };
}

function prepareOwnerEnrichmentJob(data) {
    const job = {
        job_type: 'owner_enrichment',
        owner_csv_path: data.owner_csv_path.trim(),
        owner_in_place: !!data.owner_in_place,
        owner_resume: !!data.owner_resume,
        owner_skip_existing: !!data.owner_skip_existing
    };

    if (data.owner_output_path && data.owner_output_path.trim().length > 0) {
        job.owner_output_path = data.owner_output_path.trim();
    }

    if (data.owner_model && data.owner_model.trim().length > 0) {
        job.owner_model = data.owner_model.trim();
    }

    return job;
}

function setupSettingsForms() {
    const apiForm = document.getElementById('openrouter-api-form');
    const apiInput = document.getElementById('openrouter-api-key');
    const clearButton = document.getElementById('openrouter-api-clear');
    const modelForm = document.getElementById('openrouter-model-form');

    if (apiForm && apiInput) {
        apiForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            const apiKey = apiInput.value.trim();
            if (!apiKey) {
                showToast('Enter an API key or use Clear to remove it.', 'info');
                return;
            }
            await submitOpenRouterSettings({ api_key: apiKey }, 'OpenRouter API key saved');
            apiInput.value = '';
        });
    }

    if (clearButton) {
        clearButton.addEventListener('click', async () => {
            if (!confirm('Remove the stored OpenRouter API key? This will disable owner enrichment until a new key is provided.')) {
                return;
            }
            await submitOpenRouterSettings({ api_key: '' }, 'OpenRouter API key cleared');
        });
    }

    if (modelForm) {
        modelForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            const modelInput = document.getElementById('openrouter-default-model');
            const allowFreeInput = document.getElementById('openrouter-allow-free');
            const defaultModel = modelInput ? modelInput.value.trim() : '';
            if (!defaultModel) {
                showToast('Default model cannot be blank.', 'error');
                return;
            }
            await submitOpenRouterSettings(
                {
                    default_model: defaultModel,
                    allow_free_models_only: Boolean(allowFreeInput && allowFreeInput.checked),
                },
                'Owner enrichment defaults saved',
            );
        });
    }
}

async function loadSettings() {
    try {
        const response = await fetch('/api/settings/openrouter');
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Failed to load settings');
        }
        updateSettingsUI(data);
    } catch (error) {
        console.error('Error loading settings:', error);
        showToast(error.message || 'Failed to load settings', 'error');
    }
}

async function submitOpenRouterSettings(payload, successMessage) {
    try {
        showLoadingOverlay('Saving settings...');
        const response = await fetch('/api/settings/openrouter', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload),
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || 'Failed to update settings');
        }
        updateSettingsUI(data);
        if (successMessage) {
            showToast(successMessage, 'success');
        }
    } catch (error) {
        console.error('Error updating settings:', error);
        showToast(error.message || 'Failed to update settings', 'error');
        throw error;
    } finally {
        hideLoadingOverlay();
    }
}

function updateSettingsUI(data) {
    const envSpan = document.getElementById('settings-api-key-env');
    const statusBadge = document.getElementById('settings-api-key-status');
    const apiInput = document.getElementById('openrouter-api-key');
    const modelInput = document.getElementById('openrouter-default-model');
    const allowFreeInput = document.getElementById('openrouter-allow-free');

    if (envSpan && data.api_key_env) {
        envSpan.textContent = data.api_key_env;
    }

    if (statusBadge) {
        const badgeClass = data.api_key_set ? 'status-badge status-completed' : 'status-badge status-failed';
        statusBadge.className = badgeClass;
        statusBadge.textContent = data.api_key_set ? 'Configured' : 'Not configured';
    }

    if (apiInput) {
        apiInput.value = '';
        apiInput.placeholder = data.api_key_set ? 'Key stored • enter to replace' : 'Enter OpenRouter API key';
    }

    if (modelInput && typeof data.default_model === 'string') {
        modelInput.value = data.default_model;
    }

    if (allowFreeInput && typeof data.allow_free_models_only === 'boolean') {
        allowFreeInput.checked = data.allow_free_models_only;
    }
}

// Job management
async function loadJobs() {
    try {
        const completedStatusQuery = currentJobStatusFilter || 'completed,failed,cancelled';
        const [activeResponse, completedResponse] = await Promise.all([
            fetch('/api/jobs?limit=50&status=running,pending'),
            fetch(`/api/jobs?limit=50&status=${encodeURIComponent(completedStatusQuery)}`)
        ]);

        const activeData = await activeResponse.json();
        const completedData = await completedResponse.json();

        if (!activeResponse.ok) {
            throw new Error(activeData.error || 'Failed to load active jobs');
        }
        if (!completedResponse.ok) {
            throw new Error(completedData.error || 'Failed to load completed jobs');
        }

        const activeJobs = activeData.jobs || [];
        const completedJobs = completedData.jobs || [];

        displayActiveJobs(activeJobs);
        displayCompletedJobs(completedJobs);

        // Setup real-time monitoring for active jobs
        setupRealTimeMonitoring(activeJobs);
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
    const progress = job.progress || {};
    const isOwnerJob = job.config.job_type === 'owner_enrichment';
    const percentage = Math.round(progress.percentage || 0);

    const title = isOwnerJob
        ? `Owner enrichment: ${job.config.owner_csv_path || 'Job'}`
        : job.config.search_term;

    const progressValue = (() => {
        if (isOwnerJob) {
            const processed = progress.processed_rows || 0;
            const total = progress.total_rows || 0;
            return `${processed} / ${total} (${percentage}%)`;
        }
        const current = progress.current || 0;
        const total = progress.total || 0;
        return `${current} / ${total} (${percentage}%)`;
    })();

    const progressBar = isActive
        ? `
            <div class="job-progress">
                <div class="progress-label">
                    <span>Progress</span>
                    <span>${progressValue}</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${percentage}%"></div>
                </div>
            </div>
          `
        : '';

    const statsMarkup = isOwnerJob
        ? `
            <div class="job-stats">
                <div class="stat-item">
                    <i class="fas fa-list"></i>
                    <span class="stat-value">${(progress.total_rows || 0).toLocaleString()}</span>
                    <span class="stat-label">Rows</span>
                </div>
                <div class="stat-item">
                    <i class="fas fa-check"></i>
                    <span class="stat-value">${(progress.processed_rows || 0).toLocaleString()}</span>
                    <span class="stat-label">Processed</span>
                </div>
                <div class="stat-item">
                    <i class="fas fa-user-tie"></i>
                    <span class="stat-value">${(progress.owners_found || 0).toLocaleString()}</span>
                    <span class="stat-label">Owners Found</span>
                </div>
            </div>
        `
        : `
            <div class="job-stats">
                <div class="stat-item">
                    <i class="fas fa-target"></i>
                    <span class="stat-value">${(progress.total || 0).toLocaleString()}</span>
                    <span class="stat-label">Target</span>
                </div>
                <div class="stat-item">
                    <i class="fas fa-check"></i>
                    <span class="stat-value">${(progress.current || 0).toLocaleString()}</span>
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
        `;

    const ownerExtras = isOwnerJob ? '' : (
        job.progress.cell_distribution && Object.keys(job.progress.cell_distribution).length > 0 ? `
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
        ` : ''
    );

    return `
        <div class="job-item" data-job-id="${job.job_id}">
            <div class="job-header">
                <h3 class="job-title">${title}</h3>
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

            ${progressBar}

            ${statsMarkup}
            ${ownerExtras}

            <div class="job-actions">
                <a href="/jobs/${job.job_id}" class="btn btn-primary btn-sm">
                    <i class="fas fa-info-circle"></i> Details
                </a>
                ${job.status === 'completed' ? `
                    <a href="/results/${job.job_id}" class="btn btn-secondary btn-sm">
                        <i class="fas fa-table"></i> View Results
                    </a>
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

    const isOwnerJob = job.config && job.config.job_type === 'owner_enrichment';
    const progress = job.progress || {};
    
    // Update progress bar
    const progressBar = jobElement.querySelector('.progress-fill');
    const progressLabel = jobElement.querySelector('.progress-label span:last-child');
    
    if (progressBar && progressLabel) {
        const percentage = Math.round(progress.percentage || 0);
        progressBar.style.width = `${percentage}%`;
        if (isOwnerJob) {
            const processed = Number(progress.processed_rows ?? 0);
            const totalRows = Number(progress.total_rows ?? 0);
            progressLabel.textContent = `${processed.toLocaleString()} / ${totalRows.toLocaleString()} (${percentage}%)`;
        } else {
            const current = Number(progress.current ?? 0);
            const total = Number(progress.total ?? 0);
            progressLabel.textContent = `${current.toLocaleString()} / ${total.toLocaleString()} (${percentage}%)`;
        }
    }
    
    // Update stats
    const statValues = jobElement.querySelectorAll('.stat-value');
    if (isOwnerJob) {
        if (statValues[0]) {
            const totalRows = Number(progress.total_rows ?? 0);
            statValues[0].textContent = totalRows.toLocaleString();
        }
        if (statValues[1]) {
            const processedRows = Number(progress.processed_rows ?? 0);
            statValues[1].textContent = processedRows.toLocaleString();
        }
        if (statValues[2]) {
            const ownersFound = Number(progress.owners_found ?? 0);
            statValues[2].textContent = ownersFound.toLocaleString();
        }
    } else if (statValues.length >= 2) {
        const total = Number(progress.total ?? 0);
        const current = Number(progress.current ?? 0);
        statValues[0].textContent = total.toLocaleString();
        statValues[1].textContent = current.toLocaleString();
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

    // Update status badge text/class if present
    const statusBadge = jobElement.querySelector('.status-badge');
    if (statusBadge) {
        const statusClass = getStatusClass(job.status);
        const statusIcon = getStatusIcon(job.status);
        statusBadge.className = `status-badge ${statusClass}`;
        statusBadge.innerHTML = `<i class="fas ${statusIcon}"></i> ${job.status.toUpperCase()}`;
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
        const link = document.createElement('a');
        link.href = `/api/jobs/${jobId}/download/all`;
        link.download = `${jobId}_results.zip`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        showToast('ZIP download started', 'success');
        
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
    const statusFilter = document.getElementById('status-filter').value || '';
    currentJobStatusFilter = statusFilter;
    loadJobs();
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
