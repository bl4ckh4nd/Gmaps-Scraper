/**
 * Settings Panel JavaScript
 * Handles settings management and configuration
 */

// Settings state
let currentSettings = {
    system: {},
    user: {},
    hasChanges: false
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    setupTabNavigation();
    loadSettings();
    setupGridSizeSlider();
});

/**
 * Setup tab navigation
 */
function setupTabNavigation() {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', function() {
            const tab = this.dataset.tab;
            switchTab(tab);
        });
    });
}

/**
 * Switch to a specific tab
 */
function switchTab(tabName) {
    // Update nav items
    document.querySelectorAll('.nav-item').forEach(item => {
        if (item.dataset.tab === tabName) {
            item.classList.add('active');
        } else {
            item.classList.remove('active');
        }
    });

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        if (content.dataset.tab === tabName) {
            content.classList.add('active');
        } else {
            content.classList.remove('active');
        }
    });
}

/**
 * Load current settings
 */
async function loadSettings() {
    showLoading('Loading settings...');

    try {
        const response = await fetch('/api/settings');
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Failed to load settings');
        }

        currentSettings.system = data.system_settings || {};
        currentSettings.user = data.user_preferences || {};

        // Populate form fields
        populateForm();

        hideLoading();
        showToast('Settings loaded successfully', 'success');

    } catch (error) {
        hideLoading();
        showToast('Failed to load settings: ' + error.message, 'error');
    }
}

/**
 * Populate form with current settings
 */
function populateForm() {
    // Browser settings
    if (currentSettings.system.chrome_path) {
        document.getElementById('chromePath').value = currentSettings.system.chrome_path;
        updateChromeStatus();
    }

    if (currentSettings.user.default_headless !== undefined) {
        document.getElementById('defaultHeadless').checked = currentSettings.user.default_headless;
    }

    // Owner enrichment settings
    if (currentSettings.system.owner_enrichment_enabled !== undefined) {
        document.getElementById('ownerEnrichmentEnabled').checked = currentSettings.system.owner_enrichment_enabled;
        toggleOwnerFields();
    }

    // Don't show actual API key for security
    if (currentSettings.system.openrouter_api_key && currentSettings.system.openrouter_api_key !== '***') {
        document.getElementById('apiKey').placeholder = '********** (saved)';
    }

    if (currentSettings.system.openrouter_model) {
        document.getElementById('apiModel').value = currentSettings.system.openrouter_model;
    }

    // Scraping preferences
    if (currentSettings.user.default_scraping_mode) {
        document.getElementById('defaultScrapingMode').value = currentSettings.user.default_scraping_mode;
    }

    if (currentSettings.user.default_grid_size !== undefined) {
        document.getElementById('defaultGridSize').value = currentSettings.user.default_grid_size;
        updateGridSizeDisplay();
    }
}

/**
 * Update Chrome status indicator
 */
function updateChromeStatus() {
    const statusDiv = document.getElementById('chromeStatus');
    const statusText = document.getElementById('chromeStatusText');

    if (currentSettings.system.chrome_validated) {
        statusDiv.style.display = 'flex';
        statusDiv.className = 'status-indicator success';
        statusText.textContent = 'Validated';

        if (currentSettings.system.chrome_last_validated) {
            const date = new Date(currentSettings.system.chrome_last_validated);
            statusText.textContent += ` (${date.toLocaleDateString()})`;
        }
    } else if (currentSettings.system.chrome_path) {
        statusDiv.style.display = 'flex';
        statusDiv.className = 'status-indicator warning';
        statusText.textContent = 'Not validated - click Test to verify';
    }
}

/**
 * Update API status indicator
 */
function updateApiStatus(validated) {
    const statusDiv = document.getElementById('apiStatus');
    const statusText = document.getElementById('apiStatusText');

    if (validated) {
        statusDiv.style.display = 'flex';
        statusDiv.className = 'status-indicator success';
        statusText.textContent = 'Validated';
    } else {
        statusDiv.style.display = 'flex';
        statusDiv.className = 'status-indicator warning';
        statusText.textContent = 'Not validated';
    }
}

/**
 * Setup grid size slider
 */
function setupGridSizeSlider() {
    const slider = document.getElementById('defaultGridSize');
    slider.addEventListener('input', updateGridSizeDisplay);
}

/**
 * Update grid size display
 */
function updateGridSizeDisplay() {
    const size = document.getElementById('defaultGridSize').value;
    document.getElementById('gridSizeValue').textContent = size;
    document.getElementById('gridSizeValue2').textContent = size;
}

/**
 * Toggle owner enrichment fields
 */
function toggleOwnerFields() {
    const enabled = document.getElementById('ownerEnrichmentEnabled').checked;
    const fields = document.getElementById('ownerFields');

    if (enabled) {
        fields.style.display = 'block';
    } else {
        fields.style.display = 'none';
    }
}

/**
 * Toggle API key visibility
 */
function toggleApiKeyVisibility() {
    const input = document.getElementById('apiKey');
    const icon = document.getElementById('apiKeyIcon');

    if (input.type === 'password') {
        input.type = 'text';
        icon.classList.remove('fa-eye');
        icon.classList.add('fa-eye-slash');
    } else {
        input.type = 'password';
        icon.classList.remove('fa-eye-slash');
        icon.classList.add('fa-eye');
    }
}

/**
 * Detect browser
 */
async function detectBrowser() {
    showLoading('Detecting browsers...');

    try {
        const response = await fetch('/api/system/detect-browser');
        const data = await response.json();

        hideLoading();

        if (!response.ok) {
            throw new Error(data.error || 'Detection failed');
        }

        if (data.best_candidate) {
            document.getElementById('chromePath').value = data.best_candidate;
            showToast('Browser detected: ' + data.best_candidate, 'success');
        } else {
            showToast('No browsers detected. Please enter path manually.', 'warning');
        }

    } catch (error) {
        hideLoading();
        showToast('Detection failed: ' + error.message, 'error');
    }
}

/**
 * Test browser
 */
async function testBrowser() {
    const chromePath = document.getElementById('chromePath').value.trim();

    if (!chromePath) {
        showToast('Please enter a Chrome path first', 'warning');
        return;
    }

    showLoading('Testing browser...');

    try {
        const response = await fetch('/api/settings/test', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({chrome_path: chromePath})
        });

        const data = await response.json();
        hideLoading();

        if (data.chrome && data.chrome.is_valid) {
            showToast('Browser validated: ' + data.chrome.message, 'success');
            const statusDiv = document.getElementById('chromeStatus');
            const statusText = document.getElementById('chromeStatusText');
            statusDiv.style.display = 'flex';
            statusDiv.className = 'status-indicator success';
            statusText.textContent = 'Validated';
        } else {
            const error = data.chrome ? data.chrome.error || data.chrome.message : 'Validation failed';
            showToast('Browser validation failed: ' + error, 'error');
        }

    } catch (error) {
        hideLoading();
        showToast('Test failed: ' + error.message, 'error');
    }
}

/**
 * Test API key
 */
async function testApiKey() {
    const apiKey = document.getElementById('apiKey').value.trim();
    const model = document.getElementById('apiModel').value.trim();

    if (!apiKey) {
        showToast('Please enter an API key first', 'warning');
        return;
    }

    showLoading('Testing API key...');

    try {
        const response = await fetch('/api/settings/test', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({api_key: apiKey, model: model || null})
        });

        const data = await response.json();
        hideLoading();

        if (data.api_key && data.api_key.is_valid) {
            showToast('API key validated: ' + data.api_key.message, 'success');
            updateApiStatus(true);
        } else {
            const error = data.api_key ? data.api_key.error || data.api_key.message : 'Validation failed';
            showToast('API key validation failed: ' + error, 'error');
            updateApiStatus(false);
        }

    } catch (error) {
        hideLoading();
        showToast('Test failed: ' + error.message, 'error');
    }
}

/**
 * Save settings
 */
async function saveSettings() {
    const updates = {};

    // Browser settings
    const chromePath = document.getElementById('chromePath').value.trim();
    if (chromePath && chromePath !== currentSettings.system.chrome_path) {
        updates.chrome_path = chromePath;
        updates.chrome_validated = false; // Mark as not validated on change
    }

    // Owner enrichment
    const ownerEnabled = document.getElementById('ownerEnrichmentEnabled').checked;
    if (ownerEnabled !== currentSettings.system.owner_enrichment_enabled) {
        updates.owner_enrichment_enabled = ownerEnabled;
    }

    if (ownerEnabled) {
        const apiKey = document.getElementById('apiKey').value.trim();
        if (apiKey && !apiKey.startsWith('***')) {
            updates.api_key = apiKey;
            updates.api_key_validated = false;
        }

        const model = document.getElementById('apiModel').value.trim();
        if (model && model !== currentSettings.system.openrouter_model) {
            updates.model = model;
        }
    }

    // User preferences
    const defaultHeadless = document.getElementById('defaultHeadless').checked;
    if (defaultHeadless !== currentSettings.user.default_headless) {
        updates.default_headless = defaultHeadless;
    }

    const defaultMode = document.getElementById('defaultScrapingMode').value;
    if (defaultMode !== currentSettings.user.default_scraping_mode) {
        updates.default_scraping_mode = defaultMode;
    }

    const defaultGridSize = parseInt(document.getElementById('defaultGridSize').value);
    if (defaultGridSize !== currentSettings.user.default_grid_size) {
        updates.default_grid_size = defaultGridSize;
    }

    if (Object.keys(updates).length === 0) {
        showToast('No changes to save', 'info');
        return;
    }

    showLoading('Saving settings...');

    try {
        const response = await fetch('/api/settings', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(updates)
        });

        const data = await response.json();
        hideLoading();

        if (!response.ok) {
            throw new Error(data.error || 'Failed to save settings');
        }

        showToast('Settings saved successfully', 'success');

        // Update last saved indicator
        const lastSaved = document.getElementById('lastSaved');
        const lastSavedTime = document.getElementById('lastSavedTime');
        lastSavedTime.textContent = 'just now';
        lastSaved.style.display = 'flex';

        // Reload settings
        setTimeout(() => {
            loadSettings();
        }, 1000);

    } catch (error) {
        hideLoading();
        showToast('Failed to save settings: ' + error.message, 'error');
    }
}

/**
 * Cancel changes
 */
function cancelChanges() {
    if (confirm('Discard all unsaved changes?')) {
        loadSettings();
    }
}

/**
 * View effective configuration
 */
async function viewEffectiveConfig() {
    showLoading('Loading configuration...');

    try {
        const response = await fetch('/api/system/status');
        const data = await response.json();

        hideLoading();

        if (!response.ok) {
            throw new Error(data.error || 'Failed to load config');
        }

        // Show modal with JSON
        document.getElementById('configJson').textContent = JSON.stringify(data, null, 2);
        document.getElementById('configModal').style.display = 'flex';

    } catch (error) {
        hideLoading();
        showToast('Failed to load configuration: ' + error.message, 'error');
    }
}

/**
 * Close config modal
 */
function closeConfigModal() {
    document.getElementById('configModal').style.display = 'none';
}

/**
 * Reset to defaults
 */
async function resetToDefaults() {
    if (!confirm('Reset all settings to defaults? This will clear all database settings and reload from config.yaml. This action cannot be undone.')) {
        return;
    }

    showToast('Reset functionality not yet implemented', 'warning');
}

/**
 * Show loading overlay
 */
function showLoading(message) {
    const overlay = document.getElementById('loadingOverlay');
    const msgElem = document.getElementById('loadingMessage');
    msgElem.textContent = message || 'Processing...';
    overlay.style.display = 'flex';
}

/**
 * Hide loading overlay
 */
function hideLoading() {
    document.getElementById('loadingOverlay').style.display = 'none';
}

/**
 * Show toast notification
 */
function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <i class="fas ${getToastIcon(type)}"></i>
        <span>${message}</span>
        <button onclick="this.parentElement.remove()"><i class="fas fa-times"></i></button>
    `;

    container.appendChild(toast);

    // Auto-remove after 5 seconds
    setTimeout(() => {
        toast.remove();
    }, 5000);
}

/**
 * Get icon for toast type
 */
function getToastIcon(type) {
    const icons = {
        'success': 'fa-check-circle',
        'error': 'fa-times-circle',
        'warning': 'fa-exclamation-triangle',
        'info': 'fa-info-circle'
    };
    return icons[type] || icons.info;
}

// Close modal on outside click
document.addEventListener('click', function(e) {
    const modal = document.getElementById('configModal');
    if (e.target === modal) {
        closeConfigModal();
    }
});
