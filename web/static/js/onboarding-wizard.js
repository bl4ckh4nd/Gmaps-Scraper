/**
 * Onboarding Wizard JavaScript
 * Handles multi-step wizard flow for initial setup
 */

// Wizard state
let wizardState = {
    currentStep: 0,
    chromePath: null,
    chromeValidated: false,
    apiKey: null,
    apiModel: 'google/gemini-2.0-flash-exp:free',
    skipOwnerEnrichment: true,
    apiKeyValidated: false,
    verificationPassed: false
};

// Initialize wizard on page load
document.addEventListener('DOMContentLoaded', function() {
    updateWizardDisplay();
});

/**
 * Navigate to next step
 */
function nextStep() {
    if (wizardState.currentStep < 4) {
        wizardState.currentStep++;
        updateWizardDisplay();

        // Auto-run verification when reaching that step
        if (wizardState.currentStep === 3) {
            runVerificationChecks();
        }
    }
}

/**
 * Navigate to previous step
 */
function previousStep() {
    if (wizardState.currentStep > 0) {
        wizardState.currentStep--;
        updateWizardDisplay();
    }
}

/**
 * Update wizard display based on current step
 */
function updateWizardDisplay() {
    // Update progress indicators
    document.querySelectorAll('.progress-step').forEach((step, index) => {
        if (index < wizardState.currentStep) {
            step.classList.add('completed');
            step.classList.remove('active');
        } else if (index === wizardState.currentStep) {
            step.classList.add('active');
            step.classList.remove('completed');
        } else {
            step.classList.remove('active', 'completed');
        }
    });

    // Update step visibility
    document.querySelectorAll('.wizard-step').forEach((step, index) => {
        if (index === wizardState.currentStep) {
            step.classList.add('active');
        } else {
            step.classList.remove('active');
        }
    });

    // Scroll to top
    window.scrollTo(0, 0);
}

/**
 * Skip wizard for advanced users
 */
async function skipWizard() {
    if (confirm('Skip the setup wizard? You can configure settings later from the Settings panel.')) {
        showLoading('Completing setup...');
        try {
            const response = await fetch('/api/onboarding/complete', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'}
            });

            if (response.ok) {
                window.location.href = '/';
            } else {
                alert('Failed to complete onboarding');
                hideLoading();
            }
        } catch (error) {
            alert('Error: ' + error.message);
            hideLoading();
        }
    }
}

/**
 * Auto-detect Chrome browser
 */
async function autoDetectBrowser() {
    showLoading('Detecting browsers...');

    try {
        const response = await fetch('/api/system/detect-browser');
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Detection failed');
        }

        hideLoading();

        if (data.candidates && data.candidates.length > 0) {
            // Show detection results
            const resultsDiv = document.getElementById('detectionResults');
            const browserList = document.getElementById('browserList');

            browserList.innerHTML = '';

            data.candidates.forEach(candidate => {
                const item = document.createElement('div');
                item.className = 'browser-item ' + (candidate.is_valid ? 'valid' : 'invalid');
                item.innerHTML = `
                    <div class="browser-info">
                        <i class="fas ${candidate.is_valid ? 'fa-check-circle' : 'fa-times-circle'}"></i>
                        <div>
                            <strong>${candidate.path}</strong>
                            ${candidate.version ? `<br><small>Version: ${candidate.version}</small>` : ''}
                            ${candidate.validation_error ? `<br><small class="error">${candidate.validation_error}</small>` : ''}
                            <br><small>Detection: ${candidate.detection_method}</small>
                        </div>
                    </div>
                    ${candidate.is_valid ? `<button class="btn btn-sm btn-primary" onclick="selectBrowser('${candidate.path.replace(/\\/g, '\\\\')}')">Use This</button>` : ''}
                `;
                browserList.appendChild(item);
            });

            resultsDiv.style.display = 'block';

            // Auto-select best candidate
            if (data.best_candidate) {
                document.getElementById('chromePath').value = data.best_candidate;
                wizardState.chromePath = data.best_candidate;
                showValidationMessage('Best browser detected and selected', 'success');
            }
        } else {
            showValidationMessage('No Chrome installations detected. Please enter path manually.', 'warning');
        }
    } catch (error) {
        hideLoading();
        showValidationMessage('Detection failed: ' + error.message, 'error');
    }
}

/**
 * Select a detected browser
 */
function selectBrowser(path) {
    document.getElementById('chromePath').value = path;
    wizardState.chromePath = path;
    showValidationMessage('Browser selected', 'success');
}

/**
 * Validate browser and proceed to next step
 */
async function validateAndNext() {
    const chromePath = document.getElementById('chromePath').value.trim();

    if (!chromePath) {
        showValidationMessage('Please enter or detect a Chrome path', 'error');
        return;
    }

    showLoading('Validating Chrome browser...');

    try {
        const response = await fetch('/api/system/validate-browser', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({chrome_path: chromePath})
        });

        const result = await response.json();
        hideLoading();

        if (result.is_valid) {
            wizardState.chromePath = chromePath;
            wizardState.chromeValidated = true;

            // Save to settings
            await saveSettings({
                chrome_path: chromePath,
                chrome_validated: true
            });

            showValidationMessage(result.message, 'success');

            setTimeout(() => {
                nextStep();
            }, 1000);
        } else {
            showValidationMessage(result.message + (result.error ? ': ' + result.error : ''), 'error');
        }
    } catch (error) {
        hideLoading();
        showValidationMessage('Validation failed: ' + error.message, 'error');
    }
}

/**
 * Toggle owner enrichment input visibility
 */
function toggleOwnerEnrichmentInputs() {
    const checkbox = document.getElementById('enableOwnerEnrichment');
    const inputs = document.getElementById('ownerEnrichmentInputs');

    if (checkbox.checked) {
        inputs.style.display = 'block';
        wizardState.skipOwnerEnrichment = false;
    } else {
        inputs.style.display = 'none';
        wizardState.skipOwnerEnrichment = true;
    }
}

/**
 * Toggle API key visibility
 */
function toggleApiKeyVisibility() {
    const input = document.getElementById('apiKey');
    const icon = document.getElementById('apiKeyToggleIcon');

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
 * Validate API key and proceed
 */
async function validateApiKeyAndNext() {
    // If skipping owner enrichment, just proceed
    if (wizardState.skipOwnerEnrichment) {
        nextStep();
        return;
    }

    const apiKey = document.getElementById('apiKey').value.trim();
    const model = document.getElementById('modelSelect').value.trim();

    if (!apiKey) {
        showApiValidationMessage('Please enter an API key or disable owner enrichment', 'error');
        return;
    }

    showLoading('Validating API key...');

    try {
        const response = await fetch('/api/system/validate-api-key', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({api_key: apiKey, model: model || null})
        });

        const result = await response.json();
        hideLoading();

        if (result.is_valid) {
            wizardState.apiKey = apiKey;
            wizardState.apiModel = model;
            wizardState.apiKeyValidated = true;

            // Save to settings
            await saveSettings({
                api_key: apiKey,
                model: model,
                api_key_validated: true,
                owner_enrichment_enabled: true
            });

            showApiValidationMessage(result.message, 'success');

            setTimeout(() => {
                nextStep();
            }, 1000);
        } else {
            showApiValidationMessage(result.message + (result.error ? ': ' + result.error : ''), 'error');
        }
    } catch (error) {
        hideLoading();
        showApiValidationMessage('Validation failed: ' + error.message, 'error');
    }
}

/**
 * Run system verification checks
 */
async function runVerificationChecks() {
    const checks = ['python', 'playwright', 'chrome'];

    if (!wizardState.skipOwnerEnrichment) {
        checks.push('crawl4ai', 'api');
        document.getElementById('check-crawl4ai').style.display = 'flex';
        document.getElementById('check-api').style.display = 'flex';
    }

    try {
        const response = await fetch('/api/system/status');
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Status check failed');
        }

        const systemChecks = data.system_checks || {};
        let allPassed = true;

        // Update check results
        if (systemChecks.python_version) {
            updateCheckItem('check-python', systemChecks.python_version);
            if (!systemChecks.python_version.is_valid) allPassed = false;
        }

        if (systemChecks.playwright) {
            updateCheckItem('check-playwright', systemChecks.playwright);
            if (!systemChecks.playwright.is_valid) allPassed = false;
        }

        if (systemChecks.chrome) {
            updateCheckItem('check-chrome', systemChecks.chrome);
            if (!systemChecks.chrome.is_valid) allPassed = false;
        }

        if (!wizardState.skipOwnerEnrichment) {
            if (systemChecks.crawl4ai) {
                updateCheckItem('check-crawl4ai', systemChecks.crawl4ai);
                // Crawl4AI is optional, don't fail if missing
            }

            // API check is based on saved validation status
            if (data.api_key_validated) {
                updateCheckItem('check-api', {
                    is_valid: true,
                    message: 'API key validated successfully'
                });
            } else if (wizardState.apiKeyValidated) {
                updateCheckItem('check-api', {
                    is_valid: true,
                    message: 'API key validated in wizard'
                });
            } else {
                updateCheckItem('check-api', {
                    is_valid: false,
                    message: 'API key not validated'
                });
                allPassed = false;
            }
        }

        wizardState.verificationPassed = allPassed;

        // Show summary
        const summary = document.getElementById('verificationSummary');
        if (allPassed) {
            summary.className = 'verification-summary success';
            summary.innerHTML = '<i class="fas fa-check-circle"></i> All checks passed! Your system is ready.';
        } else {
            summary.className = 'verification-summary warning';
            summary.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Some checks failed. Please review and fix issues before proceeding.';
        }
        summary.style.display = 'block';

        // Enable/disable complete button
        document.getElementById('completeBtn').disabled = !allPassed;

    } catch (error) {
        console.error('Verification failed:', error);
        const summary = document.getElementById('verificationSummary');
        summary.className = 'verification-summary error';
        summary.innerHTML = '<i class="fas fa-times-circle"></i> Verification failed: ' + error.message;
        summary.style.display = 'block';
    }
}

/**
 * Update check item display
 */
function updateCheckItem(itemId, result) {
    const item = document.getElementById(itemId);
    const icon = item.querySelector('i');
    const text = item.querySelector('span');

    icon.classList.remove('fa-spinner', 'fa-spin');

    if (result.is_valid) {
        icon.classList.add('fa-check-circle');
        icon.style.color = '#28a745';
        text.textContent = result.message;
    } else {
        icon.classList.add('fa-times-circle');
        icon.style.color = '#dc3545';
        text.textContent = result.message + (result.error ? ': ' + result.error : '');
    }
}

/**
 * Complete setup wizard
 */
async function completeSetup() {
    if (!wizardState.verificationPassed) {
        alert('Please fix validation errors before completing setup');
        return;
    }

    showLoading('Completing setup...');

    try {
        const response = await fetch('/api/onboarding/complete', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'}
        });

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.error || 'Failed to complete onboarding');
        }

        hideLoading();
        nextStep(); // Go to success screen
    } catch (error) {
        hideLoading();
        alert('Error completing setup: ' + error.message);
    }
}

/**
 * Go to dashboard
 */
function goToDashboard() {
    window.location.href = '/';
}

/**
 * Save settings to database
 */
async function saveSettings(settings) {
    try {
        const response = await fetch('/api/settings', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(settings)
        });

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.error || 'Failed to save settings');
        }
    } catch (error) {
        console.error('Failed to save settings:', error);
        // Don't block wizard flow, just log
    }
}

/**
 * Show validation message
 */
function showValidationMessage(message, type) {
    const msgDiv = document.getElementById('validationMessage');
    msgDiv.className = 'validation-message ' + type;
    msgDiv.innerHTML = `<i class="fas ${type === 'success' ? 'fa-check-circle' : type === 'warning' ? 'fa-exclamation-triangle' : 'fa-times-circle'}"></i> ${message}`;
    msgDiv.style.display = 'block';

    if (type === 'success') {
        setTimeout(() => {
            msgDiv.style.display = 'none';
        }, 5000);
    }
}

/**
 * Show API validation message
 */
function showApiValidationMessage(message, type) {
    const msgDiv = document.getElementById('apiValidationMessage');
    msgDiv.className = 'validation-message ' + type;
    msgDiv.innerHTML = `<i class="fas ${type === 'success' ? 'fa-check-circle' : type === 'warning' ? 'fa-exclamation-triangle' : 'fa-times-circle'}"></i> ${message}`;
    msgDiv.style.display = 'block';

    if (type === 'success') {
        setTimeout(() => {
            msgDiv.style.display = 'none';
        }, 5000);
    }
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
