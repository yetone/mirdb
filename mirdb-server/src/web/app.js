/**
 * MirDB Web Dashboard - JavaScript
 *
 * Owners:
 * - Scenario 12: Accessibility (keyboard navigation, ARIA updates)
 * - Scenario 13: SPA behavior (no reloads, loading states, toasts)
 * - Scenario 14: Form validation (inline errors)
 *
 * Expected functionality:
 * - API client functions (fetchStatus, getKey, setKey, deleteKey, triggerCompact)
 * - Event handlers for all forms and buttons
 * - Loading state management
 * - Toast notification system
 * - Form validation with inline errors
 * - Keyboard navigation support
 */

// API base URL
const API_BASE = '/api';

// Toast notification system
function showToast(message, type = 'success') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    toast.setAttribute('role', 'alert');
    container.appendChild(toast);

    // Auto-remove after 3 seconds
    setTimeout(() => {
        toast.style.animation = 'slideIn 0.3s ease-out reverse';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// Loading state management
function setLoading(button, isLoading) {
    if (isLoading) {
        button.classList.add('loading');
        button.disabled = true;
    } else {
        button.classList.remove('loading');
        button.disabled = false;
    }
}

// Form validation
function validateSetKeyForm() {
    const keyInput = document.getElementById('set-key-input');
    const keyError = document.getElementById('set-key-error');
    const key = keyInput.value.trim();

    if (!key) {
        keyInput.classList.add('error');
        keyError.textContent = 'Key is required';
        keyInput.setAttribute('aria-invalid', 'true');
        return false;
    }

    keyInput.classList.remove('error');
    keyError.textContent = '';
    keyInput.setAttribute('aria-invalid', 'false');
    return true;
}

function validateGetKeyForm() {
    const keyInput = document.getElementById('get-key-input');
    const keyError = document.getElementById('get-key-error');
    const key = keyInput.value.trim();

    if (!key) {
        keyInput.classList.add('error');
        keyError.textContent = 'Key is required';
        keyInput.setAttribute('aria-invalid', 'true');
        return false;
    }

    keyInput.classList.remove('error');
    keyError.textContent = '';
    keyInput.setAttribute('aria-invalid', 'false');
    return true;
}

// Clear validation errors on input
function clearValidationOnInput(inputId, errorId) {
    const input = document.getElementById(inputId);
    const error = document.getElementById(errorId);

    input.addEventListener('input', () => {
        input.classList.remove('error');
        error.textContent = '';
        input.setAttribute('aria-invalid', 'false');
    });
}

// API Functions

// Set a key-value pair (Scenario 3)
async function setKey(key, value, flags = 0, ttl = 0) {
    const response = await fetch(`${API_BASE}/key`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ key, value, flags, ttl }),
    });

    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.error || 'Failed to set key');
    }

    return data;
}

// Get a key's value (Scenario 2)
async function getKey(key) {
    const response = await fetch(`${API_BASE}/key/${encodeURIComponent(key)}`);
    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.error || 'Key not found');
    }

    return data;
}

// Delete a key (Scenario 4)
async function deleteKey(key) {
    const response = await fetch(`${API_BASE}/key/${encodeURIComponent(key)}`, {
        method: 'DELETE',
    });
    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.error || 'Failed to delete key');
    }

    return data;
}

// Fetch server status (Scenario 1)
async function fetchStatus() {
    const response = await fetch(`${API_BASE}/status`);
    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.error || 'Failed to fetch status');
    }

    return data;
}

// Trigger compaction (Scenario 5)
async function triggerCompact() {
    const response = await fetch(`${API_BASE}/operations/compact`, {
        method: 'POST',
    });
    const data = await response.json();

    if (!response.ok) {
        throw new Error(data.error || 'Failed to trigger compaction');
    }

    return data;
}

// Event Handlers

// Set Key Form Handler (Scenario 3)
function handleSetKeySubmit(event) {
    event.preventDefault();

    if (!validateSetKeyForm()) {
        return;
    }

    const button = document.getElementById('set-btn');
    const keyInput = document.getElementById('set-key-input');
    const valueInput = document.getElementById('set-value-input');
    const flagsInput = document.getElementById('set-flags-input');
    const ttlInput = document.getElementById('set-ttl-input');

    const key = keyInput.value.trim();
    const value = valueInput.value;
    const flags = parseInt(flagsInput.value, 10) || 0;
    const ttl = parseInt(ttlInput.value, 10) || 0;

    setLoading(button, true);

    setKey(key, value, flags, ttl)
        .then(() => {
            showToast(`Key "${key}" set successfully`, 'success');
            // Clear form on success
            keyInput.value = '';
            valueInput.value = '';
            flagsInput.value = '0';
            ttlInput.value = '0';
        })
        .catch((error) => {
            showToast(error.message, 'error');
        })
        .finally(() => {
            setLoading(button, false);
        });
}

// Current key being viewed (for delete operation)
let currentViewedKey = null;

// Get Key Form Handler (Scenario 2)
function handleGetKeySubmit(event) {
    event.preventDefault();

    if (!validateGetKeyForm()) {
        return;
    }

    const button = document.getElementById('lookup-btn');
    const keyInput = document.getElementById('get-key-input');
    const resultBox = document.getElementById('get-result');
    const deleteAction = document.getElementById('delete-action');
    const key = keyInput.value.trim();

    setLoading(button, true);
    resultBox.textContent = 'Loading...';
    deleteAction.style.display = 'none';
    currentViewedKey = null;

    getKey(key)
        .then((data) => {
            resultBox.innerHTML = `<strong>Value:</strong> ${escapeHtml(data.value)}
<strong>Flags:</strong> ${data.flags}    <strong>TTL:</strong> ${data.ttl}    <strong>Bytes:</strong> ${data.bytes}`;
            // Show delete button after successful lookup (Scenario 4)
            currentViewedKey = key;
            deleteAction.style.display = 'block';
        })
        .catch((error) => {
            resultBox.textContent = `Error: ${error.message}`;
            deleteAction.style.display = 'none';
            currentViewedKey = null;
        })
        .finally(() => {
            setLoading(button, false);
        });
}

// Delete Key Handlers (Scenario 4)

// Store the element that opened the dialog for focus restoration
let dialogTriggerElement = null;

// Show delete confirmation dialog
function showDeleteConfirmDialog() {
    if (!currentViewedKey) {
        showToast('No key selected for deletion', 'error');
        return;
    }

    // Store the triggering element for focus restoration (Scenario 12 - Accessibility)
    dialogTriggerElement = document.activeElement;

    const dialog = document.getElementById('delete-confirm-dialog');
    const message = document.getElementById('delete-confirm-message');
    message.textContent = `Are you sure you want to delete key "${currentViewedKey}"?`;
    dialog.style.display = 'flex';

    // Focus the cancel button for safety
    document.getElementById('delete-cancel-btn').focus();

    // Set up focus trap within dialog (Scenario 12 - Accessibility)
    setupDialogFocusTrap(dialog);
}

// Hide delete confirmation dialog
function hideDeleteConfirmDialog() {
    const dialog = document.getElementById('delete-confirm-dialog');
    dialog.style.display = 'none';

    // Restore focus to the element that opened the dialog (Scenario 12 - Accessibility)
    if (dialogTriggerElement && typeof dialogTriggerElement.focus === 'function') {
        dialogTriggerElement.focus();
    }
    dialogTriggerElement = null;
}

// Set up focus trap within dialog for accessibility (Scenario 12)
function setupDialogFocusTrap(dialog) {
    const focusableElements = dialog.querySelectorAll(
        'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
    );
    const firstFocusable = focusableElements[0];
    const lastFocusable = focusableElements[focusableElements.length - 1];

    // Remove existing trap listener if any
    if (dialog._focusTrapHandler) {
        dialog.removeEventListener('keydown', dialog._focusTrapHandler);
    }

    dialog._focusTrapHandler = function (e) {
        if (e.key !== 'Tab') return;

        if (e.shiftKey) {
            // Shift + Tab: if on first element, wrap to last
            if (document.activeElement === firstFocusable) {
                e.preventDefault();
                lastFocusable.focus();
            }
        } else {
            // Tab: if on last element, wrap to first
            if (document.activeElement === lastFocusable) {
                e.preventDefault();
                firstFocusable.focus();
            }
        }
    };

    dialog.addEventListener('keydown', dialog._focusTrapHandler);
}

// Handle delete confirmation
function handleDeleteConfirm() {
    if (!currentViewedKey) {
        hideDeleteConfirmDialog();
        return;
    }

    const confirmBtn = document.getElementById('delete-confirm-btn');
    setLoading(confirmBtn, true);

    const keyToDelete = currentViewedKey;

    deleteKey(keyToDelete)
        .then(() => {
            showToast(`Key "${keyToDelete}" deleted successfully`, 'success');
            // Clear the result and hide delete button
            const resultBox = document.getElementById('get-result');
            resultBox.textContent = 'Key deleted. Enter a new key to lookup.';
            document.getElementById('delete-action').style.display = 'none';
            currentViewedKey = null;
            hideDeleteConfirmDialog();
        })
        .catch((error) => {
            showToast(error.message, 'error');
            hideDeleteConfirmDialog();
        })
        .finally(() => {
            setLoading(confirmBtn, false);
        });
}

// Handle delete button click (shows confirmation)
function handleDeleteClick() {
    showDeleteConfirmDialog();
}

// Refresh Status Handler (Scenario 1)
function handleRefreshStatus() {
    const button = document.getElementById('refresh-status');
    setLoading(button, true);

    fetchStatus()
        .then((data) => {
            document.getElementById('level-0-files').textContent = data.levels?.[0]?.files || 0;
            document.getElementById('level-1-files').textContent = data.levels?.[1]?.files || 0;
            document.getElementById('memory-usage').textContent =
                `${data.memory?.percentage?.toFixed(1) || 0}% used`;
        })
        .catch((error) => {
            showToast(error.message, 'error');
        })
        .finally(() => {
            setLoading(button, false);
        });
}

// Compact Handler (Scenario 5)
function handleCompact() {
    const button = document.getElementById('compact-btn');
    setLoading(button, true);

    triggerCompact()
        .then(() => {
            showToast('Compaction triggered successfully', 'success');
        })
        .catch((error) => {
            showToast(error.message, 'error');
        })
        .finally(() => {
            setLoading(button, false);
        });
}

// Utility: Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Initialize event listeners
document.addEventListener('DOMContentLoaded', () => {
    // Set Key form
    const setKeyForm = document.getElementById('set-key-form');
    if (setKeyForm) {
        setKeyForm.addEventListener('submit', handleSetKeySubmit);
        clearValidationOnInput('set-key-input', 'set-key-error');
    }

    // Get Key form
    const getKeyForm = document.getElementById('get-key-form');
    if (getKeyForm) {
        getKeyForm.addEventListener('submit', handleGetKeySubmit);
        clearValidationOnInput('get-key-input', 'get-key-error');
    }

    // Refresh status button
    const refreshBtn = document.getElementById('refresh-status');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', handleRefreshStatus);
    }

    // Compact button
    const compactBtn = document.getElementById('compact-btn');
    if (compactBtn) {
        compactBtn.addEventListener('click', handleCompact);
    }

    // Delete button (Scenario 4)
    const deleteBtn = document.getElementById('delete-btn');
    if (deleteBtn) {
        deleteBtn.addEventListener('click', handleDeleteClick);
    }

    // Delete confirmation buttons (Scenario 4)
    const deleteConfirmBtn = document.getElementById('delete-confirm-btn');
    if (deleteConfirmBtn) {
        deleteConfirmBtn.addEventListener('click', handleDeleteConfirm);
    }

    const deleteCancelBtn = document.getElementById('delete-cancel-btn');
    if (deleteCancelBtn) {
        deleteCancelBtn.addEventListener('click', hideDeleteConfirmDialog);
    }

    // Close dialog on Escape key (Scenario 4 - accessibility)
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            const dialog = document.getElementById('delete-confirm-dialog');
            if (dialog && dialog.style.display === 'flex') {
                hideDeleteConfirmDialog();
            }
        }
    });

    // Initial status fetch
    handleRefreshStatus();
});
