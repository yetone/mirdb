/**
 * MirDB Homepage JavaScript
 * Owner: Scenario 10 (polling), 11 (forms), 13 (theme)
 * Scenario 2 provides base JavaScript for static content serving
 *
 * Features:
 * - Metrics polling: Fetch /api/metrics every 5 seconds
 * - Dashboard update: Update metric card values
 * - Try It Out forms: Handle SET/GET/DELETE form submissions
 * - Theme toggle: Switch themes, persist to localStorage
 */

// Theme Management
function loadThemePreference() {
    const savedTheme = localStorage.getItem('mirdb-theme');
    if (savedTheme) {
        document.documentElement.setAttribute('data-theme', savedTheme);
        updateThemeIcon(savedTheme);
    } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
        document.documentElement.setAttribute('data-theme', 'dark');
        updateThemeIcon('dark');
    }
}

function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('mirdb-theme', newTheme);
    updateThemeIcon(newTheme);
}

function updateThemeIcon(theme) {
    const themeIcon = document.querySelector('.theme-icon');
    if (themeIcon) {
        themeIcon.textContent = theme === 'dark' ? '\u2600\uFE0F' : '\uD83C\uDF19';
    }
}

// Metrics Polling
let metricsInterval = null;

function initMetricsPolling() {
    // Initial fetch
    fetchMetrics();

    // Poll every 5 seconds
    metricsInterval = setInterval(fetchMetrics, 5000);
}

async function fetchMetrics() {
    try {
        const response = await fetch('/api/metrics');
        if (response.ok) {
            const data = await response.json();
            updateDashboard(data);
        }
    } catch (error) {
        console.error('Failed to fetch metrics:', error);
    }
}

function updateDashboard(metrics) {
    const memoryValue = document.getElementById('memory-value');
    const diskValue = document.getElementById('disk-value');
    const keysValue = document.getElementById('keys-value');
    const connectionsValue = document.getElementById('connections-value');

    if (memoryValue && metrics.memory_used_bytes !== undefined) {
        memoryValue.textContent = formatBytes(metrics.memory_used_bytes);
    }
    if (diskValue && metrics.disk_used_bytes !== undefined) {
        diskValue.textContent = formatBytes(metrics.disk_used_bytes);
    }
    if (keysValue && metrics.key_count !== undefined) {
        keysValue.textContent = metrics.key_count.toLocaleString();
    }
    if (connectionsValue && metrics.active_connections !== undefined) {
        connectionsValue.textContent = metrics.active_connections;
    }
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Configuration Fetching
async function fetchConfig() {
    try {
        const response = await fetch('/api/config');
        if (response.ok) {
            const data = await response.json();
            updateConfigPanel(data);
        }
    } catch (error) {
        console.error('Failed to fetch config:', error);
    }
}

function updateConfigPanel(config) {
    const listenAddr = document.getElementById('config-listen-addr');
    const workDir = document.getElementById('config-work-dir');
    const maxLevel = document.getElementById('config-max-level');
    const sstMaxSize = document.getElementById('config-sst-max-size');

    if (listenAddr && config.listen_addr !== undefined) {
        listenAddr.textContent = config.listen_addr;
    }
    if (workDir && config.work_dir !== undefined) {
        workDir.textContent = config.work_dir;
    }
    if (maxLevel && config.max_lsm_levels !== undefined) {
        maxLevel.textContent = config.max_lsm_levels;
    }
    if (sstMaxSize && config.sstable_max_size !== undefined) {
        sstMaxSize.textContent = formatBytes(config.sstable_max_size);
    }
}

// Form Handlers
function showResult(message, isError = false) {
    const resultDiv = document.getElementById('result');
    if (resultDiv) {
        resultDiv.textContent = message;
        resultDiv.className = 'result ' + (isError ? 'error' : 'success');
    }
}

// Form validation helper
function validateForm(key, requireValue = false, value = null) {
    if (!key || key.trim() === '') {
        showResult('Validation Error: Key is required', true);
        return false;
    }
    if (requireValue && (!value || value.trim() === '')) {
        showResult('Validation Error: Value is required', true);
        return false;
    }
    return true;
}

async function handleSetForm(event) {
    event.preventDefault();
    const formData = new FormData(event.target);
    const key = formData.get('key');
    const value = formData.get('value');

    // Validate before submit
    if (!validateForm(key, true, value)) {
        return;
    }

    try {
        const response = await fetch('/api/kv/set', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ key, value })
        });
        const data = await response.json();
        if (data.success) {
            showResult(`STORED: ${key} = ${value}`);
        } else {
            showResult(data.error || 'SET operation failed', true);
        }
    } catch (error) {
        showResult('Error: ' + error.message, true);
    }
}

async function handleGetForm(event) {
    event.preventDefault();
    const formData = new FormData(event.target);
    const key = formData.get('key');

    // Validate before submit
    if (!validateForm(key)) {
        return;
    }

    try {
        const response = await fetch(`/api/kv/get?key=${encodeURIComponent(key)}`);
        const data = await response.json();
        if (data.success && data.data) {
            showResult(`GET ${key} = ${data.data.value}`);
        } else {
            showResult(data.error || 'NOT_FOUND', true);
        }
    } catch (error) {
        showResult('Error: ' + error.message, true);
    }
}

async function handleDeleteForm(event) {
    event.preventDefault();
    const formData = new FormData(event.target);
    const key = formData.get('key');

    // Validate before submit
    if (!validateForm(key)) {
        return;
    }

    try {
        const response = await fetch(`/api/kv/delete?key=${encodeURIComponent(key)}`, {
            method: 'DELETE'
        });
        const data = await response.json();
        if (data.success) {
            showResult(data.message || 'DELETED');
        } else {
            showResult(data.error || 'DELETE operation failed', true);
        }
    } catch (error) {
        showResult('Error: ' + error.message, true);
    }
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function() {
    // Load theme preference
    loadThemePreference();

    // Theme toggle handler
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', toggleTheme);
    }

    // Form handlers
    const setForm = document.getElementById('set-form');
    const getForm = document.getElementById('get-form');
    const deleteForm = document.getElementById('delete-form');

    if (setForm) setForm.addEventListener('submit', handleSetForm);
    if (getForm) getForm.addEventListener('submit', handleGetForm);
    if (deleteForm) deleteForm.addEventListener('submit', handleDeleteForm);

    // Start metrics polling
    initMetricsPolling();

    // Fetch configuration (only once on page load)
    fetchConfig();
});
