/**
 * MirDB Homepage - Metrics Refresh Logic
 *
 * Owner: Scenario 4 - Metrics Auto-Refresh
 * Co-owner: Scenario 15 - Error Handling
 *
 * Responsibilities:
 * - Fetch metrics from /api/metrics
 * - Update dashboard display
 * - 30-second auto-refresh interval
 * - Handle fetch errors gracefully
 */

(function() {
    'use strict';

    // =============================================
    // Constants and Configuration
    // =============================================
    const API_BASE_URL = '/api';
    const METRICS_ENDPOINT = `${API_BASE_URL}/metrics`;
    const METRICS_REFRESH_INTERVAL = 30000; // 30 seconds
    const FETCH_TIMEOUT = 10000; // 10 seconds timeout for API calls

    // =============================================
    // State Management
    // =============================================
    let refreshIntervalId = null;
    let lastMetricsUpdate = null;
    let isRefreshing = false;
    let isInErrorState = false;
    let consecutiveErrors = 0;

    // =============================================
    // DOM Element References
    // =============================================
    let metricsElements = {};
    let dashboardSection = null;
    let metricsErrorDisplay = null;

    /**
     * Initialize DOM element references for metrics display
     */
    function initializeElements() {
        metricsElements = {
            uptime: document.querySelector('[data-metric="uptime"]'),
            memory: document.querySelector('[data-metric="memory"]'),
            keys: document.querySelector('[data-metric="keys"]'),
            ops: document.querySelector('[data-metric="ops"]'),
            storage: document.querySelector('[data-metric="storage"]')
        };
        dashboardSection = document.getElementById('dashboard');

        // Create or find the error display element for metrics
        metricsErrorDisplay = document.getElementById('metrics-error-display');
        if (!metricsErrorDisplay && dashboardSection) {
            metricsErrorDisplay = createErrorDisplayElement();
        }
    }

    /**
     * Create the error display element for the metrics section
     * @returns {HTMLElement} The created error display element
     */
    function createErrorDisplayElement() {
        const errorDiv = document.createElement('div');
        errorDiv.id = 'metrics-error-display';
        errorDiv.className = 'metrics-error';
        errorDiv.setAttribute('role', 'alert');
        errorDiv.setAttribute('aria-live', 'assertive');
        errorDiv.setAttribute('aria-atomic', 'true');
        errorDiv.style.display = 'none';

        errorDiv.innerHTML = `
            <span class="error-icon" aria-hidden="true">⚠</span>
            <span class="error-message">Unable to load metrics</span>
            <span class="error-detail"></span>
        `;

        // Insert after the section title
        const sectionTitle = dashboardSection.querySelector('.section-title');
        if (sectionTitle && sectionTitle.nextSibling) {
            dashboardSection.insertBefore(errorDiv, sectionTitle.nextSibling);
        } else {
            dashboardSection.appendChild(errorDiv);
        }

        return errorDiv;
    }

    // =============================================
    // Formatting Functions
    // =============================================

    /**
     * Format uptime seconds into human-readable string
     * @param {number} seconds - Total uptime in seconds
     * @returns {string} Formatted uptime string (e.g., "2h 15m 30s")
     */
    function formatUptime(seconds) {
        if (typeof seconds !== 'number' || isNaN(seconds)) {
            return '0h 0m 0s';
        }

        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = Math.floor(seconds % 60);

        return `${hours}h ${minutes}m ${secs}s`;
    }

    /**
     * Format bytes into human-readable string
     * @param {number} bytes - Size in bytes
     * @returns {string} Formatted size string (e.g., "128 MB")
     */
    function formatBytes(bytes) {
        if (typeof bytes !== 'number' || isNaN(bytes) || bytes < 0) {
            return '0 MB';
        }

        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let unitIndex = 0;
        let size = bytes;

        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }

        return `${size.toFixed(unitIndex > 0 ? 1 : 0)} ${units[unitIndex]}`;
    }

    /**
     * Format number with commas for thousands
     * @param {number} num - Number to format
     * @returns {string} Formatted number string
     */
    function formatNumber(num) {
        if (typeof num !== 'number' || isNaN(num)) {
            return '0';
        }
        return num.toLocaleString();
    }

    /**
     * Format operations per second
     * @param {number} opsPerSec - Operations per second value
     * @returns {string} Formatted ops/sec string
     */
    function formatOpsPerSec(opsPerSec) {
        if (typeof opsPerSec !== 'number' || isNaN(opsPerSec)) {
            return '0';
        }

        if (opsPerSec >= 1000000) {
            return `${(opsPerSec / 1000000).toFixed(2)}M`;
        } else if (opsPerSec >= 1000) {
            return `${(opsPerSec / 1000).toFixed(2)}K`;
        }

        return opsPerSec.toFixed(2);
    }

    // =============================================
    // Error State Functions - Scenario 15 Error Handling
    // =============================================

    /**
     * Show error state in the metrics dashboard
     * @param {string} errorType - Type of error ('fetch', 'timeout', 'server')
     * @param {string} [detail] - Additional error details
     */
    function showMetricsError(errorType, detail) {
        isInErrorState = true;
        consecutiveErrors++;

        if (!metricsErrorDisplay) return;

        const errorMessage = metricsErrorDisplay.querySelector('.error-message');
        const errorDetail = metricsErrorDisplay.querySelector('.error-detail');

        let message = 'Unable to load metrics';
        let detailText = '';

        switch (errorType) {
            case 'timeout':
                message = 'Metrics request timed out';
                detailText = 'The server is taking too long to respond. Will retry automatically.';
                break;
            case 'server':
                message = 'Metrics server error';
                detailText = detail || 'The server returned an error. Will retry automatically.';
                break;
            case 'network':
                message = 'Network error';
                detailText = 'Unable to connect to the server. Check your connection.';
                break;
            default:
                message = 'Unable to load metrics';
                detailText = detail || 'Will retry automatically.';
        }

        if (errorMessage) errorMessage.textContent = message;
        if (errorDetail) errorDetail.textContent = detailText;

        metricsErrorDisplay.style.display = 'flex';
        metricsErrorDisplay.classList.add('error-visible');

        // Add error class to metric cards
        const metricsGrid = dashboardSection ? dashboardSection.querySelector('.metrics-grid') : null;
        if (metricsGrid) {
            metricsGrid.classList.add('metrics-error-state');
        }

        // Dispatch custom event for error state
        const event = new CustomEvent('mirdb:metrics-error-displayed', {
            detail: { errorType, message, consecutiveErrors }
        });
        document.dispatchEvent(event);
    }

    /**
     * Clear error state and restore normal display
     */
    function clearMetricsError() {
        if (!isInErrorState) return;

        isInErrorState = false;
        consecutiveErrors = 0;

        if (metricsErrorDisplay) {
            metricsErrorDisplay.style.display = 'none';
            metricsErrorDisplay.classList.remove('error-visible');
        }

        // Remove error class from metric cards
        const metricsGrid = dashboardSection ? dashboardSection.querySelector('.metrics-grid') : null;
        if (metricsGrid) {
            metricsGrid.classList.remove('metrics-error-state');
        }

        // Dispatch custom event for recovery
        const event = new CustomEvent('mirdb:metrics-recovered', {
            detail: { timestamp: Date.now() }
        });
        document.dispatchEvent(event);
    }

    /**
     * Check if metrics are currently in error state
     * @returns {boolean} True if in error state
     */
    function isMetricsInError() {
        return isInErrorState;
    }

    // =============================================
    // Metrics Update Functions
    // =============================================

    /**
     * Update the metrics display with new data
     * @param {Object} metrics - Metrics data from API
     * @param {number} metrics.uptime_seconds - Server uptime in seconds
     * @param {number} metrics.memory_usage_bytes - Memory usage in bytes
     * @param {number} metrics.total_keys - Total number of keys
     * @param {number} metrics.ops_per_second - Operations per second
     * @param {number} metrics.storage_used_bytes - Storage used in bytes
     */
    function updateMetricsDisplay(metrics) {
        if (!metrics) {
            console.warn('No metrics data provided');
            return;
        }

        // Update uptime
        if (metricsElements.uptime) {
            metricsElements.uptime.textContent = formatUptime(metrics.uptime_seconds);
        }

        // Update memory
        if (metricsElements.memory) {
            metricsElements.memory.textContent = formatBytes(metrics.memory_usage_bytes);
        }

        // Update total keys
        if (metricsElements.keys) {
            metricsElements.keys.textContent = formatNumber(metrics.total_keys);
        }

        // Update ops/sec
        if (metricsElements.ops) {
            metricsElements.ops.textContent = formatOpsPerSec(metrics.ops_per_second);
        }

        // Update storage
        if (metricsElements.storage) {
            metricsElements.storage.textContent = formatBytes(metrics.storage_used_bytes);
        }

        // Store last update timestamp
        lastMetricsUpdate = Date.now();

        // Dispatch custom event for other components
        const event = new CustomEvent('mirdb:metrics-updated', {
            detail: {
                metrics: metrics,
                timestamp: lastMetricsUpdate
            }
        });
        document.dispatchEvent(event);
    }

    /**
     * Fetch metrics from the API with timeout support
     * @returns {Promise<Object>} Metrics response object
     * @throws {Error} Throws error on fetch failure, timeout, or server error
     */
    async function fetchMetrics() {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT);

        try {
            const response = await fetch(METRICS_ENDPOINT, {
                signal: controller.signal
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                const error = new Error(`HTTP error! status: ${response.status}`);
                error.type = 'server';
                error.status = response.status;
                throw error;
            }

            const data = await response.json();
            return data;
        } catch (error) {
            clearTimeout(timeoutId);

            // Determine error type for better error messaging
            if (error.name === 'AbortError') {
                const timeoutError = new Error('Request timed out');
                timeoutError.type = 'timeout';
                throw timeoutError;
            }

            if (error.type === 'server') {
                throw error;
            }

            // Network or other fetch errors
            error.type = 'network';
            console.error('Failed to fetch metrics:', error);
            throw error;
        }
    }

    /**
     * Refresh metrics by fetching from API and updating display
     * Preserves DOM state (scroll position, focused element)
     * Handles errors gracefully and supports recovery
     * @returns {Promise<Object|null>} Metrics data or null on error
     */
    async function refreshMetrics() {
        // Prevent concurrent refresh requests
        if (isRefreshing) {
            return null;
        }

        isRefreshing = true;

        // Store current DOM state
        const activeElement = document.activeElement;
        const scrollPosition = {
            x: window.scrollX,
            y: window.scrollY
        };

        try {
            const metrics = await fetchMetrics();

            // Clear any existing error state on successful fetch (recovery)
            if (isInErrorState) {
                clearMetricsError();
            }

            updateMetricsDisplay(metrics);

            // Restore DOM state (should already be preserved since no page reload)
            // This is a safeguard in case any script inadvertently affects state
            if (activeElement && typeof activeElement.focus === 'function') {
                activeElement.focus();
            }

            // Verify scroll position is preserved
            if (window.scrollX !== scrollPosition.x || window.scrollY !== scrollPosition.y) {
                window.scrollTo(scrollPosition.x, scrollPosition.y);
            }

            return metrics;
        } catch (error) {
            // Show error state in UI (Scenario 15)
            const errorType = error.type || 'fetch';
            const detail = error.status ? `Server returned status ${error.status}` : error.message;
            showMetricsError(errorType, detail);

            // Dispatch error event for error handling (Scenario 15)
            const event = new CustomEvent('mirdb:metrics-error', {
                detail: {
                    error: error.message,
                    type: errorType,
                    consecutiveErrors: consecutiveErrors
                }
            });
            document.dispatchEvent(event);

            return null;
        } finally {
            isRefreshing = false;
        }
    }

    // =============================================
    // Auto-Refresh Management
    // =============================================

    /**
     * Start automatic metrics refresh
     * Uses setInterval to refresh metrics every 30 seconds
     * @param {number} [interval=30000] - Refresh interval in milliseconds
     * @returns {number} Interval ID for clearing if needed
     */
    function startMetricsRefresh(interval = METRICS_REFRESH_INTERVAL) {
        // Stop any existing refresh interval
        stopMetricsRefresh();

        // Perform initial fetch
        refreshMetrics();

        // Set up periodic refresh using setInterval with 30000ms (30 seconds)
        refreshIntervalId = setInterval(refreshMetrics, interval);

        console.log(`Metrics auto-refresh started with ${interval}ms interval`);

        return refreshIntervalId;
    }

    /**
     * Stop automatic metrics refresh
     */
    function stopMetricsRefresh() {
        if (refreshIntervalId !== null) {
            clearInterval(refreshIntervalId);
            refreshIntervalId = null;
            console.log('Metrics auto-refresh stopped');
        }
    }

    /**
     * Check if auto-refresh is currently active
     * @returns {boolean} True if auto-refresh is active
     */
    function isAutoRefreshActive() {
        return refreshIntervalId !== null;
    }

    /**
     * Get the configured refresh interval
     * @returns {number} Refresh interval in milliseconds
     */
    function getRefreshInterval() {
        return METRICS_REFRESH_INTERVAL;
    }

    /**
     * Get the last metrics update timestamp
     * @returns {number|null} Timestamp of last update or null
     */
    function getLastUpdateTime() {
        return lastMetricsUpdate;
    }

    // =============================================
    // Mock Data for Testing
    // =============================================

    /**
     * Generate mock metrics data for testing
     * @returns {Object} Mock metrics object
     */
    function generateMockMetrics() {
        return {
            uptime_seconds: Math.floor(Date.now() / 1000) % 86400,
            memory_usage_bytes: Math.floor(Math.random() * 1073741824), // 0-1GB
            total_keys: Math.floor(Math.random() * 100000),
            ops_per_second: Math.random() * 10000,
            storage_used_bytes: Math.floor(Math.random() * 10737418240) // 0-10GB
        };
    }

    /**
     * Update display with mock metrics (for testing without API)
     */
    function updateWithMockMetrics() {
        const mockMetrics = generateMockMetrics();
        updateMetricsDisplay(mockMetrics);
        return mockMetrics;
    }

    // =============================================
    // Initialization
    // =============================================

    /**
     * Initialize the metrics module
     */
    function init() {
        // Initialize DOM references
        initializeElements();

        // Check if we have metrics elements to update
        const hasMetricsElements = Object.values(metricsElements).some(el => el !== null);

        if (!hasMetricsElements) {
            console.warn('No metrics elements found in the DOM');
            return;
        }

        // If not on file protocol, start auto-refresh with real API
        if (window.location.protocol !== 'file:') {
            startMetricsRefresh();
        } else {
            // For file protocol testing, use mock data
            console.log('Running in file protocol mode - using mock metrics');
            updateWithMockMetrics();

            // Still set up interval for mock data
            refreshIntervalId = setInterval(updateWithMockMetrics, METRICS_REFRESH_INTERVAL);
        }

        console.log('MirDB Metrics module initialized');
    }

    // =============================================
    // Export for Testing and External Access
    // =============================================
    window.MirDB = window.MirDB || {};
    window.MirDB.metrics = {
        // Core functions
        refresh: refreshMetrics,
        startRefresh: startMetricsRefresh,
        stopRefresh: stopMetricsRefresh,
        updateDisplay: updateMetricsDisplay,

        // State queries
        isRefreshActive: isAutoRefreshActive,
        getRefreshInterval: getRefreshInterval,
        getLastUpdateTime: getLastUpdateTime,

        // Error handling functions - Scenario 15
        showError: showMetricsError,
        clearError: clearMetricsError,
        isInError: isMetricsInError,

        // Formatting utilities
        formatUptime: formatUptime,
        formatBytes: formatBytes,
        formatNumber: formatNumber,
        formatOpsPerSec: formatOpsPerSec,

        // Testing utilities
        generateMockMetrics: generateMockMetrics,
        updateWithMock: updateWithMockMetrics,

        // Constants (exposed for testing)
        REFRESH_INTERVAL: METRICS_REFRESH_INTERVAL,
        FETCH_TIMEOUT: FETCH_TIMEOUT
    };

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
