/**
 * MirDB Homepage - Main JavaScript
 *
 * Owner: Scenario 2 - System Status Display
 * Co-owner: Scenario 15 - Error Handling
 *
 * Responsibilities:
 * - Initialize page components
 * - Fetch and display system status
 * - Handle error states
 * - Mobile navigation toggle
 */

(function() {
    'use strict';

    // =============================================
    // Constants and Configuration
    // =============================================
    const API_BASE_URL = '/api';
    const STATUS_ENDPOINT = `${API_BASE_URL}/status`;
    const STATUS_REFRESH_INTERVAL = 30000; // 30 seconds

    // Status indicator states
    const STATUS_RUNNING = 'running';
    const STATUS_STOPPED = 'stopped';
    const STATUS_ERROR = 'error';

    // =============================================
    // DOM Element References
    // =============================================
    let statusIndicator = null;
    let statusDot = null;
    let statusText = null;

    // =============================================
    // Status Display Functions
    // =============================================

    /**
     * Initialize DOM element references
     */
    function initializeElements() {
        statusIndicator = document.getElementById('status-indicator');
        if (statusIndicator) {
            statusDot = statusIndicator.querySelector('.status-dot');
            statusText = statusIndicator.querySelector('.status-text');
        }
    }

    /**
     * Update the status indicator display
     * @param {string} status - The server status ('running', 'stopped', or 'error')
     * @param {string} [message] - Optional custom message to display
     */
    function updateStatusDisplay(status, message) {
        if (!statusIndicator || !statusDot || !statusText) {
            console.warn('Status indicator elements not found');
            return;
        }

        // Remove all status classes
        statusDot.classList.remove('running', 'stopped', 'error');

        // Determine display text and ARIA label
        let displayText;
        let ariaLabel;

        switch (status) {
            case STATUS_RUNNING:
                displayText = message || 'Running';
                ariaLabel = 'Server status: Running. The MirDB server is operational.';
                statusDot.classList.add('running');
                break;
            case STATUS_STOPPED:
                displayText = message || 'Stopped';
                ariaLabel = 'Server status: Stopped. The MirDB server is not running.';
                statusDot.classList.add('stopped');
                break;
            case STATUS_ERROR:
                displayText = message || 'Error';
                ariaLabel = 'Server status: Error. Unable to connect to the MirDB server.';
                statusDot.classList.add('error');
                break;
            default:
                displayText = message || 'Unknown';
                ariaLabel = 'Server status: Unknown. Unable to determine server status.';
        }

        // Update display
        statusText.textContent = displayText;

        // Update ARIA attributes for accessibility
        statusIndicator.setAttribute('aria-label', ariaLabel);
        statusIndicator.setAttribute('aria-live', 'polite');
        statusIndicator.setAttribute('role', 'status');
    }

    /**
     * Fetch server status from the API
     * @returns {Promise<Object>} The status response object
     */
    async function fetchStatus() {
        try {
            const response = await fetch(STATUS_ENDPOINT);

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            return data;
        } catch (error) {
            console.error('Failed to fetch status:', error);
            throw error;
        }
    }

    /**
     * Refresh the status display by fetching from API
     */
    async function refreshStatus() {
        try {
            const status = await fetchStatus();
            updateStatusDisplay(status.status);

            // Dispatch custom event for other components that might need status data
            const event = new CustomEvent('mirdb:status-updated', {
                detail: status
            });
            document.dispatchEvent(event);

            return status;
        } catch (error) {
            // Update display to show error state
            updateStatusDisplay(STATUS_ERROR, 'Unavailable');

            // Dispatch error event
            const event = new CustomEvent('mirdb:status-error', {
                detail: { error: error.message }
            });
            document.dispatchEvent(event);

            return null;
        }
    }

    /**
     * Start automatic status refresh
     * @param {number} [interval] - Refresh interval in milliseconds
     * @returns {number} Interval ID for clearing if needed
     */
    function startStatusRefresh(interval = STATUS_REFRESH_INTERVAL) {
        // Initial fetch
        refreshStatus();

        // Set up periodic refresh
        return setInterval(refreshStatus, interval);
    }

    // =============================================
    // Mock Status Functions (for testing without API)
    // =============================================

    /**
     * Set status display to running state (for testing)
     */
    function setRunningStatus() {
        updateStatusDisplay(STATUS_RUNNING);
    }

    /**
     * Set status display to stopped state (for testing)
     */
    function setStoppedStatus() {
        updateStatusDisplay(STATUS_STOPPED);
    }

    /**
     * Set status display to error state (for testing)
     */
    function setErrorStatus() {
        updateStatusDisplay(STATUS_ERROR, 'Unavailable');
    }

    // =============================================
    // Initialization
    // =============================================

    /**
     * Initialize the main application
     */
    function init() {
        // Initialize DOM references
        initializeElements();

        // Set initial status display (default to running until API responds)
        if (statusIndicator) {
            // Set initial ARIA attributes
            statusIndicator.setAttribute('role', 'status');
            statusIndicator.setAttribute('aria-live', 'polite');
            statusIndicator.setAttribute('aria-label',
                'Server status: Running. The MirDB server is operational.');
        }

        // If we have a status indicator, try to fetch real status
        if (statusIndicator) {
            // Check if API is available (non-file protocol)
            if (window.location.protocol !== 'file:') {
                // Start status refresh with API
                startStatusRefresh();
            } else {
                // File protocol - just show default running status
                updateStatusDisplay(STATUS_RUNNING);
            }
        }

        console.log('MirDB Homepage initialized');
    }

    // =============================================
    // Export for testing and external access
    // =============================================
    window.MirDB = window.MirDB || {};
    window.MirDB.status = {
        updateDisplay: updateStatusDisplay,
        refresh: refreshStatus,
        startRefresh: startStatusRefresh,
        setRunning: setRunningStatus,
        setStopped: setStoppedStatus,
        setError: setErrorStatus,
        STATUS_RUNNING: STATUS_RUNNING,
        STATUS_STOPPED: STATUS_STOPPED,
        STATUS_ERROR: STATUS_ERROR
    };

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
