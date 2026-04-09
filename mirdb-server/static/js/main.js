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
    const FETCH_TIMEOUT = 10000; // 10 seconds timeout

    // Status indicator states
    const STATUS_RUNNING = 'running';
    const STATUS_STOPPED = 'stopped';
    const STATUS_ERROR = 'error';
    const STATUS_UNKNOWN = 'unknown';

    // Error state tracking
    let isInErrorState = false;
    let consecutiveErrors = 0;
    let statusRefreshIntervalId = null;

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
     * @param {string} status - The server status ('running', 'stopped', 'error', or 'unknown')
     * @param {string} [message] - Optional custom message to display
     */
    function updateStatusDisplay(status, message) {
        if (!statusIndicator || !statusDot || !statusText) {
            console.warn('Status indicator elements not found');
            return;
        }

        // Remove all status classes
        statusDot.classList.remove('running', 'stopped', 'error', 'unknown');

        // Determine display text and ARIA label
        let displayText;
        let ariaLabel;

        switch (status) {
            case STATUS_RUNNING:
                displayText = message || 'Running';
                ariaLabel = 'Server status: Running. The MirDB server is operational.';
                statusDot.classList.add('running');
                isInErrorState = false;
                consecutiveErrors = 0;
                break;
            case STATUS_STOPPED:
                displayText = message || 'Stopped';
                ariaLabel = 'Server status: Stopped. The MirDB server is not running.';
                statusDot.classList.add('stopped');
                isInErrorState = false;
                consecutiveErrors = 0;
                break;
            case STATUS_ERROR:
                displayText = message || 'Error';
                ariaLabel = 'Server status: Error. Unable to connect to the MirDB server. Will retry automatically.';
                statusDot.classList.add('error');
                isInErrorState = true;
                consecutiveErrors++;
                break;
            case STATUS_UNKNOWN:
                displayText = message || 'Unknown';
                ariaLabel = 'Server status: Unknown. Unable to determine server status. Will retry automatically.';
                statusDot.classList.add('unknown');
                isInErrorState = true;
                consecutiveErrors++;
                break;
            default:
                displayText = message || 'Unknown';
                ariaLabel = 'Server status: Unknown. Unable to determine server status.';
                statusDot.classList.add('unknown');
        }

        // Update display
        statusText.textContent = displayText;

        // Update ARIA attributes for accessibility
        statusIndicator.setAttribute('aria-label', ariaLabel);
        statusIndicator.setAttribute('aria-live', 'assertive');
        statusIndicator.setAttribute('role', 'status');

        // Dispatch status change event
        const event = new CustomEvent('mirdb:status-changed', {
            detail: { status, message: displayText, isError: isInErrorState }
        });
        document.dispatchEvent(event);
    }

    /**
     * Fetch server status from the API with timeout support
     * @returns {Promise<Object>} The status response object
     * @throws {Error} Throws error on fetch failure, timeout, or server error
     */
    async function fetchStatus() {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT);

        try {
            const response = await fetch(STATUS_ENDPOINT, {
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
            console.error('Failed to fetch status:', error);
            throw error;
        }
    }

    /**
     * Refresh the status display by fetching from API
     * Handles errors gracefully and supports recovery
     * @returns {Promise<Object|null>} Status data or null on error
     */
    async function refreshStatus() {
        const wasInErrorState = isInErrorState;

        try {
            const status = await fetchStatus();
            updateStatusDisplay(status.status);

            // If recovering from error state, dispatch recovery event
            if (wasInErrorState) {
                const recoveryEvent = new CustomEvent('mirdb:status-recovered', {
                    detail: { status: status.status, timestamp: Date.now() }
                });
                document.dispatchEvent(recoveryEvent);
            }

            // Dispatch custom event for other components that might need status data
            const event = new CustomEvent('mirdb:status-updated', {
                detail: status
            });
            document.dispatchEvent(event);

            return status;
        } catch (error) {
            // Determine appropriate error message based on error type
            let errorMessage = 'Unavailable';
            if (error.type === 'timeout') {
                errorMessage = 'Timeout';
            } else if (error.type === 'server' && error.status) {
                errorMessage = 'Server Error';
            } else if (error.type === 'network') {
                errorMessage = 'Offline';
            }

            // Update display to show error/unknown state
            updateStatusDisplay(STATUS_UNKNOWN, errorMessage);

            // Dispatch error event
            const event = new CustomEvent('mirdb:status-error', {
                detail: {
                    error: error.message,
                    type: error.type || 'unknown',
                    consecutiveErrors: consecutiveErrors
                }
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
        // Stop any existing refresh
        stopStatusRefresh();

        // Initial fetch
        refreshStatus();

        // Set up periodic refresh
        statusRefreshIntervalId = setInterval(refreshStatus, interval);
        return statusRefreshIntervalId;
    }

    /**
     * Stop automatic status refresh
     */
    function stopStatusRefresh() {
        if (statusRefreshIntervalId !== null) {
            clearInterval(statusRefreshIntervalId);
            statusRefreshIntervalId = null;
        }
    }

    /**
     * Check if status is currently in error state
     * @returns {boolean} True if in error state
     */
    function isStatusInError() {
        return isInErrorState;
    }

    /**
     * Get the number of consecutive errors
     * @returns {number} Number of consecutive errors
     */
    function getConsecutiveErrors() {
        return consecutiveErrors;
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

    /**
     * Set status display to unknown state (for testing)
     */
    function setUnknownStatus() {
        updateStatusDisplay(STATUS_UNKNOWN, 'Unknown');
    }

    // =============================================
    // Mobile Navigation Toggle - Scenario 9
    // =============================================

    let hamburgerMenu = null;
    let navList = null;

    /**
     * Initialize mobile navigation elements
     */
    function initializeMobileNav() {
        hamburgerMenu = document.getElementById('hamburger-menu');
        navList = document.getElementById('nav-list');

        if (hamburgerMenu && navList) {
            // Add click handler for hamburger menu
            hamburgerMenu.addEventListener('click', toggleMobileNav);

            // Close menu when clicking outside
            document.addEventListener('click', function(event) {
                if (navList.classList.contains('nav-open') &&
                    !hamburgerMenu.contains(event.target) &&
                    !navList.contains(event.target)) {
                    closeMobileNav();
                }
            });

            // Close menu when pressing Escape
            document.addEventListener('keydown', function(event) {
                if (event.key === 'Escape' && navList.classList.contains('nav-open')) {
                    closeMobileNav();
                    hamburgerMenu.focus();
                }
            });

            // Close menu when a nav link is clicked
            navList.querySelectorAll('.nav-link').forEach(function(link) {
                link.addEventListener('click', function() {
                    closeMobileNav();
                });
            });
        }
    }

    /**
     * Toggle mobile navigation menu
     */
    function toggleMobileNav() {
        if (!hamburgerMenu || !navList) return;

        const isOpen = navList.classList.contains('nav-open');

        if (isOpen) {
            closeMobileNav();
        } else {
            openMobileNav();
        }
    }

    /**
     * Open mobile navigation menu
     */
    function openMobileNav() {
        if (!hamburgerMenu || !navList) return;

        navList.classList.add('nav-open');
        hamburgerMenu.setAttribute('aria-expanded', 'true');
    }

    /**
     * Close mobile navigation menu
     */
    function closeMobileNav() {
        if (!hamburgerMenu || !navList) return;

        navList.classList.remove('nav-open');
        hamburgerMenu.setAttribute('aria-expanded', 'false');
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

        // Initialize endpoint display elements - Scenario 14
        initializeEndpointElements();

        // Initialize mobile navigation - Scenario 9
        initializeMobileNav();

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
    // Endpoint Display Functions - Scenario 14
    // =============================================

    let endpointHost = null;
    let endpointPort = null;
    let endpointConnectionString = null;
    let quickstartEndpoint = null;
    let endpointCopyBtn = null;

    /**
     * Initialize endpoint display elements
     */
    function initializeEndpointElements() {
        endpointHost = document.getElementById('endpoint-host');
        endpointPort = document.getElementById('endpoint-port');
        endpointConnectionString = document.getElementById('endpoint-connection-string');
        quickstartEndpoint = document.getElementById('quickstart-endpoint');
        endpointCopyBtn = document.getElementById('endpoint-copy-btn');

        if (endpointCopyBtn) {
            endpointCopyBtn.addEventListener('click', copyEndpointToClipboard);
        }
    }

    /**
     * Update the endpoint display with server information
     * @param {Object} endpoint - Endpoint object with host and port
     */
    function updateEndpointDisplay(endpoint) {
        if (!endpoint) return;

        const { host, port } = endpoint;
        const connectionString = `${host}:${port}`;

        if (endpointHost) {
            endpointHost.textContent = host;
        }
        if (endpointPort) {
            endpointPort.textContent = port;
        }
        if (endpointConnectionString) {
            endpointConnectionString.textContent = connectionString;
        }
        if (quickstartEndpoint) {
            quickstartEndpoint.textContent = connectionString;
        }

        // Dispatch event for endpoint update
        const event = new CustomEvent('mirdb:endpoint-updated', {
            detail: { host, port, connectionString }
        });
        document.dispatchEvent(event);
    }

    /**
     * Copy the endpoint connection string to clipboard
     */
    async function copyEndpointToClipboard() {
        if (!endpointConnectionString) return;

        const text = endpointConnectionString.textContent;

        try {
            await navigator.clipboard.writeText(text);
            showCopySuccess();
        } catch (err) {
            // Fallback for older browsers
            fallbackCopyToClipboard(text);
        }
    }

    /**
     * Fallback copy method for browsers without clipboard API
     * @param {string} text - Text to copy
     */
    function fallbackCopyToClipboard(text) {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        textArea.style.top = '-9999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();

        try {
            document.execCommand('copy');
            showCopySuccess();
        } catch (err) {
            console.error('Fallback copy failed:', err);
        }

        document.body.removeChild(textArea);
    }

    /**
     * Show copy success feedback
     */
    function showCopySuccess() {
        if (!endpointCopyBtn) return;

        endpointCopyBtn.classList.add('copied');
        endpointCopyBtn.setAttribute('aria-label', 'Copied!');

        // Reset after 2 seconds
        setTimeout(function() {
            endpointCopyBtn.classList.remove('copied');
            endpointCopyBtn.setAttribute('aria-label', 'Copy connection string to clipboard');
        }, 2000);
    }

    // Listen for status updates to refresh endpoint info
    document.addEventListener('mirdb:status-updated', function(event) {
        if (event.detail && event.detail.endpoint) {
            updateEndpointDisplay(event.detail.endpoint);
        }
    });

    // =============================================
    // Export for testing and external access
    // =============================================
    window.MirDB = window.MirDB || {};
    window.MirDB.status = {
        updateDisplay: updateStatusDisplay,
        refresh: refreshStatus,
        startRefresh: startStatusRefresh,
        stopRefresh: stopStatusRefresh,
        setRunning: setRunningStatus,
        setStopped: setStoppedStatus,
        setError: setErrorStatus,
        setUnknown: setUnknownStatus,
        isInError: isStatusInError,
        getConsecutiveErrors: getConsecutiveErrors,
        STATUS_RUNNING: STATUS_RUNNING,
        STATUS_STOPPED: STATUS_STOPPED,
        STATUS_ERROR: STATUS_ERROR,
        STATUS_UNKNOWN: STATUS_UNKNOWN,
        FETCH_TIMEOUT: FETCH_TIMEOUT
    };

    // Endpoint exports - Scenario 14
    window.MirDB.endpoint = {
        updateDisplay: updateEndpointDisplay,
        copyToClipboard: copyEndpointToClipboard
    };

    // Mobile navigation exports - Scenario 9
    window.MirDB.mobileNav = {
        toggle: toggleMobileNav,
        open: openMobileNav,
        close: closeMobileNav,
        isOpen: function() {
            return navList ? navList.classList.contains('nav-open') : false;
        }
    };

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
