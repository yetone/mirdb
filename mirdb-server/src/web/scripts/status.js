/**
 * Status Dashboard Logic
 * Owner: Scenario 3 - Server Status Dashboard
 *
 * Expected exports:
 * - initStatusDashboard() - Initialize metrics display
 * - updateStatus() - Fetch and render status metrics
 * - startAutoRefresh() - 5-second refresh interval
 * - stopAutoRefresh() - Stop auto-refresh
 * - isRefreshing() - Check if auto-refresh is active
 */

var MirDBStatus = (function() {
    'use strict';

    var refreshInterval = null;
    var REFRESH_INTERVAL_MS = 5000; // 5 seconds
    var statusPanelId = 'status-panel';
    var isUpdating = false;

    /**
     * Format bytes to human-readable string
     * @param {number} bytes - Bytes to format
     * @returns {string} Formatted string (e.g., "1.5 MB")
     */
    function formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        var k = 1024;
        var sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        var i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    /**
     * Create a metric card element
     * @param {string} label - Metric label
     * @param {string} value - Metric value
     * @param {string} icon - Icon type (memory, connection, database, table)
     * @returns {HTMLElement} Metric card element
     */
    function createMetricCard(label, value, icon) {
        var card = document.createElement('div');
        card.className = 'status-metric-card';
        card.setAttribute('data-metric', label.toLowerCase().replace(/\s+/g, '-'));

        var iconSvg = getMetricIcon(icon);

        card.innerHTML =
            '<div class="metric-icon">' + iconSvg + '</div>' +
            '<div class="metric-content">' +
                '<span class="metric-label">' + label + '</span>' +
                '<span class="metric-value">' + value + '</span>' +
            '</div>';

        return card;
    }

    /**
     * Get SVG icon for metric type
     * @param {string} type - Icon type
     * @returns {string} SVG string
     */
    function getMetricIcon(type) {
        var icons = {
            memory: '<svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2"/><line x1="9" y1="9" x2="15" y2="9"/><line x1="9" y1="13" x2="15" y2="13"/><line x1="9" y1="17" x2="12" y2="17"/></svg>',
            connection: '<svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M12 2v4m0 12v4M2 12h4m12 0h4"/><circle cx="12" cy="12" r="9"/></svg>',
            database: '<svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>',
            table: '<svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/></svg>'
        };
        return icons[type] || icons.database;
    }

    /**
     * Create refresh indicator element
     * @returns {HTMLElement} Refresh indicator element
     */
    function createRefreshIndicator() {
        var indicator = document.createElement('div');
        indicator.className = 'status-refresh-indicator';
        indicator.id = 'status-refresh-indicator';
        indicator.innerHTML =
            '<span class="refresh-icon">' +
                '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2">' +
                    '<path d="M23 4v6h-6"/>' +
                    '<path d="M1 20v-6h6"/>' +
                    '<path d="M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/>' +
                '</svg>' +
            '</span>' +
            '<span class="refresh-text">Auto-refresh: <span class="refresh-status">Active</span></span>' +
            '<span class="refresh-countdown" id="refresh-countdown">5s</span>';
        return indicator;
    }

    /**
     * Render status panel with metrics
     * @param {Object} status - Status response object
     */
    function renderStatusPanel(status) {
        var panel = document.getElementById(statusPanelId);
        if (!panel) return;

        // Clear existing content
        panel.innerHTML = '';

        // Create grid container for metrics
        var metricsGrid = document.createElement('div');
        metricsGrid.className = 'status-metrics-grid';

        // Add metric cards
        metricsGrid.appendChild(createMetricCard(
            'Memory Usage',
            formatBytes(status.memory_usage),
            'memory'
        ));

        metricsGrid.appendChild(createMetricCard(
            'Active Connections',
            String(status.active_connections),
            'connection'
        ));

        metricsGrid.appendChild(createMetricCard(
            'Database Size',
            formatBytes(status.database_size),
            'database'
        ));

        metricsGrid.appendChild(createMetricCard(
            'Memtable Count',
            String(status.memtable_count),
            'table'
        ));

        metricsGrid.appendChild(createMetricCard(
            'SSTable Count',
            String(status.sstable_count),
            'table'
        ));

        metricsGrid.appendChild(createMetricCard(
            'Total Size',
            formatBytes(status.total_size),
            'database'
        ));

        panel.appendChild(metricsGrid);

        // Add refresh indicator
        var existingIndicator = document.getElementById('status-refresh-indicator');
        if (!existingIndicator) {
            panel.appendChild(createRefreshIndicator());
        }
    }

    /**
     * Show loading state
     */
    function showLoading() {
        var panel = document.getElementById(statusPanelId);
        if (!panel) return;

        var cards = panel.querySelectorAll('.status-metric-card');
        cards.forEach(function(card) {
            card.classList.add('loading');
        });
    }

    /**
     * Hide loading state
     */
    function hideLoading() {
        var panel = document.getElementById(statusPanelId);
        if (!panel) return;

        var cards = panel.querySelectorAll('.status-metric-card');
        cards.forEach(function(card) {
            card.classList.remove('loading');
        });
    }

    /**
     * Show error state
     * @param {string} message - Error message
     */
    function showError(message) {
        var panel = document.getElementById(statusPanelId);
        if (!panel) return;

        panel.innerHTML =
            '<div class="status-error">' +
                '<span class="error-icon">⚠</span>' +
                '<span class="error-message">' + message + '</span>' +
                '<button class="error-retry-btn" onclick="MirDBStatus.updateStatus()">Retry</button>' +
            '</div>';
    }

    /**
     * Update countdown display
     * @param {number} seconds - Seconds remaining
     */
    function updateCountdown(seconds) {
        var countdown = document.getElementById('refresh-countdown');
        if (countdown) {
            countdown.textContent = seconds + 's';
        }
    }

    /**
     * Initialize status dashboard
     * Sets up the initial UI and starts auto-refresh
     */
    function initStatusDashboard() {
        var panel = document.getElementById(statusPanelId);
        if (!panel) {
            console.warn('Status panel element not found');
            return;
        }

        // Show initial loading state
        panel.innerHTML = '<div class="status-placeholder">Loading status...</div>';

        // Add dashboard CSS if not already present
        addDashboardStyles();

        // Fetch initial status
        updateStatus();

        // Start auto-refresh
        startAutoRefresh();
    }

    /**
     * Add dashboard-specific CSS styles
     */
    function addDashboardStyles() {
        if (document.getElementById('status-dashboard-styles')) return;

        var styles = document.createElement('style');
        styles.id = 'status-dashboard-styles';
        styles.textContent =
            '.status-metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 1rem; }' +
            '.status-metric-card { background: linear-gradient(135deg, #fff 0%, #f8f9fa 100%); border: 1px solid #e0e0e0; border-radius: 8px; padding: 1rem; display: flex; align-items: center; gap: 0.75rem; transition: all 0.3s ease; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }' +
            '.status-metric-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-color: #E65100; }' +
            '.status-metric-card.loading { opacity: 0.6; pointer-events: none; }' +
            '.status-metric-card.loading .metric-value { animation: pulse 1.5s ease-in-out infinite; }' +
            '@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }' +
            '.metric-icon { width: 40px; height: 40px; background-color: #FFF3E0; border-radius: 8px; display: flex; align-items: center; justify-content: center; color: #E65100; flex-shrink: 0; }' +
            '.metric-content { display: flex; flex-direction: column; min-width: 0; }' +
            '.metric-label { font-size: 0.75rem; color: #616161; text-transform: uppercase; letter-spacing: 0.5px; white-space: nowrap; }' +
            '.metric-value { font-size: 1.25rem; font-weight: 600; color: #212121; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }' +
            '.status-refresh-indicator { display: flex; align-items: center; gap: 0.5rem; padding: 0.5rem 1rem; background-color: #e8f5e9; border-radius: 6px; font-size: 0.875rem; color: #2e7d32; }' +
            '.refresh-icon { display: flex; align-items: center; animation: spin 2s linear infinite; }' +
            '.refresh-icon svg { color: #2e7d32; }' +
            '@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }' +
            '.refresh-text { flex: 1; }' +
            '.refresh-status { font-weight: 600; }' +
            '.refresh-countdown { font-weight: 600; font-family: monospace; }' +
            '.status-error { display: flex; flex-direction: column; align-items: center; gap: 1rem; padding: 2rem; text-align: center; background-color: #ffebee; border-radius: 8px; }' +
            '.error-icon { font-size: 2rem; }' +
            '.error-message { color: #c62828; }' +
            '.error-retry-btn { padding: 0.5rem 1rem; background-color: #E65100; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: 500; }' +
            '.error-retry-btn:hover { background-color: #d84315; }';

        document.head.appendChild(styles);
    }

    /**
     * Update status metrics by fetching from API
     * @returns {Promise} Promise that resolves when update is complete
     */
    function updateStatus() {
        if (isUpdating) {
            return Promise.resolve();
        }

        isUpdating = true;
        showLoading();

        return MirDBApi.fetchStatus()
            .then(function(status) {
                renderStatusPanel(status);
                hideLoading();
                isUpdating = false;
            })
            .catch(function(error) {
                console.error('Failed to fetch status:', error);
                showError('Failed to load server status: ' + error.message);
                isUpdating = false;
            });
    }

    /**
     * Start auto-refresh with 5-second interval
     */
    function startAutoRefresh() {
        if (refreshInterval) {
            return; // Already running
        }

        var countdown = 5;

        // Update countdown every second
        var countdownInterval = setInterval(function() {
            countdown--;
            if (countdown <= 0) {
                countdown = 5;
            }
            updateCountdown(countdown);
        }, 1000);

        // Refresh status every 5 seconds
        refreshInterval = setInterval(function() {
            updateStatus();
            countdown = 5;
        }, REFRESH_INTERVAL_MS);

        // Store countdown interval for cleanup
        refreshInterval.countdownInterval = countdownInterval;

        // Update indicator
        var statusEl = document.querySelector('.refresh-status');
        if (statusEl) {
            statusEl.textContent = 'Active';
        }
    }

    /**
     * Stop auto-refresh
     */
    function stopAutoRefresh() {
        if (refreshInterval) {
            clearInterval(refreshInterval);
            if (refreshInterval.countdownInterval) {
                clearInterval(refreshInterval.countdownInterval);
            }
            refreshInterval = null;
        }

        // Update indicator
        var statusEl = document.querySelector('.refresh-status');
        if (statusEl) {
            statusEl.textContent = 'Paused';
        }
    }

    /**
     * Check if auto-refresh is active
     * @returns {boolean} True if auto-refresh is running
     */
    function isRefreshing() {
        return refreshInterval !== null;
    }

    /**
     * Get refresh interval in milliseconds
     * @returns {number} Refresh interval in ms
     */
    function getRefreshInterval() {
        return REFRESH_INTERVAL_MS;
    }

    // Public API
    return {
        initStatusDashboard: initStatusDashboard,
        updateStatus: updateStatus,
        startAutoRefresh: startAutoRefresh,
        stopAutoRefresh: stopAutoRefresh,
        isRefreshing: isRefreshing,
        getRefreshInterval: getRefreshInterval,
        formatBytes: formatBytes
    };
})();

// Auto-initialize when DOM is ready
(function() {
    'use strict';

    function onReady(fn) {
        if (document.readyState !== 'loading') {
            fn();
        } else {
            document.addEventListener('DOMContentLoaded', fn);
        }
    }

    onReady(function() {
        // Only initialize if status panel exists
        if (document.getElementById('status-panel')) {
            MirDBStatus.initStatusDashboard();
        }
    });
})();
