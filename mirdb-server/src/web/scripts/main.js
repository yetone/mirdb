/**
 * Main Application JavaScript
 * Owner: First builder (application initialization)
 *
 * Includes:
 * - Application initialization
 * - Auto-refresh setup (5-second interval)
 * - Event listeners for navigation
 */

/**
 * Initialize the application when DOM is ready
 */
document.addEventListener('DOMContentLoaded', function() {
    initApp();
});

/**
 * Main application initialization
 */
function initApp() {
    // Initialize UI utilities
    if (window.UI && window.UI.initCopyButtons) {
        window.UI.initCopyButtons();
    }

    // Initialize navigation
    initNavigation();

    // Initialize smooth scrolling
    initSmoothScroll();

    // Initialize Key Browser (Scenario 4)
    if (typeof MirDBKeys !== 'undefined' && MirDBKeys.initKeyBrowser) {
        MirDBKeys.initKeyBrowser();
    }

    // Initialize auto-refresh
    initAutoRefresh();

    // Initialize configuration display
    initConfigDisplay();

    console.log('MirDB Homepage initialized');
}

/**
 * Initialize navigation highlighting based on scroll position
 */
function initNavigation() {
    var navLinks = document.querySelectorAll('.nav-link');
    var sections = document.querySelectorAll('section[id]');

    function updateActiveNav() {
        var scrollPosition = window.scrollY + 100;

        sections.forEach(function(section) {
            var sectionTop = section.offsetTop;
            var sectionHeight = section.offsetHeight;
            var sectionId = section.getAttribute('id');

            if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
                navLinks.forEach(function(link) {
                    link.classList.remove('active');
                    if (link.getAttribute('href') === '#' + sectionId) {
                        link.classList.add('active');
                    }
                });
            }
        });

        // If at top of page, highlight Home
        if (scrollPosition < 200) {
            navLinks.forEach(function(link) { link.classList.remove('active'); });
            var homeLink = document.querySelector('.nav-link[href="#home"]');
            if (homeLink) homeLink.classList.add('active');
        }
    }

    window.addEventListener('scroll', updateActiveNav);
    updateActiveNav();
}

/**
 * Initialize smooth scrolling for anchor links
 */
function initSmoothScroll() {
    document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
        anchor.addEventListener('click', function(e) {
            var href = this.getAttribute('href');
            if (href === '#' || href === '#home') {
                e.preventDefault();
                window.scrollTo({ top: 0, behavior: 'smooth' });
                return;
            }

            var target = document.querySelector(href);
            if (target) {
                e.preventDefault();
                target.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });
}

/**
 * Initialize auto-refresh for status dashboard (5-second interval)
 */
function initAutoRefresh() {
    // Status refresh will be initialized by status.js
    // This is a placeholder for the refresh coordinator
}

/**
 * Initialize configuration display
 * Owner: Scenario 8 - Configuration Display
 *
 * Fetches server configuration from /api/config and renders
 * it as a tabular display in the config panel.
 */
function initConfigDisplay() {
    var configPanel = document.getElementById('config-panel');
    if (!configPanel) {
        return;
    }

    loadConfig();
}

/**
 * Load and display configuration
 */
function loadConfig() {
    var configPanel = document.getElementById('config-panel');
    if (!configPanel) {
        return;
    }

    // Show loading state
    configPanel.innerHTML = '<div class="config-loading">Loading configuration...</div>';

    MirDBApi.fetchConfig()
        .then(function(config) {
            renderConfig(config);
        })
        .catch(function(error) {
            configPanel.innerHTML = '<div class="config-error">Failed to load configuration: ' + error.message + '</div>';
        });
}

/**
 * Render configuration as a table
 * @param {Object} config - Configuration object from API
 */
function renderConfig(config) {
    var configPanel = document.getElementById('config-panel');
    if (!configPanel) {
        return;
    }

    var configItems = [
        { key: 'work_dir', label: 'Work Directory', value: config.work_dir, description: 'Directory for database files' },
        { key: 'port', label: 'Port', value: config.port, description: 'Server listening port' },
        { key: 'max_levels', label: 'Max Levels', value: config.max_levels, description: 'Maximum LSM tree levels' },
        { key: 'memtable_size', label: 'Memtable Size', value: formatBytes(config.memtable_size), description: 'Maximum memtable size' },
        { key: 'memtable_height', label: 'Memtable Height', value: config.memtable_height, description: 'Skip list maximum height' },
        { key: 'imm_memtable_count', label: 'Immutable Memtables', value: config.imm_memtable_count, description: 'Max immutable memtable count' },
        { key: 'sst_max_size', label: 'SSTable Max Size', value: formatBytes(config.sst_max_size), description: 'Maximum SSTable file size' },
        { key: 'l0_compaction_trigger', label: 'L0 Compaction Trigger', value: config.l0_compaction_trigger, description: 'L0 files before compaction' },
        { key: 'block_size', label: 'Block Size', value: formatBytes(config.block_size), description: 'SSTable block size' },
        { key: 'block_restart_interval', label: 'Block Restart Interval', value: config.block_restart_interval, description: 'Keys between restarts' },
        { key: 'thread_sleep_ms', label: 'Thread Sleep', value: config.thread_sleep_ms + ' ms', description: 'Background thread sleep interval' }
    ];

    var html = '<table class="config-table" role="table" aria-label="Server Configuration">';
    html += '<thead><tr><th scope="col">Setting</th><th scope="col">Value</th><th scope="col">Description</th></tr></thead>';
    html += '<tbody>';

    for (var i = 0; i < configItems.length; i++) {
        var item = configItems[i];
        html += '<tr data-config-key="' + item.key + '">';
        html += '<td class="config-key">' + escapeHtml(item.label) + '</td>';
        html += '<td class="config-value">' + escapeHtml(String(item.value)) + '</td>';
        html += '<td class="config-description">' + escapeHtml(item.description) + '</td>';
        html += '</tr>';
    }

    html += '</tbody></table>';

    configPanel.innerHTML = html;
}

/**
 * Format bytes to human readable string
 * @param {number} bytes - Number of bytes
 * @returns {string} Formatted string (e.g., "4 MB")
 */
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';

    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = 0;
    var value = bytes;

    while (value >= 1024 && i < units.length - 1) {
        value = value / 1024;
        i++;
    }

    // Round to 2 decimal places if needed
    if (value === Math.floor(value)) {
        return value + ' ' + units[i];
    }
    return value.toFixed(2) + ' ' + units[i];
}

/**
 * Escape HTML special characters
 * @param {string} str - String to escape
 * @returns {string} Escaped string
 */
function escapeHtml(str) {
    var div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initApp: initApp,
        initNavigation: initNavigation,
        initSmoothScroll: initSmoothScroll,
        initConfigDisplay: initConfigDisplay,
        loadConfig: loadConfig,
        renderConfig: renderConfig,
        formatBytes: formatBytes
    };
}
