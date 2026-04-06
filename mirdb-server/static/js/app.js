/**
 * MirDB Dashboard JavaScript Application
 * Owner: Scenario 1 creates base, multiple scenarios add functionality
 *
 * Modules (inline or ES modules):
 * - Stats: Fetch and render /api/stats data (Scenario 2)
 * - KeyBrowser: Pagination, table rendering (Scenario 3)
 * - Search: Filter keys by prefix (Scenario 4)
 * - KeyDetail: Fetch and display single key (Scenario 5)
 * - Compaction: Poll and display compaction status (Scenario 6)
 *
 * Expected functions:
 * - fetchStats() -> Updates stats cards
 * - fetchKeys(offset, limit, search) -> Updates key table
 * - fetchKeyDetail(key) -> Shows detail modal
 * - fetchCompactionStatus() -> Updates status indicator
 *
 * Auto-refresh:
 * - Stats refresh every 5 seconds (NFR-4)
 * - Compaction status refresh during active compaction
 */

(function() {
    'use strict';

    // Configuration
    const CONFIG = {
        statsRefreshInterval: 5000, // 5 seconds
        compactionRefreshInterval: 2000, // 2 seconds when active
        apiBasePath: '/api',
        defaultPageLimit: 20
    };

    // State
    const state = {
        currentPage: 1,
        totalPages: 1,
        searchQuery: '',
        isCompactionActive: false
    };

    // DOM Elements
    const elements = {
        // Stats elements
        statTotalKeys: document.getElementById('stat-total-keys'),
        statMemory: document.getElementById('stat-memory'),
        statStorage: document.getElementById('stat-storage'),
        statVersion: document.getElementById('stat-version'),

        // Keys elements
        keysTableBody: document.getElementById('keys-table-body'),
        searchInput: document.getElementById('search-input'),
        searchClearBtn: document.getElementById('search-clear-btn'), // Scenario 4
        paginationPrev: document.querySelector('.pagination__btn--prev'),
        paginationNext: document.querySelector('.pagination__btn--next'),
        paginationInfo: document.querySelector('.pagination__info'),

        // Compaction elements (Scenario 6: Compaction Status Display)
        compactionIndicator: document.querySelector('.compaction-status__indicator'),
        compactionProgressBar: document.querySelector('.compaction-progress__bar'),
        compactionProgress: document.querySelector('.compaction-progress'),
        compactionType: document.getElementById('compaction-type'),
        compactionLastRun: document.getElementById('compaction-last-run'),

        // Modal elements
        modal: document.getElementById('key-detail-modal'),
        modalBody: document.getElementById('modal-body'),
        modalClose: document.querySelector('.modal__close')
    };

    // API Functions
    async function apiRequest(endpoint) {
        try {
            const response = await fetch(`${CONFIG.apiBasePath}${endpoint}`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error(`API request failed: ${endpoint}`, error);
            return null;
        }
    }

    // Stats Module (Scenario 2 will fully implement)
    async function fetchStats() {
        const data = await apiRequest('/stats');
        if (data) {
            updateStatsDisplay(data);
        }
    }

    function updateStatsDisplay(data) {
        if (elements.statTotalKeys) {
            elements.statTotalKeys.textContent = formatNumber(data.total_keys || 0);
        }
        if (elements.statMemory) {
            elements.statMemory.textContent = formatBytes(data.memory_usage || 0);
        }
        if (elements.statStorage) {
            elements.statStorage.textContent = formatBytes(data.storage_size || 0);
        }
        if (elements.statVersion) {
            elements.statVersion.textContent = data.version || '0.1.0';
        }
    }

    // Keys Module (Scenario 3 will fully implement)
    async function fetchKeys(offset = 0, limit = CONFIG.defaultPageLimit, search = '') {
        let endpoint = `/keys?offset=${offset}&limit=${limit}`;
        if (search) {
            endpoint += `&search=${encodeURIComponent(search)}`;
        }
        const data = await apiRequest(endpoint);
        if (data) {
            updateKeysDisplay(data);
        }
    }

    function updateKeysDisplay(data) {
        const tbody = elements.keysTableBody;
        if (!tbody) return;

        if (!data.keys || data.keys.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3" class="keys-table__empty">No keys found</td></tr>';
        } else {
            tbody.innerHTML = data.keys.map(key => `
                <tr>
                    <td><a href="#" class="key-link" data-key="${escapeHtml(key)}">${escapeHtml(key)}</a></td>
                    <td>-</td>
                    <td><button class="view-key-btn" data-key="${escapeHtml(key)}">View</button></td>
                </tr>
            `).join('');
        }

        // Update pagination
        state.totalPages = Math.max(1, Math.ceil((data.total || 0) / CONFIG.defaultPageLimit));
        state.currentPage = Math.floor((data.offset || 0) / CONFIG.defaultPageLimit) + 1;
        updatePagination();
    }

    function updatePagination() {
        if (elements.paginationInfo) {
            elements.paginationInfo.textContent = `Page ${state.currentPage} of ${state.totalPages}`;
        }
        if (elements.paginationPrev) {
            elements.paginationPrev.disabled = state.currentPage <= 1;
        }
        if (elements.paginationNext) {
            elements.paginationNext.disabled = state.currentPage >= state.totalPages;
        }
    }

    // Key Detail Module (Scenario 5 will fully implement)
    async function fetchKeyDetail(key) {
        const data = await apiRequest(`/keys/${encodeURIComponent(key)}`);
        if (data) {
            showKeyDetail(data);
        } else {
            showModal('Key not found', `<p>The key "${escapeHtml(key)}" was not found.</p>`);
        }
    }

    function showKeyDetail(data) {
        const content = `
            <dl>
                <dt>Key</dt>
                <dd>${escapeHtml(data.key)}</dd>
                <dt>Value</dt>
                <dd><pre>${escapeHtml(data.value)}</pre></dd>
                <dt>Size</dt>
                <dd>${formatBytes(data.size)}</dd>
                <dt>Flags</dt>
                <dd>${data.flags}</dd>
            </dl>
        `;
        showModal('Key Details', content);
    }

    function showModal(title, content) {
        if (elements.modalBody) {
            elements.modalBody.innerHTML = content;
        }
        if (elements.modal) {
            elements.modal.setAttribute('aria-hidden', 'false');
        }
    }

    function hideModal() {
        if (elements.modal) {
            elements.modal.setAttribute('aria-hidden', 'true');
        }
    }

    // =========================================================================
    // Scenario 6: Compaction Status Display
    // =========================================================================

    /**
     * Fetch compaction status from the API (Scenario 6)
     * Called periodically to update the compaction section
     */
    async function fetchCompactionStatus() {
        const data = await apiRequest('/compaction');
        if (data) {
            updateCompactionDisplay(data);
        }
    }

    /**
     * Update the compaction display with current status (Scenario 6)
     * Shows running/idle state, compaction type, progress, and last run time
     * @param {Object} data - Compaction status from API
     * @param {string} data.status - "idle" or "running"
     * @param {string} [data.compaction_type] - "minor" or "major" when running
     * @param {number} [data.progress] - Progress percentage (0-100) when running
     * @param {number} [data.last_compaction] - Unix timestamp of last completed compaction
     */
    function updateCompactionDisplay(data) {
        const isRunning = data.status === 'running';
        state.isCompactionActive = isRunning;

        // Update status indicator
        if (elements.compactionIndicator) {
            if (isRunning) {
                const typeLabel = data.compaction_type ? ` (${capitalizeFirst(data.compaction_type)})` : '';
                elements.compactionIndicator.textContent = `Running${typeLabel}`;
            } else {
                elements.compactionIndicator.textContent = 'Idle';
            }
            elements.compactionIndicator.className = `compaction-status__indicator compaction-status__indicator--${data.status}`;
        }

        // Update compaction type display
        if (elements.compactionType) {
            if (isRunning && data.compaction_type) {
                elements.compactionType.textContent = capitalizeFirst(data.compaction_type) + ' Compaction';
                elements.compactionType.style.display = 'inline';
            } else {
                elements.compactionType.style.display = 'none';
            }
        }

        // Update progress bar
        const progress = isRunning && data.progress !== undefined ? data.progress : 0;
        if (elements.compactionProgressBar) {
            elements.compactionProgressBar.style.width = `${progress}%`;
        }
        if (elements.compactionProgress) {
            elements.compactionProgress.setAttribute('aria-valuenow', progress);
            // Show progress bar only when running
            elements.compactionProgress.style.display = isRunning ? 'block' : 'none';
        }

        // Update last compaction timestamp
        if (elements.compactionLastRun) {
            if (data.last_compaction) {
                const lastRunDate = new Date(data.last_compaction * 1000);
                elements.compactionLastRun.textContent = 'Last compaction: ' + formatRelativeTime(lastRunDate);
                elements.compactionLastRun.setAttribute('title', lastRunDate.toLocaleString());
                elements.compactionLastRun.style.display = 'block';
            } else {
                elements.compactionLastRun.textContent = 'No compactions yet';
                elements.compactionLastRun.style.display = 'block';
            }
        }
    }

    /**
     * Capitalize the first letter of a string (Scenario 6)
     * @param {string} str - String to capitalize
     * @returns {string} Capitalized string
     */
    function capitalizeFirst(str) {
        if (!str) return '';
        return str.charAt(0).toUpperCase() + str.slice(1);
    }

    /**
     * Format a date as relative time (e.g., "2 minutes ago") (Scenario 6)
     * @param {Date} date - Date to format
     * @returns {string} Relative time string
     */
    function formatRelativeTime(date) {
        const now = new Date();
        const diffMs = now - date;
        const diffSec = Math.floor(diffMs / 1000);
        const diffMin = Math.floor(diffSec / 60);
        const diffHour = Math.floor(diffMin / 60);
        const diffDay = Math.floor(diffHour / 24);

        if (diffSec < 60) {
            return 'just now';
        } else if (diffMin < 60) {
            return `${diffMin} minute${diffMin === 1 ? '' : 's'} ago`;
        } else if (diffHour < 24) {
            return `${diffHour} hour${diffHour === 1 ? '' : 's'} ago`;
        } else if (diffDay < 7) {
            return `${diffDay} day${diffDay === 1 ? '' : 's'} ago`;
        } else {
            return date.toLocaleDateString();
        }
    }

    // Utility Functions
    function formatNumber(num) {
        if (num === undefined || num === null) return '-';
        return num.toLocaleString();
    }

    function formatBytes(bytes) {
        if (bytes === undefined || bytes === null || bytes === 0) return '-';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let unitIndex = 0;
        let value = bytes;
        while (value >= 1024 && unitIndex < units.length - 1) {
            value /= 1024;
            unitIndex++;
        }
        return `${value.toFixed(1)} ${units[unitIndex]}`;
    }

    function escapeHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // =========================================================================
    // Scenario 4: Key Search and Filter Functions
    // =========================================================================

    /**
     * Clear the search input and reset to showing all keys
     */
    function clearSearch() {
        if (elements.searchInput) {
            elements.searchInput.value = '';
        }
        state.searchQuery = '';
        state.currentPage = 1;
        updateClearButtonVisibility('');
        fetchKeys(0, CONFIG.defaultPageLimit, '');
    }

    /**
     * Update the visibility of the clear button based on search input value
     * @param {string} value - Current search input value
     */
    function updateClearButtonVisibility(value) {
        if (elements.searchClearBtn) {
            elements.searchClearBtn.style.display = value.length > 0 ? 'flex' : 'none';
        }
    }

    // Event Handlers
    function setupEventListeners() {
        // Modal close
        if (elements.modalClose) {
            elements.modalClose.addEventListener('click', hideModal);
        }

        // Close modal on backdrop click
        if (elements.modal) {
            elements.modal.addEventListener('click', function(e) {
                if (e.target === elements.modal) {
                    hideModal();
                }
            });
        }

        // Close modal on Escape key
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                hideModal();
            }
        });

        // Search input - Scenario 4: Key Search and Filter
        if (elements.searchInput) {
            let searchTimeout;
            elements.searchInput.addEventListener('input', function(e) {
                clearTimeout(searchTimeout);
                const searchValue = e.target.value;
                // Show/hide clear button based on input
                updateClearButtonVisibility(searchValue);
                searchTimeout = setTimeout(() => {
                    state.searchQuery = searchValue;
                    state.currentPage = 1;
                    fetchKeys(0, CONFIG.defaultPageLimit, state.searchQuery);
                }, 300);
            });
        }

        // Search clear button - Scenario 4: Key Search and Filter
        if (elements.searchClearBtn) {
            elements.searchClearBtn.addEventListener('click', function() {
                clearSearch();
            });
        }

        // Pagination buttons
        if (elements.paginationPrev) {
            elements.paginationPrev.addEventListener('click', function() {
                if (state.currentPage > 1) {
                    state.currentPage--;
                    const offset = (state.currentPage - 1) * CONFIG.defaultPageLimit;
                    fetchKeys(offset, CONFIG.defaultPageLimit, state.searchQuery);
                }
            });
        }

        if (elements.paginationNext) {
            elements.paginationNext.addEventListener('click', function() {
                if (state.currentPage < state.totalPages) {
                    state.currentPage++;
                    const offset = (state.currentPage - 1) * CONFIG.defaultPageLimit;
                    fetchKeys(offset, CONFIG.defaultPageLimit, state.searchQuery);
                }
            });
        }

        // Key detail clicks (delegated)
        document.addEventListener('click', function(e) {
            if (e.target.classList.contains('key-link') || e.target.classList.contains('view-key-btn')) {
                e.preventDefault();
                const key = e.target.getAttribute('data-key');
                if (key) {
                    fetchKeyDetail(key);
                }
            }
        });

        // Navigation links smooth scroll
        document.querySelectorAll('.nav__link[href^="#"]').forEach(link => {
            link.addEventListener('click', function(e) {
                const targetId = this.getAttribute('href').slice(1);
                const target = document.getElementById(targetId);
                if (target) {
                    e.preventDefault();
                    target.scrollIntoView({ behavior: 'smooth' });
                    // Update active nav link
                    document.querySelectorAll('.nav__link').forEach(l => l.classList.remove('nav__link--active'));
                    this.classList.add('nav__link--active');
                }
            });
        });
    }

    // Auto-refresh
    function startAutoRefresh() {
        // Stats refresh every 5 seconds
        setInterval(fetchStats, CONFIG.statsRefreshInterval);

        // Compaction status - refresh more frequently when active
        setInterval(() => {
            fetchCompactionStatus();
        }, CONFIG.compactionRefreshInterval);
    }

    // Initialize
    function init() {
        console.log('MirDB Dashboard initializing...');

        setupEventListeners();

        // Initial data fetch
        fetchStats();
        fetchKeys();
        fetchCompactionStatus();

        // Start auto-refresh
        startAutoRefresh();

        console.log('MirDB Dashboard initialized');
    }

    // Run when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
