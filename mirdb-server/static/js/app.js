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
        paginationPrev: document.querySelector('.pagination__btn--prev'),
        paginationNext: document.querySelector('.pagination__btn--next'),
        paginationInfo: document.querySelector('.pagination__info'),

        // Compaction elements
        compactionIndicator: document.querySelector('.compaction-status__indicator'),
        compactionProgressBar: document.querySelector('.compaction-progress__bar'),
        compactionProgress: document.querySelector('.compaction-progress'),

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

    // Compaction Module (Scenario 6 will fully implement)
    async function fetchCompactionStatus() {
        const data = await apiRequest('/compaction');
        if (data) {
            updateCompactionDisplay(data);
        }
    }

    function updateCompactionDisplay(data) {
        const isRunning = data.status === 'running';
        state.isCompactionActive = isRunning;

        if (elements.compactionIndicator) {
            elements.compactionIndicator.textContent = data.status === 'running' ? 'Running' : 'Idle';
            elements.compactionIndicator.className = `compaction-status__indicator compaction-status__indicator--${data.status}`;
        }

        if (elements.compactionProgressBar) {
            elements.compactionProgressBar.style.width = `${data.progress || 0}%`;
        }

        if (elements.compactionProgress) {
            elements.compactionProgress.setAttribute('aria-valuenow', data.progress || 0);
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

        // Search input (Scenario 4 will enhance)
        if (elements.searchInput) {
            let searchTimeout;
            elements.searchInput.addEventListener('input', function(e) {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    state.searchQuery = e.target.value;
                    state.currentPage = 1;
                    fetchKeys(0, CONFIG.defaultPageLimit, state.searchQuery);
                }, 300);
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
