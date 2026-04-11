/**
 * Key Browser Logic
 * Owner: Scenario 4 - Key Browser View Keys
 * Co-owners: Scenarios 5, 6, 7 (Set, Get, Delete operations)
 *
 * Expected exports:
 * - initKeyBrowser() - Initialize key list table
 * - loadKeys(page) - Fetch and render key list
 * - showKeyValue(key) - Display value in modal
 * - showSetKeyForm() - Open set key form modal
 * - deleteKey(key) - Delete with confirmation
 * - filterKeys(query) - Search/filter keys
 */

var MirDBKeys = (function() {
    'use strict';

    // State variables
    var currentPage = 1;
    var pageSize = 10;
    var totalKeys = 0;
    var allKeysCache = [];
    var currentFilter = '';

    /**
     * Initialize key browser
     */
    function initKeyBrowser() {
        var keyBrowser = document.getElementById('key-browser');
        if (!keyBrowser) return;

        // Build the key browser UI
        keyBrowser.innerHTML = '';

        // Create toolbar with search and add button
        var toolbar = document.createElement('div');
        toolbar.className = 'key-browser-toolbar';

        // Search input
        var searchWrapper = document.createElement('div');
        searchWrapper.className = 'key-search-wrapper';
        var searchInput = document.createElement('input');
        searchInput.type = 'text';
        searchInput.id = 'key-search-input';
        searchInput.className = 'key-search-input';
        searchInput.placeholder = 'Search keys...';
        searchInput.setAttribute('aria-label', 'Search keys');
        searchInput.addEventListener('input', function(e) {
            filterKeys(e.target.value);
        });
        searchWrapper.appendChild(searchInput);
        toolbar.appendChild(searchWrapper);

        // Add key button (placeholder for Scenario 5)
        var addBtn = document.createElement('button');
        addBtn.className = 'btn btn-primary add-key-btn';
        addBtn.textContent = '+ Add Key';
        addBtn.setAttribute('aria-label', 'Add new key');
        addBtn.onclick = function() {
            showSetKeyForm();
        };
        toolbar.appendChild(addBtn);

        keyBrowser.appendChild(toolbar);

        // Create table container
        var tableContainer = document.createElement('div');
        tableContainer.className = 'key-table-container';
        tableContainer.id = 'key-table-container';
        keyBrowser.appendChild(tableContainer);

        // Create pagination container
        var paginationContainer = document.createElement('div');
        paginationContainer.className = 'key-pagination';
        paginationContainer.id = 'key-pagination';
        keyBrowser.appendChild(paginationContainer);

        // Load initial keys
        loadKeys(1);
    }

    /**
     * Load keys with pagination
     * @param {number} page - Page number
     */
    function loadKeys(page) {
        currentPage = page || 1;
        var tableContainer = document.getElementById('key-table-container');
        if (!tableContainer) return;

        // Show loading state
        tableContainer.innerHTML = '<div class="loading-state">Loading keys...</div>';

        MirDBApi.fetchKeys(currentPage, pageSize)
            .then(function(response) {
                allKeysCache = response.keys || [];
                totalKeys = response.total || 0;
                currentFilter = '';

                // Clear search input
                var searchInput = document.getElementById('key-search-input');
                if (searchInput) {
                    searchInput.value = '';
                }

                renderKeyTable(allKeysCache);
                renderPagination(totalKeys, currentPage, pageSize);
            })
            .catch(function(error) {
                tableContainer.innerHTML = '<div class="error-state">Failed to load keys: ' + error.message + '</div>';
            });
    }

    /**
     * Render the key table
     * @param {Array} keys - Array of key objects
     */
    function renderKeyTable(keys) {
        var tableContainer = document.getElementById('key-table-container');
        if (!tableContainer) return;

        if (!keys || keys.length === 0) {
            tableContainer.innerHTML = '<div class="empty-state">No keys found</div>';
            return;
        }

        var table = document.createElement('table');
        table.className = 'key-table';
        table.setAttribute('role', 'grid');

        // Table header
        var thead = document.createElement('thead');
        var headerRow = document.createElement('tr');

        var headers = ['Key', 'Size', 'TTL', 'Actions'];
        headers.forEach(function(headerText) {
            var th = document.createElement('th');
            th.textContent = headerText;
            th.setAttribute('scope', 'col');
            headerRow.appendChild(th);
        });

        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Table body
        var tbody = document.createElement('tbody');
        keys.forEach(function(keyInfo) {
            var row = document.createElement('tr');
            row.className = 'key-row';
            row.setAttribute('data-key', keyInfo.key);

            // Key name cell
            var keyCell = document.createElement('td');
            keyCell.className = 'key-name-cell';
            keyCell.textContent = keyInfo.key;
            row.appendChild(keyCell);

            // Size cell
            var sizeCell = document.createElement('td');
            sizeCell.className = 'key-size-cell';
            sizeCell.textContent = formatBytes(keyInfo.size);
            row.appendChild(sizeCell);

            // TTL cell
            var ttlCell = document.createElement('td');
            ttlCell.className = 'key-ttl-cell';
            ttlCell.textContent = keyInfo.ttl === 0 ? 'No expiration' : keyInfo.ttl + 's';
            row.appendChild(ttlCell);

            // Actions cell
            var actionsCell = document.createElement('td');
            actionsCell.className = 'key-actions-cell';

            var viewBtn = document.createElement('button');
            viewBtn.className = 'btn btn-secondary btn-sm view-key-btn';
            viewBtn.textContent = 'View';
            viewBtn.setAttribute('aria-label', 'View key ' + keyInfo.key);
            viewBtn.onclick = function(e) {
                e.stopPropagation();
                showKeyValue(keyInfo.key);
            };
            actionsCell.appendChild(viewBtn);

            var deleteBtn = document.createElement('button');
            deleteBtn.className = 'btn btn-danger btn-sm delete-key-btn';
            deleteBtn.textContent = 'Delete';
            deleteBtn.setAttribute('aria-label', 'Delete key ' + keyInfo.key);
            deleteBtn.onclick = function(e) {
                e.stopPropagation();
                deleteKey(keyInfo.key);
            };
            actionsCell.appendChild(deleteBtn);

            row.appendChild(actionsCell);

            // Click row to view
            row.onclick = function() {
                showKeyValue(keyInfo.key);
            };
            row.style.cursor = 'pointer';

            tbody.appendChild(row);
        });

        table.appendChild(tbody);
        tableContainer.innerHTML = '';
        tableContainer.appendChild(table);
    }

    /**
     * Render pagination controls
     * @param {number} total - Total number of keys
     * @param {number} page - Current page
     * @param {number} limit - Items per page
     */
    function renderPagination(total, page, limit) {
        var paginationContainer = document.getElementById('key-pagination');
        if (!paginationContainer) return;

        paginationContainer.innerHTML = '';

        var totalPages = Math.ceil(total / limit);
        if (totalPages <= 1) return;

        // Previous button
        var prevBtn = document.createElement('button');
        prevBtn.className = 'btn btn-secondary pagination-btn';
        prevBtn.textContent = '< Prev';
        prevBtn.disabled = page <= 1;
        prevBtn.onclick = function() {
            if (page > 1) loadKeys(page - 1);
        };
        paginationContainer.appendChild(prevBtn);

        // Page info
        var pageInfo = document.createElement('span');
        pageInfo.className = 'pagination-info';
        pageInfo.textContent = 'Page ' + page + ' of ' + totalPages + ' (' + total + ' keys)';
        paginationContainer.appendChild(pageInfo);

        // Next button
        var nextBtn = document.createElement('button');
        nextBtn.className = 'btn btn-secondary pagination-btn';
        nextBtn.textContent = 'Next >';
        nextBtn.disabled = page >= totalPages;
        nextBtn.onclick = function() {
            if (page < totalPages) loadKeys(page + 1);
        };
        paginationContainer.appendChild(nextBtn);
    }

    /**
     * Show key value in modal
     * @param {string} key - Key name
     */
    function showKeyValue(key) {
        // Fetch key value from API
        MirDBApi.fetchKey(key)
            .then(function(response) {
                // Build modal content
                var modalContent = buildKeyValueModal(key, response);
                MirDBUI.showModal(modalContent);
            })
            .catch(function(error) {
                MirDBUI.showToast('Failed to fetch key: ' + error.message, 'error');
            });
    }

    /**
     * Build key value modal content
     * @param {string} key - Key name
     * @param {Object} data - Key data from API
     * @returns {HTMLElement} Modal content element
     */
    function buildKeyValueModal(key, data) {
        var container = document.createElement('div');
        container.className = 'key-value-modal';

        // Modal title
        var title = document.createElement('h2');
        title.className = 'modal-title';
        title.textContent = 'Key Details';
        container.appendChild(title);

        // Key name section
        var keySection = document.createElement('div');
        keySection.className = 'key-value-section';
        var keyLabel = document.createElement('label');
        keyLabel.textContent = 'Key';
        keyLabel.className = 'key-value-label';
        var keyValue = document.createElement('div');
        keyValue.className = 'key-value-display key-name';
        keyValue.textContent = data.key || key;
        keySection.appendChild(keyLabel);
        keySection.appendChild(keyValue);
        container.appendChild(keySection);

        // Value section with copy button
        var valueSection = document.createElement('div');
        valueSection.className = 'key-value-section';
        var valueLabel = document.createElement('label');
        valueLabel.textContent = 'Value';
        valueLabel.className = 'key-value-label';
        var valueWrapper = document.createElement('div');
        valueWrapper.className = 'key-value-wrapper';
        var valueDisplay = document.createElement('pre');
        valueDisplay.className = 'key-value-display value-content';
        valueDisplay.id = 'key-value-content';
        valueDisplay.textContent = data.value || '';
        var copyBtn = document.createElement('button');
        copyBtn.className = 'btn btn-secondary copy-value-btn';
        copyBtn.setAttribute('aria-label', 'Copy value to clipboard');
        copyBtn.innerHTML = '<span class="copy-icon">&#128203;</span> <span class="copy-text">Copy</span>';
        copyBtn.onclick = function() {
            copyKeyValue(data.value || '', copyBtn);
        };
        valueWrapper.appendChild(valueDisplay);
        valueWrapper.appendChild(copyBtn);
        valueSection.appendChild(valueLabel);
        valueSection.appendChild(valueWrapper);
        container.appendChild(valueSection);

        // Metadata section
        var metaSection = document.createElement('div');
        metaSection.className = 'key-value-section metadata-section';
        var metaLabel = document.createElement('label');
        metaLabel.textContent = 'Metadata';
        metaLabel.className = 'key-value-label';
        metaSection.appendChild(metaLabel);

        var metaGrid = document.createElement('div');
        metaGrid.className = 'metadata-grid';

        // Flags
        var flagsItem = createMetaItem('Flags', data.flags !== undefined ? data.flags : 0);
        metaGrid.appendChild(flagsItem);

        // TTL
        var ttlValue = data.ttl === 0 ? 'No expiration' : data.ttl + 's';
        var ttlItem = createMetaItem('TTL', ttlValue);
        metaGrid.appendChild(ttlItem);

        // Size
        var sizeValue = formatBytes(data.size || 0);
        var sizeItem = createMetaItem('Size', sizeValue);
        metaGrid.appendChild(sizeItem);

        metaSection.appendChild(metaGrid);
        container.appendChild(metaSection);

        // Close button
        var buttonSection = document.createElement('div');
        buttonSection.className = 'modal-buttons';
        var closeBtn = document.createElement('button');
        closeBtn.className = 'btn btn-primary';
        closeBtn.textContent = 'Close';
        closeBtn.onclick = function() {
            MirDBUI.hideModal();
        };
        buttonSection.appendChild(closeBtn);
        container.appendChild(buttonSection);

        return container;
    }

    /**
     * Create a metadata display item
     * @param {string} label - Label text
     * @param {string|number} value - Value to display
     * @returns {HTMLElement} Metadata item element
     */
    function createMetaItem(label, value) {
        var item = document.createElement('div');
        item.className = 'metadata-item';
        var labelEl = document.createElement('span');
        labelEl.className = 'meta-label';
        labelEl.textContent = label + ':';
        var valueEl = document.createElement('span');
        valueEl.className = 'meta-value';
        valueEl.textContent = value;
        item.appendChild(labelEl);
        item.appendChild(valueEl);
        return item;
    }

    /**
     * Format bytes to human readable string
     * @param {number} bytes - Byte count
     * @returns {string} Formatted string
     */
    function formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        var k = 1024;
        var sizes = ['B', 'KB', 'MB', 'GB'];
        var i = Math.floor(Math.log(bytes) / Math.log(k));
        i = Math.min(i, sizes.length - 1);
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    /**
     * Copy key value to clipboard
     * @param {string} value - Value to copy
     * @param {HTMLElement} button - Copy button element
     */
    function copyKeyValue(value, button) {
        MirDBUI.copyToClipboard(value)
            .then(function(success) {
                if (success) {
                    var copyText = button.querySelector('.copy-text');
                    var originalText = copyText ? copyText.textContent : 'Copy';
                    button.classList.add('copied');
                    if (copyText) {
                        copyText.textContent = 'Copied!';
                    }
                    MirDBUI.showToast('Value copied to clipboard', 'success');
                    setTimeout(function() {
                        button.classList.remove('copied');
                        if (copyText) {
                            copyText.textContent = originalText;
                        }
                    }, 2000);
                } else {
                    MirDBUI.showToast('Failed to copy value', 'error');
                }
            })
            .catch(function() {
                MirDBUI.showToast('Failed to copy value', 'error');
            });
    }

    /**
     * Show set key form
     */
    function showSetKeyForm() {
        // Placeholder - to be implemented by Scenario 5
    }

    /**
     * Delete key with confirmation
     * @param {string} key - Key name
     */
    function deleteKey(key) {
        // Placeholder - to be implemented by Scenario 7
    }

    /**
     * Filter keys by query
     * @param {string} query - Search query
     */
    function filterKeys(query) {
        currentFilter = (query || '').toLowerCase().trim();

        if (!currentFilter) {
            // Show all cached keys
            renderKeyTable(allKeysCache);
            return;
        }

        // Filter keys client-side
        var filteredKeys = allKeysCache.filter(function(keyInfo) {
            return keyInfo.key.toLowerCase().indexOf(currentFilter) !== -1;
        });

        renderKeyTable(filteredKeys);

        // Update pagination info to show filtered count
        var paginationContainer = document.getElementById('key-pagination');
        if (paginationContainer && currentFilter) {
            paginationContainer.innerHTML = '<span class="pagination-info">Showing ' + filteredKeys.length + ' of ' + allKeysCache.length + ' keys (filtered)</span>';
        }
    }

    return {
        initKeyBrowser: initKeyBrowser,
        loadKeys: loadKeys,
        showKeyValue: showKeyValue,
        showSetKeyForm: showSetKeyForm,
        deleteKey: deleteKey,
        filterKeys: filterKeys
    };
})();
