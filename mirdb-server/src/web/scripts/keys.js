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

    /**
     * Initialize key browser
     */
    function initKeyBrowser() {
        // Placeholder - to be implemented by Scenario 4
    }

    /**
     * Load keys with pagination
     * @param {number} page - Page number
     */
    function loadKeys(page) {
        // Placeholder - to be implemented by Scenario 4
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
        // Placeholder - to be implemented by Scenario 4
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
