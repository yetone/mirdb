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
     * Show set key form modal
     * Owner: Scenario 5 - Key Operations - Set Key
     */
    function showSetKeyForm() {
        var modalContent = buildSetKeyForm();
        MirDBUI.showModal(modalContent);
    }

    /**
     * Build the set key form modal content
     * @returns {HTMLElement} Form element for setting a key
     */
    function buildSetKeyForm() {
        var container = document.createElement('div');
        container.className = 'set-key-form';

        // Modal title
        var title = document.createElement('h2');
        title.className = 'modal-title';
        title.textContent = 'Set Key';
        container.appendChild(title);

        // Create form element
        var form = document.createElement('form');
        form.id = 'set-key-form';
        form.setAttribute('aria-label', 'Set key form');

        // Key input
        var keyGroup = createFormGroup('key', 'Key', 'text', 'Enter key name', true);
        form.appendChild(keyGroup);

        // Value input (textarea for multiline values)
        var valueGroup = document.createElement('div');
        valueGroup.className = 'form-group';
        var valueLabel = document.createElement('label');
        valueLabel.setAttribute('for', 'set-key-value');
        valueLabel.textContent = 'Value';
        valueLabel.className = 'form-label';
        var valueInput = document.createElement('textarea');
        valueInput.id = 'set-key-value';
        valueInput.name = 'value';
        valueInput.className = 'form-input form-textarea';
        valueInput.placeholder = 'Enter value';
        valueInput.rows = 4;
        valueInput.setAttribute('aria-label', 'Value');
        valueGroup.appendChild(valueLabel);
        valueGroup.appendChild(valueInput);
        form.appendChild(valueGroup);

        // Flags input
        var flagsGroup = createFormGroup('flags', 'Flags', 'number', '0', false);
        var flagsInput = flagsGroup.querySelector('input');
        flagsInput.value = '0';
        flagsInput.min = '0';
        form.appendChild(flagsGroup);

        // TTL input
        var ttlGroup = createFormGroup('ttl', 'TTL (seconds)', 'number', '0 (no expiration)', false);
        var ttlInput = ttlGroup.querySelector('input');
        ttlInput.value = '0';
        ttlInput.min = '0';
        var ttlHint = document.createElement('span');
        ttlHint.className = 'form-hint';
        ttlHint.textContent = '0 = no expiration';
        ttlGroup.appendChild(ttlHint);
        form.appendChild(ttlGroup);

        // Button section
        var buttonSection = document.createElement('div');
        buttonSection.className = 'modal-buttons';

        var cancelBtn = document.createElement('button');
        cancelBtn.type = 'button';
        cancelBtn.className = 'btn btn-secondary';
        cancelBtn.textContent = 'Cancel';
        cancelBtn.onclick = function() {
            MirDBUI.hideModal();
        };

        var submitBtn = document.createElement('button');
        submitBtn.type = 'submit';
        submitBtn.id = 'set-key-submit';
        submitBtn.className = 'btn btn-primary';
        submitBtn.textContent = 'Set Key';

        buttonSection.appendChild(cancelBtn);
        buttonSection.appendChild(submitBtn);
        form.appendChild(buttonSection);

        // Handle form submission
        form.onsubmit = function(e) {
            e.preventDefault();
            handleSetKeySubmit(form);
        };

        container.appendChild(form);
        return container;
    }

    /**
     * Create a form group with label and input
     * @param {string} name - Input name
     * @param {string} label - Label text
     * @param {string} type - Input type
     * @param {string} placeholder - Placeholder text
     * @param {boolean} required - Whether the field is required
     * @returns {HTMLElement} Form group element
     */
    function createFormGroup(name, label, type, placeholder, required) {
        var group = document.createElement('div');
        group.className = 'form-group';

        var labelEl = document.createElement('label');
        labelEl.setAttribute('for', 'set-key-' + name);
        labelEl.textContent = label;
        labelEl.className = 'form-label';
        if (required) {
            var requiredSpan = document.createElement('span');
            requiredSpan.className = 'required';
            requiredSpan.textContent = ' *';
            requiredSpan.setAttribute('aria-label', 'required');
            labelEl.appendChild(requiredSpan);
        }

        var input = document.createElement('input');
        input.type = type;
        input.id = 'set-key-' + name;
        input.name = name;
        input.className = 'form-input';
        input.placeholder = placeholder;
        input.setAttribute('aria-label', label);
        if (required) {
            input.required = true;
        }

        group.appendChild(labelEl);
        group.appendChild(input);
        return group;
    }

    /**
     * Handle set key form submission
     * @param {HTMLFormElement} form - The form element
     */
    function handleSetKeySubmit(form) {
        var keyInput = form.querySelector('#set-key-key');
        var valueInput = form.querySelector('#set-key-value');
        var flagsInput = form.querySelector('#set-key-flags');
        var ttlInput = form.querySelector('#set-key-ttl');
        var submitBtn = form.querySelector('#set-key-submit');

        var key = keyInput.value.trim();
        var value = valueInput.value;
        var flags = parseInt(flagsInput.value, 10) || 0;
        var ttl = parseInt(ttlInput.value, 10) || 0;

        // Validate key
        if (!key) {
            MirDBUI.showToast('Key name is required', 'error');
            keyInput.focus();
            return;
        }

        // Disable submit button and show loading state
        submitBtn.disabled = true;
        submitBtn.textContent = 'Setting...';

        // Call API to set key
        MirDBApi.setKey(key, value, flags, ttl)
            .then(function(response) {
                MirDBUI.showToast('Key "' + key + '" stored successfully', 'success');
                MirDBUI.hideModal();
                // Refresh key list if available
                if (typeof loadKeys === 'function') {
                    loadKeys(1);
                }
            })
            .catch(function(error) {
                MirDBUI.showToast('Failed to set key: ' + error.message, 'error');
                submitBtn.disabled = false;
                submitBtn.textContent = 'Set Key';
            });
    }

    /**
     * Delete key with confirmation
     * @param {string} key - Key name
     */
    function deleteKey(key) {
        // Show confirmation dialog before deleting
        MirDBUI.showConfirm(
            'Are you sure you want to delete the key "' + key + '"? This action cannot be undone.',
            function() {
                // User confirmed - proceed with deletion
                performDeleteKey(key);
            },
            function() {
                // User cancelled - do nothing
                MirDBUI.showToast('Delete cancelled', 'info');
            }
        );
    }

    /**
     * Perform the actual key deletion after confirmation
     * @param {string} key - Key name to delete
     */
    function performDeleteKey(key) {
        MirDBApi.deleteKey(key)
            .then(function(response) {
                MirDBUI.showToast('Key "' + key + '" deleted successfully', 'success');
                // Refresh the key list to reflect the deletion
                if (typeof loadKeys === 'function') {
                    loadKeys(1); // Reload first page
                }
            })
            .catch(function(error) {
                MirDBUI.showToast('Failed to delete key: ' + error.message, 'error');
            });
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

// Export MirDBKeys to window for browser usage
if (typeof window !== 'undefined') {
    window.MirDBKeys = MirDBKeys;
}
