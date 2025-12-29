/**
 * MirDB Homepage JavaScript
 * Handles copy-to-clipboard functionality for code blocks
 */

document.addEventListener('DOMContentLoaded', function() {
    initializeCopyButtons();
});

/**
 * Initialize all copy buttons with click handlers
 */
function initializeCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-btn');

    copyButtons.forEach(function(button) {
        button.addEventListener('click', function() {
            handleCopyClick(button);
        });
    });
}

/**
 * Handle click on copy button
 * @param {HTMLElement} button - The copy button that was clicked
 */
function handleCopyClick(button) {
    const targetSelector = button.getAttribute('data-clipboard-target');
    if (!targetSelector) {
        console.error('Copy button missing data-clipboard-target attribute');
        return;
    }

    const targetElement = document.querySelector(targetSelector);
    if (!targetElement) {
        console.error('Target element not found:', targetSelector);
        return;
    }

    const textToCopy = targetElement.textContent;

    copyToClipboard(textToCopy)
        .then(function() {
            showCopySuccess(button);
        })
        .catch(function(err) {
            console.error('Failed to copy text:', err);
            showCopyError(button);
        });
}

/**
 * Copy text to clipboard using modern API with fallback
 * @param {string} text - Text to copy
 * @returns {Promise}
 */
function copyToClipboard(text) {
    // Try modern Clipboard API first
    if (navigator.clipboard && window.isSecureContext) {
        return navigator.clipboard.writeText(text);
    }

    // Fallback for older browsers
    return new Promise(function(resolve, reject) {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        textArea.style.top = '-999999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();

        try {
            var successful = document.execCommand('copy');
            document.body.removeChild(textArea);
            if (successful) {
                resolve();
            } else {
                reject(new Error('execCommand returned false'));
            }
        } catch (err) {
            document.body.removeChild(textArea);
            reject(err);
        }
    });
}

/**
 * Show success feedback on copy button
 * @param {HTMLElement} button - The copy button
 */
function showCopySuccess(button) {
    const originalText = button.querySelector('.copy-text');
    const originalIcon = button.querySelector('.copy-icon');

    if (originalText) {
        originalText.textContent = 'Copied!';
    }
    if (originalIcon) {
        originalIcon.textContent = '✓';
    }

    button.classList.add('copied');

    // Reset after 2 seconds
    setTimeout(function() {
        if (originalText) {
            originalText.textContent = 'Copy';
        }
        if (originalIcon) {
            originalIcon.textContent = '📋';
        }
        button.classList.remove('copied');
    }, 2000);
}

/**
 * Show error feedback on copy button
 * @param {HTMLElement} button - The copy button
 */
function showCopyError(button) {
    const originalText = button.querySelector('.copy-text');

    if (originalText) {
        originalText.textContent = 'Error';
    }

    // Reset after 2 seconds
    setTimeout(function() {
        if (originalText) {
            originalText.textContent = 'Copy';
        }
    }, 2000);
}
