/**
 * MirDB Landing Page JavaScript
 * Handles copy-to-clipboard functionality for code blocks
 */

document.addEventListener('DOMContentLoaded', function() {
    initCopyButtons();
});

/**
 * Initialize copy-to-clipboard functionality for all code blocks
 */
function initCopyButtons() {
    var copyButtons = document.querySelectorAll('.copy-btn');

    copyButtons.forEach(function(button) {
        button.addEventListener('click', function() {
            handleCopyClick(button);
        });
    });
}

/**
 * Handle copy button click
 * @param {HTMLElement} button - The copy button element
 */
function handleCopyClick(button) {
    var targetId = button.getAttribute('data-copy-target');
    var codeElement = document.getElementById(targetId);

    if (!codeElement) {
        console.error('Copy target not found:', targetId);
        return;
    }

    var textToCopy = codeElement.textContent;

    copyToClipboard(textToCopy)
        .then(function() {
            showCopySuccess(button);
        })
        .catch(function(error) {
            console.error('Failed to copy:', error);
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

    // Fallback for older browsers or non-secure contexts
    return new Promise(function(resolve, reject) {
        var textArea = document.createElement('textarea');
        textArea.value = text;

        // Make it invisible
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        textArea.style.top = '-999999px';
        textArea.setAttribute('aria-hidden', 'true');

        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();

        try {
            var successful = document.execCommand('copy');
            document.body.removeChild(textArea);

            if (successful) {
                resolve();
            } else {
                reject(new Error('Copy command failed'));
            }
        } catch (error) {
            document.body.removeChild(textArea);
            reject(error);
        }
    });
}

/**
 * Show success state on copy button
 * @param {HTMLElement} button - The copy button element
 */
function showCopySuccess(button) {
    var span = button.querySelector('span');
    var originalText = span.textContent;

    button.classList.add('copied');
    span.textContent = 'Copied!';

    setTimeout(function() {
        button.classList.remove('copied');
        span.textContent = originalText;
    }, 2000);
}

/**
 * Show error state on copy button
 * @param {HTMLElement} button - The copy button element
 */
function showCopyError(button) {
    var span = button.querySelector('span');
    var originalText = span.textContent;

    span.textContent = 'Error';

    setTimeout(function() {
        span.textContent = originalText;
    }, 2000);
}
