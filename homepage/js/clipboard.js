/**
 * Clipboard Module for Code Copy Functionality
 * Owner: Scenario 3 - Quick Start Code Examples
 *
 * Provides copy-to-clipboard functionality for code blocks
 * with toast notification feedback.
 */

/**
 * Copy text to clipboard using the modern Clipboard API
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - Resolves to true if successful, false otherwise
 */
async function copyToClipboard(text) {
    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(text);
            return true;
        }
        // Fallback for older browsers
        return fallbackCopyToClipboard(text);
    } catch (err) {
        console.error('Failed to copy to clipboard:', err);
        return fallbackCopyToClipboard(text);
    }
}

/**
 * Fallback copy method using execCommand for older browsers
 * @param {string} text - The text to copy
 * @returns {boolean} - True if successful
 */
function fallbackCopyToClipboard(text) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    textArea.style.top = '-999999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
        const successful = document.execCommand('copy');
        document.body.removeChild(textArea);
        return successful;
    } catch (err) {
        console.error('Fallback copy failed:', err);
        document.body.removeChild(textArea);
        return false;
    }
}

/**
 * Show a toast notification near the element
 * @param {HTMLElement} element - The element near which to show the notification
 * @param {string} message - The message to display (default: 'Copied!')
 * @param {number} duration - How long to show the notification in ms (default: 2000)
 */
function showCopyNotification(element, message = 'Copied!', duration = 2000) {
    const container = document.getElementById('toast-container');
    if (!container) {
        console.warn('Toast container not found');
        return;
    }

    const toast = document.createElement('div');
    toast.className = 'toast toast-success';
    toast.setAttribute('role', 'status');
    toast.setAttribute('aria-live', 'polite');
    toast.textContent = message;

    container.appendChild(toast);

    // Trigger reflow to enable animation
    toast.offsetHeight;
    toast.classList.add('toast-visible');

    setTimeout(() => {
        toast.classList.remove('toast-visible');
        toast.classList.add('toast-hiding');
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    }, duration);
}

/**
 * Handle copy button click
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
    const button = event.currentTarget;
    const codeBlock = button.closest('.code-block');

    if (!codeBlock) {
        console.error('Code block not found');
        return;
    }

    const codeElement = codeBlock.querySelector('code');
    if (!codeElement) {
        console.error('Code element not found');
        return;
    }

    const text = codeElement.textContent;
    const success = await copyToClipboard(text);

    if (success) {
        showCopyNotification(button);
        // Update button state temporarily
        const buttonText = button.querySelector('span');
        if (buttonText) {
            const originalText = buttonText.textContent;
            buttonText.textContent = 'Copied!';
            setTimeout(() => {
                buttonText.textContent = originalText;
            }, 2000);
        }
    } else {
        showCopyNotification(button, 'Failed to copy', 2000);
    }
}

/**
 * Initialize copy buttons on all code blocks
 */
function initClipboard() {
    const copyButtons = document.querySelectorAll('.copy-btn');
    copyButtons.forEach(button => {
        button.addEventListener('click', handleCopyClick);
    });
}

// Initialize when DOM is ready
if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initClipboard);
    } else {
        initClipboard();
    }
}

// Export functions for testing and external use
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { copyToClipboard, showCopyNotification, initClipboard, handleCopyClick };
}
