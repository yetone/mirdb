/**
 * MirDB Homepage JavaScript
 * Owner: Scenario 4 - Quick Start Section with Copy Functionality
 *
 * Provides copy-to-clipboard functionality for code blocks
 */

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} Whether the copy was successful
 */
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        return true;
    } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        document.body.appendChild(textArea);
        textArea.select();
        try {
            document.execCommand('copy');
            document.body.removeChild(textArea);
            return true;
        } catch (e) {
            document.body.removeChild(textArea);
            return false;
        }
    }
}

/**
 * Show visual feedback after copy
 * @param {HTMLElement} button - The copy button element
 */
function showCopyFeedback(button) {
    const originalText = button.textContent;
    button.textContent = 'Copied!';
    button.classList.add('copied');

    setTimeout(() => {
        button.textContent = originalText;
        button.classList.remove('copied');
    }, 2000);
}

/**
 * Initialize all copy buttons on the page
 */
function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-btn');

    copyButtons.forEach(button => {
        button.addEventListener('click', async function() {
            const textToCopy = this.getAttribute('data-clipboard-text');
            if (textToCopy) {
                const success = await copyToClipboard(textToCopy);
                if (success) {
                    showCopyFeedback(this);
                }
            }
        });
    });
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
} else {
    initCopyButtons();
}

// Export for testing (if module environment)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { copyToClipboard, showCopyFeedback, initCopyButtons };
}
