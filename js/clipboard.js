/**
 * Clipboard Module - Copy-to-Clipboard Functionality
 * Owner: Scenario 3 - Quick Start Section
 *
 * Provides copy button functionality for code blocks.
 * Progressive enhancement - code is visible without JS.
 *
 * Expected exports:
 * - initClipboard(): Initialize copy buttons
 * - copyToClipboard(text): Copy text to clipboard
 */

/**
 * Copy text to clipboard
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} - Whether the copy was successful
 */
async function copyToClipboard(text) {
    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(text);
            return true;
        }
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        textArea.style.top = '-9999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        const success = document.execCommand('copy');
        document.body.removeChild(textArea);
        return success;
    } catch (err) {
        console.error('Failed to copy text: ', err);
        return false;
    }
}

/**
 * Get plain text from code block (strips HTML tags)
 * @param {HTMLElement} codeBlock - The code block element
 * @returns {string} - Plain text content
 */
function getCodeText(codeBlock) {
    // Get text content which strips HTML tags
    return codeBlock.textContent || codeBlock.innerText || '';
}

/**
 * Show copy success feedback
 * @param {HTMLElement} button - The copy button element
 */
function showCopySuccess(button) {
    const textSpan = button.querySelector('.copy-text');
    const originalText = textSpan ? textSpan.textContent : 'Copy';

    button.classList.add('copied');
    if (textSpan) {
        textSpan.textContent = 'Copied!';
    }

    setTimeout(() => {
        button.classList.remove('copied');
        if (textSpan) {
            textSpan.textContent = originalText;
        }
    }, 2000);
}

/**
 * Handle copy button click
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
    const button = event.currentTarget;
    const targetId = button.getAttribute('data-copy-target');

    if (!targetId) return;

    const codeBlock = document.getElementById(targetId);
    if (!codeBlock) return;

    const text = getCodeText(codeBlock);
    const success = await copyToClipboard(text);

    if (success) {
        showCopySuccess(button);
    }
}

/**
 * Initialize clipboard functionality
 */
function initClipboard() {
    const copyButtons = document.querySelectorAll('.copy-btn');

    copyButtons.forEach(button => {
        button.addEventListener('click', handleCopyClick);
    });
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initClipboard);
} else {
    initClipboard();
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { initClipboard, copyToClipboard };
}
