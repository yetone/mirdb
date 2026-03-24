/**
 * Copy-to-Clipboard Module
 * Owner: Scenario 4 - Quick Start Section
 *
 * Expected exports:
 * - initCopyButtons(): Add copy buttons to all code blocks
 * - copyToClipboard(text): Copy text to clipboard
 *
 * Features:
 * - Show button on hover
 * - Visual feedback on copy
 */

/**
 * Copy text to clipboard using the Clipboard API with fallback
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - Promise resolving to success status
 */
async function copyToClipboard(text) {
    // Try modern Clipboard API first
    if (navigator.clipboard && navigator.clipboard.writeText) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (err) {
            console.warn('Clipboard API failed, trying fallback:', err);
        }
    }

    // Fallback for older browsers or when Clipboard API is not available
    try {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        textArea.style.top = '-9999px';
        textArea.setAttribute('readonly', '');
        document.body.appendChild(textArea);
        textArea.select();
        textArea.setSelectionRange(0, text.length);
        const success = document.execCommand('copy');
        document.body.removeChild(textArea);
        return success;
    } catch (err) {
        console.error('Fallback copy failed:', err);
        return false;
    }
}

/**
 * Extract plain text from code block, removing prompt symbols
 * @param {HTMLElement} codeElement - The code element to extract text from
 * @returns {string} - The clean text content
 */
function extractCodeText(codeElement) {
    // Clone the element to avoid modifying the original
    const clone = codeElement.cloneNode(true);

    // Remove prompt elements (they have user-select: none for a reason)
    const prompts = clone.querySelectorAll('.token-prompt');
    prompts.forEach(prompt => prompt.remove());

    // Get text content and clean it up
    let text = clone.textContent || '';

    // Remove leading/trailing whitespace from each line while preserving structure
    text = text.split('\n')
        .map(line => line.trimEnd())
        .join('\n')
        .trim();

    return text;
}

/**
 * Show copy feedback on button
 * @param {HTMLButtonElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
    const originalText = button.textContent;

    if (success) {
        button.textContent = 'Copied!';
        button.classList.add('copied');
    } else {
        button.textContent = 'Failed';
    }

    // Reset button after delay
    setTimeout(() => {
        button.textContent = originalText;
        button.classList.remove('copied');
    }, 2000);
}

/**
 * Create a copy button element
 * @returns {HTMLButtonElement} - The created button element
 */
function createCopyButton() {
    const button = document.createElement('button');
    button.className = 'copy-btn';
    button.textContent = 'Copy';
    button.setAttribute('type', 'button');
    button.setAttribute('aria-label', 'Copy code to clipboard');
    return button;
}

/**
 * Handle copy button click
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
    const button = event.currentTarget;
    const codeBlock = button.closest('.code-block');

    if (!codeBlock) return;

    const codeElement = codeBlock.querySelector('code');
    if (!codeElement) return;

    const text = extractCodeText(codeElement);
    const success = await copyToClipboard(text);
    showCopyFeedback(button, success);
}

/**
 * Initialize copy buttons for all code blocks
 * Finds all .code-block elements and adds copy functionality
 */
function initCopyButtons() {
    const codeBlocks = document.querySelectorAll('.code-block');

    codeBlocks.forEach(block => {
        // Skip if button already exists
        if (block.querySelector('.copy-btn')) return;

        const button = createCopyButton();
        button.addEventListener('click', handleCopyClick);
        block.appendChild(button);
    });
}

// Export for module systems and make available globally
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { initCopyButtons, copyToClipboard, extractCodeText };
}

// Make available globally for browser
if (typeof window !== 'undefined') {
    window.CopyCode = { initCopyButtons, copyToClipboard, extractCodeText };
}
