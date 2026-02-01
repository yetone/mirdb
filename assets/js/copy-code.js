/**
 * Copy-to-clipboard functionality for code blocks.
 * Owner: Scenario 3 - Demo Section with Usage Examples
 *
 * Functions:
 * - copyToClipboard(text): Copy text to clipboard
 * - initCopyButtons(): Initialize copy buttons on code blocks
 */

/**
 * Copy text to clipboard
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} True if successful, false otherwise
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
        textArea.style.top = '-999999px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {
            document.execCommand('copy');
            document.body.removeChild(textArea);
            return true;
        } catch (execErr) {
            document.body.removeChild(textArea);
            return false;
        }
    }
}

/**
 * Handle copy button click
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
    const button = event.currentTarget;
    const targetId = button.getAttribute('data-target');
    const codeElement = document.getElementById(targetId);

    if (!codeElement) {
        console.error('Target code element not found:', targetId);
        return;
    }

    const text = codeElement.textContent;
    const success = await copyToClipboard(text);

    if (success) {
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.classList.add('copied');

        setTimeout(() => {
            button.textContent = originalText;
            button.classList.remove('copied');
        }, 2000);
    } else {
        button.textContent = 'Failed';
        setTimeout(() => {
            button.textContent = 'Copy';
        }, 2000);
    }
}

/**
 * Initialize copy buttons on code blocks
 */
function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-button');
    copyButtons.forEach(button => {
        button.addEventListener('click', handleCopyClick);
    });
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
} else {
    initCopyButtons();
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        copyToClipboard,
        initCopyButtons,
        handleCopyClick
    };
}
