/**
 * MirDB Homepage JavaScript
 * Handles copy-to-clipboard functionality with visual feedback
 */

(function() {
    'use strict';

    /**
     * Initialize copy functionality for all copy buttons
     */
    function initCopyButtons() {
        const copyButtons = document.querySelectorAll('.copy-btn');

        copyButtons.forEach(button => {
            button.addEventListener('click', handleCopyClick);
        });
    }

    /**
     * Handle click on copy button
     * @param {Event} event - Click event
     */
    async function handleCopyClick(event) {
        const button = event.currentTarget;
        const codeBlock = button.closest('.code-block');
        const codeElement = codeBlock.querySelector('code');

        if (!codeElement) {
            console.error('Code element not found');
            return;
        }

        const textToCopy = codeElement.textContent;

        try {
            await copyToClipboard(textToCopy);
            showCopyFeedback(button, true);
        } catch (error) {
            console.error('Failed to copy:', error);
            showCopyFeedback(button, false);
        }
    }

    /**
     * Copy text to clipboard
     * @param {string} text - Text to copy
     * @returns {Promise<void>}
     */
    async function copyToClipboard(text) {
        // Use modern Clipboard API if available
        if (navigator.clipboard && navigator.clipboard.writeText) {
            return navigator.clipboard.writeText(text);
        }

        // Fallback for older browsers
        return new Promise((resolve, reject) => {
            const textArea = document.createElement('textarea');
            textArea.value = text;
            textArea.style.position = 'fixed';
            textArea.style.left = '-9999px';
            textArea.style.top = '-9999px';
            textArea.setAttribute('readonly', '');
            document.body.appendChild(textArea);

            try {
                textArea.select();
                textArea.setSelectionRange(0, 99999);
                const successful = document.execCommand('copy');
                document.body.removeChild(textArea);

                if (successful) {
                    resolve();
                } else {
                    reject(new Error('execCommand failed'));
                }
            } catch (error) {
                document.body.removeChild(textArea);
                reject(error);
            }
        });
    }

    /**
     * Show visual feedback after copy attempt
     * @param {HTMLElement} button - The copy button
     * @param {boolean} success - Whether copy was successful
     */
    function showCopyFeedback(button, success) {
        const copyText = button.querySelector('.copy-text');
        const copyIcon = button.querySelector('.copy-icon');

        if (success) {
            button.classList.add('copied');
            copyText.textContent = 'Copied';
            copyIcon.textContent = '✓';

            // Dispatch custom event for testing
            button.dispatchEvent(new CustomEvent('copy-success', {
                bubbles: true,
                detail: { success: true }
            }));
        } else {
            copyText.textContent = 'Failed';
            copyIcon.textContent = '✗';
        }

        // Reset after 2 seconds
        setTimeout(() => {
            button.classList.remove('copied');
            copyText.textContent = 'Copy';
            copyIcon.textContent = '📋';
        }, 2000);
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initCopyButtons);
    } else {
        initCopyButtons();
    }
})();
