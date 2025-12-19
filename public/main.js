/**
 * MirDB Homepage JavaScript
 * Handles copy-to-clipboard functionality
 */

document.addEventListener('DOMContentLoaded', function() {
    initCopyButtons();
});

/**
 * Initialize copy-to-clipboard functionality for code blocks
 */
function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-btn');

    copyButtons.forEach(function(button) {
        button.addEventListener('click', async function() {
            const codeBlock = this.closest('.code-block-wrapper').querySelector('code');
            const textToCopy = codeBlock.textContent;

            try {
                await navigator.clipboard.writeText(textToCopy);
                showCopySuccess(this);
            } catch (err) {
                // Fallback for older browsers
                fallbackCopy(textToCopy, this);
            }
        });
    });
}

/**
 * Show success state on copy button
 */
function showCopySuccess(button) {
    const copyIcon = button.querySelector('.copy-icon');
    const checkIcon = button.querySelector('.check-icon');
    const copyText = button.querySelector('.copy-text');

    // Update button state
    button.classList.add('copied');
    copyIcon.classList.add('hidden');
    checkIcon.classList.remove('hidden');
    copyText.textContent = 'Copied!';

    // Reset after 2 seconds
    setTimeout(function() {
        button.classList.remove('copied');
        copyIcon.classList.remove('hidden');
        checkIcon.classList.add('hidden');
        copyText.textContent = 'Copy';
    }, 2000);
}

/**
 * Fallback copy method for browsers without clipboard API
 */
function fallbackCopy(text, button) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
        document.execCommand('copy');
        showCopySuccess(button);
    } catch (err) {
        console.error('Failed to copy text:', err);
    }

    document.body.removeChild(textArea);
}
