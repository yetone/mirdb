/**
 * Copy to Clipboard Module
 *
 * Provides shared copy functionality for code blocks.
 */

/**
 * Initialize all copy buttons on the page
 */
function initCopyButtons() {
  document.querySelectorAll('[data-copy-target]').forEach(button => {
    button.addEventListener('click', async () => {
      const targetSelector = button.getAttribute('data-copy-target');
      const targetElement = document.querySelector(targetSelector);

      if (targetElement) {
        const text = targetElement.textContent || '';
        const success = await copyToClipboard(text);
        showCopyFeedback(button, success);
      }
    });
  });
}

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
    document.body.appendChild(textArea);
    textArea.select();

    try {
      document.execCommand('copy');
      return true;
    } finally {
      document.body.removeChild(textArea);
    }
  } catch (error) {
    console.error('Failed to copy text:', error);
    return false;
  }
}

/**
 * Show visual feedback after copy attempt
 * @param {HTMLElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  const originalText = button.textContent;
  button.textContent = success ? 'Copied!' : 'Failed';
  button.classList.add(success ? 'copy-success' : 'copy-error');

  setTimeout(() => {
    button.textContent = originalText;
    button.classList.remove('copy-success', 'copy-error');
  }, 2000);
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initCopyButtons, copyToClipboard, showCopyFeedback };
}
