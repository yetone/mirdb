/**
 * Copy to Clipboard Functionality
 * Owner: Scenario 4 - Quick Start Section
 *
 * Provides copy-to-clipboard functionality for code blocks
 * - copyToClipboard(text): Copy text to clipboard
 * - initCodeCopyButtons(): Add copy buttons to code blocks
 * - showCopyFeedback(button): Visual feedback on copy
 */

/**
 * Copy text to clipboard using the Clipboard API
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} - Whether the copy was successful
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
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
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
 * Show visual feedback when code is copied
 * @param {HTMLElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success = true) {
  const originalText = button.querySelector('.copy-text');
  const originalIcon = button.querySelector('.copy-icon');

  if (success) {
    button.classList.add('copied');
    if (originalText) originalText.textContent = 'Copied!';
    if (originalIcon) originalIcon.textContent = '✓';
    button.setAttribute('aria-label', 'Copied to clipboard');
  } else {
    button.classList.add('copy-failed');
    if (originalText) originalText.textContent = 'Failed';
    if (originalIcon) originalIcon.textContent = '✗';
    button.setAttribute('aria-label', 'Failed to copy');
  }

  // Reset after 2 seconds
  setTimeout(() => {
    button.classList.remove('copied', 'copy-failed');
    if (originalText) originalText.textContent = 'Copy';
    if (originalIcon) originalIcon.textContent = '📋';
    button.setAttribute('aria-label', 'Copy to clipboard');
  }, 2000);
}

/**
 * Get the code content from a code block
 * @param {HTMLElement} codeBlock - The code block container
 * @returns {string} - The code text content
 */
function getCodeContent(codeBlock) {
  const codeElement = codeBlock.querySelector('code');
  if (!codeElement) return '';

  // Get text content, stripping any HTML tags but preserving line structure
  return codeElement.textContent.trim();
}

/**
 * Handle copy button click event
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const codeBlock = button.closest('.code-block');

  if (!codeBlock) return;

  const code = getCodeContent(codeBlock);
  const success = await copyToClipboard(code);
  showCopyFeedback(button, success);
}

/**
 * Initialize copy buttons for all code blocks
 * Attaches click handlers to existing copy buttons
 */
function initCodeCopyButtons() {
  const copyButtons = document.querySelectorAll('.code-block .copy-btn');

  copyButtons.forEach(button => {
    // Remove any existing listeners to prevent duplicates
    button.removeEventListener('click', handleCopyClick);
    // Add click handler
    button.addEventListener('click', handleCopyClick);
  });
}

// Initialize when DOM is ready
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCodeCopyButtons);
  } else {
    initCodeCopyButtons();
  }
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    copyToClipboard,
    showCopyFeedback,
    getCodeContent,
    initCodeCopyButtons,
    handleCopyClick
  };
}
