/**
 * Copy to Clipboard Functionality
 * Owner: Scenario 2 - Quick Start Section
 *
 * Handles copy button functionality for code blocks:
 * - Adds copy buttons to all code blocks
 * - Copies code content to clipboard on click
 * - Shows visual feedback on copy success
 * - Handles keyboard activation (Enter key)
 */

/**
 * Copy text to the clipboard
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} - Whether the copy was successful
 */
async function copyToClipboard(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (err) {
    // Fallback for older browsers
    try {
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
    } catch (fallbackErr) {
      console.error('Copy to clipboard failed:', fallbackErr);
      return false;
    }
  }
}

/**
 * Show copy success feedback on a button
 * @param {HTMLElement} button - The copy button element
 */
function showCopySuccess(button) {
  const originalText = button.querySelector('.code-block__copy-text');
  const originalIcon = button.querySelector('.code-block__copy-icon');

  if (originalText) {
    originalText.textContent = 'Copied!';
  }
  if (originalIcon) {
    originalIcon.textContent = '✓';
  }

  button.classList.add('code-block__copy--copied');

  setTimeout(() => {
    if (originalText) {
      originalText.textContent = 'Copy';
    }
    if (originalIcon) {
      originalIcon.textContent = '📋';
    }
    button.classList.remove('code-block__copy--copied');
  }, 2000);
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

  const text = codeElement.textContent.trim();
  const success = await copyToClipboard(text);

  if (success) {
    showCopySuccess(button);
  }
}

/**
 * Handle keyboard activation on copy buttons
 * @param {KeyboardEvent} event - The keyboard event
 */
function handleCopyKeydown(event) {
  if (event.key === 'Enter' || event.key === ' ') {
    event.preventDefault();
    handleCopyClick(event);
  }
}

/**
 * Initialize all copy buttons on the page
 */
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('.code-block__copy');

  copyButtons.forEach((button) => {
    button.addEventListener('click', handleCopyClick);
    button.addEventListener('keydown', handleCopyKeydown);
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
  module.exports = { initCopyButtons, copyToClipboard };
}
