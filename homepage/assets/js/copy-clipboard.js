/**
 * Copy to Clipboard Module
 * Owner: Scenario 4 - Quick Start Section
 *
 * Provides copy-to-clipboard functionality for code blocks
 * with visual feedback on successful copy.
 */

/**
 * Copy text to clipboard using the Clipboard API
 * Falls back to execCommand for older browsers
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} - Whether the copy was successful
 */
export async function copyToClipboard(text) {
  // Try modern Clipboard API first
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.warn('Clipboard API failed, trying fallback:', err);
    }
  }

  // Fallback for older browsers
  try {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '0';
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
 * Show visual feedback after copy action
 * @param {HTMLElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  const copyIcon = button.querySelector('.copy-icon');
  const checkIcon = button.querySelector('.check-icon');
  const btnText = button.querySelector('.copy-btn-text');

  if (success) {
    // Show success state
    if (copyIcon) copyIcon.style.display = 'none';
    if (checkIcon) checkIcon.style.display = 'block';
    if (btnText) btnText.textContent = 'Copied!';
    button.classList.add('copied');
    button.setAttribute('aria-label', 'Copied to clipboard');

    // Reset after 2 seconds
    setTimeout(() => {
      if (copyIcon) copyIcon.style.display = 'block';
      if (checkIcon) checkIcon.style.display = 'none';
      if (btnText) btnText.textContent = 'Copy';
      button.classList.remove('copied');
      button.setAttribute('aria-label', 'Copy to clipboard');
    }, 2000);
  } else {
    // Show error state briefly
    if (btnText) btnText.textContent = 'Failed';
    button.classList.add('copy-failed');

    setTimeout(() => {
      if (btnText) btnText.textContent = 'Copy';
      button.classList.remove('copy-failed');
    }, 2000);
  }
}

/**
 * Get the code content from a code block, stripping syntax highlighting spans
 * @param {string} targetId - The ID of the code element
 * @returns {string} - The plain text code content
 */
function getCodeContent(targetId) {
  const codeElement = document.getElementById(targetId);
  if (!codeElement) return '';

  // Get the text content, which automatically strips HTML tags
  return codeElement.textContent || '';
}

/**
 * Handle copy button click
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const targetId = button.getAttribute('data-copy-target');

  if (!targetId) {
    console.error('Copy button missing data-copy-target attribute');
    return;
  }

  const codeContent = getCodeContent(targetId);
  if (!codeContent) {
    console.error('Could not find code content for target:', targetId);
    showCopyFeedback(button, false);
    return;
  }

  const success = await copyToClipboard(codeContent);
  showCopyFeedback(button, success);
}

/**
 * Initialize all copy buttons on the page
 * Should be called after DOM is ready
 */
export function initCopyButtons() {
  const copyButtons = document.querySelectorAll('.copy-btn');

  copyButtons.forEach(button => {
    // Remove any existing listeners to prevent duplicates
    button.removeEventListener('click', handleCopyClick);
    button.addEventListener('click', handleCopyClick);
  });
}

// Export for testing
export { showCopyFeedback, getCodeContent, handleCopyClick };
