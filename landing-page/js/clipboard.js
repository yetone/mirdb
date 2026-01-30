/**
 * MirDB Landing Page - Clipboard Module
 * Owner: Scenario 3 - Usage Section with Code Examples
 *
 * This file handles:
 * - Copy-to-clipboard functionality for code blocks
 * - Visual feedback on copy action
 * - Fallback for older browsers
 *
 * Expected Exports:
 * - initClipboard(): void - Initialize clipboard handlers
 * - copyToClipboard(text: string): Promise<boolean> - Copy text to clipboard
 */

/**
 * Copy text to clipboard using modern API with fallback
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} - True if copy succeeded
 */
async function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.warn('Clipboard API failed, trying fallback:', err);
      return fallbackCopy(text);
    }
  }
  return fallbackCopy(text);
}

/**
 * Fallback copy method for older browsers
 * @param {string} text - Text to copy
 * @returns {boolean} - True if copy succeeded
 */
function fallbackCopy(text) {
  const textArea = document.createElement('textarea');
  textArea.value = text;
  textArea.style.position = 'fixed';
  textArea.style.left = '-9999px';
  textArea.style.top = '0';
  textArea.setAttribute('readonly', '');
  document.body.appendChild(textArea);

  try {
    textArea.select();
    textArea.setSelectionRange(0, text.length);
    const success = document.execCommand('copy');
    return success;
  } catch (err) {
    console.error('Fallback copy failed:', err);
    return false;
  } finally {
    document.body.removeChild(textArea);
  }
}

/**
 * Extract text content from a code element, removing prompt characters
 * @param {HTMLElement} codeElement - The code element
 * @returns {string} - Clean text for copying
 */
function extractCodeText(codeElement) {
  const lines = codeElement.textContent.split('\n');
  return lines
    .map(line => line.replace(/^\$\s*/, '').trim())
    .filter(line => line.length > 0)
    .join('\n');
}

/**
 * Show visual feedback on copy button
 * @param {HTMLElement} button - The copy button
 * @param {boolean} success - Whether copy succeeded
 */
function showCopyFeedback(button, success) {
  const textSpan = button.querySelector('.copy-text');
  const originalText = textSpan ? textSpan.textContent : 'Copy';

  button.classList.add(success ? 'copy-success' : 'copy-error');
  if (textSpan) {
    textSpan.textContent = success ? 'Copied!' : 'Failed';
  }
  button.setAttribute('aria-label', success ? 'Copied to clipboard' : 'Copy failed');

  setTimeout(() => {
    button.classList.remove('copy-success', 'copy-error');
    if (textSpan) {
      textSpan.textContent = originalText;
    }
    button.setAttribute('aria-label', `Copy ${button.dataset.copyTarget || 'code'} to clipboard`);
  }, 2000);
}

/**
 * Handle copy button click
 * @param {Event} event - Click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const targetId = button.dataset.copyTarget;

  if (!targetId) {
    console.warn('Copy button missing data-copy-target attribute');
    return;
  }

  const codeElement = document.getElementById(targetId);
  if (!codeElement) {
    console.warn(`Target element #${targetId} not found`);
    showCopyFeedback(button, false);
    return;
  }

  const textToCopy = extractCodeText(codeElement);
  const success = await copyToClipboard(textToCopy);
  showCopyFeedback(button, success);
}

/**
 * Initialize clipboard functionality for all copy buttons
 */
function initClipboard() {
  const copyButtons = document.querySelectorAll('.copy-btn');

  copyButtons.forEach(button => {
    button.addEventListener('click', handleCopyClick);
  });
}

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initClipboard);
} else {
  initClipboard();
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initClipboard, copyToClipboard };
}
