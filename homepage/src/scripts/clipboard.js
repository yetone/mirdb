/**
 * Clipboard Utilities
 * Owner: Scenario 3 - Quick Start Section
 *
 * Provides copy-to-clipboard functionality with browser fallback support.
 */

/**
 * Copy text to clipboard using the Clipboard API with fallback
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - True if copy succeeded, false otherwise
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

  // Fallback for older browsers or when Clipboard API is not available
  return copyToClipboardFallback(text);
}

/**
 * Fallback method using execCommand for older browsers
 * @param {string} text - The text to copy
 * @returns {boolean} - True if copy succeeded
 */
function copyToClipboardFallback(text) {
  const textArea = document.createElement('textarea');
  textArea.value = text;

  // Prevent scrolling to bottom of page
  textArea.style.position = 'fixed';
  textArea.style.top = '0';
  textArea.style.left = '0';
  textArea.style.width = '2em';
  textArea.style.height = '2em';
  textArea.style.padding = '0';
  textArea.style.border = 'none';
  textArea.style.outline = 'none';
  textArea.style.boxShadow = 'none';
  textArea.style.background = 'transparent';

  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  let success = false;
  try {
    success = document.execCommand('copy');
  } catch (err) {
    console.error('Fallback copy failed:', err);
  }

  document.body.removeChild(textArea);
  return success;
}

/**
 * Show visual feedback after copying
 * @param {HTMLElement} element - The element to show feedback on (copy button)
 * @param {number} duration - How long to show the feedback in ms
 */
export function showCopyFeedback(element, duration = 2000) {
  if (!element) return;

  element.classList.add('copied');

  const textSpan = element.querySelector('.code-block__copy-text');
  const originalText = textSpan?.textContent;
  if (textSpan) {
    textSpan.textContent = 'Copied!';
  }

  setTimeout(() => {
    element.classList.remove('copied');
    if (textSpan && originalText) {
      textSpan.textContent = originalText;
    }
  }, duration);
}
