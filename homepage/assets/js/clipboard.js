/**
 * Clipboard Module
 * Owner: Scenario 4 - Code Examples Display
 *
 * Expected exports:
 * - initClipboard(): Set up copy buttons
 * - copyToClipboard(text): Copy text to clipboard
 * - showCopyFeedback(button): Visual feedback on copy success
 *
 * Features:
 * - Copy code blocks to clipboard
 * - Visual feedback (tooltip/icon change)
 * - Fallback for older browsers
 */

/**
 * Initialize clipboard functionality for all copy buttons
 */
function initClipboard() {
  const copyButtons = document.querySelectorAll('.quickstart__copy-btn, .copy-btn');

  copyButtons.forEach(button => {
    button.addEventListener('click', handleCopyClick);
  });
}

/**
 * Handle copy button click event
 * @param {Event} event - Click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const codeBlock = button.closest('.quickstart__code-block, .code-block');

  if (!codeBlock) return;

  const codeElement = codeBlock.querySelector('code');
  if (!codeElement) return;

  const text = codeElement.textContent;
  const success = await copyToClipboard(text);

  if (success) {
    showCopyFeedback(button);
  }
}

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} - True if copy succeeded
 */
async function copyToClipboard(text) {
  // Modern Clipboard API
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.error('Failed to copy using Clipboard API:', err);
      return fallbackCopyToClipboard(text);
    }
  }

  // Fallback for older browsers
  return fallbackCopyToClipboard(text);
}

/**
 * Fallback copy method for older browsers
 * @param {string} text - Text to copy
 * @returns {boolean} - True if copy succeeded
 */
function fallbackCopyToClipboard(text) {
  const textArea = document.createElement('textarea');
  textArea.value = text;

  // Avoid scrolling to bottom
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
 * Show visual feedback when copy succeeds
 * @param {HTMLElement} button - Copy button element
 */
function showCopyFeedback(button) {
  const textSpan = button.querySelector('.quickstart__copy-text, .copy-text');
  const originalText = textSpan ? textSpan.textContent : 'Copy';

  // Add copied state
  button.classList.add('copied');
  if (textSpan) {
    textSpan.textContent = 'Copied!';
  }

  // Reset after delay
  setTimeout(() => {
    button.classList.remove('copied');
    if (textSpan) {
      textSpan.textContent = originalText;
    }
  }, 2000);
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', initClipboard);
