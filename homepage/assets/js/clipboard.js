/**
 * Clipboard Module
 * Owner: Scenario 4 - Code Examples Display (also used by Scenario 5 - Quick Start)
 *
 * Exports:
 * - initClipboard(): Set up copy buttons
 * - copyToClipboard(text): Copy text to clipboard
 * - showCopyFeedback(button): Visual feedback on copy success
 *
 * Features:
 * - Copy code blocks to clipboard
 * - Visual feedback (tooltip/icon change)
 * - Fallback for older browsers
 * - Supports both code examples and quickstart sections
 */

/**
 * Initialize clipboard functionality for all copy buttons
 */
function initClipboard() {
  const copyButtons = document.querySelectorAll(
    '.code-example__copy-btn, .quickstart__copy-btn, .copy-btn'
  );

  copyButtons.forEach((button) => {
    button.addEventListener('click', handleCopyClick);
  });
}

/**
 * Handle click on copy button
 * @param {Event} event - Click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;

  // Check for code example (Scenario 4)
  const codeExample = button.closest('.code-example');
  if (codeExample) {
    const codeBlock = codeExample.querySelector('.code-example__code code');
    if (codeBlock) {
      const text = extractCodeText(codeBlock);
      await copyToClipboard(text, button);
      return;
    }
  }

  // Check for quickstart/generic code block (Scenario 5)
  const quickstartBlock = button.closest('.quickstart__code-block, .code-block');
  if (quickstartBlock) {
    const codeElement = quickstartBlock.querySelector('code');
    if (codeElement) {
      const text = codeElement.textContent;
      const success = await copyToClipboardSimple(text);
      if (success) {
        showCopyFeedbackSimple(button);
      }
    }
  }
}

/**
 * Extract plain text from code block, stripping HTML syntax highlighting
 * @param {HTMLElement} codeBlock - The code element containing the code
 * @returns {string} Plain text code without comments
 */
function extractCodeText(codeBlock) {
  const clone = codeBlock.cloneNode(true);

  const comments = clone.querySelectorAll('.code-comment');
  comments.forEach((comment) => comment.remove());

  const lines = clone.textContent.split('\n');
  const filtered = lines.filter((line) => line.trim() !== '');

  return filtered.join('\n').trim();
}

/**
 * Copy text to clipboard with fallback for older browsers
 * @param {string} text - Text to copy
 * @param {HTMLElement} button - Button element for feedback
 * @returns {Promise<boolean>} Success status
 */
async function copyToClipboard(text, button) {
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      showCopyFeedback(button, true);
      return true;
    } else {
      const success = fallbackCopyToClipboard(text);
      showCopyFeedback(button, success);
      return success;
    }
  } catch (err) {
    console.error('Failed to copy to clipboard:', err);
    showCopyFeedback(button, false);
    return false;
  }
}

/**
 * Simple copy to clipboard (for quickstart section)
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} - True if copy succeeded
 */
async function copyToClipboardSimple(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.error('Failed to copy using Clipboard API:', err);
      return fallbackCopyToClipboard(text);
    }
  }

  return fallbackCopyToClipboard(text);
}

/**
 * Fallback copy method for browsers without Clipboard API
 * @param {string} text - Text to copy
 * @returns {boolean} Success status
 */
function fallbackCopyToClipboard(text) {
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.left = '-9999px';
  textarea.style.top = '0';
  textarea.style.width = '2em';
  textarea.style.height = '2em';
  textarea.style.padding = '0';
  textarea.style.border = 'none';
  textarea.style.outline = 'none';
  textarea.style.boxShadow = 'none';
  textarea.style.background = 'transparent';
  textarea.setAttribute('readonly', '');
  document.body.appendChild(textarea);

  try {
    textarea.focus();
    textarea.select();
    textarea.setSelectionRange(0, textarea.value.length);
    const success = document.execCommand('copy');
    document.body.removeChild(textarea);
    return success;
  } catch (err) {
    console.error('Fallback copy failed:', err);
    document.body.removeChild(textarea);
    return false;
  }
}

/**
 * Show visual feedback after copy operation (for code examples)
 * @param {HTMLElement} button - Copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  if (!button) return;

  const copyIcon = button.querySelector('.code-example__copy-icon');
  const checkIcon = button.querySelector('.code-example__check-icon');
  const copyText = button.querySelector('.code-example__copy-text');

  if (success) {
    button.classList.add('copied');

    if (copyIcon) copyIcon.style.display = 'none';
    if (checkIcon) checkIcon.style.display = 'block';
    if (copyText) copyText.textContent = 'Copied!';

    setTimeout(() => {
      button.classList.remove('copied');
      if (copyIcon) copyIcon.style.display = 'block';
      if (checkIcon) checkIcon.style.display = 'none';
      if (copyText) copyText.textContent = 'Copy';
    }, 2000);
  } else {
    if (copyText) copyText.textContent = 'Failed';

    setTimeout(() => {
      if (copyText) copyText.textContent = 'Copy';
    }, 2000);
  }
}

/**
 * Show visual feedback when copy succeeds (for quickstart section)
 * @param {HTMLElement} button - Copy button element
 */
function showCopyFeedbackSimple(button) {
  const textSpan = button.querySelector('.quickstart__copy-text, .copy-text');
  const originalText = textSpan ? textSpan.textContent : 'Copy';

  button.classList.add('copied');
  if (textSpan) {
    textSpan.textContent = 'Copied!';
  }

  setTimeout(() => {
    button.classList.remove('copied');
    if (textSpan) {
      textSpan.textContent = originalText;
    }
  }, 2000);
}

document.addEventListener('DOMContentLoaded', initClipboard);

if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    initClipboard,
    copyToClipboard,
    copyToClipboardSimple,
    showCopyFeedback,
    showCopyFeedbackSimple,
    extractCodeText,
    fallbackCopyToClipboard,
  };
}
