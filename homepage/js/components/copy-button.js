/**
 * Copy to Clipboard Module
 * Owner: Scenario 4 - Code Example Section with Copy Functionality
 *
 * Features:
 * - Uses Clipboard API with fallback
 * - Shows visual feedback on copy
 * - Accessible button labels
 */

/**
 * Copy text to clipboard using the Clipboard API
 * Falls back to execCommand for older browsers
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - Resolves to true if copy succeeded
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

    // Make the textarea invisible but still functional
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    textArea.setAttribute('readonly', '');
    textArea.setAttribute('aria-hidden', 'true');

    document.body.appendChild(textArea);
    textArea.select();
    textArea.setSelectionRange(0, text.length);

    const success = document.execCommand('copy');
    document.body.removeChild(textArea);

    return success;
  } catch (err) {
    console.error('Copy fallback failed:', err);
    return false;
  }
}

/**
 * Show visual feedback when copy succeeds
 * @param {HTMLButtonElement} button - The copy button element
 * @param {number} duration - How long to show feedback (ms)
 */
function showCopyFeedback(button, duration = 2000) {
  const textSpan = button.querySelector('.code-block__copy-text');
  const originalText = textSpan?.textContent || 'Copy';

  // Add copied state
  button.classList.add('code-block__copy--copied');
  if (textSpan) {
    textSpan.textContent = 'Copied!';
  }
  button.setAttribute('aria-label', 'Code copied to clipboard');

  // Reset after duration
  setTimeout(() => {
    button.classList.remove('code-block__copy--copied');
    if (textSpan) {
      textSpan.textContent = originalText;
    }
    button.setAttribute('aria-label', 'Copy code to clipboard');
  }, duration);
}

/**
 * Handle copy button click
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const codeBlock = button.closest('.code-block');

  if (!codeBlock) {
    console.error('Could not find parent code block');
    return;
  }

  const codeElement = codeBlock.querySelector('[data-code-content]');

  if (!codeElement) {
    console.error('Could not find code content');
    return;
  }

  const textToCopy = codeElement.textContent || '';
  const success = await copyToClipboard(textToCopy);

  if (success) {
    showCopyFeedback(button);
  }
}

/**
 * Initialize all copy buttons on the page
 * Should be called when DOM is ready
 */
export function initCopyButtons() {
  const copyButtons = document.querySelectorAll('[data-copy-button]');

  copyButtons.forEach(button => {
    button.addEventListener('click', handleCopyClick);
  });

  return copyButtons.length;
}

// Auto-initialize when used as a module
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
  } else {
    // DOM already loaded
    initCopyButtons();
  }
}
