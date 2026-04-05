/**
 * Clipboard Functionality
 * Owner: Scenario 3 - Quick Start Section
 */

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} True if copy succeeded
 */
export async function copyToClipboard(text) {
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
    } catch (fallbackErr) {
      document.body.removeChild(textArea);
      console.error('Copy failed:', fallbackErr);
      return false;
    }
  }
}

/**
 * Show copied feedback on button
 * @param {HTMLElement} button - Button element
 */
function showCopiedFeedback(button) {
  const textEl = button.querySelector('.quickstart__copy-text');
  const originalText = textEl ? textEl.textContent : 'Copy';

  button.classList.add('copied');
  if (textEl) {
    textEl.textContent = 'Copied!';
  }

  setTimeout(() => {
    button.classList.remove('copied');
    if (textEl) {
      textEl.textContent = originalText;
    }
  }, 2000);
}

/**
 * Handle copy button click
 * @param {Event} event - Click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const targetId = button.dataset.clipboardTarget;

  if (!targetId) {
    console.error('No clipboard target specified');
    return;
  }

  const codeElement = document.getElementById(targetId);
  if (!codeElement) {
    console.error('Target element not found:', targetId);
    return;
  }

  // Get text content (strips HTML tags for clean copy)
  const textToCopy = codeElement.textContent;

  const success = await copyToClipboard(textToCopy);
  if (success) {
    showCopiedFeedback(button);
  }
}

/**
 * Initialize clipboard functionality
 * Finds all copy buttons and attaches event listeners
 */
export function initClipboard() {
  const copyButtons = document.querySelectorAll('.quickstart__copy-btn');

  copyButtons.forEach(button => {
    button.addEventListener('click', handleCopyClick);
  });
}
