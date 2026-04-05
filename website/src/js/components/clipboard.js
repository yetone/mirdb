/**
 * Clipboard Functionality
 * Owner: Scenario 3 - Quick Start Section
 *
 * Provides copy-to-clipboard functionality for code blocks
 * with visual feedback on successful copy.
 */

/**
 * Copy text to the user's clipboard
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} - True if copy succeeded
 */
export async function copyToClipboard(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (err) {
    // Fallback for older browsers or when clipboard API is unavailable
    try {
      const textArea = document.createElement('textarea');
      textArea.value = text;
      textArea.style.position = 'fixed';
      textArea.style.left = '-9999px';
      textArea.style.top = '-9999px';
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      return true;
    } catch (fallbackErr) {
      console.error('Failed to copy to clipboard:', fallbackErr);
      return false;
    }
  }
}

/**
 * Show visual feedback on the copy button
 * @param {HTMLElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  const textElement = button.querySelector('.quickstart__copy-text');
  const originalText = textElement.textContent;

  if (success) {
    textElement.textContent = 'Copied!';
    button.classList.add('copied');
  } else {
    textElement.textContent = 'Failed!';
  }

  // Reset after a delay
  setTimeout(() => {
    textElement.textContent = originalText;
    button.classList.remove('copied');
  }, 2000);
}

/**
 * Handle copy button click
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const targetId = button.getAttribute('data-clipboard-target');

  if (!targetId) {
    console.error('Copy button missing data-clipboard-target attribute');
    return;
  }

  const codeElement = document.getElementById(targetId);

  if (!codeElement) {
    console.error(`Code element with id "${targetId}" not found`);
    return;
  }

  const textToCopy = codeElement.textContent;
  const success = await copyToClipboard(textToCopy);
  showCopyFeedback(button, success);
}

/**
 * Initialize clipboard functionality
 * Finds all copy buttons and attaches click handlers
 */
export function initClipboard() {
  const copyButtons = document.querySelectorAll('.quickstart__copy-btn');

  copyButtons.forEach((button) => {
    button.addEventListener('click', handleCopyClick);
  });

  // Log initialization
  if (copyButtons.length > 0) {
    console.log(`Clipboard: Initialized ${copyButtons.length} copy button(s)`);
  }
}
