/**
 * Clipboard Component
 * Owner: Scenario 3 - Quick Start Code Examples
 *
 * Expected exports:
 * - initClipboard(): void - Initialize copy buttons
 * - copyToClipboard(text: string): Promise<boolean> - Copy text to clipboard
 * - showCopyFeedback(button: HTMLElement): void - Show success feedback
 *
 * Requirements: REQ-3
 */

/**
 * Initialize all copy buttons on the page.
 * Finds buttons with data-copy-target attribute and attaches click handlers.
 */
export function initClipboard() {
  const copyButtons = document.querySelectorAll('[data-copy-target]');

  copyButtons.forEach((button) => {
    button.addEventListener('click', handleCopyClick);
  });
}

/**
 * Handle click event on copy button.
 * @param {Event} event - The click event
 */
async function handleCopyClick(event) {
  const button = event.currentTarget;
  const targetId = button.getAttribute('data-copy-target');
  const targetElement = document.getElementById(targetId);

  if (!targetElement) {
    console.warn(`Copy target element not found: ${targetId}`);
    return;
  }

  // Get the text content (strip HTML tags from syntax highlighted code)
  const textToCopy = targetElement.textContent || '';

  const success = await copyToClipboard(textToCopy);

  if (success) {
    showCopyFeedback(button);
  }
}

/**
 * Copy text to the clipboard using the Clipboard API.
 * Falls back to execCommand for older browsers.
 * @param {string} text - The text to copy
 * @returns {Promise<boolean>} - True if copy was successful
 */
export async function copyToClipboard(text) {
  // Try modern Clipboard API first
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.warn('Clipboard API failed, falling back to execCommand:', err);
    }
  }

  // Fallback for older browsers
  return fallbackCopyToClipboard(text);
}

/**
 * Fallback copy method using a temporary textarea and execCommand.
 * @param {string} text - The text to copy
 * @returns {boolean} - True if copy was successful
 */
function fallbackCopyToClipboard(text) {
  const textarea = document.createElement('textarea');
  textarea.value = text;

  // Make it invisible and prevent scrolling
  textarea.style.position = 'fixed';
  textarea.style.top = '0';
  textarea.style.left = '0';
  textarea.style.width = '2em';
  textarea.style.height = '2em';
  textarea.style.padding = '0';
  textarea.style.border = 'none';
  textarea.style.outline = 'none';
  textarea.style.boxShadow = 'none';
  textarea.style.background = 'transparent';
  textarea.setAttribute('readonly', '');
  textarea.setAttribute('aria-hidden', 'true');

  document.body.appendChild(textarea);
  textarea.select();

  let success = false;
  try {
    success = document.execCommand('copy');
  } catch (err) {
    console.warn('execCommand copy failed:', err);
  }

  document.body.removeChild(textarea);
  return success;
}

/**
 * Show visual feedback on the copy button after successful copy.
 * Changes button text to "Copied!" and reverts after a delay.
 * @param {HTMLElement} button - The copy button element
 */
export function showCopyFeedback(button) {
  const textElement = button.querySelector('.quickstart__copy-text');
  const iconElement = button.querySelector('.quickstart__copy-icon');

  if (!textElement) return;

  const originalText = textElement.textContent;
  const originalIconPath = iconElement ? iconElement.innerHTML : '';

  // Add success class
  button.classList.add('quickstart__copy-btn--success');

  // Change text to "Copied!"
  textElement.textContent = 'Copied!';

  // Change icon to checkmark
  if (iconElement) {
    iconElement.innerHTML = '<path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/>';
  }

  // Revert after 2 seconds
  setTimeout(() => {
    button.classList.remove('quickstart__copy-btn--success');
    textElement.textContent = originalText;
    if (iconElement) {
      iconElement.innerHTML = originalIconPath;
    }
  }, 2000);
}
