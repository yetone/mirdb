/**
 * Copy to Clipboard Component
 * Owner: Scenario 4 - Usage Section with Code Examples
 *
 * Provides copy-to-clipboard functionality for code blocks
 * with visual feedback and accessibility support.
 */

/**
 * Initialize all copy buttons in the document
 * Attaches click event listeners to copy buttons within code blocks
 */
export function initCopyButtons() {
  const copyButtons = document.querySelectorAll('.code-block__copy');

  copyButtons.forEach(button => {
    button.addEventListener('click', handleCopyClick);
  });
}

/**
 * Handle click event on copy button
 * @param {Event} event - The click event
 */
function handleCopyClick(event) {
  const button = event.currentTarget;
  const codeBlock = button.closest('.code-block');

  if (!codeBlock) return;

  const codeElement = codeBlock.querySelector('code');
  if (!codeElement) return;

  const text = codeElement.textContent || '';
  copyText(text, button);
}

/**
 * Copy text to clipboard and show visual feedback
 * @param {string} text - The text to copy to clipboard
 * @param {HTMLElement} [button] - Optional button element to show feedback on
 * @returns {Promise<boolean>} - Returns true if copy was successful
 */
export async function copyText(text, button = null) {
  try {
    // Use modern Clipboard API if available
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      // Fallback for older browsers
      fallbackCopyText(text);
    }

    if (button) {
      showCopyFeedback(button, true);
    }

    return true;
  } catch (err) {
    console.error('Failed to copy text:', err);

    // Try fallback on Clipboard API failure
    try {
      fallbackCopyText(text);
      if (button) {
        showCopyFeedback(button, true);
      }
      return true;
    } catch (fallbackErr) {
      if (button) {
        showCopyFeedback(button, false);
      }
      return false;
    }
  }
}

/**
 * Fallback copy method using execCommand for older browsers
 * @param {string} text - The text to copy
 */
function fallbackCopyText(text) {
  const textArea = document.createElement('textarea');
  textArea.value = text;

  // Prevent scrolling to bottom of page on iOS
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

  try {
    document.execCommand('copy');
  } finally {
    document.body.removeChild(textArea);
  }
}

/**
 * Show visual feedback on the copy button
 * @param {HTMLElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  const copyIcon = button.querySelector('.copy-icon');
  const checkIcon = button.querySelector('.check-icon');
  const copyText = button.querySelector('.copy-text');

  if (success) {
    // Add copied class for styling
    button.classList.add('copied');

    // Update button content
    if (copyIcon) copyIcon.style.display = 'none';
    if (checkIcon) checkIcon.style.display = 'block';
    if (copyText) copyText.textContent = 'Copied!';

    // Update aria label for accessibility
    button.setAttribute('aria-label', 'Code copied to clipboard');

    // Reset after delay
    setTimeout(() => {
      resetCopyButton(button, copyIcon, checkIcon, copyText);
    }, 2000);
  } else {
    // Show error state briefly
    if (copyText) copyText.textContent = 'Failed';
    button.setAttribute('aria-label', 'Failed to copy code');

    setTimeout(() => {
      resetCopyButton(button, copyIcon, checkIcon, copyText);
    }, 2000);
  }
}

/**
 * Reset the copy button to its original state
 * @param {HTMLElement} button - The copy button element
 * @param {HTMLElement} copyIcon - The copy icon element
 * @param {HTMLElement} checkIcon - The check icon element
 * @param {HTMLElement} copyTextEl - The copy text element
 */
function resetCopyButton(button, copyIcon, checkIcon, copyTextEl) {
  button.classList.remove('copied');

  if (copyIcon) copyIcon.style.display = 'block';
  if (checkIcon) checkIcon.style.display = 'none';
  if (copyTextEl) copyTextEl.textContent = 'Copy';

  button.setAttribute('aria-label', 'Copy code to clipboard');
}
