/**
 * Copy to Clipboard Module
 * Owner: Scenario 3 - Code Example Section
 *
 * Expected exports:
 * - initCopyButtons(): void - Attach listeners to copy buttons
 * - copyText(text: string): Promise<boolean> - Copy text to clipboard
 *
 * Features:
 * - Visual feedback on successful copy
 * - Fallback for older browsers
 * - Accessibility announcements
 */

/**
 * Copy text to clipboard using the Clipboard API with fallback
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - Returns true if copy was successful
 */
async function copyText(text) {
  if (!text) {
    return false;
  }

  // Try modern Clipboard API first
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      // Clipboard API failed, try fallback
      console.warn('Clipboard API failed, using fallback:', err);
    }
  }

  // Fallback for older browsers using execCommand
  return copyTextFallback(text);
}

/**
 * Fallback copy method using document.execCommand
 * @param {string} text - The text to copy
 * @returns {boolean} - Returns true if copy was successful
 */
function copyTextFallback(text) {
  var textArea = document.createElement('textarea');
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

  var success = false;
  try {
    success = document.execCommand('copy');
  } catch (err) {
    console.error('Fallback copy failed:', err);
  }

  document.body.removeChild(textArea);
  return success;
}

/**
 * Show visual feedback when copy is successful
 * @param {HTMLButtonElement} button - The copy button element
 */
function showCopyFeedback(button) {
  button.classList.add('code-block__copy--copied');

  // Update text if present
  var textElement = button.querySelector('.code-block__copy-text');
  if (textElement) {
    textElement.textContent = 'Copied!';
  }

  // Announce to screen readers
  announceToScreenReader('Code copied to clipboard');

  // Reset after delay
  setTimeout(function() {
    button.classList.remove('code-block__copy--copied');
    if (textElement) {
      textElement.textContent = 'Copy';
    }
  }, 2000);
}

/**
 * Announce message to screen readers
 * @param {string} message - Message to announce
 */
function announceToScreenReader(message) {
  var announcement = document.createElement('div');
  announcement.setAttribute('role', 'status');
  announcement.setAttribute('aria-live', 'polite');
  announcement.setAttribute('aria-atomic', 'true');
  announcement.className = 'visually-hidden';
  announcement.textContent = message;

  document.body.appendChild(announcement);

  setTimeout(function() {
    document.body.removeChild(announcement);
  }, 1000);
}

/**
 * Get the text content from a code block, stripping HTML but preserving formatting
 * @param {HTMLElement} element - The element containing the code
 * @returns {string} - The plain text content
 */
function getCodeText(element) {
  // Clone the element to avoid modifying the original
  var clone = element.cloneNode(true);

  // Get text content which strips HTML tags but preserves text
  return clone.textContent || clone.innerText || '';
}

/**
 * Handle click event on copy button
 * @param {Event} event - The click event
 */
function handleCopyClick(event) {
  var button = event.currentTarget;
  var targetId = button.getAttribute('data-copy-target');

  if (!targetId) {
    console.warn('Copy button missing data-copy-target attribute');
    return;
  }

  var targetElement = document.getElementById(targetId);
  if (!targetElement) {
    console.warn('Target element with id "' + targetId + '" not found');
    return;
  }

  var textToCopy = getCodeText(targetElement);
  copyText(textToCopy).then(function(success) {
    if (success) {
      showCopyFeedback(button);
    }
  });
}

/**
 * Initialize all copy buttons on the page
 */
function initCopyButtons() {
  var copyButtons = document.querySelectorAll('[data-copy-target]');

  copyButtons.forEach(function(button) {
    // Add click listener
    button.addEventListener('click', handleCopyClick);

    // Support keyboard activation
    button.addEventListener('keydown', function(event) {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        handleCopyClick(event);
      }
    });
  });
}

// Make functions available globally for non-module usage
if (typeof window !== 'undefined') {
  window.copyText = copyText;
  window.initCopyButtons = initCopyButtons;
}
