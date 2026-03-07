/**
 * Copy to Clipboard Functionality
 * Owner: Scenarios 4, 15 - Quick Start and Copy Functionality
 *
 * Features:
 * - Copy code block content to clipboard
 * - Visual feedback on successful copy (icon change + text)
 * - Keyboard accessible
 * - Graceful fallback for older browsers
 */

/**
 * Copy text to clipboard using the Clipboard API
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} - True if copy was successful
 */
function copyToClipboard(text) {
  // Modern Clipboard API
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text).then(function() {
      return true;
    }).catch(function() {
      // Fallback to execCommand for older browsers
      return fallbackCopyToClipboard(text);
    });
  }

  // Fallback for browsers without Clipboard API
  return Promise.resolve(fallbackCopyToClipboard(text));
}

/**
 * Fallback copy method using execCommand
 * @param {string} text - Text to copy
 * @returns {boolean} - True if copy was successful
 */
function fallbackCopyToClipboard(text) {
  var textArea = document.createElement('textarea');
  textArea.value = text;
  textArea.style.position = 'fixed';
  textArea.style.left = '-9999px';
  textArea.style.top = '0';
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  var success = false;
  try {
    success = document.execCommand('copy');
  } catch (err) {
    success = false;
  }

  document.body.removeChild(textArea);
  return success;
}

/**
 * Show success feedback on copy button
 * @param {HTMLButtonElement} btn - The copy button element
 */
function showCopySuccess(btn) {
  var copyIcon = btn.querySelector('.code-block__copy-icon');
  var checkIcon = btn.querySelector('.code-block__check-icon');
  var textSpan = btn.querySelector('.code-block__copy-text');

  // Add copied class for styling
  btn.classList.add('code-block__copy-btn--copied');

  // Toggle icons
  if (copyIcon) copyIcon.style.display = 'none';
  if (checkIcon) checkIcon.style.display = 'inline';

  // Update text
  if (textSpan) textSpan.textContent = 'Copied!';

  // Reset after 2 seconds
  setTimeout(function() {
    btn.classList.remove('code-block__copy-btn--copied');
    if (copyIcon) copyIcon.style.display = 'inline';
    if (checkIcon) checkIcon.style.display = 'none';
    if (textSpan) textSpan.textContent = 'Copy';
  }, 2000);
}

/**
 * Initialize copy button event handlers
 */
function initCopyButtons() {
  var copyButtons = document.querySelectorAll('.code-block__copy-btn');

  copyButtons.forEach(function(btn) {
    btn.addEventListener('click', function(event) {
      event.preventDefault();

      var codeBlock = this.closest('.code-block');
      var code = codeBlock ? codeBlock.querySelector('code') : null;

      if (code) {
        var textToCopy = code.textContent;
        var currentBtn = this;

        copyToClipboard(textToCopy).then(function(success) {
          if (success) {
            showCopySuccess(currentBtn);
          }
        });
      }
    });

    // Keyboard accessibility - Enter and Space keys
    btn.addEventListener('keydown', function(event) {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        this.click();
      }
    });
  });
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initCopyButtons);
} else {
  initCopyButtons();
}
