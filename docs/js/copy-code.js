/**
 * MirDB Homepage - Copy Code Functionality
 * Owner: Scenario 3 - Getting Started Section with Code Examples
 *
 * Expected exports/functionality:
 * - initCopyButtons(): Add copy buttons to all code blocks
 * - copyToClipboard(text): Copy text to clipboard
 * - showCopyFeedback(button): Show visual feedback after copy
 */

(function() {
  'use strict';

  /**
   * Copy text to clipboard using the Clipboard API
   * @param {string} text - The text to copy
   * @returns {Promise<boolean>} - Success status
   */
  async function copyToClipboard(text) {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
      }
      // Fallback for older browsers
      return fallbackCopyToClipboard(text);
    } catch (err) {
      console.error('Failed to copy text:', err);
      return fallbackCopyToClipboard(text);
    }
  }

  /**
   * Fallback copy method using textarea
   * @param {string} text - The text to copy
   * @returns {boolean} - Success status
   */
  function fallbackCopyToClipboard(text) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
      const successful = document.execCommand('copy');
      document.body.removeChild(textArea);
      return successful;
    } catch (err) {
      console.error('Fallback copy failed:', err);
      document.body.removeChild(textArea);
      return false;
    }
  }

  /**
   * Show visual feedback after copy
   * @param {HTMLElement} button - The copy button element
   * @param {boolean} success - Whether the copy was successful
   */
  function showCopyFeedback(button, success) {
    const textSpan = button.querySelector('.copy-text');
    const originalText = textSpan ? textSpan.textContent : 'Copy';

    if (success) {
      button.classList.add('copied');
      if (textSpan) {
        textSpan.textContent = 'Copied!';
      }
    } else {
      if (textSpan) {
        textSpan.textContent = 'Failed';
      }
    }

    // Reset after delay
    setTimeout(function() {
      button.classList.remove('copied');
      if (textSpan) {
        textSpan.textContent = originalText;
      }
    }, 2000);
  }

  /**
   * Handle copy button click
   * @param {Event} event - Click event
   */
  async function handleCopyClick(event) {
    const button = event.currentTarget;
    const targetId = button.getAttribute('data-copy-target');

    if (!targetId) {
      console.error('Copy button missing data-copy-target attribute');
      return;
    }

    const codeElement = document.getElementById(targetId);
    if (!codeElement) {
      console.error('Code element not found:', targetId);
      return;
    }

    const text = codeElement.textContent || '';
    const success = await copyToClipboard(text);
    showCopyFeedback(button, success);
  }

  /**
   * Initialize copy buttons functionality
   */
  function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.copy-button');

    copyButtons.forEach(function(button) {
      button.addEventListener('click', handleCopyClick);

      // Keyboard support
      button.addEventListener('keydown', function(event) {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          handleCopyClick(event);
        }
      });
    });
  }

  // Expose functions globally
  window.MirDBCopyCode = {
    init: initCopyButtons,
    copy: copyToClipboard
  };

  // Initialize on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
  } else {
    initCopyButtons();
  }
})();
