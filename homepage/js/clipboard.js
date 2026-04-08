/**
 * Copy to Clipboard Functionality
 * Owner: Scenario 13 - Copy Code Functionality
 *
 * Expected exports/functionality:
 * - initCopyButtons(): Add click handlers to all copy buttons
 * - copyToClipboard(text): Copy text to clipboard
 * - showCopyFeedback(button): Show success/error feedback on button
 *
 * Dependencies: None (vanilla JS, uses Clipboard API)
 */

(function() {
  'use strict';

  /**
   * Copy text to clipboard using the Clipboard API
   * @param {string} text - The text to copy
   * @returns {Promise<boolean>} - True if successful, false otherwise
   */
  async function copyToClipboard(text) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      // Fallback for older browsers
      try {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
        return true;
      } catch (fallbackErr) {
        console.error('Copy failed:', fallbackErr);
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
    const originalText = button.querySelector('span')?.textContent || 'Copy';
    const span = button.querySelector('span');

    if (success) {
      button.classList.add('copied');
      if (span) span.textContent = 'Copied!';
      button.setAttribute('aria-label', 'Copied to clipboard');
    } else {
      if (span) span.textContent = 'Failed';
    }

    setTimeout(() => {
      button.classList.remove('copied');
      if (span) span.textContent = originalText;
      button.setAttribute('aria-label', button.dataset.originalLabel || 'Copy');
    }, 2000);
  }

  /**
   * Initialize all copy buttons on the page
   */
  function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.install-copy-btn, [data-copy]');

    copyButtons.forEach(button => {
      // Store original label for later restoration
      button.dataset.originalLabel = button.getAttribute('aria-label') || 'Copy';

      button.addEventListener('click', async (e) => {
        e.preventDefault();

        // Get the text to copy from data-copy attribute or sibling code block
        let textToCopy = button.dataset.copy;

        if (!textToCopy) {
          const codeBlock = button.closest('.install-code-block');
          if (codeBlock) {
            const codeElement = codeBlock.querySelector('code');
            textToCopy = codeElement?.textContent?.trim() || '';
          }
        }

        if (textToCopy) {
          const success = await copyToClipboard(textToCopy);
          showCopyFeedback(button, success);
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

  // Export functions for potential external use
  window.MirDBClipboard = {
    copyToClipboard,
    showCopyFeedback,
    initCopyButtons
  };
})();
