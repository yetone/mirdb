/**
 * Copy-to-Clipboard Functionality
 * Owner: Scenario 4 - Quick Start Section
 *
 * Expected behavior:
 * - Attaches click handler to copy buttons
 * - Uses navigator.clipboard.writeText() with fallback
 * - Shows visual feedback (tooltip, icon change) on success
 * - Handles errors gracefully
 */

(function () {
  'use strict';

  /**
   * Fallback copy method using document.execCommand('copy')
   * for browsers that don't support navigator.clipboard.
   */
  function fallbackCopy(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.top = '-9999px';
    textarea.style.left = '-9999px';
    textarea.setAttribute('aria-hidden', 'true');
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();

    try {
      const result = document.execCommand('copy');
      document.body.removeChild(textarea);
      return result;
    } catch (err) {
      document.body.removeChild(textarea);
      return false;
    }
  }

  /**
   * Copy text to the clipboard using the Clipboard API
   * with execCommand fallback.
   */
  function copyToClipboard(text) {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(text).then(function () {
        return true;
      }).catch(function () {
        return fallbackCopy(text);
      });
    }
    return Promise.resolve(fallbackCopy(text));
  }

  /**
   * Show visual feedback on the copy button.
   */
  function showCopyFeedback(button) {
    button.classList.add('copy-success');
    window.setTimeout(function () {
      button.classList.remove('copy-success');
    }, 2000);
  }

  /**
   * Extract plain text from a code element, preserving line breaks.
   */
  function getCodeText(codeElement) {
    // Use textContent to get plain text without HTML tags
    return codeElement.textContent || '';
  }

  /**
   * Initialize copy buttons on the page.
   */
  function initCopyButtons() {
    const buttons = document.querySelectorAll('.copy-button[data-copy-target]');

    buttons.forEach(function (button) {
      // Prevent duplicate listeners
      if (button.dataset.copyInitialized === 'true') {
        return;
      }
      button.dataset.copyInitialized = 'true';

      button.addEventListener('click', function handleCopyClick(event) {
        event.preventDefault();

        const targetId = button.getAttribute('data-copy-target');
        const codeBlock = document.getElementById(targetId);

        if (!codeBlock) {
          console.warn('Copy target not found:', targetId);
          return;
        }

        const codeText = getCodeText(codeBlock);

        copyToClipboard(codeText).then(function (success) {
          if (success) {
            showCopyFeedback(button);
          } else {
            // Fallback: select the text for manual copy
            const selection = window.getSelection();
            const range = document.createRange();
            range.selectNodeContents(codeBlock);
            selection.removeAllRanges();
            selection.addRange(range);
          }
        }).catch(function () {
          // On error, try to select text for manual copy
          const selection = window.getSelection();
          const range = document.createRange();
          range.selectNodeContents(codeBlock);
          selection.removeAllRanges();
          selection.addRange(range);
        });
      });
    });
  }

  // Initialize on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
  } else {
    initCopyButtons();
  }
})();
