/**
 * MirDB Homepage - Copy to Clipboard
 * Owner: Scenario 5 - Usage and Code Examples
 *
 * Adds copy-to-clipboard functionality to code blocks
 * Shows feedback on successful copy
 *
 * Expected exports:
 * - initCopyButtons(): Initialize copy buttons on all code blocks
 * - copyToClipboard(text): Copy text to clipboard with feedback
 */

(function() {
  'use strict';

  /**
   * Copy text to clipboard
   * @param {string} text - Text to copy
   * @returns {Promise<boolean>} - Success status
   */
  async function copyToClipboard(text) {
    try {
      // Modern clipboard API
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
      }

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
        return true;
      } finally {
        document.body.removeChild(textArea);
      }
    } catch (err) {
      console.error('Failed to copy text:', err);
      return false;
    }
  }

  /**
   * Show copy feedback on button
   * @param {HTMLElement} button - Copy button element
   * @param {boolean} success - Whether copy was successful
   */
  function showCopyFeedback(button, success) {
    const originalContent = button.innerHTML;
    const feedbackClass = success ? 'usage__copy-btn--success' : 'usage__copy-btn--error';

    // Update button content
    if (success) {
      button.innerHTML = `
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <polyline points="20 6 9 17 4 12"></polyline>
        </svg>
        <span>Copied!</span>
      `;
    } else {
      button.innerHTML = `
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <line x1="18" y1="6" x2="6" y2="18"></line>
          <line x1="6" y1="6" x2="18" y2="18"></line>
        </svg>
        <span>Error</span>
      `;
    }

    button.classList.add(feedbackClass);

    // Reset after delay
    setTimeout(function() {
      button.innerHTML = originalContent;
      button.classList.remove(feedbackClass);
    }, 2000);
  }

  /**
   * Get code text from a code block
   * @param {string} targetId - ID of the code element
   * @returns {string} - Code text
   */
  function getCodeText(targetId) {
    const codeElement = document.getElementById(targetId);
    if (!codeElement) {
      return '';
    }

    // Get the code element inside pre, or the pre itself
    const code = codeElement.querySelector('code') || codeElement;
    return code.textContent || '';
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

    const codeText = getCodeText(targetId);
    if (!codeText) {
      console.error('Could not find code element:', targetId);
      showCopyFeedback(button, false);
      return;
    }

    const success = await copyToClipboard(codeText);
    showCopyFeedback(button, success);
  }

  /**
   * Initialize copy buttons on all code blocks
   */
  function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.usage__copy-btn');

    copyButtons.forEach(function(button) {
      // Remove any existing listeners to prevent duplicates
      button.removeEventListener('click', handleCopyClick);
      button.addEventListener('click', handleCopyClick);
    });
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
  } else {
    initCopyButtons();
  }

  // Expose functions for external use
  window.CopyCode = {
    init: initCopyButtons,
    copyToClipboard: copyToClipboard
  };
})();
