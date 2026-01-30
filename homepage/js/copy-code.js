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
   * Copies text to clipboard using modern Clipboard API with fallback
   * @param {string} text - Text to copy
   * @returns {Promise<boolean>} - True if copy was successful
   */
  async function copyToClipboard(text) {
    try {
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

      const successful = document.execCommand('copy');
      document.body.removeChild(textArea);

      return successful;
    } catch (err) {
      console.error('Failed to copy to clipboard:', err);
      return false;
    }
  }

  /**
   * Shows feedback on button after copy action
   * @param {HTMLButtonElement} button - The copy button
   * @param {boolean} success - Whether copy was successful
   */
  function showCopyFeedback(button, success) {
    const originalText = button.querySelector('span').textContent;
    const feedbackText = success ? 'Copied!' : 'Failed';
    const feedbackClass = success ? 'usage__copy-btn--success' : 'usage__copy-btn--error';

    button.querySelector('span').textContent = feedbackText;
    button.classList.add(feedbackClass);

    setTimeout(function() {
      button.querySelector('span').textContent = originalText;
      button.classList.remove(feedbackClass);
    }, 2000);
  }

  /**
   * Handles click event on copy button
   * @param {Event} event - Click event
   */
  async function handleCopyClick(event) {
    const button = event.currentTarget;
    const wrapper = button.closest('.usage__code-wrapper');
    const codeBlock = wrapper.querySelector('code');

    if (codeBlock) {
      const text = codeBlock.textContent;
      const success = await copyToClipboard(text);
      showCopyFeedback(button, success);
    }
  }

  /**
   * Initializes copy buttons on all code blocks
   */
  function initCopyButtons() {
    const copyButtons = document.querySelectorAll('.usage__copy-btn');

    copyButtons.forEach(function(button) {
      // Remove any existing listeners to prevent duplicates
      button.removeEventListener('click', handleCopyClick);
      // Add click listener
      button.addEventListener('click', handleCopyClick);
    });
  }

  // Auto-initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyButtons);
  } else {
    initCopyButtons();
  }

  // Expose functions globally
  window.copyToClipboard = copyToClipboard;
  window.initCopyButtons = initCopyButtons;
})();
