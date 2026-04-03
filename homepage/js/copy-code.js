/**
 * Code Block Copy Functionality
 * Owner: Scenario 4 - Quick Start Section / Scenario 17 - Code Block Interactions
 *
 * Functionality:
 * - Add copy buttons to all code blocks
 * - Copy code content to clipboard on click
 * - Show visual feedback (button text change)
 * - Handle clipboard API with fallback
 */

(function() {
  'use strict';

  /**
   * Initialize copy functionality for all code blocks
   */
  function initCopyCode() {
    const copyButtons = document.querySelectorAll('.code-block__copy');

    copyButtons.forEach(button => {
      button.addEventListener('click', handleCopyClick);
    });
  }

  /**
   * Handle copy button click event
   * @param {Event} event - The click event
   */
  async function handleCopyClick(event) {
    const button = event.currentTarget;
    const codeBlock = button.closest('.code-block');

    if (!codeBlock) return;

    const codeElement = codeBlock.querySelector('code');
    if (!codeElement) return;

    const codeText = getCodeText(codeElement);

    try {
      await copyToClipboard(codeText);
      showCopySuccess(button);
    } catch (err) {
      console.error('Failed to copy code:', err);
      showCopyError(button);
    }
  }

  /**
   * Extract plain text from code element (strips HTML tags)
   * @param {HTMLElement} codeElement - The code element
   * @returns {string} - Plain text content
   */
  function getCodeText(codeElement) {
    // Clone the element to avoid modifying the original
    const clone = codeElement.cloneNode(true);

    // Get text content, which strips all HTML tags
    return clone.textContent || clone.innerText || '';
  }

  /**
   * Copy text to clipboard using modern API with fallback
   * @param {string} text - Text to copy
   * @returns {Promise<void>}
   */
  async function copyToClipboard(text) {
    // Try modern Clipboard API first
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }

    // Fallback for older browsers
    return fallbackCopyToClipboard(text);
  }

  /**
   * Fallback copy method using execCommand
   * @param {string} text - Text to copy
   * @returns {Promise<void>}
   */
  function fallbackCopyToClipboard(text) {
    return new Promise((resolve, reject) => {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.style.position = 'fixed';
      textarea.style.left = '-9999px';
      textarea.style.top = '-9999px';
      textarea.setAttribute('readonly', '');

      document.body.appendChild(textarea);
      textarea.select();

      try {
        const successful = document.execCommand('copy');
        document.body.removeChild(textarea);

        if (successful) {
          resolve();
        } else {
          reject(new Error('execCommand copy failed'));
        }
      } catch (err) {
        document.body.removeChild(textarea);
        reject(err);
      }
    });
  }

  /**
   * Show success feedback on copy button
   * @param {HTMLElement} button - The copy button element
   */
  function showCopySuccess(button) {
    const textElement = button.querySelector('.code-block__copy-text');
    const originalText = textElement ? textElement.textContent : 'Copy';

    // Update button state
    button.setAttribute('data-copy-state', 'copied');
    if (textElement) {
      textElement.textContent = 'Copied!';
    }

    // Reset after delay
    setTimeout(() => {
      button.setAttribute('data-copy-state', 'idle');
      if (textElement) {
        textElement.textContent = originalText;
      }
    }, 2000);
  }

  /**
   * Show error feedback on copy button
   * @param {HTMLElement} button - The copy button element
   */
  function showCopyError(button) {
    const textElement = button.querySelector('.code-block__copy-text');
    const originalText = textElement ? textElement.textContent : 'Copy';

    // Update button state
    button.setAttribute('data-copy-state', 'error');
    if (textElement) {
      textElement.textContent = 'Error';
    }

    // Reset after delay
    setTimeout(() => {
      button.setAttribute('data-copy-state', 'idle');
      if (textElement) {
        textElement.textContent = originalText;
      }
    }, 2000);
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCopyCode);
  } else {
    initCopyCode();
  }

  // Export for testing purposes
  if (typeof window !== 'undefined') {
    window.CopyCode = {
      init: initCopyCode,
      copyToClipboard: copyToClipboard,
      getCodeText: getCodeText
    };
  }
})();
