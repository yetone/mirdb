/**
 * Clipboard functionality for code examples
 * Owner: Scenario 3 - Usage Examples Section
 *
 * Expected exports:
 * - initCopyButtons(): void - Initialize all copy buttons
 * - copyToClipboard(text: string): Promise<boolean> - Copy text to clipboard
 * - showCopyFeedback(button: Element, success: boolean): void - Visual feedback
 */

/**
 * Copy text to clipboard using the Clipboard API
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - True if successful, false otherwise
 */
async function copyToClipboard(text) {
  if (!text || typeof text !== 'string') {
    return false;
  }

  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
    // Fallback for older browsers
    return fallbackCopyToClipboard(text);
  } catch (error) {
    // Silently fail and return false
    return false;
  }
}

/**
 * Fallback copy method for browsers without Clipboard API
 * @param {string} text - The text to copy
 * @returns {boolean} - True if successful, false otherwise
 */
function fallbackCopyToClipboard(text) {
  try {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    const success = document.execCommand('copy');
    document.body.removeChild(textArea);
    return success;
  } catch (error) {
    return false;
  }
}

/**
 * Show visual feedback on the copy button
 * @param {Element} button - The button element to show feedback on
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  if (!button || !(button instanceof Element)) {
    return;
  }

  const copyText = button.querySelector('.copy-text');
  const originalText = copyText ? copyText.textContent : 'Copy';

  if (success) {
    button.classList.add('copied');
    if (copyText) {
      copyText.textContent = 'Copied!';
    }
  } else {
    button.classList.add('error');
    if (copyText) {
      copyText.textContent = 'Failed';
    }
  }

  // Reset after delay
  setTimeout(function() {
    button.classList.remove('copied', 'error');
    if (copyText) {
      copyText.textContent = originalText;
    }
  }, 2000);
}

/**
 * Initialize all copy buttons on the page
 */
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('.copy-btn');

  copyButtons.forEach(function(button) {
    button.addEventListener('click', async function(event) {
      event.preventDefault();

      // Get code from data attribute or from adjacent code block
      let codeText = button.getAttribute('data-code');

      if (!codeText) {
        // Try to find code in the parent container
        const container = button.closest('.code-block-container');
        if (container) {
          const codeBlock = container.querySelector('.code-block code');
          if (codeBlock) {
            codeText = codeBlock.textContent;
          }
        }
      }

      // Decode HTML entities in data-code attribute
      if (codeText) {
        codeText = codeText.replace(/&#10;/g, '\n');
      }

      const success = await copyToClipboard(codeText);
      showCopyFeedback(button, success);
    });
  });
}

// Export for testing (CommonJS/Node.js)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    copyToClipboard: copyToClipboard,
    showCopyFeedback: showCopyFeedback,
    initCopyButtons: initCopyButtons,
    fallbackCopyToClipboard: fallbackCopyToClipboard
  };
}
