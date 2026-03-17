/**
 * Clipboard Module
 * Owner: Scenario 5 - Getting Started Section
 *
 * Exports:
 * - initClipboard(): Set up copy buttons on code blocks
 * - copyToClipboard(text): Copy text to clipboard
 *
 * Uses Clipboard API with fallback for older browsers
 */

/**
 * Copy text to clipboard using modern Clipboard API with fallback
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - Whether the copy was successful
 */
async function copyToClipboard(text) {
  try {
    // Modern Clipboard API
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
    console.error('Failed to copy text:', err);
    return false;
  }
}

/**
 * Show visual feedback on the copy button
 * @param {HTMLElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  const originalText = button.textContent;
  const originalAriaLabel = button.getAttribute('aria-label');

  button.textContent = success ? 'Copied!' : 'Failed';
  button.setAttribute('aria-label', success ? 'Copied to clipboard' : 'Copy failed');
  button.classList.add(success ? 'copy-btn--success' : 'copy-btn--error');

  setTimeout(() => {
    button.textContent = originalText;
    button.setAttribute('aria-label', originalAriaLabel);
    button.classList.remove('copy-btn--success', 'copy-btn--error');
  }, 2000);
}

/**
 * Create a copy button element
 * @returns {HTMLButtonElement} - The copy button element
 */
function createCopyButton() {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'copy-btn';
  button.textContent = 'Copy';
  button.setAttribute('aria-label', 'Copy code to clipboard');
  button.setAttribute('data-copy-btn', 'true');
  return button;
}

/**
 * Initialize clipboard functionality on all code blocks
 */
function initClipboard() {
  const codeBlocks = document.querySelectorAll('.code-block');

  codeBlocks.forEach((block) => {
    // Skip if already initialized
    if (block.querySelector('.copy-btn')) {
      return;
    }

    const pre = block.querySelector('pre');
    const code = block.querySelector('code');

    if (!pre || !code) {
      return;
    }

    // Ensure code block has syntax highlighting class
    if (!code.classList.contains('syntax-highlight')) {
      code.classList.add('syntax-highlight');
    }

    // Create and add copy button
    const copyBtn = createCopyButton();
    block.appendChild(copyBtn);

    // Add click handler
    copyBtn.addEventListener('click', async () => {
      const textToCopy = code.textContent || '';
      const success = await copyToClipboard(textToCopy);
      showCopyFeedback(copyBtn, success);
    });
  });
}

// Auto-initialize on DOMContentLoaded
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initClipboard);
} else {
  initClipboard();
}

// Export for use in other modules
if (typeof window !== 'undefined') {
  window.initClipboard = initClipboard;
  window.copyToClipboard = copyToClipboard;
}
