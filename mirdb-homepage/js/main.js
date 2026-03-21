/**
 * MirDB Homepage JavaScript
 *
 * Functions owned by Scenario 3 (Quick Start):
 * - copyToClipboard() - Copy installation command
 * - showCopyFeedback() - Display copy confirmation
 *
 * Functions owned by Scenario 7 (Dark Mode):
 * - initTheme() - Initialize theme from storage/system
 * - toggleTheme() - Switch between light/dark mode
 * - saveThemePreference() - Persist to localStorage
 *
 * Keep JavaScript minimal per PRD requirements.
 */

// === Copy Functionality (Scenario 3) ===

/**
 * Copy text to clipboard and show feedback
 * @param {HTMLButtonElement} button - The copy button element
 * @param {string} targetSelector - Selector for the code element to copy
 */
async function copyToClipboard(button, targetSelector) {
  const codeElement = document.querySelector(targetSelector);
  if (!codeElement) {
    console.error('Target element not found:', targetSelector);
    return;
  }

  const textToCopy = codeElement.textContent.trim();

  try {
    await navigator.clipboard.writeText(textToCopy);
    showCopyFeedback(button, true);
  } catch (err) {
    // Fallback for browsers that don't support clipboard API
    try {
      const textArea = document.createElement('textarea');
      textArea.value = textToCopy;
      textArea.style.position = 'fixed';
      textArea.style.left = '-9999px';
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      showCopyFeedback(button, true);
    } catch (fallbackErr) {
      console.error('Failed to copy:', fallbackErr);
      showCopyFeedback(button, false);
    }
  }
}

/**
 * Show visual feedback after copy action
 * @param {HTMLButtonElement} button - The copy button element
 * @param {boolean} success - Whether the copy was successful
 */
function showCopyFeedback(button, success) {
  const originalText = button.getAttribute('data-original-text') || button.textContent;
  const originalAriaLabel = button.getAttribute('aria-label');

  if (success) {
    button.classList.add('copied');
    button.textContent = 'Copied!';
    button.setAttribute('aria-label', 'Copied to clipboard');
  } else {
    button.textContent = 'Failed';
    button.setAttribute('aria-label', 'Failed to copy');
  }

  // Reset button after delay
  setTimeout(() => {
    button.classList.remove('copied');
    button.textContent = originalText;
    if (originalAriaLabel) {
      button.setAttribute('aria-label', originalAriaLabel);
    }
  }, 2000);
}

/**
 * Initialize copy buttons on the page
 */
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('[data-copy-target]');

  copyButtons.forEach((button) => {
    // Store original text
    button.setAttribute('data-original-text', button.textContent);

    button.addEventListener('click', () => {
      const targetSelector = button.getAttribute('data-copy-target');
      copyToClipboard(button, targetSelector);
    });
  });
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  initCopyButtons();
});

// === Theme Toggle (Scenario 7) ===
// Placeholder for dark mode functionality - owned by Scenario 7
