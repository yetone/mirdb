/**
 * Main JavaScript Entry Point
 * Owner: Scenario 5 (navigation), Scenario 8 (theme init)
 *
 * Expected content:
 * - DOM ready handler
 * - Theme initialization
 * - Smooth scroll navigation
 * - Mobile menu toggle
 * - Copy to clipboard functionality (Scenario 4)
 */

// Placeholder - to be implemented by Scenario 5 and 8
document.addEventListener('DOMContentLoaded', function() {
  // Theme initialization will be added by Scenario 8
  // Navigation functionality will be added by Scenario 5

  // ====================
  // COPY TO CLIPBOARD (Scenario 4)
  // ====================
  initCopyButtons();
});

/**
 * Initialize copy to clipboard functionality for code blocks
 * Owner: Scenario 4 - Quick Start Section
 */
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('[data-copy]');

  copyButtons.forEach(function(button) {
    button.addEventListener('click', async function() {
      const container = button.closest('.quick-start__code-container');
      if (!container) return;

      const codeBlock = container.querySelector('code');
      if (!codeBlock) return;

      const textToCopy = codeBlock.textContent;

      try {
        await navigator.clipboard.writeText(textToCopy);

        // Update button state
        const textSpan = button.querySelector('.quick-start__copy-text');
        const originalText = textSpan ? textSpan.textContent : 'Copy';

        if (textSpan) {
          textSpan.textContent = 'Copied!';
        }
        button.classList.add('copied');

        // Reset after delay
        setTimeout(function() {
          if (textSpan) {
            textSpan.textContent = originalText;
          }
          button.classList.remove('copied');
        }, 2000);
      } catch (err) {
        console.error('Failed to copy text:', err);
        // Fallback for older browsers
        fallbackCopyText(textToCopy);
      }
    });
  });
}

/**
 * Fallback copy function for browsers without clipboard API
 */
function fallbackCopyText(text) {
  const textArea = document.createElement('textarea');
  textArea.value = text;
  textArea.style.position = 'fixed';
  textArea.style.left = '-999999px';
  textArea.style.top = '-999999px';
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  try {
    document.execCommand('copy');
  } catch (err) {
    console.error('Fallback copy failed:', err);
  }

  document.body.removeChild(textArea);
}
