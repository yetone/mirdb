/**
 * Main Entry Point
 *
 * Initializes all scripts and components.
 */

// Theme is already initialized via theme.js which runs before DOM ready
// ThemeToggle component auto-initializes on DOM ready

// Copy to clipboard functionality for code blocks
function initCopyButtons() {
  const copyButtons = document.querySelectorAll('.code-block__copy');

  copyButtons.forEach(button => {
    button.addEventListener('click', async () => {
      const codeBlock = button.closest('.code-block');
      const code = codeBlock?.querySelector('code');

      if (code) {
        try {
          await navigator.clipboard.writeText(code.textContent || '');
          button.classList.add('copied');
          const span = button.querySelector('span');
          if (span) span.textContent = 'Copied!';

          setTimeout(() => {
            button.classList.remove('copied');
            if (span) span.textContent = 'Copy';
          }, 2000);
        } catch (err) {
          console.error('Failed to copy:', err);
        }
      }
    });
  });
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  initCopyButtons();
});
