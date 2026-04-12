/**
 * Copy to Clipboard Module
 * Owner: Scenario 3 - Quickstart Installation Section
 *
 * Exports:
 * - initCopyButtons(): Set up copy functionality for code blocks
 * - copyToClipboard(text): Copy text to clipboard
 */

export async function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }

  // Fallback for browsers without Clipboard API
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand('copy');
  document.body.removeChild(textarea);
}

export function initCopyButtons() {
  const copyButtons = document.querySelectorAll('.copy-btn');

  copyButtons.forEach(button => {
    button.addEventListener('click', async () => {
      const codeBlock = button.closest('.code-block');
      const code = codeBlock.querySelector('code').textContent;

      try {
        await copyToClipboard(code);
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        setTimeout(() => {
          button.textContent = originalText;
        }, 2000);
      } catch (err) {
        console.error('Failed to copy:', err);
      }
    });
  });
}
