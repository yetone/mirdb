/**
 * Main JavaScript Entry Point
 * Owner: First Builder
 *
 * Initializes all JavaScript functionality:
 * - Navigation smooth scrolling
 * - Code copy functionality
 * - Mermaid diagram rendering
 * - Theme detection
 *
 * Imports and initializes modules from components/ and utils/
 */

import { initAndRenderMermaid } from './components/mermaid-init.js';
import { initNavigation } from './components/navigation.js';

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', async () => {
  // Initialize navigation (smooth scrolling, mobile menu, scroll effects)
  initNavigation();
  // Initialize code copy functionality
  initCodeCopy();

  // Initialize and render Mermaid diagrams
  await initAndRenderMermaid();
});


/**
 * Initialize code copy functionality for code blocks
 * Adds click handlers to copy buttons that copy code content to clipboard
 */
function initCodeCopy() {
  document.querySelectorAll('.copy-button').forEach(button => {
    button.addEventListener('click', async () => {
      const codeBlock = button.closest('.code-block');
      if (!codeBlock) return;

      const codeContent = codeBlock.querySelector('.code-content code');
      if (!codeContent) return;

      const text = codeContent.textContent || '';

      try {
        await navigator.clipboard.writeText(text);

        // Update button state to show success
        button.classList.add('copied');
        const copyText = button.querySelector('.copy-text');
        if (copyText) {
          copyText.textContent = 'Copied!';
        }

        // Reset after 2 seconds
        setTimeout(() => {
          button.classList.remove('copied');
          if (copyText) {
            copyText.textContent = 'Copy';
          }
        }, 2000);
      } catch (err) {
        console.error('Failed to copy text:', err);
      }
    });
  });
}
