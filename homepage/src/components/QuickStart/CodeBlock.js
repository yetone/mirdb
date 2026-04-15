/**
 * CodeBlock Component
 * Owner: Scenario 3 - Quick Start Section
 *
 * Provides syntax highlighting and copy-to-clipboard functionality
 * for code blocks in the Quick Start section.
 */

import { copyToClipboard, showCopyFeedback } from '../../scripts/clipboard.js';

/**
 * Initialize all code blocks on the page
 */
export function initCodeBlock() {
  const codeBlocks = document.querySelectorAll('.code-block');
  codeBlocks.forEach((block) => {
    addCopyButton(block);
  });
}

/**
 * Add copy functionality to a code block
 * @param {HTMLElement} element - The code block element
 */
export function addCopyButton(element) {
  const copyBtn = element.querySelector('.code-block__copy-btn');
  const codeContent = element.querySelector('.code-block__content code');

  if (!copyBtn || !codeContent) {
    return;
  }

  copyBtn.addEventListener('click', async () => {
    // Get the plain text content from the code block
    const code = codeContent.textContent || '';

    const success = await copyToClipboard(code);

    if (success) {
      showCopyFeedback(copyBtn);
    }
  });
}

/**
 * Get the code content from a code block element
 * @param {HTMLElement} element - The code block element
 * @returns {string} - The code content
 */
export function getCodeContent(element) {
  const codeElement = element.querySelector('.code-block__content code');
  return codeElement?.textContent || '';
}
