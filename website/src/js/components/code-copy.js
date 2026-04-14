/**
 * Code Copy Component
 * Owner: Scenario 14 - Code Copy Functionality
 *
 * Expected exports:
 * - initCodeCopy(): void - Add copy buttons to code blocks
 * - handleCopyClick(event: Event): Promise<void> - Handle copy button click
 */

import { copyToClipboard } from '../utils/clipboard.js';

// Duration to show "Copied!" feedback in milliseconds
const FEEDBACK_DURATION = 2000;

/**
 * Initialize code copy functionality for all code blocks on the page
 * Adds click event listeners to all copy buttons
 */
export function initCodeCopy() {
  const copyButtons = document.querySelectorAll('.copy-button');

  copyButtons.forEach((button) => {
    // Remove any existing listeners to prevent duplicates
    button.removeEventListener('click', handleCopyClick);
    button.addEventListener('click', handleCopyClick);
  });
}

/**
 * Handle click event on a copy button
 * Copies the associated code content to clipboard and shows feedback
 * @param {Event} event - The click event
 * @returns {Promise<void>}
 */
export async function handleCopyClick(event) {
  const button = event.currentTarget;

  // Find the associated code block
  const codeBlock = button.closest('.code-block');
  if (!codeBlock) {
    console.error('Copy button not inside a code block');
    return;
  }

  // Find the code content
  const codeContent = codeBlock.querySelector('.code-content code');
  if (!codeContent) {
    console.error('No code content found in code block');
    return;
  }

  // Get the text content (strips HTML tags)
  const text = codeContent.textContent || '';

  // Copy to clipboard
  const success = await copyToClipboard(text);

  if (success) {
    showCopySuccess(button);
  } else {
    showCopyError(button);
  }
}

/**
 * Show success feedback on the copy button
 * @param {HTMLElement} button - The copy button element
 */
function showCopySuccess(button) {
  // Add copied class for styling
  button.classList.add('copied');

  // Update button text
  const copyText = button.querySelector('.copy-text');
  if (copyText) {
    copyText.textContent = 'Copied!';
  }

  // Show check icon, hide copy icon
  const copyIcon = button.querySelector('.copy-icon');
  const checkIcon = button.querySelector('.check-icon');

  if (copyIcon) {
    copyIcon.style.display = 'none';
  }
  if (checkIcon) {
    checkIcon.style.display = 'block';
  }

  // Reset after duration
  setTimeout(() => {
    resetCopyButton(button);
  }, FEEDBACK_DURATION);
}

/**
 * Show error feedback on the copy button
 * @param {HTMLElement} button - The copy button element
 */
function showCopyError(button) {
  button.classList.add('copy-error');

  const copyText = button.querySelector('.copy-text');
  if (copyText) {
    copyText.textContent = 'Failed';
  }

  setTimeout(() => {
    resetCopyButton(button);
  }, FEEDBACK_DURATION);
}

/**
 * Reset the copy button to its default state
 * @param {HTMLElement} button - The copy button element
 */
function resetCopyButton(button) {
  button.classList.remove('copied', 'copy-error');

  const copyText = button.querySelector('.copy-text');
  if (copyText) {
    copyText.textContent = 'Copy';
  }

  // Reset icons
  const copyIcon = button.querySelector('.copy-icon');
  const checkIcon = button.querySelector('.check-icon');

  if (copyIcon) {
    copyIcon.style.display = '';
  }
  if (checkIcon) {
    checkIcon.style.display = 'none';
  }
}
