/**
 * Clipboard Module
 * Owner: Scenario 3 - Code Examples Section
 *
 * Handles:
 * - Copy button click events
 * - Clipboard API interaction
 * - Visual feedback on copy success
 */

import { $$, supportsClipboard } from '../utils/helpers.js';

const COPIED_CLASS = 'copied';
const FEEDBACK_DURATION = 2000;

/**
 * Extract plain text from a code element, stripping HTML
 * @param {HTMLElement} codeElement - The code element to extract text from
 * @returns {string} Plain text content
 */
const extractCodeText = (codeElement) => {
  // Clone the element to avoid modifying the original
  const clone = codeElement.cloneNode(true);
  // Get the text content, which strips all HTML tags
  return clone.textContent || '';
};

/**
 * Copy text to clipboard using the Clipboard API
 * @param {string} text - Text to copy
 * @returns {Promise<boolean>} Success status
 */
export const copyToClipboard = async (text) => {
  if (!supportsClipboard()) {
    // Fallback for older browsers
    return fallbackCopy(text);
  }

  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (err) {
    console.error('Clipboard write failed:', err);
    return fallbackCopy(text);
  }
};

/**
 * Fallback copy method using textarea
 * @param {string} text - Text to copy
 * @returns {boolean} Success status
 */
const fallbackCopy = (text) => {
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.left = '-9999px';
  textarea.style.top = '0';
  textarea.setAttribute('readonly', '');
  document.body.appendChild(textarea);

  try {
    textarea.select();
    const success = document.execCommand('copy');
    document.body.removeChild(textarea);
    return success;
  } catch (err) {
    console.error('Fallback copy failed:', err);
    document.body.removeChild(textarea);
    return false;
  }
};

/**
 * Show visual feedback on copy button
 * @param {HTMLButtonElement} button - Copy button element
 * @param {boolean} success - Whether copy was successful
 */
const showFeedback = (button, success) => {
  const copyIcon = button.querySelector('.copy-icon');
  const checkIcon = button.querySelector('.check-icon');
  const copyText = button.querySelector('.copy-text');

  if (success) {
    button.classList.add(COPIED_CLASS);
    if (copyIcon) copyIcon.style.display = 'none';
    if (checkIcon) checkIcon.style.display = 'block';
    if (copyText) copyText.textContent = 'Copied!';

    setTimeout(() => {
      button.classList.remove(COPIED_CLASS);
      if (copyIcon) copyIcon.style.display = 'block';
      if (checkIcon) checkIcon.style.display = 'none';
      if (copyText) copyText.textContent = 'Copy';
    }, FEEDBACK_DURATION);
  }
};

/**
 * Handle copy button click
 * @param {Event} event - Click event
 */
const handleCopyClick = async (event) => {
  const button = event.currentTarget;
  const targetId = button.dataset.copyTarget;

  if (!targetId) {
    console.error('Copy button missing data-copy-target attribute');
    return;
  }

  const codeElement = document.getElementById(targetId);

  if (!codeElement) {
    console.error(`Code element not found: ${targetId}`);
    return;
  }

  const codeText = extractCodeText(codeElement);
  const success = await copyToClipboard(codeText);
  showFeedback(button, success);
};

/**
 * Initialize clipboard functionality
 * Attaches click handlers to all copy buttons
 */
export const init = () => {
  const copyButtons = $$('.copy-btn');

  copyButtons.forEach((button) => {
    button.addEventListener('click', handleCopyClick);
  });
};

// Export for testing
export { extractCodeText, showFeedback, handleCopyClick };
