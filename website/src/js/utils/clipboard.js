/**
 * Clipboard Utility
 * Owner: Scenario 14 - Code Copy Functionality
 *
 * Expected exports:
 * - copyToClipboard(text: string): Promise<boolean> - Copy text to clipboard
 * - isClipboardSupported(): boolean - Check if clipboard API is available
 */

/**
 * Check if the Clipboard API is supported in the current browser
 * @returns {boolean} True if clipboard API is available
 */
export function isClipboardSupported() {
  return !!(
    navigator &&
    navigator.clipboard &&
    typeof navigator.clipboard.writeText === 'function'
  );
}

/**
 * Copy the provided text to the clipboard
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} True if copy was successful, false otherwise
 */
export async function copyToClipboard(text) {
  // Validate input
  if (typeof text !== 'string') {
    console.error('copyToClipboard: text must be a string');
    return false;
  }

  // Try modern Clipboard API first
  if (isClipboardSupported()) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      console.error('Clipboard API failed:', err);
      // Fall through to fallback
    }
  }

  // Fallback for older browsers using execCommand
  try {
    const textArea = document.createElement('textarea');
    textArea.value = text;

    // Make the textarea out of viewport
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    textArea.style.opacity = '0';
    textArea.setAttribute('readonly', '');
    textArea.setAttribute('aria-hidden', 'true');

    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    const success = document.execCommand('copy');
    document.body.removeChild(textArea);

    return success;
  } catch (err) {
    console.error('Fallback clipboard method failed:', err);
    return false;
  }
}
