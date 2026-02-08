/**
 * Clipboard Utility Functions
 * Owner: Scenario 3 - Installation Instructions
 *
 * Provides copy-to-clipboard functionality with fallback support.
 */

/**
 * Copies the provided text to the system clipboard.
 *
 * @param text - The text to copy to clipboard
 * @returns Promise<boolean> - true if copy was successful, false otherwise
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  // Modern Clipboard API
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Fall through to fallback
    }
  }

  // Fallback for older browsers or when Clipboard API is not available
  return fallbackCopyToClipboard(text);
}

/**
 * Fallback clipboard copy using execCommand.
 * Used when the Clipboard API is not available.
 */
function fallbackCopyToClipboard(text: string): boolean {
  const textArea = document.createElement('textarea');
  textArea.value = text;

  // Avoid scrolling to bottom
  textArea.style.top = '0';
  textArea.style.left = '0';
  textArea.style.position = 'fixed';
  textArea.style.opacity = '0';
  textArea.style.pointerEvents = 'none';

  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();

  let success = false;
  try {
    success = document.execCommand('copy');
  } catch {
    success = false;
  }

  document.body.removeChild(textArea);
  return success;
}
