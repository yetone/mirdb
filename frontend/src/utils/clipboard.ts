/**
 * Clipboard Utility
 * Owner: Scenario 2 - Guest URL Shortening
 *
 * Utility functions for clipboard operations.
 * Uses modern Clipboard API with fallback for older browsers.
 */

/**
 * Copies text to the clipboard
 * @param text - The text to copy to clipboard
 * @returns Promise<boolean> - true if successful, false otherwise
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  // Try modern Clipboard API first
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Fall through to fallback
    }
  }

  // Fallback for older browsers or non-secure contexts
  try {
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

    const successful = document.execCommand('copy');
    document.body.removeChild(textArea);

    return successful;
  } catch {
    return false;
  }
}
