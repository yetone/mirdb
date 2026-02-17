/**
 * Clipboard API utilities.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Provides cross-browser clipboard functionality:
 * - Copy to clipboard with fallback
 * - Clipboard API feature detection
 */

/**
 * Checks if the Clipboard API is supported in the current browser
 * @returns true if navigator.clipboard is available
 */
export function isClipboardSupported(): boolean {
  return !!(navigator && navigator.clipboard && navigator.clipboard.writeText)
}

/**
 * Copies text to the clipboard with fallback for older browsers
 * @param text - The text to copy to the clipboard
 * @returns Promise that resolves to true if copy was successful, false otherwise
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  if (!text) {
    return false
  }

  // Try modern Clipboard API first
  if (isClipboardSupported()) {
    try {
      await navigator.clipboard.writeText(text)
      return true
    } catch (err) {
      // Fall through to fallback method
      console.warn('Clipboard API failed, trying fallback:', err)
    }
  }

  // Fallback for older browsers using execCommand
  try {
    const textArea = document.createElement('textarea')
    textArea.value = text

    // Prevent scrolling to bottom of page
    textArea.style.position = 'fixed'
    textArea.style.top = '0'
    textArea.style.left = '0'
    textArea.style.width = '2em'
    textArea.style.height = '2em'
    textArea.style.padding = '0'
    textArea.style.border = 'none'
    textArea.style.outline = 'none'
    textArea.style.boxShadow = 'none'
    textArea.style.background = 'transparent'

    document.body.appendChild(textArea)
    textArea.focus()
    textArea.select()

    const successful = document.execCommand('copy')
    document.body.removeChild(textArea)

    return successful
  } catch (err) {
    console.error('Fallback copy failed:', err)
    return false
  }
}
