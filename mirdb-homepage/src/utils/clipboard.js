/**
 * Clipboard Utilities
 * Owner: Scenario 3 - Interactive Demo Section
 *
 * Provides clipboard functionality with modern Clipboard API and legacy fallback.
 */

/**
 * Copy text to clipboard using the modern Clipboard API with fallback
 * @param {string} text - The text to copy to clipboard
 * @returns {Promise<boolean>} - True if copy was successful, false otherwise
 */
export async function copyToClipboard(text) {
  // Try modern Clipboard API first
  if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
    try {
      await navigator.clipboard.writeText(text)
      return true
    } catch (err) {
      console.warn('Clipboard API failed, trying fallback:', err)
      return fallbackCopyToClipboard(text)
    }
  }

  // Fallback for older browsers
  return fallbackCopyToClipboard(text)
}

/**
 * Fallback copy method using execCommand (deprecated but widely supported)
 * @param {string} text - The text to copy
 * @returns {boolean} - True if copy was successful
 */
function fallbackCopyToClipboard(text) {
  const textArea = document.createElement('textarea')
  textArea.value = text

  // Make the textarea invisible and position off-screen
  textArea.style.cssText = `
    position: fixed;
    top: -9999px;
    left: -9999px;
    width: 1px;
    height: 1px;
    opacity: 0;
    pointer-events: none;
  `

  document.body.appendChild(textArea)
  textArea.focus()
  textArea.select()

  let success = false
  try {
    success = document.execCommand('copy')
  } catch (err) {
    console.error('Fallback copy failed:', err)
    success = false
  }

  document.body.removeChild(textArea)
  return success
}

export default copyToClipboard
