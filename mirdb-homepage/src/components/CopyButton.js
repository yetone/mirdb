/**
 * Copy Button Component
 * Owner: Scenario 3 - Interactive Demo Section
 *
 * A reusable copy-to-clipboard button with visual feedback.
 */

import { copyToClipboard } from '../utils/clipboard.js'

/**
 * SVG icon for copy state
 */
const copyIcon = `<svg class="copy-icon w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"></path>
</svg>`

/**
 * SVG icon for success/check state
 */
const checkIcon = `<svg class="check-icon w-5 h-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
</svg>`

/**
 * Creates a copy button HTML string
 * @param {Object} props - Button properties
 * @param {string} props.text - Text to copy when button is clicked
 * @param {string} [props.id] - Optional unique identifier for the button
 * @param {string} [props.label] - Optional accessible label (defaults to 'Copy to clipboard')
 * @returns {string} HTML string for the copy button
 */
export function CopyButton({ text, id = '', label = 'Copy to clipboard' }) {
  const buttonId = id ? `copy-btn-${id}` : `copy-btn-${Date.now()}`

  return `
    <button
      type="button"
      class="copy-button inline-flex items-center justify-center p-2 rounded-md text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-700 transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2"
      data-copy-text="${escapeHtml(text)}"
      data-testid="copy-button-${id || 'default'}"
      id="${buttonId}"
      aria-label="${label}"
      title="${label}"
    >
      <span class="copy-icon-wrapper">
        ${copyIcon}
      </span>
      <span class="sr-only">${label}</span>
    </button>
  `
}

/**
 * Escape HTML special characters to prevent XSS
 * @param {string} str - String to escape
 * @returns {string} Escaped string safe for HTML attributes
 */
function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;')
}

/**
 * Initialize copy button event handlers
 * Call this after DOM content is loaded
 */
export function initCopyButtons() {
  document.addEventListener('click', async (event) => {
    const button = event.target.closest('.copy-button')
    if (!button) return

    const textToCopy = button.getAttribute('data-copy-text')
    if (!textToCopy) return

    const success = await copyToClipboard(textToCopy)

    if (success) {
      showCopySuccess(button)
    } else {
      showCopyError(button)
    }
  })
}

/**
 * Show success feedback on the copy button
 * @param {HTMLElement} button - The copy button element
 */
function showCopySuccess(button) {
  const iconWrapper = button.querySelector('.copy-icon-wrapper')
  const originalContent = iconWrapper.innerHTML

  // Show check icon
  iconWrapper.innerHTML = checkIcon
  button.classList.add('copy-success')
  button.setAttribute('aria-label', 'Copied!')

  // Show toast notification
  showToast('Copied to clipboard!', 'success')

  // Reset after delay
  setTimeout(() => {
    iconWrapper.innerHTML = originalContent
    button.classList.remove('copy-success')
    button.setAttribute('aria-label', 'Copy to clipboard')
  }, 2000)
}

/**
 * Show error feedback on the copy button
 * @param {HTMLElement} button - The copy button element
 */
function showCopyError(button) {
  button.classList.add('copy-error')
  showToast('Failed to copy', 'error')

  setTimeout(() => {
    button.classList.remove('copy-error')
  }, 2000)
}

/**
 * Display a toast notification
 * @param {string} message - Toast message
 * @param {string} type - Toast type ('success' or 'error')
 */
export function showToast(message, type = 'success') {
  // Remove existing toasts
  const existingToast = document.querySelector('.copy-toast')
  if (existingToast) {
    existingToast.remove()
  }

  const toast = document.createElement('div')
  toast.className = `copy-toast fixed bottom-4 right-4 px-4 py-2 rounded-lg shadow-lg transition-all duration-300 transform translate-y-0 opacity-100 z-50 ${
    type === 'success'
      ? 'bg-green-500 text-white'
      : 'bg-red-500 text-white'
  }`
  toast.setAttribute('role', 'status')
  toast.setAttribute('aria-live', 'polite')
  toast.setAttribute('data-testid', 'copy-toast')
  toast.textContent = message

  document.body.appendChild(toast)

  // Animate out and remove
  setTimeout(() => {
    toast.classList.add('opacity-0', 'translate-y-2')
    setTimeout(() => toast.remove(), 300)
  }, 2000)
}

export default CopyButton
