/**
 * Scroll Utilities
 * Owner: Scenario 8 - Navigation Component
 *
 * Provides smooth scrolling functionality with support for fixed header offset
 * and reduced motion preferences.
 */

/**
 * Gets the height of the fixed navigation header for offset calculations
 * @returns {number} The height of the navigation header in pixels
 */
export function getHeaderOffset() {
  const nav = document.querySelector('[data-testid="navigation-bar"]')
  if (nav) {
    return nav.offsetHeight
  }
  return 0
}

/**
 * Checks if the user prefers reduced motion
 * @returns {boolean} True if user prefers reduced motion
 */
export function prefersReducedMotion() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches
}

/**
 * Smooth scrolls to an element by its ID
 * @param {string} elementId - The ID of the target element (without # prefix)
 * @returns {void}
 */
export function smoothScrollTo(elementId) {
  const element = document.getElementById(elementId)
  if (!element) {
    return
  }

  const headerOffset = getHeaderOffset()
  const elementPosition = element.getBoundingClientRect().top
  const offsetPosition = elementPosition + window.scrollY - headerOffset

  const behavior = prefersReducedMotion() ? 'auto' : 'smooth'

  window.scrollTo({
    top: offsetPosition,
    behavior: behavior
  })
}

/**
 * Gets the current scroll position
 * @returns {number} Current vertical scroll position in pixels
 */
export function getScrollPosition() {
  return window.scrollY || document.documentElement.scrollTop || 0
}

/**
 * Checks if an element is currently visible in the viewport
 * @param {string} elementId - The ID of the element to check
 * @returns {boolean} True if the element is visible in the viewport
 */
export function isElementInView(elementId) {
  const element = document.getElementById(elementId)
  if (!element) {
    return false
  }

  const rect = element.getBoundingClientRect()
  const windowHeight = window.innerHeight || document.documentElement.clientHeight

  // Element is considered in view if any part of it is visible
  return rect.top < windowHeight && rect.bottom > 0
}

/**
 * Handles navigation link clicks with smooth scrolling
 * @param {Event} event - The click event
 */
export function handleNavLinkClick(event) {
  const href = event.currentTarget.getAttribute('href')

  // Only handle internal anchor links
  if (href && href.startsWith('#')) {
    event.preventDefault()
    const targetId = href.substring(1)
    smoothScrollTo(targetId)

    // Update URL hash without triggering scroll
    history.pushState(null, '', href)
  }
}

export default {
  smoothScrollTo,
  getScrollPosition,
  isElementInView,
  getHeaderOffset,
  prefersReducedMotion,
  handleNavLinkClick
}
