/**
 * Scroll Utilities
 * Owner: Scenario 8 - Navigation Component
 *
 * Provides smooth scrolling functionality with accessibility support,
 * fixed header offset handling, and reduced motion preference respect.
 */

/**
 * Get the height of the fixed navigation bar for offset calculations
 * @returns {number} Height in pixels of the navigation bar
 */
export function getNavbarHeight() {
  const navbar = document.querySelector('[data-testid="navigation"]')
  return navbar ? navbar.offsetHeight : 0
}

/**
 * Smooth scroll to an element by ID
 * Respects reduced motion preference and handles fixed header offset
 * @param {string} elementId - The ID of the element to scroll to (without #)
 * @returns {boolean} True if scroll was initiated, false if element not found
 */
export function smoothScrollTo(elementId) {
  const id = elementId.startsWith('#') ? elementId.slice(1) : elementId
  const element = document.getElementById(id)

  if (!element) {
    return false
  }

  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  const navbarHeight = getNavbarHeight()
  const elementPosition = element.getBoundingClientRect().top
  const offsetPosition = elementPosition + window.scrollY - navbarHeight

  window.scrollTo({
    top: offsetPosition,
    behavior: prefersReducedMotion ? 'auto' : 'smooth'
  })

  return true
}

/**
 * Get current scroll position
 * @returns {number} Current vertical scroll position in pixels
 */
export function getScrollPosition() {
  return window.scrollY || window.pageYOffset || document.documentElement.scrollTop || 0
}

/**
 * Check if an element is currently visible in the viewport
 * @param {string} elementId - The ID of the element to check (without #)
 * @returns {boolean} True if element is in view, false otherwise
 */
export function isElementInView(elementId) {
  const id = elementId.startsWith('#') ? elementId.slice(1) : elementId
  const element = document.getElementById(id)

  if (!element) {
    return false
  }

  const rect = element.getBoundingClientRect()
  const windowHeight = window.innerHeight || document.documentElement.clientHeight

  // Element is in view if any part of it is visible
  return rect.top < windowHeight && rect.bottom > 0
}

/**
 * Initialize smooth scroll for anchor links
 * Attaches click handlers to all anchor links that point to page sections
 */
export function initSmoothScroll() {
  document.addEventListener('click', (event) => {
    const anchor = event.target.closest('a[href^="#"]')
    if (!anchor) return

    const href = anchor.getAttribute('href')
    if (!href || href === '#') return

    const elementId = href.slice(1)
    const element = document.getElementById(elementId)

    if (element) {
      event.preventDefault()
      smoothScrollTo(elementId)

      // Update URL hash without jumping
      history.pushState(null, '', href)
    }
  })
}

export default {
  smoothScrollTo,
  getScrollPosition,
  isElementInView,
  getNavbarHeight,
  initSmoothScroll
}
