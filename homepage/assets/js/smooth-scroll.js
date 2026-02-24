/**
 * Smooth Scroll Module
 * Owner: Scenario 14 - Smooth Scroll Navigation
 *
 * Provides smooth scrolling functionality for anchor links with
 * proper offset accounting for fixed headers.
 *
 * Exports:
 * - initSmoothScroll(): Set up smooth scroll for anchor links
 * - scrollToElement(elementId, offset): Scroll to element with offset
 * - getScrollBehavior(): Get current CSS scroll-behavior value
 * - getHeaderOffset(): Calculate header offset dynamically
 */

/**
 * Default header selector for offset calculation
 * @type {string}
 */
const HEADER_SELECTOR = '.header, #site-header, header';

/**
 * Default fallback header height if header not found
 * @type {number}
 */
const DEFAULT_HEADER_HEIGHT = 64;

/**
 * Get the current CSS scroll-behavior value from the html element
 * @returns {string} The scroll-behavior value ('smooth', 'auto', or '')
 */
export function getScrollBehavior() {
  if (typeof window === 'undefined' || typeof document === 'undefined') {
    return '';
  }

  const htmlElement = document.documentElement;
  const computedStyle = window.getComputedStyle(htmlElement);

  return computedStyle.scrollBehavior || '';
}

/**
 * Calculate the header offset dynamically based on fixed header height
 * @returns {number} The header height in pixels
 */
export function getHeaderOffset() {
  if (typeof document === 'undefined') {
    return DEFAULT_HEADER_HEIGHT;
  }

  const header = document.querySelector(HEADER_SELECTOR);

  if (!header) {
    return DEFAULT_HEADER_HEIGHT;
  }

  // Check if header is fixed or sticky
  const style = window.getComputedStyle(header);
  const position = style.position;

  if (position === 'fixed' || position === 'sticky') {
    const rect = header.getBoundingClientRect();
    return rect.height;
  }

  // If header is not fixed, still return its height as it might affect layout
  const rect = header.getBoundingClientRect();
  return rect.height || DEFAULT_HEADER_HEIGHT;
}

/**
 * Scroll to an element by its ID with proper offset for fixed header
 * @param {string} elementId - The ID of the target element (with or without #)
 * @param {number} [offset] - Optional custom offset (defaults to header height)
 */
export function scrollToElement(elementId, offset) {
  if (typeof document === 'undefined' || typeof window === 'undefined') {
    return;
  }

  // Normalize element ID (remove # if present)
  const normalizedId = elementId.startsWith('#') ? elementId.slice(1) : elementId;

  if (!normalizedId) {
    return;
  }

  const targetElement = document.getElementById(normalizedId);

  if (!targetElement) {
    return;
  }

  // Calculate offset - use provided value or get header offset
  const headerOffset = typeof offset === 'number' ? offset : getHeaderOffset();

  // Calculate the scroll position
  const elementPosition = targetElement.getBoundingClientRect().top;
  const currentScroll = window.pageYOffset || window.scrollY || 0;
  const offsetPosition = elementPosition + currentScroll - headerOffset;

  // Scroll to the calculated position with smooth behavior
  window.scrollTo({
    top: offsetPosition,
    behavior: 'smooth'
  });
}

/**
 * Handle anchor link click events
 * @param {Event} event - The click event
 */
function handleAnchorClick(event) {
  const link = event.currentTarget;
  const href = link.getAttribute('href');

  // Ignore if href is just "#" or empty
  if (!href || href === '#') {
    return;
  }

  // Only handle internal anchor links
  if (!href.startsWith('#')) {
    return;
  }

  const targetId = href.slice(1);
  const targetElement = document.getElementById(targetId);

  if (targetElement) {
    event.preventDefault();

    // Scroll to the target element
    scrollToElement(targetId);

    // Update URL hash without triggering scroll
    if (window.history && window.history.pushState) {
      window.history.pushState(null, '', href);
    }
  }
}

/**
 * Initialize smooth scroll behavior for all anchor links
 * Sets up click event listeners on all links with href starting with #
 */
export function initSmoothScroll() {
  if (typeof document === 'undefined') {
    return;
  }

  // Find all internal anchor links
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  // Attach click handlers
  anchorLinks.forEach(link => {
    link.addEventListener('click', handleAnchorClick);
  });
}

// Export for CommonJS/testing environments
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    initSmoothScroll,
    scrollToElement,
    getScrollBehavior,
    getHeaderOffset
  };
}
