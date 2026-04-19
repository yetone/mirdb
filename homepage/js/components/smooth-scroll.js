/**
 * Smooth Scroll Module
 * Owner: Scenario 15 - Smooth Scroll and Anchor Navigation
 *
 * Provides smooth scrolling for anchor links with accessibility considerations.
 * Works in conjunction with CSS scroll-behavior: smooth in reset.css.
 *
 * Features:
 * - Smooth scroll for anchor links
 * - Respects prefers-reduced-motion
 * - Updates URL hash without adding to history
 * - Handles focus management for accessibility
 */

/**
 * Check if user prefers reduced motion
 * @returns {boolean} True if user prefers reduced motion
 */
export function prefersReducedMotion() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/**
 * Scroll to an element by its ID
 * @param {string} id - The ID of the element to scroll to (without #)
 * @param {Object} options - Scroll options
 * @param {boolean} options.updateHash - Whether to update the URL hash (default: true)
 * @param {boolean} options.focus - Whether to focus the target element (default: true)
 * @returns {boolean} True if scroll was successful
 */
export function scrollToElement(id, options = {}) {
  const { updateHash = true, focus = true } = options;

  // Remove leading # if present
  const elementId = id.startsWith('#') ? id.slice(1) : id;
  const targetElement = document.getElementById(elementId);

  if (!targetElement) {
    console.warn(`Smooth scroll: Element with ID "${elementId}" not found`);
    return false;
  }

  // Determine scroll behavior based on user preference
  const scrollBehavior = prefersReducedMotion() ? 'auto' : 'smooth';

  // Scroll to the element
  targetElement.scrollIntoView({
    behavior: scrollBehavior,
    block: 'start',
  });

  // Update URL hash without adding to browser history
  if (updateHash) {
    history.replaceState(null, '', `#${elementId}`);
  }

  // Set focus on the target for accessibility (after scroll completes)
  if (focus) {
    // Make element focusable if it isn't already
    if (!targetElement.hasAttribute('tabindex')) {
      targetElement.setAttribute('tabindex', '-1');
    }

    // Use requestAnimationFrame to wait for scroll to start
    requestAnimationFrame(() => {
      targetElement.focus({ preventScroll: true });
    });
  }

  return true;
}

/**
 * Handle click events on anchor links
 * @param {Event} event - Click event
 */
function handleAnchorClick(event) {
  const link = event.currentTarget;
  const href = link.getAttribute('href');

  // Only handle internal anchor links
  if (!href || !href.startsWith('#')) {
    return;
  }

  // Prevent default anchor behavior
  event.preventDefault();

  // Get the target ID (remove the #)
  const targetId = href.slice(1);

  if (targetId) {
    scrollToElement(targetId);
  }
}

/**
 * Initialize smooth scroll behavior for all anchor links
 * @returns {number} Number of anchor links initialized
 */
export function initSmoothScroll() {
  // Find all internal anchor links
  const anchorLinks = document.querySelectorAll('a[href^="#"]');
  let count = 0;

  anchorLinks.forEach((link) => {
    const href = link.getAttribute('href');

    // Skip empty anchors or just "#"
    if (!href || href === '#') {
      return;
    }

    // Add click handler
    link.addEventListener('click', handleAnchorClick);
    count++;
  });

  // Handle initial hash on page load
  if (window.location.hash) {
    // Wait for page to fully load before scrolling
    requestAnimationFrame(() => {
      const targetId = window.location.hash.slice(1);
      if (targetId) {
        scrollToElement(targetId, { updateHash: false });
      }
    });
  }

  return count;
}

/**
 * Clean up smooth scroll event listeners
 * Call this when removing the module
 */
export function destroySmoothScroll() {
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach((link) => {
    link.removeEventListener('click', handleAnchorClick);
  });
}
