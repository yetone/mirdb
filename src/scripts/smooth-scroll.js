/**
 * Smooth Scroll Utilities
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Features:
 * - Smooth scroll animation for navigation links
 * - Offset for sticky header
 * - Browser fallback support
 */

/**
 * Initialize smooth scroll behavior for all anchor links
 */
export function initSmoothScroll() {
  // CSS scroll-behavior: smooth handles most cases
  // This adds enhanced handling for header offset
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(link => {
    link.addEventListener('click', handleAnchorClick);
  });
}

/**
 * Handle anchor link click with smooth scroll
 * @param {Event} event - Click event
 */
function handleAnchorClick(event) {
  const href = event.currentTarget.getAttribute('href');

  // Skip if it's just "#" or empty
  if (!href || href === '#') return;

  const targetId = href.slice(1);
  const targetElement = document.getElementById(targetId);

  if (targetElement) {
    event.preventDefault();
    scrollToSection(targetId);
  }
}

/**
 * Scroll to a specific section by ID
 * @param {string} sectionId - The ID of the section to scroll to
 * @param {Object} options - Scroll options
 * @param {number} options.offset - Additional offset from top (default: header height)
 * @param {ScrollBehavior} options.behavior - Scroll behavior ('smooth' or 'auto')
 */
export function scrollToSection(sectionId, options = {}) {
  const section = document.getElementById(sectionId);
  if (!section) return;

  const header = document.getElementById('header');
  const headerHeight = header ? header.offsetHeight : 0;
  const offset = options.offset ?? headerHeight;
  const behavior = options.behavior ?? 'smooth';

  const targetPosition = section.getBoundingClientRect().top + window.scrollY - offset;

  // Use native smooth scroll if supported
  if ('scrollBehavior' in document.documentElement.style) {
    window.scrollTo({
      top: targetPosition,
      behavior: behavior
    });
  } else {
    // Fallback for older browsers
    smoothScrollFallback(targetPosition);
  }

  // Update URL hash without jumping
  if (history.pushState) {
    history.pushState(null, '', `#${sectionId}`);
  }
}

/**
 * Fallback smooth scroll for browsers that don't support CSS scroll-behavior
 * @param {number} targetPosition - Target scroll position
 */
function smoothScrollFallback(targetPosition) {
  const startPosition = window.scrollY;
  const distance = targetPosition - startPosition;
  const duration = 500;
  let startTime = null;

  function animation(currentTime) {
    if (startTime === null) startTime = currentTime;
    const timeElapsed = currentTime - startTime;
    const progress = Math.min(timeElapsed / duration, 1);

    // Easing function (ease-out-cubic)
    const easeOutCubic = 1 - Math.pow(1 - progress, 3);

    window.scrollTo(0, startPosition + distance * easeOutCubic);

    if (timeElapsed < duration) {
      requestAnimationFrame(animation);
    }
  }

  requestAnimationFrame(animation);
}

/**
 * Check if the browser supports CSS smooth scroll
 * @returns {boolean} True if smooth scroll is supported
 */
export function supportsSmoothScroll() {
  return 'scrollBehavior' in document.documentElement.style;
}

/**
 * Get the computed scroll-behavior value from the html element
 * @returns {string} The scroll-behavior CSS value
 */
export function getScrollBehavior() {
  const html = document.documentElement;
  return window.getComputedStyle(html).scrollBehavior || 'auto';
}

/**
 * Check if smooth scroll is enabled via CSS
 * @returns {boolean} True if scroll-behavior is 'smooth'
 */
export function isSmoothScrollEnabled() {
  return getScrollBehavior() === 'smooth';
}

// Auto-initialize on DOM ready
if (typeof document !== 'undefined') {
  document.addEventListener('DOMContentLoaded', initSmoothScroll);
}
