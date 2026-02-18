/**
 * Smooth Scroll Module
 * Owner: Scenario 12 - Smooth Scrolling & Interactions
 *
 * Expected exports:
 * - initSmoothScroll(): Initialize smooth scrolling for anchor links
 * - scrollToElement(selector): Programmatically scroll to element
 *
 * Features:
 * - Smooth scrolling for all anchor links
 * - Account for sticky header offset
 * - Respect user's prefers-reduced-motion setting
 */

(function (root, factory) {
  // UMD pattern for browser and Node.js/CommonJS compatibility
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.SmoothScroll = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /**
   * Check if user prefers reduced motion
   * @returns {boolean} True if user prefers reduced motion
   */
  function prefersReducedMotion() {
    if (typeof window === 'undefined' || !window.matchMedia) {
      return false;
    }
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  /**
   * Get the height of the sticky header
   * @returns {number} Header height in pixels
   */
  function getHeaderOffset() {
    const header = document.querySelector('.header');
    if (!header) return 0;

    const style = window.getComputedStyle(header);
    const position = style.position;

    // Only account for sticky or fixed headers
    if (position === 'sticky' || position === 'fixed') {
      return header.offsetHeight || 0;
    }

    return 0;
  }

  /**
   * Scroll to a specific element on the page
   * @param {string|Element} target - CSS selector or DOM element to scroll to
   * @param {Object} options - Scroll options
   * @param {number} options.offset - Additional offset in pixels (default: 0)
   * @param {string} options.behavior - Scroll behavior: 'smooth' or 'auto' (default: 'smooth')
   */
  function scrollToElement(target, options = {}) {
    const element =
      typeof target === 'string' ? document.querySelector(target) : target;

    if (!element) {
      console.warn('scrollToElement: Target element not found:', target);
      return;
    }

    const {
      offset = 0,
      behavior = prefersReducedMotion() ? 'auto' : 'smooth',
    } = options;

    const headerOffset = getHeaderOffset();
    const elementPosition = element.getBoundingClientRect().top;
    const offsetPosition =
      elementPosition + window.pageYOffset - headerOffset - offset;

    window.scrollTo({
      top: offsetPosition,
      behavior: behavior,
    });
  }

  /**
   * Handle click events on anchor links
   * @param {Event} event - Click event
   */
  function handleAnchorClick(event) {
    const link = event.currentTarget;
    const href = link.getAttribute('href');

    // Only handle same-page anchor links
    if (!href || !href.startsWith('#') || href === '#') {
      return;
    }

    const targetId = href.substring(1);
    const targetElement = document.getElementById(targetId);

    if (targetElement) {
      event.preventDefault();

      // Update URL hash without jumping
      if (history.pushState) {
        history.pushState(null, null, href);
      }

      scrollToElement(targetElement);

      // Move focus to target for accessibility
      targetElement.setAttribute('tabindex', '-1');
      targetElement.focus({ preventScroll: true });
    }
  }

  /**
   * Initialize smooth scrolling for all anchor links
   */
  function initSmoothScroll() {
    // Find all anchor links pointing to same-page targets
    const anchorLinks = document.querySelectorAll('a[href^="#"]');

    anchorLinks.forEach(function (link) {
      const href = link.getAttribute('href');

      // Skip empty hashes or external links
      if (!href || href === '#') {
        return;
      }

      link.addEventListener('click', handleAnchorClick);
    });

    // Listen for changes to reduced motion preference
    if (typeof window !== 'undefined' && window.matchMedia) {
      const motionQuery = window.matchMedia('(prefers-reduced-motion: reduce)');

      // Handle preference changes
      if (motionQuery.addEventListener) {
        motionQuery.addEventListener('change', function () {
          // CSS will handle the visual changes, but log for debugging
          console.log(
            'Reduced motion preference changed:',
            motionQuery.matches ? 'enabled' : 'disabled'
          );
        });
      }
    }
  }

  // Return public API
  return {
    initSmoothScroll: initSmoothScroll,
    scrollToElement: scrollToElement,
    prefersReducedMotion: prefersReducedMotion,
    getHeaderOffset: getHeaderOffset,
  };
});
