/**
 * Main JavaScript
 * Owner: Scenario 14 - Smooth Scrolling Navigation
 *
 * Expected exports/functionality:
 * - initSmoothScroll(): Initialize smooth scroll for anchor links
 * - handleReducedMotion(): Check prefers-reduced-motion
 *
 * Dependencies: None (vanilla JS)
 */

(function() {
  'use strict';

  /**
   * Check if user prefers reduced motion
   * @returns {boolean} - True if user prefers reduced motion
   */
  function prefersReducedMotion() {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  /**
   * Initialize smooth scrolling for anchor links
   */
  function initSmoothScroll() {
    // Skip smooth scroll if user prefers reduced motion
    if (prefersReducedMotion()) {
      return;
    }

    const anchorLinks = document.querySelectorAll('a[href^="#"]');

    anchorLinks.forEach(link => {
      link.addEventListener('click', (e) => {
        const targetId = link.getAttribute('href');
        if (targetId === '#') return;

        const targetElement = document.querySelector(targetId);
        if (targetElement) {
          e.preventDefault();
          targetElement.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
          });

          // Update focus for accessibility
          targetElement.setAttribute('tabindex', '-1');
          targetElement.focus({ preventScroll: true });
        }
      });
    });
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initSmoothScroll);
  } else {
    initSmoothScroll();
  }

  // Export functions for potential external use
  window.MirDBMain = {
    initSmoothScroll,
    prefersReducedMotion
  };
})();
