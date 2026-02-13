/**
 * MirDB Homepage - Main JavaScript Entry Point
 *
 * This file initializes all JavaScript modules.
 * First builder creates the initialization structure.
 *
 * Imports and initializes:
 * - Theme toggle (Scenario 6)
 * - Copy to clipboard (Scenario 3)
 * - Smooth scroll (Scenario 8)
 */

// Initialize smooth scroll for hero CTA buttons
document.addEventListener('DOMContentLoaded', function() {
  // Initialize smooth scroll for anchor links
  initSmoothScroll();
});

/**
 * Initialize smooth scroll for anchor links
 * This is a basic implementation - Scenario 8 will enhance this
 */
function initSmoothScroll() {
  const anchors = document.querySelectorAll('a[href^="#"]');

  anchors.forEach(function(anchor) {
    anchor.addEventListener('click', function(e) {
      const href = this.getAttribute('href');

      // Skip if just '#' or empty
      if (!href || href === '#') return;

      const target = document.querySelector(href);

      if (target) {
        e.preventDefault();

        // Check for reduced motion preference
        const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

        target.scrollIntoView({
          behavior: prefersReducedMotion ? 'auto' : 'smooth',
          block: 'start'
        });

        // Update URL hash
        history.pushState(null, '', href);
      }
    });
  });
}
