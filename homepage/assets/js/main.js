/**
 * Main JavaScript Entry Point
 * Owner: First scenario builder
 *
 * Initializes all modules on page load.
 */

import { initMobileNav } from './mobile-nav.js';

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  // Initialize smooth scroll for anchor links
  initSmoothScroll();

  // Initialize mobile navigation (Scenario 2)
  initMobileNav();
});

/**
 * Initialize smooth scroll for anchor links
 * Basic implementation - Scenario 14 will enhance this
 */
function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
      const targetId = this.getAttribute('href');
      if (targetId === '#') return;

      const targetElement = document.querySelector(targetId);
      if (targetElement) {
        e.preventDefault();
        const headerOffset = 64; // header height
        const elementPosition = targetElement.getBoundingClientRect().top;
        const offsetPosition = elementPosition + window.pageYOffset - headerOffset;

        window.scrollTo({
          top: offsetPosition,
          behavior: 'smooth'
        });
      }
    });
  });
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initSmoothScroll };
}
