/**
 * Main JavaScript functionality.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Functions:
 * - Page initialization
 * - Scroll animations
 * - Mobile menu toggle (if applicable)
 */

(function() {
  'use strict';

  /**
   * Initialize the page when DOM is ready
   */
  function init() {
    // Add loaded class for CSS animations
    document.body.classList.add('loaded');

    // Initialize smooth scroll for internal links
    initSmoothScroll();
  }

  /**
   * Initialize smooth scrolling for anchor links
   */
  function initSmoothScroll() {
    document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
      anchor.addEventListener('click', function(e) {
        var href = this.getAttribute('href');
        if (href && href !== '#') {
          var target = document.querySelector(href);
          if (target) {
            e.preventDefault();
            target.scrollIntoView({
              behavior: 'smooth',
              block: 'start'
            });
          }
        }
      });
    });
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
