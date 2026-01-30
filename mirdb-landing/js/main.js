/**
 * Main JavaScript Entry Point
 * Owner: Scenario 2 - Navigation and Smooth Scrolling
 *
 * Imports and initializes:
 * - Smooth scroll functionality
 * - Tab interactions (Scenario 4)
 * - Copy to clipboard (Scenario 4)
 * - Scroll animations (Scenario 3)
 *
 * Progressive enhancement: Page works without JS
 */

(function() {
  'use strict';

  /**
   * Initialize all JavaScript functionality
   */
  function init() {
    // Initialize scroll animations if available
    if (typeof window.initScrollAnimations === 'function') {
      window.initScrollAnimations();
    }

    // Initialize smooth scroll if available
    if (typeof window.initSmoothScroll === 'function') {
      window.initSmoothScroll();
    }

    // Initialize tabs if available
    if (typeof window.initTabs === 'function') {
      window.initTabs();
    }

    // Initialize copy buttons if available
    if (typeof window.initCopyButtons === 'function') {
      window.initCopyButtons();
    }
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
