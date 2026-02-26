/**
 * Main JavaScript Entry Point
 * Owner: Shared
 *
 * Initializes all JavaScript modules and handles:
 * - DOM ready event
 * - Module initialization
 * - Global event listeners
 */

(function() {
  'use strict';

  /**
   * Initialize all modules when DOM is ready
   */
  function init() {
    // Initialize syntax highlighting (Scenario 3)
    if (window.SyntaxHighlight) {
      window.SyntaxHighlight.highlightCode();
    }

    // Initialize navigation (Scenario 5)
    if (window.Navigation) {
      window.Navigation.init();
    }

    // Set footer year dynamically
    var footerYear = document.getElementById('footer-year');
    if (footerYear) {
      footerYear.textContent = new Date().getFullYear();
    }

    console.log('MirDB Homepage initialized');
  }

  // Wait for DOM to be ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
