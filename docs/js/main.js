/**
 * MirDB Homepage - Main JavaScript
 * Owner: Scenario 4 - Navigation Links
 *
 * This file contains:
 * - Smooth scrolling for anchor links
 * - Mobile navigation toggle
 * - Event delegation setup
 *
 * Note: Site should be functional without JS (progressive enhancement)
 */

// Wait for DOM to be ready
document.addEventListener('DOMContentLoaded', function() {
  // Smooth scroll for anchor links
  document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
    anchor.addEventListener('click', function(e) {
      var target = document.querySelector(this.getAttribute('href'));
      if (target) {
        e.preventDefault();
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
});
