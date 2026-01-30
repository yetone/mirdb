/**
 * MirDB Homepage - Main JavaScript
 * Owners: Scenario 1 (Navigation), Scenario 2 (Hero interactions)
 *
 * Responsibilities:
 * - Smooth scroll navigation
 * - Mobile hamburger menu toggle
 * - CTA button interactions
 * - General page interactions
 */

// Navigation functionality
// Hero section interactions
// Mobile menu handling

document.addEventListener('DOMContentLoaded', function() {
  // Initialize smooth scroll for anchor links
  const anchorLinks = document.querySelectorAll('a[href^="#"]');
  anchorLinks.forEach(function(link) {
    link.addEventListener('click', function(e) {
      const href = this.getAttribute('href');
      if (href !== '#') {
        const target = document.querySelector(href);
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
});
