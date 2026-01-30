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

(function() {
  'use strict';

  // Wait for DOM to be ready
  document.addEventListener('DOMContentLoaded', function() {
    initSmoothScrolling();
  });

  /**
   * Initialize smooth scrolling for anchor links
   * Handles navigation links that point to page sections
   */
  function initSmoothScrolling() {
    // Handle both header nav links and all anchor links (for hero CTAs etc)
    const anchorLinks = document.querySelectorAll('a[href^="#"]');

    anchorLinks.forEach(function(link) {
      link.addEventListener('click', function(event) {
        const href = this.getAttribute('href');

        // Skip if href is just "#" or empty
        if (!href || href === '#') {
          return;
        }

        const targetElement = document.querySelector(href);

        if (targetElement) {
          event.preventDefault();

          // Calculate offset to account for sticky header
          const header = document.querySelector('.header');
          const headerHeight = header ? header.offsetHeight : 0;
          const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset - headerHeight;

          window.scrollTo({
            top: targetPosition,
            behavior: 'smooth'
          });

          // Update URL hash without jumping
          history.pushState(null, null, href);
        }
      });
    });
  }

  // Mobile menu handling will be added by responsive scenarios
})();
