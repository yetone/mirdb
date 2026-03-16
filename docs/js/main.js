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

(function() {
  'use strict';

  // Mobile navigation toggle
  function initMobileNav() {
    const navToggle = document.querySelector('.nav-toggle');
    const navLinks = document.querySelector('.nav-links');

    if (navToggle && navLinks) {
      navToggle.addEventListener('click', function() {
        const isOpen = navLinks.classList.toggle('open');
        navToggle.setAttribute('aria-expanded', isOpen);
      });

      // Close menu when clicking a link
      navLinks.addEventListener('click', function(e) {
        if (e.target.tagName === 'A') {
          navLinks.classList.remove('open');
          navToggle.setAttribute('aria-expanded', 'false');
        }
      });
    }
  }

  // Smooth scrolling for anchor links
  function initSmoothScroll() {
    document.addEventListener('click', function(e) {
      const target = e.target.closest('a[href^="#"]');
      if (!target) return;

      const hash = target.getAttribute('href');
      if (hash === '#') return;

      const element = document.querySelector(hash);
      if (element) {
        e.preventDefault();
        element.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });

        // Update URL hash without jumping
        history.pushState(null, null, hash);
      }
    });
  }

  // Initialize on DOM ready
  function init() {
    initMobileNav();
    initSmoothScroll();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
