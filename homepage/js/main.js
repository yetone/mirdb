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
    initMobileMenu();
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

  /**
   * Initialize mobile hamburger menu
   * Handles toggle button click and menu visibility
   */
  function initMobileMenu() {
    var menuToggle = document.querySelector('.header__menu-toggle');
    var nav = document.querySelector('.header__nav');

    if (!menuToggle || !nav) {
      return;
    }

    menuToggle.addEventListener('click', function() {
      var isExpanded = menuToggle.getAttribute('aria-expanded') === 'true';
      menuToggle.setAttribute('aria-expanded', !isExpanded);
      nav.classList.toggle('header__nav--open');
    });

    // Close menu when clicking on a nav link
    var navLinks = nav.querySelectorAll('.header__nav-link');
    navLinks.forEach(function(link) {
      link.addEventListener('click', function() {
        menuToggle.setAttribute('aria-expanded', 'false');
        nav.classList.remove('header__nav--open');
      });
    });

    // Close menu when clicking outside
    document.addEventListener('click', function(event) {
      var isClickInsideNav = nav.contains(event.target);
      var isClickOnToggle = menuToggle.contains(event.target);

      if (!isClickInsideNav && !isClickOnToggle && nav.classList.contains('header__nav--open')) {
        menuToggle.setAttribute('aria-expanded', 'false');
        nav.classList.remove('header__nav--open');
      }
    });

    // Close menu on escape key
    document.addEventListener('keydown', function(event) {
      if (event.key === 'Escape' && nav.classList.contains('header__nav--open')) {
        menuToggle.setAttribute('aria-expanded', 'false');
        nav.classList.remove('header__nav--open');
        menuToggle.focus();
      }
    });
  }
})();
