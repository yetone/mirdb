/**
 * Navigation and smooth scrolling functionality.
 * Owner: Scenario 7 - Navigation & Smooth Scrolling
 *
 * Expected behavior:
 * - Smooth scroll to anchor links
 * - Mobile hamburger menu toggle
 * - Active section highlighting during scroll
 * - Sticky header shadow on scroll
 * - Close mobile menu on link click
 * - Keyboard escape to close mobile menu
 */

(function () {
  'use strict';

  document.addEventListener('DOMContentLoaded', function () {
    const menuToggle = document.querySelector('.mobile-menu-toggle');
    const navLinks = document.querySelector('.nav-links');

    // Mobile menu toggle
    if (menuToggle && navLinks) {
      menuToggle.addEventListener('click', function () {
        const isOpen = navLinks.classList.toggle('open');
        menuToggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      });

      // Close mobile menu on link click
      navLinks.querySelectorAll('a').forEach(function (link) {
        link.addEventListener('click', function () {
          navLinks.classList.remove('open');
          menuToggle.setAttribute('aria-expanded', 'false');
        });
      });
    }

    // Keyboard escape to close mobile menu
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && navLinks && navLinks.classList.contains('open')) {
        navLinks.classList.remove('open');
        if (menuToggle) {
          menuToggle.setAttribute('aria-expanded', 'false');
        }
      }
    });

    // Smooth scroll for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(function (link) {
      link.addEventListener('click', function (e) {
        const href = this.getAttribute('href');
        if (href === '#') return;
        const target = document.querySelector(href);
        if (target) {
          e.preventDefault();
          target.scrollIntoView({ behavior: 'smooth' });
          // Move focus to target for accessibility
          target.setAttribute('tabindex', '-1');
          target.focus({ preventScroll: true });
        }
      });
    });
  });
})();
