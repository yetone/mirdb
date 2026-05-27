/**
 * Navigation and smooth scrolling functionality.
 * Owner: Scenario 7 - Navigation & Smooth Scrolling
 *
 * Behavior:
 * - Smooth scroll to anchor links via scroll-behavior + JS fallback
 * - Mobile hamburger menu toggle
 * - Active section highlighting during scroll
 * - Close mobile menu on link click and Escape key
 */
(function () {
  'use strict';

  const hamburger = document.querySelector('.hamburger');
  const navLinks = document.querySelector('.nav-links');
  const navAnchors = document.querySelectorAll('.nav-links a[href^="#"]');
  const sections = document.querySelectorAll('section[id]');

  /** Toggle mobile navigation menu open/closed */
  function toggleMenu() {
    if (!hamburger || !navLinks) return;
    const isOpen = navLinks.classList.toggle('is-open');
    hamburger.setAttribute('aria-expanded', String(isOpen));
  }

  /** Close the mobile navigation menu */
  function closeMenu() {
    if (!hamburger || !navLinks) return;
    navLinks.classList.remove('is-open');
    hamburger.setAttribute('aria-expanded', 'false');
  }

  // Hamburger click handler
  if (hamburger) {
    hamburger.addEventListener('click', toggleMenu);
  }

  // Close menu when clicking a nav link
  navAnchors.forEach(function (anchor) {
    anchor.addEventListener('click', function () {
      closeMenu();
    });
  });

  // Close menu on Escape key
  document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape') {
      closeMenu();
    }
  });

  /**
   * Highlight the active section in the nav while scrolling.
   * Uses IntersectionObserver for performance.
   */
  function initActiveSection() {
    if (!window.IntersectionObserver || sections.length === 0) return;

    const observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            const id = entry.target.getAttribute('id');
            navAnchors.forEach(function (link) {
              link.classList.remove('active');
              if (link.getAttribute('href') === '#' + id) {
                link.classList.add('active');
              }
            });
          }
        });
      },
      {
        rootMargin: '-50% 0px -50% 0px',
        threshold: 0,
      }
    );

    sections.forEach(function (section) {
      observer.observe(section);
    });
  }

  initActiveSection();
})();
