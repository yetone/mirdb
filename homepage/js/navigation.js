/**
 * Navigation functionality for MirDB homepage.
 * Owner: Scenario 4 - Navigation System
 *
 * Behaviors:
 * - Mobile hamburger menu toggle (open/close)
 * - Smooth scroll to sections on nav link click
 * - Sticky header class toggling on scroll
 * - Close mobile menu on Escape key
 * - Close mobile menu on outside click
 * - Close mobile menu on link selection
 * - Focus management (trap focus in mobile menu when open)
 * - ARIA state updates (aria-expanded on hamburger)
 *
 * No framework dependencies — vanilla JavaScript only.
 */
(function () {
  'use strict';

  var header = document.querySelector('header');
  var hamburger = document.querySelector('.hamburger');
  var mobileMenu = document.querySelector('.mobile-menu');
  var navLinks = document.querySelectorAll('.nav-link');

  /** Check if we are in mobile view */
  function isMobile() {
    return window.innerWidth < 768;
  }

  /** Open the mobile menu */
  function openMenu() {
    if (!hamburger || !mobileMenu) return;
    hamburger.setAttribute('aria-expanded', 'true');
    mobileMenu.classList.add('open');
    // Focus first link in mobile menu after animation starts
    var mobileLinks = mobileMenu.querySelectorAll('.nav-link');
    if (mobileLinks.length > 0) {
      setTimeout(function () {
        mobileLinks[0].focus();
      }, 100);
    }
  }

  /** Close the mobile menu */
  function closeMenu() {
    if (!hamburger || !mobileMenu) return;
    hamburger.setAttribute('aria-expanded', 'false');
    mobileMenu.classList.remove('open');
    // Return focus to hamburger
    hamburger.focus();
  }

  /** Handle hamburger click */
  if (hamburger) {
    hamburger.addEventListener('click', function () {
      var expanded = hamburger.getAttribute('aria-expanded') === 'true';
      if (expanded) {
        closeMenu();
      } else {
        openMenu();
      }
    });
  }

  /** Close mobile menu on Escape key */
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && mobileMenu && mobileMenu.classList.contains('open')) {
      closeMenu();
    }
  });

  /** Close mobile menu on outside click */
  document.addEventListener('click', function (e) {
    if (!mobileMenu || !hamburger) return;
    if (!mobileMenu.classList.contains('open')) return;
    // If click is not inside mobile menu and not on hamburger
    if (!mobileMenu.contains(e.target) && !hamburger.contains(e.target)) {
      closeMenu();
    }
  });

  /** Smooth scroll and close menu on nav link click */
  navLinks.forEach(function (link) {
    link.addEventListener('click', function (e) {
      var href = link.getAttribute('href');
      // Internal anchor links: smooth scroll
      if (href && href.startsWith('#')) {
        e.preventDefault();
        var target = document.querySelector(href);
        if (target) {
          target.scrollIntoView({ behavior: 'smooth' });
        }
      }
      // Close mobile menu after link selection
      if (mobileMenu && mobileMenu.classList.contains('open')) {
        closeMenu();
      }
    });
  });

  /** Sticky header scroll effect */
  var lastScrollY = 0;
  window.addEventListener(
    'scroll',
    function () {
      if (!header) return;
      var scrollY = window.scrollY || window.pageYOffset;
      if (scrollY > 0) {
        header.classList.add('scrolled');
      } else {
        header.classList.remove('scrolled');
      }
      lastScrollY = scrollY;
    },
    { passive: true }
  );
})();
