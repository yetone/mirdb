/**
 * Navigation Module
 * Owner: Scenario 7 - Navigation and Repository Links
 *
 * Expected exports:
 * - initNavigation(): Set up navigation handlers
 * - smoothScrollTo(sectionId): Animate scroll to section
 * - toggleMobileMenu(): Show/hide mobile navigation
 *
 * Features:
 * - Smooth scroll for anchor links
 * - Mobile hamburger menu toggle
 * - Active section highlighting
 */

(function() {
  'use strict';

  // Configuration
  const CONFIG = {
    scrollOffset: 80, // Header height offset for scroll position
    scrollDuration: 800, // Duration of smooth scroll animation in ms
    activeClass: 'header__nav-link--active',
    menuOpenClass: 'header--menu-open',
    bodyMenuOpenClass: 'body--menu-open'
  };

  // DOM Elements
  let menuToggle = null;
  let mainNav = null;
  let navLinks = null;
  let header = null;

  /**
   * Initialize navigation functionality
   */
  function initNavigation() {
    // Get DOM elements
    menuToggle = document.getElementById('menu-toggle');
    mainNav = document.getElementById('main-nav');
    header = document.getElementById('header');
    navLinks = document.querySelectorAll('.header__nav-link[href^="#"]');

    if (!header) {
      console.warn('Navigation: Header element not found');
      return;
    }

    // Set up event listeners
    setupSmoothScroll();
    setupMobileMenu();
    setupActiveStateOnScroll();
    setupHeaderScrollBehavior();
  }

  /**
   * Set up smooth scrolling for anchor links
   */
  function setupSmoothScroll() {
    // Handle all anchor links in the page
    document.addEventListener('click', function(e) {
      const link = e.target.closest('a[href^="#"]');

      if (!link) return;

      const targetId = link.getAttribute('href');

      // Skip if it's just "#" or the skip link
      if (targetId === '#' || link.classList.contains('skip-link')) return;

      const targetElement = document.querySelector(targetId);

      if (targetElement) {
        e.preventDefault();
        smoothScrollTo(targetId);

        // Close mobile menu if open
        if (mainNav && mainNav.classList.contains('header__nav--open')) {
          toggleMobileMenu();
        }

        // Update URL hash without scrolling
        history.pushState(null, '', targetId);
      }
    });
  }

  /**
   * Smoothly scroll to a target element
   * @param {string} targetId - The ID of the target element (including #)
   */
  function smoothScrollTo(targetId) {
    const targetElement = document.querySelector(targetId);

    if (!targetElement) {
      console.warn('Navigation: Target element not found:', targetId);
      return;
    }

    const headerHeight = header ? header.offsetHeight : 0;
    const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset - headerHeight;

    // Use native smooth scroll if available, otherwise use CSS scroll-behavior
    window.scrollTo({
      top: targetPosition,
      behavior: 'smooth'
    });

    // Set focus to the target section for accessibility
    targetElement.setAttribute('tabindex', '-1');
    targetElement.focus({ preventScroll: true });
  }

  /**
   * Set up mobile menu toggle functionality
   */
  function setupMobileMenu() {
    if (!menuToggle || !mainNav) return;

    menuToggle.addEventListener('click', toggleMobileMenu);

    // Close menu when clicking outside
    document.addEventListener('click', function(e) {
      if (mainNav.classList.contains('header__nav--open') &&
          !mainNav.contains(e.target) &&
          !menuToggle.contains(e.target)) {
        toggleMobileMenu();
      }
    });

    // Close menu on escape key
    document.addEventListener('keydown', function(e) {
      if (e.key === 'Escape' && mainNav.classList.contains('header__nav--open')) {
        toggleMobileMenu();
        menuToggle.focus();
      }
    });
  }

  /**
   * Toggle mobile navigation menu
   */
  function toggleMobileMenu() {
    if (!menuToggle || !mainNav) return;

    const isOpen = mainNav.classList.contains('header__nav--open');

    mainNav.classList.toggle('header__nav--open');
    menuToggle.classList.toggle('header__menu-toggle--active');
    header.classList.toggle(CONFIG.menuOpenClass);
    document.body.classList.toggle(CONFIG.bodyMenuOpenClass);

    // Update ARIA attributes
    menuToggle.setAttribute('aria-expanded', !isOpen);

    // Trap focus in menu when open
    if (!isOpen) {
      // Menu is now open, focus first link
      const firstLink = mainNav.querySelector('.header__nav-link');
      if (firstLink) {
        firstLink.focus();
      }
    }
  }

  /**
   * Set up active state highlighting on scroll
   */
  function setupActiveStateOnScroll() {
    if (!navLinks.length) return;

    // Get all sections that correspond to nav links
    const sections = [];
    navLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href && href.startsWith('#')) {
        const section = document.querySelector(href);
        if (section) {
          sections.push({ id: href, element: section, link: link });
        }
      }
    });

    // Update active state on scroll
    let ticking = false;

    function updateActiveState() {
      const headerHeight = header ? header.offsetHeight : 0;
      const scrollPosition = window.pageYOffset + headerHeight + 100;

      let currentSection = null;

      sections.forEach(section => {
        const sectionTop = section.element.offsetTop;
        const sectionBottom = sectionTop + section.element.offsetHeight;

        if (scrollPosition >= sectionTop && scrollPosition < sectionBottom) {
          currentSection = section;
        }
      });

      // Remove active class from all links
      navLinks.forEach(link => {
        link.classList.remove(CONFIG.activeClass);
      });

      // Add active class to current section's link
      if (currentSection) {
        currentSection.link.classList.add(CONFIG.activeClass);
      }

      ticking = false;
    }

    window.addEventListener('scroll', function() {
      if (!ticking) {
        requestAnimationFrame(updateActiveState);
        ticking = true;
      }
    });

    // Initial check
    updateActiveState();
  }

  /**
   * Set up header scroll behavior (sticky header with shadow)
   */
  function setupHeaderScrollBehavior() {
    if (!header) return;

    let lastScrollTop = 0;
    let ticking = false;

    function updateHeaderState() {
      const scrollTop = window.pageYOffset || document.documentElement.scrollTop;

      // Add shadow when scrolled
      if (scrollTop > 10) {
        header.classList.add('header--scrolled');
      } else {
        header.classList.remove('header--scrolled');
      }

      lastScrollTop = scrollTop;
      ticking = false;
    }

    window.addEventListener('scroll', function() {
      if (!ticking) {
        requestAnimationFrame(updateHeaderState);
        ticking = true;
      }
    });

    // Initial check
    updateHeaderState();
  }

  // Expose functions globally
  window.initNavigation = initNavigation;
  window.smoothScrollTo = smoothScrollTo;
  window.toggleMobileMenu = toggleMobileMenu;

  // Initialize on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initNavigation);
  } else {
    initNavigation();
  }
})();
