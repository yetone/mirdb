/**
 * Navigation Module
 * Owner: Scenario 5 - Navigation and Layout
 *
 * Expected exports:
 * - initNavigation(): void - Initialize sticky header and scroll behavior
 * - toggleMobileMenu(): void - Toggle mobile menu visibility
 * - scrollToSection(sectionId: string): void - Smooth scroll to section
 */

(function() {
  'use strict';

  /**
   * Navigation state
   */
  const state = {
    isMobileMenuOpen: false,
    lastScrollY: 0
  };

  /**
   * DOM element references
   */
  let header = null;
  let mobileMenuButton = null;
  let mobileNav = null;
  let navLinks = null;

  /**
   * Initialize navigation functionality
   */
  function initNavigation() {
    // Get DOM references
    header = document.querySelector('.header');
    mobileMenuButton = document.querySelector('.nav-mobile-toggle');
    mobileNav = document.querySelector('.nav-links');
    navLinks = document.querySelectorAll('.nav-link');

    if (!header) {
      console.warn('Navigation: Header element not found');
      return;
    }

    // Set up event listeners
    setupScrollListener();
    setupMobileMenuToggle();
    setupSmoothScrollLinks();

    // Initialize header state
    updateHeaderOnScroll();

    console.log('Navigation module initialized');
  }

  /**
   * Set up scroll listener for sticky header behavior
   */
  function setupScrollListener() {
    window.addEventListener('scroll', updateHeaderOnScroll, { passive: true });
  }

  /**
   * Update header classes based on scroll position
   */
  function updateHeaderOnScroll() {
    const scrollY = window.scrollY;

    // Add scrolled class when page is scrolled
    if (scrollY > 10) {
      header.classList.add('header-scrolled');
    } else {
      header.classList.remove('header-scrolled');
    }

    state.lastScrollY = scrollY;
  }

  /**
   * Set up mobile menu toggle button
   */
  function setupMobileMenuToggle() {
    if (!mobileMenuButton) return;

    mobileMenuButton.addEventListener('click', toggleMobileMenu);

    // Close menu when clicking outside
    document.addEventListener('click', function(event) {
      if (state.isMobileMenuOpen &&
          !mobileMenuButton.contains(event.target) &&
          !mobileNav.contains(event.target)) {
        closeMobileMenu();
      }
    });

    // Close menu on escape key
    document.addEventListener('keydown', function(event) {
      if (event.key === 'Escape' && state.isMobileMenuOpen) {
        closeMobileMenu();
        mobileMenuButton.focus();
      }
    });
  }

  /**
   * Toggle mobile menu visibility
   */
  function toggleMobileMenu() {
    if (state.isMobileMenuOpen) {
      closeMobileMenu();
    } else {
      openMobileMenu();
    }
  }

  /**
   * Open mobile menu
   */
  function openMobileMenu() {
    state.isMobileMenuOpen = true;
    mobileNav.classList.add('nav-links-open');
    mobileMenuButton.classList.add('nav-mobile-toggle-open');
    mobileMenuButton.setAttribute('aria-expanded', 'true');
    document.body.classList.add('mobile-menu-open');
  }

  /**
   * Close mobile menu
   */
  function closeMobileMenu() {
    state.isMobileMenuOpen = false;
    mobileNav.classList.remove('nav-links-open');
    mobileMenuButton.classList.remove('nav-mobile-toggle-open');
    mobileMenuButton.setAttribute('aria-expanded', 'false');
    document.body.classList.remove('mobile-menu-open');
  }

  /**
   * Set up smooth scroll for navigation links
   */
  function setupSmoothScrollLinks() {
    navLinks.forEach(function(link) {
      link.addEventListener('click', handleNavLinkClick);
    });
  }

  /**
   * Handle navigation link click
   * @param {Event} event - Click event
   */
  function handleNavLinkClick(event) {
    const href = this.getAttribute('href');

    // Only handle internal anchor links
    if (href && href.startsWith('#')) {
      event.preventDefault();
      const sectionId = href.substring(1);
      scrollToSection(sectionId);

      // Close mobile menu if open
      if (state.isMobileMenuOpen) {
        closeMobileMenu();
      }
    }
  }

  /**
   * Smooth scroll to a section
   * @param {string} sectionId - The ID of the section to scroll to
   */
  function scrollToSection(sectionId) {
    const section = document.getElementById(sectionId);

    if (!section) {
      console.warn('Navigation: Section not found:', sectionId);
      return;
    }

    const headerHeight = header ? header.offsetHeight : 0;
    const sectionTop = section.getBoundingClientRect().top + window.scrollY;
    const scrollTarget = sectionTop - headerHeight - 20; // 20px extra padding

    window.scrollTo({
      top: scrollTarget,
      behavior: 'smooth'
    });

    // Update URL hash without jumping
    if (history.pushState) {
      history.pushState(null, null, '#' + sectionId);
    }
  }

  // Expose functions to global scope for use by main.js
  window.Navigation = {
    init: initNavigation,
    toggleMobileMenu: toggleMobileMenu,
    scrollToSection: scrollToSection
  };

})();
