/**
 * Navigation Module
 * Owner: Scenario 6 - Navigation and GitHub Links
 * Updated: Scenario 7 - Mobile Navigation Toggle
 *
 * Exports:
 * - initNavigation(): Set up smooth scroll and mobile nav
 * - scrollToSection(sectionId): Smooth scroll to section
 * - toggleMobileNav(): Toggle mobile navigation menu
 *
 * Handles:
 * - Smooth scroll for anchor links
 * - Active nav highlighting
 * - Mobile hamburger menu toggle (Scenario 7)
 */

(function() {
  'use strict';

  // =============================================
  // SCENARIO 7 - Mobile Navigation Toggle
  // =============================================

  /**
   * Toggle mobile navigation menu
   */
  function toggleMobileNav() {
    const toggle = document.querySelector('[data-testid="mobile-nav-toggle"]');
    const nav = document.querySelector('.header__nav');

    if (!toggle || !nav) return;

    const isExpanded = toggle.getAttribute('aria-expanded') === 'true';
    const newExpandedState = !isExpanded;

    toggle.setAttribute('aria-expanded', String(newExpandedState));
    nav.setAttribute('aria-hidden', String(!newExpandedState));

    // Prevent body scroll when menu is open
    document.body.style.overflow = newExpandedState ? 'hidden' : '';
  }

  /**
   * Initialize mobile navigation
   */
  function initMobileNav() {
    const toggle = document.querySelector('[data-testid="mobile-nav-toggle"]');
    const nav = document.querySelector('.header__nav');

    if (!toggle || !nav) return;

    // Set initial state
    toggle.setAttribute('aria-expanded', 'false');
    nav.setAttribute('aria-hidden', 'true');

    // Toggle button click handler
    toggle.addEventListener('click', toggleMobileNav);

    // Close menu when a nav link is clicked
    const navLinks = nav.querySelectorAll('.header__nav-link');
    navLinks.forEach(link => {
      link.addEventListener('click', () => {
        if (window.innerWidth < 600) {
          toggleMobileNav();
        }
      });
    });

    // Close menu on escape key
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        const isExpanded = toggle.getAttribute('aria-expanded') === 'true';
        if (isExpanded) {
          toggleMobileNav();
          toggle.focus();
        }
      }
    });

    // Handle window resize - close mobile menu if viewport becomes larger
    window.addEventListener('resize', () => {
      if (window.innerWidth >= 600) {
        toggle.setAttribute('aria-expanded', 'false');
        nav.setAttribute('aria-hidden', 'true');
        document.body.style.overflow = '';
      }
    });
  }

  /**
   * Smooth scroll to a section by ID
   * @param {string} sectionId - The ID of the section to scroll to (without #)
   */
  function scrollToSection(sectionId) {
    const section = document.getElementById(sectionId);
    if (section) {
      const headerOffset = 80; // Account for sticky header
      const elementPosition = section.getBoundingClientRect().top;
      const offsetPosition = elementPosition + window.pageYOffset - headerOffset;

      window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth'
      });
    }
  }

  /**
   * Handle navigation link clicks for smooth scrolling
   * @param {Event} event - The click event
   */
  function handleNavClick(event) {
    const link = event.target.closest('a[href^="#"]');
    if (!link) return;

    const href = link.getAttribute('href');
    if (href && href.startsWith('#') && href.length > 1) {
      event.preventDefault();
      const sectionId = href.substring(1);
      scrollToSection(sectionId);

      // Update URL hash without jumping
      if (history.pushState) {
        history.pushState(null, null, href);
      }
    }
  }

  /**
   * Update active navigation state based on scroll position
   */
  function updateActiveNav() {
    const sections = document.querySelectorAll('section[id]');
    const navLinks = document.querySelectorAll('.nav__link[data-nav-link]');
    const headerOffset = 100;

    let currentSection = '';

    sections.forEach(section => {
      const sectionTop = section.offsetTop - headerOffset;
      const sectionHeight = section.offsetHeight;

      if (window.scrollY >= sectionTop && window.scrollY < sectionTop + sectionHeight) {
        currentSection = section.getAttribute('id');
      }
    });

    navLinks.forEach(link => {
      link.classList.remove('nav__link--active');
      const href = link.getAttribute('href');
      if (href === `#${currentSection}`) {
        link.classList.add('nav__link--active');
      }
    });
  }

  /**
   * Initialize navigation functionality
   */
  function initNavigation() {
    // Initialize mobile navigation (Scenario 7)
    initMobileNav();

    // Attach click handlers for smooth scroll
    document.addEventListener('click', handleNavClick);

    // Update active nav state on scroll (debounced)
    let scrollTimeout;
    window.addEventListener('scroll', function() {
      if (scrollTimeout) {
        window.cancelAnimationFrame(scrollTimeout);
      }
      scrollTimeout = window.requestAnimationFrame(updateActiveNav);
    });

    // Initial active state
    updateActiveNav();
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initNavigation);
  } else {
    initNavigation();
  }

  // Expose functions globally for external use
  window.MirDBNavigation = {
    scrollToSection: scrollToSection,
    initNavigation: initNavigation,
    toggleMobileNav: toggleMobileNav
  };
})();
