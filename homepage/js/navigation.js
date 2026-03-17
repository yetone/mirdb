/**
 * Navigation Module
 * Owner: Scenario 6 - Navigation and GitHub Links
 *
 * Exports:
 * - initNavigation(): Set up smooth scroll and mobile nav
 * - scrollToSection(sectionId): Smooth scroll to section
 *
 * Handles:
 * - Smooth scroll for anchor links
 * - Active nav highlighting
 */

(function() {
  'use strict';

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
    initNavigation: initNavigation
  };
})();
