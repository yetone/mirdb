/**
 * MirDB Homepage Main JavaScript
 * Owner: Scenario 8 - Navigation and Header
 *
 * Expected functionality:
 * - Mobile hamburger menu toggle
 * - Smooth scroll to sections
 * - Active navigation link highlighting
 * - Accessibility keyboard navigation support
 */

(function() {
  'use strict';

  /**
   * Initialize navigation functionality when DOM is ready
   */
  document.addEventListener('DOMContentLoaded', function() {
    initSmoothScroll();
    initMobileMenu();
    initActiveNavHighlighting();
    initKeyboardNavigation();
  });

  /**
   * Smooth scroll implementation for anchor links
   * Handles clicking on navigation links that point to page sections
   */
  function initSmoothScroll() {
    const anchorLinks = document.querySelectorAll('a[href^="#"]');

    anchorLinks.forEach(function(anchor) {
      anchor.addEventListener('click', function(event) {
        const targetId = this.getAttribute('href');

        // Skip if it's just "#" (home link)
        if (targetId === '#') {
          event.preventDefault();
          window.scrollTo({
            top: 0,
            behavior: 'smooth'
          });
          return;
        }

        const targetElement = document.querySelector(targetId);
        if (targetElement) {
          event.preventDefault();

          // Get header height for offset calculation
          const header = document.querySelector('.header');
          const headerHeight = header ? header.offsetHeight : 0;
          const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset - headerHeight;

          window.scrollTo({
            top: targetPosition,
            behavior: 'smooth'
          });

          // Update URL hash without jumping
          history.pushState(null, null, targetId);

          // Set focus to the target section for accessibility
          targetElement.setAttribute('tabindex', '-1');
          targetElement.focus({ preventScroll: true });

          // Close mobile menu if open
          closeMobileMenu();
        }
      });
    });
  }

  /**
   * Mobile hamburger menu functionality
   * Handles toggle, aria attributes, and closing menu on link click
   */
  function initMobileMenu() {
    const hamburgerButton = document.querySelector('.hamburger-menu');
    const navLinks = document.querySelector('.nav-links');
    const header = document.querySelector('.header');

    if (!hamburgerButton || !navLinks) return;

    // Toggle menu on button click
    hamburgerButton.addEventListener('click', function() {
      const isExpanded = this.getAttribute('aria-expanded') === 'true';
      toggleMobileMenu(!isExpanded);
    });

    // Close menu when clicking outside
    document.addEventListener('click', function(event) {
      const isMenuOpen = hamburgerButton.getAttribute('aria-expanded') === 'true';
      if (isMenuOpen && !header.contains(event.target)) {
        closeMobileMenu();
      }
    });

    // Close menu on escape key
    document.addEventListener('keydown', function(event) {
      if (event.key === 'Escape') {
        closeMobileMenu();
        hamburgerButton.focus();
      }
    });

    // Close menu when nav link is clicked
    const navLinkElements = navLinks.querySelectorAll('a');
    navLinkElements.forEach(function(link) {
      link.addEventListener('click', function() {
        closeMobileMenu();
      });
    });
  }

  /**
   * Toggle mobile menu state
   * @param {boolean} open - Whether to open or close the menu
   */
  function toggleMobileMenu(open) {
    const hamburgerButton = document.querySelector('.hamburger-menu');
    const navLinks = document.querySelector('.nav-links');

    if (!hamburgerButton || !navLinks) return;

    hamburgerButton.setAttribute('aria-expanded', open.toString());
    navLinks.classList.toggle('nav-open', open);

    if (open) {
      // Focus first nav link when opening
      const firstLink = navLinks.querySelector('a');
      if (firstLink) {
        firstLink.focus();
      }
    }
  }

  /**
   * Close mobile menu
   */
  function closeMobileMenu() {
    toggleMobileMenu(false);
  }

  /**
   * Active navigation link highlighting based on scroll position
   * Updates the active state of nav links as user scrolls through sections
   */
  function initActiveNavHighlighting() {
    const sections = document.querySelectorAll('section[id]');
    const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

    if (sections.length === 0 || navLinks.length === 0) return;

    // Use Intersection Observer for efficient scroll detection
    const observerOptions = {
      root: null,
      rootMargin: '-20% 0px -60% 0px',
      threshold: 0
    };

    const observer = new IntersectionObserver(function(entries) {
      entries.forEach(function(entry) {
        if (entry.isIntersecting) {
          const sectionId = entry.target.getAttribute('id');
          updateActiveNavLink(sectionId);
        }
      });
    }, observerOptions);

    sections.forEach(function(section) {
      observer.observe(section);
    });

    // Fallback scroll listener for cases where IntersectionObserver might not fire
    let scrollTimeout;
    window.addEventListener('scroll', function() {
      clearTimeout(scrollTimeout);
      scrollTimeout = setTimeout(function() {
        updateActiveNavOnScroll(sections, navLinks);
      }, 100);
    }, { passive: true });
  }

  /**
   * Update active navigation link
   * @param {string} sectionId - The ID of the currently active section
   */
  function updateActiveNavLink(sectionId) {
    const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

    navLinks.forEach(function(link) {
      const href = link.getAttribute('href');
      if (href === '#' + sectionId) {
        link.classList.add('active');
        link.setAttribute('aria-current', 'true');
      } else {
        link.classList.remove('active');
        link.removeAttribute('aria-current');
      }
    });
  }

  /**
   * Fallback function to update active nav link based on scroll position
   * @param {NodeList} sections - All page sections
   * @param {NodeList} navLinks - All navigation links
   */
  function updateActiveNavOnScroll(sections, navLinks) {
    const header = document.querySelector('.header');
    const headerHeight = header ? header.offsetHeight : 0;
    const scrollPosition = window.pageYOffset + headerHeight + 100;

    let currentSection = '';

    sections.forEach(function(section) {
      const sectionTop = section.offsetTop;
      const sectionHeight = section.offsetHeight;

      if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
        currentSection = section.getAttribute('id');
      }
    });

    if (currentSection) {
      updateActiveNavLink(currentSection);
    }
  }

  /**
   * Keyboard navigation support
   * Enables arrow key navigation within the nav menu
   */
  function initKeyboardNavigation() {
    const navLinks = document.querySelector('.nav-links');
    if (!navLinks) return;

    const links = navLinks.querySelectorAll('a');

    navLinks.addEventListener('keydown', function(event) {
      const currentFocus = document.activeElement;
      const currentIndex = Array.from(links).indexOf(currentFocus);

      if (currentIndex === -1) return;

      let newIndex;

      switch (event.key) {
        case 'ArrowRight':
        case 'ArrowDown':
          event.preventDefault();
          newIndex = (currentIndex + 1) % links.length;
          links[newIndex].focus();
          break;

        case 'ArrowLeft':
        case 'ArrowUp':
          event.preventDefault();
          newIndex = currentIndex === 0 ? links.length - 1 : currentIndex - 1;
          links[newIndex].focus();
          break;

        case 'Home':
          event.preventDefault();
          links[0].focus();
          break;

        case 'End':
          event.preventDefault();
          links[links.length - 1].focus();
          break;
      }
    });
  }
})();
