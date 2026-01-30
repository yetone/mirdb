/**
 * MirDB Landing Page - Navigation Module
 * Owner: Scenario 8 - Navigation Bar Functionality
 *
 * This file handles:
 * - Smooth scroll to anchor links
 * - Active navigation state tracking
 * - Mobile menu toggle (if applicable)
 *
 * Expected Exports:
 * - initNavigation(): void - Initialize navigation handlers
 * - scrollToSection(sectionId: string): void - Smooth scroll to section
 */

/**
 * Smooth scroll to a section by ID
 * @param {string} sectionId - The ID of the section to scroll to (without #)
 */
function scrollToSection(sectionId) {
  const section = document.getElementById(sectionId);
  if (section) {
    // Check if user prefers reduced motion
    const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    section.scrollIntoView({
      behavior: prefersReducedMotion ? 'auto' : 'smooth',
      block: 'start'
    });
  }
}

/**
 * Handle navigation link clicks for smooth scrolling
 * @param {Event} event - The click event
 */
function handleNavLinkClick(event) {
  const link = event.currentTarget;
  const href = link.getAttribute('href');

  // Only handle internal anchor links
  if (href && href.startsWith('#')) {
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
 * Track active navigation state based on scroll position
 */
function updateActiveNavState() {
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

  // Get current scroll position with offset for nav height
  const scrollPosition = window.scrollY + 100;

  let currentSection = '';

  sections.forEach(section => {
    const sectionTop = section.offsetTop;
    const sectionHeight = section.offsetHeight;

    if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
      currentSection = section.getAttribute('id');
    }
  });

  navLinks.forEach(link => {
    const href = link.getAttribute('href');
    if (href === `#${currentSection}`) {
      link.classList.add('nav-link--active');
    } else {
      link.classList.remove('nav-link--active');
    }
  });
}

/**
 * Initialize navigation handlers
 */
function initNavigation() {
  // Get all navigation links (both in nav and logo)
  const navLinks = document.querySelectorAll('.nav a[href^="#"]');

  // Attach click handlers for smooth scrolling
  navLinks.forEach(link => {
    link.addEventListener('click', handleNavLinkClick);
  });

  // Track active navigation state on scroll
  let scrollTimeout;
  window.addEventListener('scroll', () => {
    // Debounce scroll event
    if (scrollTimeout) {
      clearTimeout(scrollTimeout);
    }
    scrollTimeout = setTimeout(updateActiveNavState, 50);
  });

  // Set initial active state
  updateActiveNavState();

  // Handle keyboard navigation
  navLinks.forEach(link => {
    link.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        link.click();
      }
    });
  });
}

// Export functions for use by main.js
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initNavigation, scrollToSection };
}
