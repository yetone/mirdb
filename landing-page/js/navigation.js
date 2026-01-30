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
 * Smoothly scroll to a section by its ID
 * @param {string} sectionId - The ID of the section to scroll to
 */
function scrollToSection(sectionId) {
  const section = document.getElementById(sectionId);
  if (section) {
    section.scrollIntoView({
      behavior: 'smooth',
      block: 'start'
    });
  }
}

/**
 * Initialize navigation handlers for smooth scrolling
 * Sets up click handlers on all internal navigation links
 */
function initNavigation() {
  const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');
  const navLogo = document.querySelector('.nav-logo');

  // Handle navigation link clicks
  navLinks.forEach(link => {
    link.addEventListener('click', handleNavClick);
  });

  // Handle logo click (scroll to hero)
  if (navLogo && navLogo.getAttribute('href')?.startsWith('#')) {
    navLogo.addEventListener('click', handleNavClick);
  }

  // Track active section on scroll
  trackActiveSection();
}

/**
 * Handle navigation link click events
 * @param {Event} event - The click event
 */
function handleNavClick(event) {
  const href = event.currentTarget.getAttribute('href');

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
 * Updates the active class on navigation links
 */
function trackActiveSection() {
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

  if (sections.length === 0 || navLinks.length === 0) return;

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const sectionId = entry.target.getAttribute('id');
        updateActiveLink(sectionId, navLinks);
      }
    });
  }, {
    rootMargin: '-50% 0px -50% 0px',
    threshold: 0
  });

  sections.forEach(section => {
    observer.observe(section);
  });
}

/**
 * Update the active link in navigation
 * @param {string} sectionId - The ID of the currently visible section
 * @param {NodeList} navLinks - The navigation links
 */
function updateActiveLink(sectionId, navLinks) {
  navLinks.forEach(link => {
    const href = link.getAttribute('href');
    if (href === `#${sectionId}`) {
      link.classList.add('active');
    } else {
      link.classList.remove('active');
    }
  });
}

// Export for use in main.js and testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initNavigation, scrollToSection };
}
