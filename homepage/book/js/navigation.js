/**
 * Navigation Functionality
 * Owners:
 *   - Scenario 2: Smooth scroll for nav links
 *   - Scenario 11: Mobile hamburger menu toggle
 *
 * Expected exports/functionality:
 * - initSmoothScroll(): Smooth scrolling for anchor links
 * - toggleMobileMenu(): Show/hide mobile navigation
 * - closeMobileMenuOnSelect(): Close menu after selection
 */

/* ===== SMOOTH SCROLL - Scenario 2 ===== */

/**
 * Initialize smooth scrolling for anchor links
 * Handles navigation to sections with smooth scroll behavior
 */
function initSmoothScroll() {
  // Get all anchor links that start with #
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(function(link) {
    link.addEventListener('click', function(event) {
      const href = this.getAttribute('href');

      // Skip empty anchors and the skip-nav link
      if (href === '#' || href === '#content') {
        return;
      }

      const targetId = href.substring(1);
      const targetElement = document.getElementById(targetId);

      if (targetElement) {
        event.preventDefault();

        // Get header height for offset
        var header = document.getElementById('site-header');
        var headerHeight = header ? header.offsetHeight : 0;

        // Calculate scroll position
        var targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset - headerHeight;

        // Smooth scroll to target
        window.scrollTo({
          top: targetPosition,
          behavior: 'smooth'
        });

        // Update URL hash without jumping
        history.pushState(null, null, href);

        // Set focus to target for accessibility
        targetElement.setAttribute('tabindex', '-1');
        targetElement.focus({ preventScroll: true });
      }
    });
  });
}

/* ===== MOBILE MENU - Scenario 11 ===== */

/**
 * Toggle mobile navigation menu visibility
 * Will be fully implemented by Scenario 11
 */
function toggleMobileMenu() {
  var nav = document.getElementById('main-nav');
  var toggle = document.getElementById('mobile-menu-toggle');

  if (nav && toggle) {
    var isExpanded = toggle.getAttribute('aria-expanded') === 'true';
    toggle.setAttribute('aria-expanded', !isExpanded);
    nav.classList.toggle('is-open');
  }
}

/**
 * Close mobile menu when a nav item is selected
 * Will be fully implemented by Scenario 11
 */
function closeMobileMenuOnSelect() {
  var nav = document.getElementById('main-nav');
  var toggle = document.getElementById('mobile-menu-toggle');

  if (nav && toggle && nav.classList.contains('is-open')) {
    toggle.setAttribute('aria-expanded', 'false');
    nav.classList.remove('is-open');
  }
}

/* ===== INITIALIZATION ===== */

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
  initSmoothScroll();

  // Set up mobile menu toggle (basic setup, Scenario 11 will enhance)
  var mobileToggle = document.getElementById('mobile-menu-toggle');
  if (mobileToggle) {
    mobileToggle.addEventListener('click', toggleMobileMenu);
  }

  // Close mobile menu when nav link is clicked
  var navLinks = document.querySelectorAll('.nav-link');
  navLinks.forEach(function(link) {
    link.addEventListener('click', closeMobileMenuOnSelect);
  });
});

// Export for testing (CommonJS)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    initSmoothScroll: initSmoothScroll,
    toggleMobileMenu: toggleMobileMenu,
    closeMobileMenuOnSelect: closeMobileMenuOnSelect
  };
}
