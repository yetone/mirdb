/**
 * Navigation behavior.
 * Owner: Scenario 7 - Navigation and GitHub Links
 * Co-owner: Scenario 9 - Responsive Design (mobile menu)
 *
 * Expected exports:
 * - initMobileMenu(): void
 * - toggleMobileMenu(): void
 * - highlightActiveSection(): void
 */

/**
 * Initialize the mobile hamburger menu toggle.
 * Binds click event to the mobile menu toggle button.
 */
function initMobileMenu() {
  var toggle = document.querySelector('.mobile-menu-toggle');
  var menu = document.getElementById('nav-menu');
  if (!toggle || !menu) return;

  toggle.addEventListener('click', function() {
    var isExpanded = toggle.getAttribute('aria-expanded') === 'true';
    toggle.setAttribute('aria-expanded', String(!isExpanded));
    menu.classList.toggle('is-open');
  });

  // Close menu when clicking a nav link (on mobile)
  menu.querySelectorAll('.nav-link').forEach(function(link) {
    link.addEventListener('click', function() {
      toggle.setAttribute('aria-expanded', 'false');
      menu.classList.remove('is-open');
    });
  });
}

/**
 * Toggle the mobile menu open/closed state.
 * Used by tests and external callers.
 */
function toggleMobileMenu() {
  var toggle = document.querySelector('.mobile-menu-toggle');
  var menu = document.getElementById('nav-menu');
  if (!toggle || !menu) return;

  var isExpanded = toggle.getAttribute('aria-expanded') === 'true';
  toggle.setAttribute('aria-expanded', String(!isExpanded));
  menu.classList.toggle('is-open');
}

/**
 * Highlight the active navigation link based on scroll position.
 * Uses IntersectionObserver to detect which section is in view.
 */
function highlightActiveSection() {
  var navLinks = document.querySelectorAll('.nav-link[href^="#"]');
  if (navLinks.length === 0) return;

  var sectionIds = [];
  navLinks.forEach(function(link) {
    var href = link.getAttribute('href');
    if (href && href.startsWith('#') && href.length > 1) {
      sectionIds.push(href.substring(1));
    }
  });

  var observerOptions = {
    root: null,
    rootMargin: '-50% 0px -50% 0px',
    threshold: 0
  };

  var observer = new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) {
      if (entry.isIntersecting) {
        var id = entry.target.id;
        navLinks.forEach(function(link) {
          link.classList.remove('active');
          var linkHref = link.getAttribute('href');
          if (linkHref === '#' + id) {
            link.classList.add('active');
          }
        });
      }
    });
  }, observerOptions);

  sectionIds.forEach(function(id) {
    var section = document.getElementById(id);
    if (section) {
      observer.observe(section);
    }
  });
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function() {
  initMobileMenu();
  highlightActiveSection();
});

// Exports for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initMobileMenu, toggleMobileMenu, highlightActiveSection };
}
