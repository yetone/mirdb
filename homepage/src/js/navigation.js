/**
 * Navigation functionality
 * Owner: Scenario 4 - Navigation and External Links
 *
 * Exports:
 * - initMobileMenu(): void - Initialize mobile menu toggle
 * - toggleMenu(): void - Toggle mobile menu visibility
 * - handleSmoothScroll(): void - Smooth scroll to sections
 * - initExternalLinks(): void - Setup external link attributes
 */

/**
 * Toggle mobile menu visibility
 * Toggles the 'is-open' class on the navigation and updates aria-expanded
 */
function toggleMenu() {
  const nav = document.querySelector('.header-nav');
  const menuButton = document.querySelector('.mobile-menu-toggle');

  if (!nav || !menuButton) return;

  const isOpen = nav.classList.toggle('is-open');
  menuButton.setAttribute('aria-expanded', isOpen.toString());
}

/**
 * Initialize mobile menu toggle functionality
 * Attaches click handler to the mobile menu button
 */
function initMobileMenu() {
  const menuButton = document.querySelector('.mobile-menu-toggle');

  if (!menuButton) return;

  menuButton.addEventListener('click', toggleMenu);
}

/**
 * Handle smooth scrolling for internal anchor links
 * Sets up click handlers for all links starting with #
 */
function handleSmoothScroll() {
  const internalLinks = document.querySelectorAll('a[href^="#"]');

  internalLinks.forEach((link) => {
    link.addEventListener('click', (e) => {
      const targetId = link.getAttribute('href');

      // Skip if just "#"
      if (targetId === '#') return;

      const targetElement = document.querySelector(targetId);

      if (targetElement) {
        e.preventDefault();
        targetElement.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });

        // Close mobile menu if open
        const nav = document.querySelector('.header-nav');
        if (nav && nav.classList.contains('is-open')) {
          toggleMenu();
        }
      }
    });
  });
}

/**
 * Initialize external links with proper security attributes
 * Adds target="_blank" and rel="noopener noreferrer" to external links
 */
function initExternalLinks() {
  // Select all links that start with http:// or https://
  const externalLinks = document.querySelectorAll('a[href^="http://"], a[href^="https://"]');

  externalLinks.forEach((link) => {
    // Set target to open in new tab
    link.target = '_blank';

    // Get existing rel attribute
    const existingRel = link.getAttribute('rel') || '';

    // Add noopener and noreferrer if not present
    const relValues = new Set(existingRel.split(' ').filter(Boolean));
    relValues.add('noopener');
    relValues.add('noreferrer');

    link.rel = Array.from(relValues).join(' ');
  });
}

// Export for Node.js/Jest (CommonJS)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    toggleMenu: toggleMenu,
    initMobileMenu: initMobileMenu,
    handleSmoothScroll: handleSmoothScroll,
    initExternalLinks: initExternalLinks
  };
}
