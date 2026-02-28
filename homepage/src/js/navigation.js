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
 * Adds/removes 'open' class on nav and updates aria-expanded on toggle button
 */
function toggleMenu() {
  const nav = document.querySelector('.header-nav');
  const toggle = document.querySelector('.mobile-menu-toggle');

  if (!nav || !toggle) return;

  const isOpen = nav.classList.toggle('open');
  toggle.setAttribute('aria-expanded', String(isOpen));
}

/**
 * Initialize mobile menu toggle button
 * Attaches click event handler to toggle button
 */
function initMobileMenu() {
  const toggle = document.querySelector('.mobile-menu-toggle');

  if (!toggle) return;

  toggle.addEventListener('click', toggleMenu);
}

/**
 * Setup smooth scrolling for anchor links
 * Prevents default jump and scrolls smoothly to target section
 */
function handleSmoothScroll() {
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(function(link) {
    link.addEventListener('click', function(e) {
      const href = this.getAttribute('href');

      // Skip if href is just "#" or empty
      if (!href || href === '#') return;

      const target = document.querySelector(href);

      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });

        // Close mobile menu if open
        const nav = document.querySelector('.header-nav');
        const toggle = document.querySelector('.mobile-menu-toggle');
        if (nav && nav.classList.contains('open')) {
          nav.classList.remove('open');
          if (toggle) toggle.setAttribute('aria-expanded', 'false');
        }
      }
    });
  });
}

/**
 * Initialize external link attributes for security
 * Adds target="_blank" and rel="noopener noreferrer" to external links
 */
function initExternalLinks() {
  const links = document.querySelectorAll('a[href^="http"]');

  links.forEach(function(link) {
    const href = link.getAttribute('href');

    // Skip same-origin links
    if (!href) return;

    try {
      const url = new URL(href, window.location.origin);
      if (url.origin !== window.location.origin) {
        // External link - add security attributes
        if (!link.hasAttribute('target')) {
          link.setAttribute('target', '_blank');
        }
        if (!link.hasAttribute('rel')) {
          link.setAttribute('rel', 'noopener noreferrer');
        } else {
          // Ensure rel contains noopener
          const rel = link.getAttribute('rel');
          if (!rel.includes('noopener')) {
            link.setAttribute('rel', rel + ' noopener noreferrer');
          }
        }
      }
    } catch (e) {
      // Invalid URL, skip
    }
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
