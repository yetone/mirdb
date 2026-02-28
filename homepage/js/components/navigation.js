/**
 * Navigation Component
 * Owner: Scenario 5 - Navigation & External Links
 *
 * Expected exports:
 * - initNavigation(): void - Initialize navigation handlers
 * - toggleMobileMenu(): void - Toggle mobile hamburger menu
 * - closeMobileMenu(): void - Close mobile menu
 *
 * Requirements: REQ-5, REQ-7
 */

let mobileMenuOpen = false;
let mobileMenuButton = null;
let mobileMenu = null;

/**
 * Initialize navigation handlers
 * Sets up mobile menu toggle and keyboard navigation
 */
export function initNavigation() {
  mobileMenuButton = document.querySelector('.nav__mobile-toggle');
  mobileMenu = document.querySelector('.nav__menu');

  if (mobileMenuButton && mobileMenu) {
    mobileMenuButton.addEventListener('click', toggleMobileMenu);

    // Close menu when clicking outside
    document.addEventListener('click', (event) => {
      if (mobileMenuOpen &&
          !mobileMenu.contains(event.target) &&
          !mobileMenuButton.contains(event.target)) {
        closeMobileMenu();
      }
    });

    // Close menu on Escape key
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && mobileMenuOpen) {
        closeMobileMenu();
        mobileMenuButton.focus();
      }
    });
  }

  // Set up external link indicators
  setupExternalLinks();
}

/**
 * Toggle mobile menu open/close state
 */
export function toggleMobileMenu() {
  if (mobileMenuOpen) {
    closeMobileMenu();
  } else {
    openMobileMenu();
  }
}

/**
 * Open the mobile menu
 */
function openMobileMenu() {
  mobileMenuOpen = true;
  if (mobileMenu) {
    mobileMenu.classList.add('nav__menu--open');
    mobileMenu.setAttribute('aria-hidden', 'false');
  }
  if (mobileMenuButton) {
    mobileMenuButton.setAttribute('aria-expanded', 'true');
  }
}

/**
 * Close the mobile menu
 */
export function closeMobileMenu() {
  mobileMenuOpen = false;
  if (mobileMenu) {
    mobileMenu.classList.remove('nav__menu--open');
    mobileMenu.setAttribute('aria-hidden', 'true');
  }
  if (mobileMenuButton) {
    mobileMenuButton.setAttribute('aria-expanded', 'false');
  }
}

/**
 * Setup external links with proper attributes
 * Ensures all external links have target="_blank" and rel="noopener noreferrer"
 */
function setupExternalLinks() {
  const externalLinks = document.querySelectorAll('a[href^="http"]:not([href*="' + window.location.hostname + '"])');

  externalLinks.forEach(link => {
    // Ensure external links open in new tab safely
    if (!link.hasAttribute('target')) {
      link.setAttribute('target', '_blank');
    }

    // Add security attributes
    const rel = link.getAttribute('rel') || '';
    if (!rel.includes('noopener')) {
      link.setAttribute('rel', (rel + ' noopener').trim());
    }
    if (!rel.includes('noreferrer') && !link.getAttribute('rel').includes('noreferrer')) {
      link.setAttribute('rel', (link.getAttribute('rel') + ' noreferrer').trim());
    }
  });
}

/**
 * Check if mobile menu is currently open
 * @returns {boolean} Whether the mobile menu is open
 */
export function isMobileMenuOpen() {
  return mobileMenuOpen;
}
