/**
 * Mobile Navigation Module
 * Owner: Scenario 2 - Navigation Header
 *
 * Handles hamburger menu toggle functionality for mobile viewports.
 * Provides accessible navigation with proper ARIA attributes.
 */

let isNavOpen = false;
let hamburgerBtn = null;
let mobileNav = null;

/**
 * Initialize mobile navigation
 * Sets up event listeners for hamburger button and keyboard navigation
 */
export function initMobileNav() {
  hamburgerBtn = document.getElementById('hamburger-btn');
  mobileNav = document.getElementById('mobile-nav');

  if (!hamburgerBtn || !mobileNav) {
    return;
  }

  // Toggle on hamburger button click
  hamburgerBtn.addEventListener('click', toggleMobileNav);

  // Close on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && isNavOpen) {
      closeMobileNav();
    }
  });

  // Close when clicking a nav link
  const navLinks = mobileNav.querySelectorAll('.mobile-nav-link');
  navLinks.forEach(link => {
    link.addEventListener('click', () => {
      // Only close for internal anchor links
      if (link.getAttribute('href')?.startsWith('#')) {
        closeMobileNav();
      }
    });
  });

  // Close when clicking outside the mobile nav
  document.addEventListener('click', (e) => {
    if (isNavOpen && !mobileNav.contains(e.target) && !hamburgerBtn.contains(e.target)) {
      closeMobileNav();
    }
  });
}

/**
 * Toggle mobile navigation open/closed
 */
export function toggleMobileNav() {
  if (isNavOpen) {
    closeMobileNav();
  } else {
    openMobileNav();
  }
}

/**
 * Open the mobile navigation menu
 */
export function openMobileNav() {
  if (!hamburgerBtn || !mobileNav) {
    return;
  }

  isNavOpen = true;
  hamburgerBtn.setAttribute('aria-expanded', 'true');
  hamburgerBtn.setAttribute('aria-label', 'Close navigation menu');
  mobileNav.setAttribute('aria-hidden', 'false');
  mobileNav.classList.add('is-open');
  hamburgerBtn.classList.add('is-active');

  // Prevent body scroll when nav is open
  document.body.style.overflow = 'hidden';

  // Focus the first link for accessibility
  const firstLink = mobileNav.querySelector('.mobile-nav-link');
  if (firstLink) {
    firstLink.focus();
  }
}

/**
 * Close the mobile navigation menu
 */
export function closeMobileNav() {
  if (!hamburgerBtn || !mobileNav) {
    return;
  }

  isNavOpen = false;
  hamburgerBtn.setAttribute('aria-expanded', 'false');
  hamburgerBtn.setAttribute('aria-label', 'Open navigation menu');
  mobileNav.setAttribute('aria-hidden', 'true');
  mobileNav.classList.remove('is-open');
  hamburgerBtn.classList.remove('is-active');

  // Restore body scroll
  document.body.style.overflow = '';

  // Return focus to hamburger button
  hamburgerBtn.focus();
}

/**
 * Get current navigation state
 * @returns {boolean} True if nav is open
 */
export function isOpen() {
  return isNavOpen;
}

/**
 * Reset mobile navigation state (useful for testing)
 */
export function resetMobileNav() {
  isNavOpen = false;
  hamburgerBtn = null;
  mobileNav = null;
}
