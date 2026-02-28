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

/** @type {HTMLElement|null} */
let mobileMenuBtn = null;
/** @type {HTMLElement|null} */
let navLinks = null;
/** @type {boolean} */
let isMenuOpen = false;

/**
 * Initialize navigation handlers
 * Sets up mobile menu toggle, smooth scrolling for internal links,
 * and external link handling
 */
export function initNavigation() {
  mobileMenuBtn = document.querySelector('.nav__menu-btn');
  navLinks = document.querySelector('.nav__links');

  if (mobileMenuBtn) {
    mobileMenuBtn.addEventListener('click', toggleMobileMenu);
  }

  // Set up smooth scroll for internal anchor links
  setupSmoothScroll();

  // Set up external link handling
  setupExternalLinks();

  // Close menu on escape key
  document.addEventListener('keydown', handleKeyDown);

  // Close menu when clicking outside
  document.addEventListener('click', handleOutsideClick);
}

/**
 * Toggle mobile menu open/closed state
 */
export function toggleMobileMenu() {
  isMenuOpen = !isMenuOpen;

  if (mobileMenuBtn && navLinks) {
    mobileMenuBtn.setAttribute('aria-expanded', String(isMenuOpen));
    navLinks.classList.toggle('nav__links--open', isMenuOpen);

    // Update button label
    mobileMenuBtn.setAttribute(
      'aria-label',
      isMenuOpen ? 'Close menu' : 'Open menu'
    );
  }
}

/**
 * Close mobile menu
 */
export function closeMobileMenu() {
  isMenuOpen = false;

  if (mobileMenuBtn && navLinks) {
    mobileMenuBtn.setAttribute('aria-expanded', 'false');
    navLinks.classList.remove('nav__links--open');
    mobileMenuBtn.setAttribute('aria-label', 'Open menu');
  }
}

/**
 * Set up smooth scrolling for internal anchor links
 */
function setupSmoothScroll() {
  const internalLinks = document.querySelectorAll('a[href^="#"]');

  internalLinks.forEach(link => {
    link.addEventListener('click', (event) => {
      const href = link.getAttribute('href');
      if (href && href.length > 1) {
        const target = document.querySelector(href);
        if (target) {
          event.preventDefault();
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
          closeMobileMenu();

          // Update focus for accessibility
          target.setAttribute('tabindex', '-1');
          target.focus();
        }
      }
    });
  });
}

/**
 * Set up external link handling
 * Ensures external links have proper security attributes
 */
function setupExternalLinks() {
  const externalLinks = document.querySelectorAll('a[href^="http"]');

  externalLinks.forEach(link => {
    // Ensure external links open in new tab with proper security
    if (!link.hasAttribute('target')) {
      link.setAttribute('target', '_blank');
    }
    if (!link.hasAttribute('rel')) {
      link.setAttribute('rel', 'noopener noreferrer');
    } else {
      const rel = link.getAttribute('rel') || '';
      if (!rel.includes('noopener')) {
        link.setAttribute('rel', `${rel} noopener`.trim());
      }
    }
  });
}

/**
 * Handle keyboard navigation
 * @param {KeyboardEvent} event
 */
function handleKeyDown(event) {
  if (event.key === 'Escape' && isMenuOpen) {
    closeMobileMenu();
    mobileMenuBtn?.focus();
  }
}

/**
 * Handle clicks outside the navigation menu
 * @param {MouseEvent} event
 */
function handleOutsideClick(event) {
  if (!isMenuOpen) return;

  const target = event.target;
  const nav = document.querySelector('.nav');

  if (nav && !nav.contains(target)) {
    closeMobileMenu();
  }
}

/**
 * Check if a link is external
 * @param {string} href - The link href
 * @returns {boolean} True if the link is external
 */
export function isExternalLink(href) {
  if (!href) return false;
  try {
    const url = new URL(href, window.location.origin);
    return url.origin !== window.location.origin;
  } catch {
    return false;
  }
}
