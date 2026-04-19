/**
 * Mobile Navigation Module
 * Owner: Scenario 8 - Responsive Design
 *
 * Provides hamburger menu functionality for mobile viewports.
 *
 * Features:
 * - Hamburger button toggle
 * - Slide-in/out animation
 * - Focus trap when open
 * - Close on Escape key
 * - Close on overlay click
 * - Respects prefers-reduced-motion
 */

let navElement = null;
let hamburgerButton = null;
let overlay = null;
let isOpen = false;
let focusableElements = [];
let firstFocusable = null;
let lastFocusable = null;

/**
 * Initialize mobile navigation
 * @returns {boolean} True if initialization successful
 */
export function initMobileNav() {
  hamburgerButton = document.querySelector('.header__hamburger');
  navElement = document.querySelector('.header__nav');

  if (!hamburgerButton || !navElement) {
    console.warn('Mobile nav: Required elements not found');
    return false;
  }

  // Create overlay element
  overlay = document.createElement('div');
  overlay.className = 'mobile-nav-overlay';
  overlay.setAttribute('aria-hidden', 'true');
  document.body.appendChild(overlay);

  // Set initial ARIA state
  hamburgerButton.setAttribute('aria-expanded', 'false');
  hamburgerButton.setAttribute('aria-controls', 'mobile-nav');
  hamburgerButton.setAttribute('aria-label', 'Open navigation menu');
  navElement.id = 'mobile-nav';

  // Event listeners
  hamburgerButton.addEventListener('click', toggleMenu);
  overlay.addEventListener('click', closeMenu);
  document.addEventListener('keydown', handleKeyDown);

  // Close menu on window resize to desktop size
  window.addEventListener('resize', handleResize);

  // Update focusable elements
  updateFocusableElements();

  return true;
}

/**
 * Toggle menu open/closed
 */
export function toggleMenu() {
  if (isOpen) {
    closeMenu();
  } else {
    openMenu();
  }
}

/**
 * Open the mobile menu
 */
export function openMenu() {
  if (!navElement || !hamburgerButton) return;

  isOpen = true;
  navElement.classList.add('is-open');
  overlay.classList.add('is-visible');
  hamburgerButton.setAttribute('aria-expanded', 'true');
  hamburgerButton.setAttribute('aria-label', 'Close navigation menu');

  // Prevent body scroll
  document.body.style.overflow = 'hidden';

  // Focus first focusable element in nav
  updateFocusableElements();
  if (firstFocusable) {
    firstFocusable.focus();
  }
}

/**
 * Close the mobile menu
 */
export function closeMenu() {
  if (!navElement || !hamburgerButton) return;

  isOpen = false;
  navElement.classList.remove('is-open');
  overlay.classList.remove('is-visible');
  hamburgerButton.setAttribute('aria-expanded', 'false');
  hamburgerButton.setAttribute('aria-label', 'Open navigation menu');

  // Restore body scroll
  document.body.style.overflow = '';

  // Return focus to hamburger button
  hamburgerButton.focus();
}

/**
 * Check if menu is currently open
 * @returns {boolean}
 */
export function isMenuOpen() {
  return isOpen;
}

/**
 * Handle keyboard events
 * @param {KeyboardEvent} event
 */
function handleKeyDown(event) {
  if (!isOpen) return;

  // Close on Escape
  if (event.key === 'Escape') {
    event.preventDefault();
    closeMenu();
    return;
  }

  // Tab key focus trap
  if (event.key === 'Tab') {
    handleTabKey(event);
  }
}

/**
 * Handle tab key for focus trapping
 * @param {KeyboardEvent} event
 */
function handleTabKey(event) {
  updateFocusableElements();

  if (focusableElements.length === 0) return;

  // Add hamburger button to focusable elements for trap
  const allFocusable = [hamburgerButton, ...focusableElements];
  const first = allFocusable[0];
  const last = allFocusable[allFocusable.length - 1];

  if (event.shiftKey) {
    // Shift + Tab: going backwards
    if (document.activeElement === first) {
      event.preventDefault();
      last.focus();
    }
  } else {
    // Tab: going forwards
    if (document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }
}

/**
 * Update the list of focusable elements in the nav
 */
function updateFocusableElements() {
  if (!navElement) return;

  const selector = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
  focusableElements = Array.from(navElement.querySelectorAll(selector));

  firstFocusable = focusableElements[0] || null;
  lastFocusable = focusableElements[focusableElements.length - 1] || null;
}

/**
 * Handle window resize - close menu if resized to desktop
 */
function handleResize() {
  if (isOpen && window.innerWidth > 768) {
    closeMenu();
  }
}

/**
 * Clean up event listeners and DOM elements
 */
export function destroyMobileNav() {
  if (hamburgerButton) {
    hamburgerButton.removeEventListener('click', toggleMenu);
  }
  if (overlay) {
    overlay.removeEventListener('click', closeMenu);
    overlay.remove();
  }
  document.removeEventListener('keydown', handleKeyDown);
  window.removeEventListener('resize', handleResize);

  // Reset state
  navElement = null;
  hamburgerButton = null;
  overlay = null;
  isOpen = false;
  focusableElements = [];
  firstFocusable = null;
  lastFocusable = null;
}
