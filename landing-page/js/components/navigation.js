/**
 * Navigation Component
 * Owner: Scenario 2 - Navigation Bar Functionality
 *
 * Handles mobile hamburger menu toggle and navigation interactions.
 */

let navToggle = null;
let navMenu = null;
let mainNav = null;
let navOverlay = null;

/**
 * Initialize navigation functionality
 */
export function initNavigation() {
  navToggle = document.getElementById('nav-toggle');
  navMenu = document.getElementById('nav-menu');
  mainNav = document.getElementById('main-nav');
  navOverlay = document.getElementById('nav-overlay');

  if (!navToggle || !navMenu) {
    return;
  }

  // Toggle menu on hamburger click
  navToggle.addEventListener('click', toggleMenu);

  // Close menu when clicking on nav links
  const navLinks = navMenu.querySelectorAll('.nav-link');
  navLinks.forEach(link => {
    link.addEventListener('click', closeMenu);
  });

  // Close menu when clicking the overlay
  if (navOverlay) {
    navOverlay.addEventListener('click', closeMenu);
  }

  // Close menu on escape key
  document.addEventListener('keydown', handleEscapeKey);

  // Add scroll listener for nav background change
  window.addEventListener('scroll', handleScroll);

  // Initial scroll check
  handleScroll();
}

/**
 * Toggle mobile menu open/closed
 */
export function toggleMenu() {
  if (!navMenu || !navToggle) return;

  const isOpen = navMenu.classList.contains('is-open');

  if (isOpen) {
    closeMenu();
  } else {
    openMenu();
  }
}

/**
 * Open mobile menu
 */
export function openMenu() {
  if (!navMenu || !navToggle) return;

  navMenu.classList.add('is-open');
  navToggle.setAttribute('aria-expanded', 'true');
  document.body.style.overflow = 'hidden';

  if (navOverlay) {
    navOverlay.classList.add('is-visible');
  }
}

/**
 * Close mobile menu
 */
export function closeMenu() {
  if (!navMenu || !navToggle) return;

  navMenu.classList.remove('is-open');
  navToggle.setAttribute('aria-expanded', 'false');
  document.body.style.overflow = '';

  if (navOverlay) {
    navOverlay.classList.remove('is-visible');
  }
}

/**
 * Handle escape key to close menu
 * @param {KeyboardEvent} event - Keyboard event
 */
function handleEscapeKey(event) {
  if (event.key === 'Escape') {
    closeMenu();
  }
}

/**
 * Handle scroll event for nav styling
 */
function handleScroll() {
  if (!mainNav) return;

  if (window.scrollY > 50) {
    mainNav.classList.add('scrolled');
  } else {
    mainNav.classList.remove('scrolled');
  }
}
