/**
 * Mobile Navigation Module
 * Owner: Scenario 1 - Navigation Header
 *
 * Expected exports:
 * - initMobileNav(): Initialize hamburger menu toggle
 * - openMenu(): Open mobile menu
 * - closeMenu(): Close mobile menu
 *
 * Features:
 * - Toggle hamburger icon
 * - Animate menu open/close
 * - Close on link click
 */

let hamburgerBtn = null;
let navElement = null;
let isMenuOpen = false;

/**
 * Opens the mobile navigation menu
 */
export function openMenu() {
  if (!hamburgerBtn || !navElement) return;

  isMenuOpen = true;
  hamburgerBtn.setAttribute('aria-expanded', 'true');
  navElement.classList.add('is-open');

  // Prevent body scroll when menu is open
  document.body.style.overflow = 'hidden';
}

/**
 * Closes the mobile navigation menu
 */
export function closeMenu() {
  if (!hamburgerBtn || !navElement) return;

  isMenuOpen = false;
  hamburgerBtn.setAttribute('aria-expanded', 'false');
  navElement.classList.remove('is-open');

  // Restore body scroll
  document.body.style.overflow = '';
}

/**
 * Toggles the mobile navigation menu
 */
export function toggleMenu() {
  if (isMenuOpen) {
    closeMenu();
  } else {
    openMenu();
  }
}

/**
 * Handles click outside the menu to close it
 * @param {Event} event - Click event
 */
function handleClickOutside(event) {
  if (!isMenuOpen) return;

  const isClickInsideNav = navElement?.contains(event.target);
  const isClickOnHamburger = hamburgerBtn?.contains(event.target);

  if (!isClickInsideNav && !isClickOnHamburger) {
    closeMenu();
  }
}

/**
 * Handles Escape key to close the menu
 * @param {KeyboardEvent} event - Keyboard event
 */
function handleEscapeKey(event) {
  if (event.key === 'Escape' && isMenuOpen) {
    closeMenu();
    hamburgerBtn?.focus();
  }
}

/**
 * Handles window resize to close menu when switching to desktop
 */
function handleResize() {
  if (window.innerWidth >= 769 && isMenuOpen) {
    closeMenu();
  }
}

/**
 * Initializes the mobile navigation functionality
 */
export function initMobileNav() {
  hamburgerBtn = document.getElementById('hamburger-btn');
  navElement = document.querySelector('.header__nav');

  if (!hamburgerBtn || !navElement) {
    console.warn('Mobile nav elements not found');
    return;
  }

  // Toggle menu on hamburger click
  hamburgerBtn.addEventListener('click', toggleMenu);

  // Close menu when clicking on a nav link
  const navLinks = navElement.querySelectorAll('.header__nav-link');
  navLinks.forEach(link => {
    link.addEventListener('click', () => {
      // Only close if it's an internal link (starts with #)
      if (link.getAttribute('href')?.startsWith('#')) {
        closeMenu();
      }
    });
  });

  // Close menu on click outside
  document.addEventListener('click', handleClickOutside);

  // Close menu on Escape key
  document.addEventListener('keydown', handleEscapeKey);

  // Close menu on resize to desktop
  window.addEventListener('resize', handleResize);
}

// Export for testing
export { isMenuOpen };
