/**
 * Smooth Scroll Utilities
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Expected exports:
 * - initSmoothScroll(): Initialize smooth scroll behavior
 * - scrollToSection(sectionId): Scroll to specific section
 *
 * Features:
 * - Smooth scroll animation (uses CSS scroll-behavior: smooth)
 * - Offset for sticky header
 * - Browser fallback support
 */

/**
 * Configuration for smooth scroll behavior
 */
const SCROLL_CONFIG = {
  headerSelector: '#header',
  navLinkSelector: 'a[href^="#"]',
  scrollBehavior: 'smooth',
  defaultOffset: 0,
};

/**
 * Get the height of the sticky header for offset calculation
 * @returns {number} Header height in pixels
 */
function getHeaderOffset() {
  const header = document.querySelector(SCROLL_CONFIG.headerSelector);
  return header ? header.offsetHeight : SCROLL_CONFIG.defaultOffset;
}

/**
 * Check if the browser supports smooth scroll behavior
 * @returns {boolean}
 */
function supportsSmoothScroll() {
  return 'scrollBehavior' in document.documentElement.style;
}

/**
 * Scroll to a specific section by ID
 * @param {string} sectionId - The ID of the section to scroll to (without #)
 * @param {Object} options - Optional scroll options
 * @param {number} options.offset - Additional offset from the top
 * @returns {boolean} - Whether the scroll was successful
 */
export function scrollToSection(sectionId, options = {}) {
  const section = document.getElementById(sectionId);
  if (!section) {
    console.warn(`Section with ID "${sectionId}" not found`);
    return false;
  }

  const headerOffset = getHeaderOffset();
  const additionalOffset = options.offset || 0;
  const totalOffset = headerOffset + additionalOffset;

  const targetPosition = section.getBoundingClientRect().top + window.scrollY - totalOffset;

  if (supportsSmoothScroll()) {
    // Use native smooth scroll
    window.scrollTo({
      top: targetPosition,
      behavior: SCROLL_CONFIG.scrollBehavior,
    });
  } else {
    // Fallback for older browsers - simple jump
    window.scrollTo(0, targetPosition);
  }

  // Update URL hash without triggering scroll
  if (history.pushState) {
    history.pushState(null, '', `#${sectionId}`);
  }

  // Set focus to the section for accessibility
  section.setAttribute('tabindex', '-1');
  section.focus({ preventScroll: true });

  return true;
}

/**
 * Handle click events on navigation links
 * @param {Event} event - Click event
 */
function handleNavLinkClick(event) {
  const link = event.currentTarget;
  const href = link.getAttribute('href');

  // Only handle internal anchor links
  if (!href || !href.startsWith('#') || href === '#') {
    return;
  }

  event.preventDefault();
  const sectionId = href.substring(1);
  scrollToSection(sectionId);
}

/**
 * Handle initial page load with hash in URL
 */
function handleInitialHash() {
  const hash = window.location.hash;
  if (hash && hash.length > 1) {
    // Small delay to ensure page is fully loaded
    setTimeout(() => {
      const sectionId = hash.substring(1);
      scrollToSection(sectionId);
    }, 100);
  }
}

/**
 * Initialize smooth scroll behavior
 * Sets up click handlers for all anchor links
 */
export function initSmoothScroll() {
  // Get all navigation links pointing to sections
  const navLinks = document.querySelectorAll(SCROLL_CONFIG.navLinkSelector);

  navLinks.forEach((link) => {
    link.addEventListener('click', handleNavLinkClick);
  });

  // Handle initial hash in URL
  handleInitialHash();

  // Handle hash changes (browser back/forward)
  window.addEventListener('hashchange', () => {
    const hash = window.location.hash;
    if (hash && hash.length > 1) {
      const sectionId = hash.substring(1);
      scrollToSection(sectionId);
    }
  });
}

export default {
  initSmoothScroll,
  scrollToSection,
};
