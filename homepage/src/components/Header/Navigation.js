/**
 * Navigation Component
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Expected exports:
 * - initNavigation(): Initialize navigation functionality
 * - handleStickyNav(): Handle sticky navigation on scroll
 * - handleMobileMenu(): Handle mobile menu toggle
 *
 * Features:
 * - Sticky navigation bar (CSS handles position, JS handles scroll state)
 * - Mobile hamburger menu
 * - Active section highlighting
 */

/**
 * Configuration for navigation behavior
 */
const NAV_CONFIG = {
  headerSelector: '#header',
  navLinksSelector: '.nav__links',
  navLinkSelector: '.nav__link[href^="#"]',
  sectionSelector: 'section[id]',
  activeClass: 'nav__link--active',
  scrolledClass: 'header--scrolled',
  scrollThreshold: 50,
  scrollOffset: 80,
};

/**
 * Track the current active section
 */
let currentActiveSection = null;

/**
 * Get all navigation links that point to sections
 * @returns {NodeListOf<Element>}
 */
function getNavLinks() {
  return document.querySelectorAll(NAV_CONFIG.navLinkSelector);
}

/**
 * Get all sections with IDs
 * @returns {NodeListOf<Element>}
 */
function getSections() {
  return document.querySelectorAll(NAV_CONFIG.sectionSelector);
}

/**
 * Handle sticky navigation scroll state
 * Adds/removes scrolled class based on scroll position
 */
export function handleStickyNav() {
  const header = document.querySelector(NAV_CONFIG.headerSelector);
  if (!header) return;

  const isScrolled = window.scrollY > NAV_CONFIG.scrollThreshold;
  header.classList.toggle(NAV_CONFIG.scrolledClass, isScrolled);
}

/**
 * Update active navigation link based on current scroll position
 */
function updateActiveSection() {
  const sections = getSections();
  const navLinks = getNavLinks();

  if (sections.length === 0 || navLinks.length === 0) return;

  const scrollPosition = window.scrollY + NAV_CONFIG.scrollOffset;

  let activeSection = null;

  // Find the current section
  sections.forEach((section) => {
    const sectionTop = section.offsetTop;
    const sectionHeight = section.offsetHeight;

    if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
      activeSection = section.getAttribute('id');
    }
  });

  // If we're at the top of the page, activate first section
  if (window.scrollY < NAV_CONFIG.scrollThreshold && sections.length > 0) {
    activeSection = sections[0].getAttribute('id');
  }

  // If we're at the bottom of the page, activate last section
  if (window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 10) {
    activeSection = sections[sections.length - 1].getAttribute('id');
  }

  // Only update if the active section changed
  if (activeSection !== currentActiveSection) {
    currentActiveSection = activeSection;

    navLinks.forEach((link) => {
      const href = link.getAttribute('href');
      const isActive = href === `#${activeSection}`;
      link.classList.toggle(NAV_CONFIG.activeClass, isActive);
      link.setAttribute('aria-current', isActive ? 'true' : 'false');
    });
  }
}

/**
 * Handle mobile menu toggle
 * Note: Mobile navigation toggle is handled in main.js (Scenario 6)
 * This function provides additional keyboard support
 */
export function handleMobileMenu() {
  const toggle = document.querySelector('.nav__toggle');
  const navLinks = document.querySelector(NAV_CONFIG.navLinksSelector);

  if (!toggle || !navLinks) return;

  // Close menu on Escape key
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && navLinks.classList.contains('nav__links--open')) {
      navLinks.classList.remove('nav__links--open');
      toggle.setAttribute('aria-expanded', 'false');
      toggle.focus();
    }
  });
}

/**
 * Initialize navigation functionality
 * Sets up scroll listeners for sticky nav and active section tracking
 */
export function initNavigation() {
  // Initial state
  handleStickyNav();
  updateActiveSection();

  // Scroll event listener with passive option for better performance
  let ticking = false;
  window.addEventListener(
    'scroll',
    () => {
      if (!ticking) {
        window.requestAnimationFrame(() => {
          handleStickyNav();
          updateActiveSection();
          ticking = false;
        });
        ticking = true;
      }
    },
    { passive: true }
  );

  // Handle mobile menu enhancements
  handleMobileMenu();
}

export default {
  initNavigation,
  handleStickyNav,
  handleMobileMenu,
};
