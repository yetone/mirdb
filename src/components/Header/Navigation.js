/**
 * Navigation Component
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Features:
 * - Sticky navigation bar (handled by CSS, enhanced with JS)
 * - Active section highlighting based on scroll position
 * - Mobile hamburger menu toggle
 */

/**
 * Initialize navigation functionality
 */
export function initNavigation() {
  handleStickyNav();
  handleMobileMenu();
  highlightActiveSection();
}

/**
 * Handle sticky navigation effects on scroll
 * Adds/removes scrolled class for enhanced styling
 */
export function handleStickyNav() {
  const header = document.getElementById('header');
  if (!header) return;

  const handleScroll = () => {
    if (window.scrollY > 50) {
      header.classList.add('header--scrolled');
    } else {
      header.classList.remove('header--scrolled');
    }
  };

  window.addEventListener('scroll', handleScroll, { passive: true });
  // Initial check
  handleScroll();
}

/**
 * Handle mobile menu toggle
 */
export function handleMobileMenu() {
  const menuButton = document.querySelector('.header__menu-toggle');
  const nav = document.querySelector('.header__nav');

  if (!menuButton || !nav) return;

  menuButton.addEventListener('click', () => {
    const isExpanded = menuButton.getAttribute('aria-expanded') === 'true';
    menuButton.setAttribute('aria-expanded', String(!isExpanded));
    nav.classList.toggle('header__nav--open');
  });
}

/**
 * Highlight active section in navigation based on scroll position
 */
export function highlightActiveSection() {
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.header__nav-link');

  if (sections.length === 0 || navLinks.length === 0) return;

  const observerOptions = {
    root: null,
    rootMargin: '-20% 0px -60% 0px',
    threshold: 0
  };

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const sectionId = entry.target.id;

        navLinks.forEach(link => {
          const href = link.getAttribute('href');
          if (href === `#${sectionId}`) {
            link.classList.add('header__nav-link--active');
          } else {
            link.classList.remove('header__nav-link--active');
          }
        });
      }
    });
  }, observerOptions);

  sections.forEach(section => observer.observe(section));
}

/**
 * Get all navigation links
 * @returns {NodeListOf<Element>} List of navigation link elements
 */
export function getNavLinks() {
  return document.querySelectorAll('.header__nav-link');
}

/**
 * Check if navigation bar is currently sticky (fixed at top)
 * @returns {boolean} True if navigation is sticky
 */
export function isNavSticky() {
  const header = document.getElementById('header');
  if (!header) return false;

  const styles = window.getComputedStyle(header);
  return styles.position === 'sticky' || styles.position === 'fixed';
}

// Auto-initialize on DOM ready
if (typeof document !== 'undefined') {
  document.addEventListener('DOMContentLoaded', initNavigation);
}
