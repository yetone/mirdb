/**
 * Scroll Behavior Component
 * Owner: Scenario 7 - Responsive Design
 *
 * Expected exports:
 * - initSmoothScroll(): void - Initialize smooth scrolling
 * - scrollToSection(sectionId: string): void - Scroll to section
 *
 * Requirements: REQ-7
 */

import { closeMobileMenu } from './navigation.js';

/**
 * Initialize smooth scrolling for anchor links
 * Sets up event listeners on all internal anchor links
 */
export function initSmoothScroll() {
  // Get all anchor links that point to sections on the same page
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(link => {
    link.addEventListener('click', handleAnchorClick);
  });

  // Handle browser back/forward navigation with hash changes
  window.addEventListener('hashchange', handleHashChange);
}

/**
 * Handle click events on anchor links
 * @param {Event} event - Click event
 */
function handleAnchorClick(event) {
  const href = event.currentTarget.getAttribute('href');

  // Skip if not a valid anchor link
  if (!href || href === '#') {
    return;
  }

  const targetId = href.substring(1);
  const targetElement = document.getElementById(targetId);

  if (targetElement) {
    event.preventDefault();

    // Close mobile menu if open
    closeMobileMenu();

    // Scroll to the target section
    scrollToSection(targetId);

    // Update URL hash without triggering scroll
    history.pushState(null, '', href);
  }
}

/**
 * Handle hash changes from browser navigation
 */
function handleHashChange() {
  const hash = window.location.hash;

  if (hash && hash.length > 1) {
    const targetId = hash.substring(1);
    scrollToSection(targetId, { skipHashUpdate: true });
  }
}

/**
 * Scroll smoothly to a section by its ID
 * @param {string} sectionId - The ID of the target section
 * @param {Object} options - Scroll options
 * @param {boolean} options.skipHashUpdate - Skip updating URL hash
 */
export function scrollToSection(sectionId, options = {}) {
  const targetElement = document.getElementById(sectionId);

  if (!targetElement) {
    console.warn(`Scroll target not found: #${sectionId}`);
    return;
  }

  // Get header height for offset calculation (sticky header on mobile)
  const header = document.querySelector('.header');
  const headerHeight = header ? header.offsetHeight : 0;

  // Check if user prefers reduced motion
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // Calculate scroll position with offset for sticky header
  const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset - headerHeight;

  // Perform smooth scroll (or instant if reduced motion is preferred)
  if (prefersReducedMotion) {
    window.scrollTo({
      top: targetPosition,
      behavior: 'auto'
    });
  } else {
    window.scrollTo({
      top: targetPosition,
      behavior: 'smooth'
    });
  }

  // Focus the target element for accessibility (after scroll completes)
  setTimeout(() => {
    // Make target focusable temporarily if needed
    if (!targetElement.hasAttribute('tabindex')) {
      targetElement.setAttribute('tabindex', '-1');
    }
    targetElement.focus({ preventScroll: true });
  }, prefersReducedMotion ? 0 : 500);
}

/**
 * Get current section in view (for active nav highlighting)
 * @returns {string|null} The ID of the current section in view
 */
export function getCurrentSection() {
  const sections = document.querySelectorAll('section[id]');
  const header = document.querySelector('.header');
  const headerHeight = header ? header.offsetHeight : 0;

  let currentSection = null;
  let minDistance = Infinity;

  sections.forEach(section => {
    const rect = section.getBoundingClientRect();
    const distance = Math.abs(rect.top - headerHeight);

    // Section is in view if its top is near the header
    if (rect.top <= headerHeight + 100 && rect.bottom > headerHeight) {
      if (distance < minDistance) {
        minDistance = distance;
        currentSection = section.id;
      }
    }
  });

  return currentSection;
}

/**
 * Scroll to top of page
 */
export function scrollToTop() {
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  window.scrollTo({
    top: 0,
    behavior: prefersReducedMotion ? 'auto' : 'smooth'
  });
}
