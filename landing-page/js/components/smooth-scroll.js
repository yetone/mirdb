/**
 * Smooth Scroll Component
 * Owner: Scenario 2 - Navigation Bar Functionality
 *
 * Handles smooth scrolling to page sections when clicking anchor links.
 */

import { prefersReducedMotion } from '../utils/helpers.js';

/**
 * Initialize smooth scroll for all anchor links
 */
export function initSmoothScroll() {
  // Find all anchor links that point to page sections
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(link => {
    link.addEventListener('click', handleAnchorClick);
  });
}

/**
 * Handle anchor link clicks
 * @param {Event} event - Click event
 */
function handleAnchorClick(event) {
  const href = this.getAttribute('href');

  // Skip if it's just "#" or empty
  if (!href || href === '#') {
    return;
  }

  const targetId = href.substring(1);
  const targetElement = document.getElementById(targetId);

  if (targetElement) {
    event.preventDefault();
    scrollToSection(targetId);
  }
}

/**
 * Programmatically scroll to a section by ID
 * @param {string} sectionId - The ID of the section to scroll to
 */
export function scrollToSection(sectionId) {
  const targetElement = document.getElementById(sectionId);

  if (!targetElement) {
    console.warn(`Section with ID "${sectionId}" not found`);
    return;
  }

  // Get the navigation height for offset
  const nav = document.querySelector('.main-nav');
  const navHeight = nav ? nav.offsetHeight : 0;

  // Calculate the scroll position with offset
  const targetPosition = targetElement.getBoundingClientRect().top + window.scrollY - navHeight;

  // Use smooth scroll unless user prefers reduced motion
  const behavior = prefersReducedMotion() ? 'auto' : 'smooth';

  window.scrollTo({
    top: targetPosition,
    behavior: behavior
  });

  // Update URL hash without jumping
  updateUrlHash(sectionId);
}

/**
 * Update URL hash without triggering a jump
 * @param {string} hash - The hash to set (without #)
 */
function updateUrlHash(hash) {
  if (history.pushState) {
    history.pushState(null, null, `#${hash}`);
  } else {
    // Fallback for older browsers
    window.location.hash = hash;
  }
}
