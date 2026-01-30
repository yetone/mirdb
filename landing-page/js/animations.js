/**
 * MirDB Landing Page - Animations Module
 * Owner: Scenario 18 - Animation and Motion
 *
 * This file handles:
 * - Check for prefers-reduced-motion
 * - Initialize CSS animation triggers
 * - Intersection Observer for scroll animations
 *
 * Expected Exports:
 * - initAnimations(): void - Initialize animation handlers
 * - prefersReducedMotion(): boolean - Check user preference
 */

/**
 * Check if the user prefers reduced motion
 * @returns {boolean} - True if the user prefers reduced motion
 */
function prefersReducedMotion() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/**
 * Add a class to elements when they enter the viewport
 * Used for scroll-triggered animations
 * @param {IntersectionObserverEntry[]} entries - Intersection observer entries
 * @param {IntersectionObserver} observer - The intersection observer instance
 */
function handleIntersection(entries, observer) {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('animate-in');
      // Optionally stop observing once animated
      observer.unobserve(entry.target);
    }
  });
}

/**
 * Initialize scroll-triggered animations using Intersection Observer
 */
function initScrollAnimations() {
  // Skip if user prefers reduced motion
  if (prefersReducedMotion()) {
    // Show all elements immediately without animation
    document.querySelectorAll('.animate-on-scroll').forEach(el => {
      el.classList.add('animate-in');
    });
    return;
  }

  // Create intersection observer for scroll animations
  const observerOptions = {
    root: null,
    rootMargin: '0px 0px -50px 0px',
    threshold: 0.1
  };

  const observer = new IntersectionObserver(handleIntersection, observerOptions);

  // Observe all elements with scroll animation class
  document.querySelectorAll('.animate-on-scroll').forEach(el => {
    observer.observe(el);
  });
}

/**
 * Listen for changes to the user's motion preference
 */
function watchMotionPreference() {
  const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)');

  // Handle changes to motion preference
  mediaQuery.addEventListener('change', (event) => {
    if (event.matches) {
      // User now prefers reduced motion - show all elements
      document.querySelectorAll('.animate-on-scroll').forEach(el => {
        el.classList.add('animate-in');
      });
    }
  });
}

/**
 * Initialize all animation handlers
 */
function initAnimations() {
  // Initialize scroll-triggered animations
  initScrollAnimations();

  // Watch for changes to motion preferences
  watchMotionPreference();

  // Add data attribute to body for CSS to detect JS is loaded
  document.body.setAttribute('data-animations-ready', 'true');
}

// Export functions for use by main.js
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initAnimations, prefersReducedMotion };
}
