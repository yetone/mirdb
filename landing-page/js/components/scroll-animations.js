/**
 * Scroll Animation Component
 * Owner: Scenario 15 - Animations and Transitions
 *
 * Uses Intersection Observer API to trigger fade-in/slide-up animations
 * when elements enter the viewport. Respects prefers-reduced-motion preference.
 */

import { prefersReducedMotion } from '../utils/helpers.js';

/**
 * Initialize scroll animations using Intersection Observer
 * Elements with 'scroll-reveal' class will animate when entering viewport
 */
export function initScrollAnimations() {
  // Respect user's reduced motion preference
  if (prefersReducedMotion()) {
    // Make all scroll-reveal elements visible immediately without animation
    const elements = document.querySelectorAll('.scroll-reveal');
    elements.forEach(el => {
      el.classList.add('visible');
    });
    return;
  }

  // Check for Intersection Observer support
  if (!('IntersectionObserver' in window)) {
    // Fallback: show all elements immediately
    const elements = document.querySelectorAll('.scroll-reveal');
    elements.forEach(el => {
      el.classList.add('visible');
    });
    return;
  }

  // Create Intersection Observer with threshold and rootMargin
  const observer = new IntersectionObserver(handleIntersection, {
    root: null, // viewport
    rootMargin: '0px 0px -50px 0px', // trigger slightly before element is fully visible
    threshold: 0.1 // 10% of element visible
  });

  // Observe all elements with scroll-reveal class
  const elements = document.querySelectorAll('.scroll-reveal');
  elements.forEach(el => {
    observer.observe(el);
  });
}

/**
 * Handle intersection events from observer
 * @param {IntersectionObserverEntry[]} entries - Intersection observer entries
 * @param {IntersectionObserver} observer - The observer instance
 */
function handleIntersection(entries, observer) {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      // Add visible class to trigger CSS animation
      entry.target.classList.add('visible');
      // Unobserve after animation is triggered (one-time animation)
      observer.unobserve(entry.target);
    }
  });
}

/**
 * Check if Intersection Observer API is available
 * @returns {boolean} True if Intersection Observer is supported
 */
export function isIntersectionObserverSupported() {
  return 'IntersectionObserver' in window;
}
