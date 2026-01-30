/**
 * Scroll Animations Module
 * Owner: Scenario 3 - Features Showcase Section
 *
 * Expected exports:
 * - initScrollAnimations(): Initialize Intersection Observer for animations
 *
 * Features:
 * - Fade-in animations when elements enter viewport
 * - Respects prefers-reduced-motion
 * - Performant GPU-accelerated animations
 */

(function() {
  'use strict';

  /**
   * Check if user prefers reduced motion
   * @returns {boolean} True if reduced motion is preferred
   */
  function prefersReducedMotion() {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  /**
   * Initialize scroll-triggered animations using Intersection Observer
   */
  function initScrollAnimations() {
    // If user prefers reduced motion, make all elements visible immediately
    if (prefersReducedMotion()) {
      const elements = document.querySelectorAll('.animate-on-scroll');
      elements.forEach(function(el) {
        el.classList.add('is-visible');
      });
      return;
    }

    // Check for Intersection Observer support
    if (!('IntersectionObserver' in window)) {
      // Fallback: make all elements visible
      var elements = document.querySelectorAll('.animate-on-scroll');
      elements.forEach(function(el) {
        el.classList.add('is-visible');
      });
      return;
    }

    // Create the observer
    var observerOptions = {
      root: null, // viewport
      rootMargin: '0px 0px -50px 0px', // trigger slightly before element is fully visible
      threshold: 0.1 // trigger when 10% of element is visible
    };

    var observer = new IntersectionObserver(function(entries, observer) {
      entries.forEach(function(entry) {
        if (entry.isIntersecting) {
          entry.target.classList.add('is-visible');
          // Optionally unobserve after animation (one-time animation)
          observer.unobserve(entry.target);
        }
      });
    }, observerOptions);

    // Observe all elements with animate-on-scroll class
    var animatedElements = document.querySelectorAll('.animate-on-scroll');
    animatedElements.forEach(function(el) {
      observer.observe(el);
    });
  }

  // Expose function globally
  window.initScrollAnimations = initScrollAnimations;

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initScrollAnimations);
  } else {
    initScrollAnimations();
  }
})();
