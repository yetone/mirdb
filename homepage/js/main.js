/**
 * Main JavaScript
 * Owner: Scenario 14 - Smooth Scrolling Navigation
 * Extended by: Scenario 17 - Hover Effects and Animations
 *
 * Expected exports/functionality:
 * - initSmoothScroll(): Initialize smooth scroll for anchor links
 * - prefersReducedMotion(): Check prefers-reduced-motion
 * - initScrollAnimations(): Initialize scroll-triggered animations
 *
 * Dependencies: None (vanilla JS)
 */

(function() {
  'use strict';

  /**
   * Check if user prefers reduced motion
   * @returns {boolean} - True if user prefers reduced motion
   */
  function prefersReducedMotion() {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }

  /**
   * Initialize smooth scrolling for anchor links
   */
  function initSmoothScroll() {
    // Skip smooth scroll if user prefers reduced motion
    if (prefersReducedMotion()) {
      return;
    }

    const anchorLinks = document.querySelectorAll('a[href^="#"]');

    anchorLinks.forEach(link => {
      link.addEventListener('click', (e) => {
        const targetId = link.getAttribute('href');
        if (targetId === '#') return;

        const targetElement = document.querySelector(targetId);
        if (targetElement) {
          e.preventDefault();
          targetElement.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
          });

          // Update focus for accessibility
          targetElement.setAttribute('tabindex', '-1');
          targetElement.focus({ preventScroll: true });
        }
      });
    });
  }

  /**
   * Initialize scroll-triggered animations using Intersection Observer
   * Owner: Scenario 17 - Hover Effects and Animations
   */
  function initScrollAnimations() {
    // Skip animations if user prefers reduced motion
    if (prefersReducedMotion()) {
      // Make all animated elements visible immediately
      const animatedElements = document.querySelectorAll('.animate-on-scroll, .animate-stagger');
      animatedElements.forEach(el => {
        el.classList.add('animated');
      });
      return;
    }

    // Configure Intersection Observer
    const observerOptions = {
      root: null, // Use viewport as root
      rootMargin: '0px 0px -50px 0px', // Trigger 50px before fully in view
      threshold: 0.1 // Trigger when 10% visible
    };

    const observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          // Add staggered delay for grid items
          const element = entry.target;
          const staggerDelay = element.dataset.animationDelay || 0;

          setTimeout(() => {
            element.classList.add('animated');
          }, parseInt(staggerDelay, 10));

          // Stop observing after animation is triggered
          observer.unobserve(element);
        }
      });
    }, observerOptions);

    // Observe feature cards
    const featureCards = document.querySelectorAll('.features-card');
    featureCards.forEach((card, index) => {
      card.classList.add('animate-on-scroll');
      card.dataset.animationDelay = (index * 100).toString(); // Stagger by 100ms
      observer.observe(card);
    });

    // Observe architecture components
    const archComponents = document.querySelectorAll('.arch-component');
    archComponents.forEach((component, index) => {
      component.classList.add('animate-on-scroll');
      component.dataset.animationDelay = (index * 100).toString();
      observer.observe(component);
    });

    // Observe status features
    const statusFeatures = document.querySelectorAll('.status-feature');
    statusFeatures.forEach((feature, index) => {
      feature.classList.add('animate-on-scroll');
      feature.dataset.animationDelay = (index * 75).toString();
      observer.observe(feature);
    });

    // Observe code examples
    const codeExamples = document.querySelectorAll('.code-example');
    codeExamples.forEach((example, index) => {
      example.classList.add('animate-on-scroll');
      example.dataset.animationDelay = (index * 100).toString();
      observer.observe(example);
    });

    // Observe major sections (hero tagline, section titles)
    const sections = document.querySelectorAll('.hero-tagline, .section-title, .arch-subtitle, .code-subtitle, .status-subtitle, .install-subtitle');
    sections.forEach((section) => {
      section.classList.add('animate-on-scroll');
      observer.observe(section);
    });
  }

  /**
   * Get all animatable elements
   * @returns {NodeList} - List of elements that can be animated
   */
  function getAnimatableElements() {
    return document.querySelectorAll('.animate-on-scroll, .animate-stagger');
  }

  // Initialize when DOM is ready
  function init() {
    initSmoothScroll();
    initScrollAnimations();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Export functions for potential external use and testing
  window.MirDBMain = {
    initSmoothScroll,
    prefersReducedMotion,
    initScrollAnimations,
    getAnimatableElements
  };
})();
