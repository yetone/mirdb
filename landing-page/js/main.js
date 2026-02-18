/**
 * Main JavaScript Entry Point
 *
 * This file is created by the first scenario builder.
 * Imports and initializes all JavaScript modules.
 */

// Module imports will be added by respective scenarios:
// - navigation.js (Scenario 2)
// - faq.js (Scenario 7)
// - smooth-scroll.js (Scenario 12)

document.addEventListener('DOMContentLoaded', function() {
  // Initialize modules as they are added
  console.log('Landing page initialized');

  // Initialize Smooth Scroll (Scenario 12)
  if (typeof SmoothScroll !== 'undefined' && SmoothScroll.initSmoothScroll) {
    SmoothScroll.initSmoothScroll();
    console.log('Smooth scroll initialized');
  }

  // Hero CTA click handler (Scenario 1)
  // Note: Now handled by SmoothScroll module, but keeping as fallback
  const heroCta = document.getElementById('hero-cta-primary');
  if (heroCta) {
    heroCta.addEventListener('click', function(e) {
      // Prevent default if it's a same-page anchor
      const href = heroCta.getAttribute('href');
      if (href && href.startsWith('#')) {
        e.preventDefault();
        const target = document.querySelector(href);
        if (target) {
          // Use SmoothScroll if available, otherwise fallback to scrollIntoView
          if (typeof SmoothScroll !== 'undefined' && SmoothScroll.scrollToElement) {
            SmoothScroll.scrollToElement(target);
          } else {
            target.scrollIntoView({ behavior: 'smooth' });
          }
        }
        // Dispatch custom event for tracking/testing
        heroCta.dispatchEvent(new CustomEvent('cta-click', {
          bubbles: true,
          detail: { action: 'hero-cta', target: href }
        }));
      }
    });
  }
});
