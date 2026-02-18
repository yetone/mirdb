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

  // Hero CTA click handler (Scenario 1)
  const heroCta = document.getElementById('hero-cta-primary');
  if (heroCta) {
    heroCta.addEventListener('click', function(e) {
      // Prevent default if it's a same-page anchor
      const href = heroCta.getAttribute('href');
      if (href && href.startsWith('#')) {
        e.preventDefault();
        const target = document.querySelector(href);
        if (target) {
          target.scrollIntoView({ behavior: 'smooth' });
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
