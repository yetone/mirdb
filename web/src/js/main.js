/**
 * Main JavaScript entry point.
 * Owner: Scenario 1 - HTTP Server and Routing (base setup)
 *          Scenario 4 - Navigation Links (mobile menu toggle)
 *          Scenario 5 - CTA Buttons and Actions (click tracking)
 *          Scenario 8 - Dark Mode Theme (theme toggle, localStorage)
 *
 * Expected functionality:
 * - DOMContentLoaded initialization
 * - Theme preference detection and toggle
 * - Mobile navigation menu toggle
 * - Smooth scroll for anchor links
 * - CTA button click tracking and redirect handling
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialize theme
  const savedTheme = localStorage.getItem('theme') || 'light';
  document.documentElement.setAttribute('data-theme', savedTheme);

  // Initialize CTA button handlers
  initCTAButtons();
});

/**
 * Initialize CTA button click tracking and interactivity.
 * Owner: Scenario 5 - CTA Buttons and Actions
 */
function initCTAButtons() {
  // Track CTA clicks for analytics
  const ctaSelectors = [
    '[data-testid="hero-primary-cta"]',
    '[data-testid="hero-secondary-cta"]',
    '[data-testid="cta-create-short-url"]',
    '[data-testid="cta-get-started"]'
  ];

  ctaSelectors.forEach(selector => {
    const buttons = document.querySelectorAll(selector);
    buttons.forEach(button => {
      // Ensure buttons are keyboard accessible
      if (button.tagName.toLowerCase() !== 'a') {
        button.setAttribute('role', 'button');
        button.setAttribute('tabindex', '0');
      }

      // Add click tracking
      button.addEventListener('click', function(event) {
        const ctaLabel = button.textContent.trim();
        const ctaHref = button.getAttribute('href') || '#';

        // Store CTA click info in sessionStorage for potential post-redirect use
        sessionStorage.setItem('lastCtaClicked', JSON.stringify({
          label: ctaLabel,
          href: ctaHref,
          timestamp: Date.now()
        }));

        // For external or non-fragment links, allow default navigation
        if (ctaHref && !ctaHref.startsWith('#')) {
          return; // Let the browser handle the redirect
        }

        // For fragment links, smooth scroll
        if (ctaHref && ctaHref.startsWith('#')) {
          event.preventDefault();
          const target = document.querySelector(ctaHref);
          if (target) {
            target.scrollIntoView({ behavior: 'smooth' });
          }
        }
      });

      // Add keyboard support for non-anchor elements
      button.addEventListener('keydown', function(event) {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          button.click();
        }
      });
    });
  });
}
