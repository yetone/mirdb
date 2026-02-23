/**
 * Smooth Scrolling Functionality
 * Owner: Scenario 1 - Hero Section Display
 *
 * Handles smooth scrolling for in-page navigation:
 * - Get Started button scrolls to Quick Start section
 * - Any anchor links scroll smoothly
 */

/**
 * Handle smooth scrolling for anchor links
 * @param {Event} event - The click event
 */
function handleSmoothScroll(event) {
  const href = event.currentTarget.getAttribute('href');

  if (!href || !href.startsWith('#')) return;

  const targetId = href.substring(1);
  const targetElement = document.getElementById(targetId);

  if (targetElement) {
    event.preventDefault();
    targetElement.scrollIntoView({
      behavior: 'smooth',
      block: 'start'
    });

    // Update URL without triggering navigation
    history.pushState(null, null, href);

    // Set focus to target for accessibility
    targetElement.setAttribute('tabindex', '-1');
    targetElement.focus({ preventScroll: true });
  }
}

/**
 * Initialize smooth scrolling for all anchor links
 */
function initSmoothScroll() {
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach((link) => {
    link.addEventListener('click', handleSmoothScroll);
  });
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initSmoothScroll);
} else {
  initSmoothScroll();
}

// Export for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initSmoothScroll };
}
