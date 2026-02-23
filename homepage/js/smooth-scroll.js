/**
 * Smooth Scrolling Functionality
 * Owner: Scenario 1 - Hero Section Display
 *
 * Handles smooth scrolling for in-page navigation:
 * - Get Started button scrolls to Quick Start section
 * - Any anchor links scroll smoothly
 */

/**
 * Initialize smooth scroll handlers for all anchor links
 */
export function initSmoothScroll() {
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(link => {
    link.addEventListener('click', handleSmoothScroll);
  });
}

/**
 * Handle smooth scroll to target element
 * @param {Event} event - Click event
 */
function handleSmoothScroll(event) {
  const href = event.currentTarget.getAttribute('href');

  // Skip if it's just "#"
  if (href === '#') return;

  const targetId = href.substring(1);
  const targetElement = document.getElementById(targetId);

  if (targetElement) {
    event.preventDefault();

    targetElement.scrollIntoView({
      behavior: 'smooth',
      block: 'start'
    });

    // Update URL hash without jumping
    history.pushState(null, null, href);

    // Set focus for accessibility
    targetElement.setAttribute('tabindex', '-1');
    targetElement.focus();
  }
}
