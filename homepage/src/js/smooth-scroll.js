/**
 * Smooth Scroll Module
 * Owner: Scenario 13 - User Interaction Patterns
 *
 * Expected exports:
 * - initSmoothScroll(): Enable smooth scrolling for anchor links
 *
 * Features:
 * - Smooth scroll behavior
 * - Handle offset for sticky header
 */

/**
 * Get the height of the sticky header for scroll offset calculation
 * @returns {number} - The header height in pixels
 */
function getHeaderHeight() {
  const header = document.querySelector('.header');
  if (header) {
    return header.getBoundingClientRect().height;
  }
  return 0;
}

/**
 * Smoothly scroll to a target element with header offset
 * @param {string} targetId - The ID of the target element (without #)
 */
function scrollToTarget(targetId) {
  const target = document.getElementById(targetId);
  if (!target) return;

  const headerHeight = getHeaderHeight();
  const targetPosition = target.getBoundingClientRect().top + window.scrollY;
  const offsetPosition = targetPosition - headerHeight - 16; // 16px extra padding

  window.scrollTo({
    top: offsetPosition,
    behavior: 'smooth'
  });
}

/**
 * Handle click events on anchor links
 * @param {Event} event - The click event
 */
function handleAnchorClick(event) {
  const link = event.currentTarget;
  const href = link.getAttribute('href');

  // Only handle internal anchor links
  if (!href || !href.startsWith('#')) return;

  const targetId = href.substring(1);
  if (!targetId) return;

  // Check if target element exists
  const target = document.getElementById(targetId);
  if (!target) return;

  // Prevent default behavior and scroll manually with offset
  event.preventDefault();
  scrollToTarget(targetId);

  // Update URL hash without jumping
  if (history.pushState) {
    history.pushState(null, null, href);
  }

  // Set focus on target for accessibility
  target.setAttribute('tabindex', '-1');
  target.focus({ preventScroll: true });
}

/**
 * Initialize smooth scrolling for all anchor links
 * Attaches click handlers to internal anchor links
 */
function initSmoothScroll() {
  // Find all internal anchor links
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(link => {
    // Skip if already initialized
    if (link.dataset.smoothScrollInit) return;

    link.addEventListener('click', handleAnchorClick);
    link.dataset.smoothScrollInit = 'true';
  });

  // Handle hash in URL on page load
  if (window.location.hash) {
    const targetId = window.location.hash.substring(1);
    // Delay to allow page to fully render
    setTimeout(() => {
      scrollToTarget(targetId);
    }, 100);
  }
}

// Export for ES modules
export { initSmoothScroll, scrollToTarget, getHeaderHeight };

// Make available globally for browser
if (typeof window !== 'undefined') {
  window.SmoothScroll = { initSmoothScroll, scrollToTarget, getHeaderHeight };
}
