/**
 * Main JavaScript Entry Point
 * Owner: First Builder
 *
 * Initializes all JavaScript functionality:
 * - Navigation smooth scrolling
 * - Code copy functionality
 * - Mermaid diagram rendering
 * - Theme detection
 *
 * Imports and initializes modules from components/ and utils/
 */

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  // Initialize smooth scrolling for anchor links
  initSmoothScrolling();
});

/**
 * Initialize smooth scrolling for internal anchor links
 */
function initSmoothScrolling() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', (e) => {
      const href = anchor.getAttribute('href');
      if (href && href !== '#') {
        const target = document.querySelector(href);
        if (target) {
          e.preventDefault();
          target.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
          });
        }
      }
    });
  });
}
