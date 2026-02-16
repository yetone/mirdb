/**
 * Main JavaScript Entry Point
 *
 * Initializes all JavaScript modules for the MirDB homepage.
 */

document.addEventListener('DOMContentLoaded', () => {
  // Initialize smooth scroll for anchor links
  initSmoothScroll();
});

/**
 * Initialize smooth scroll for anchor links
 */
function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', (e) => {
      const targetId = anchor.getAttribute('href');
      if (targetId === '#') return;

      const targetElement = document.querySelector(targetId);
      if (targetElement) {
        e.preventDefault();
        targetElement.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
}
