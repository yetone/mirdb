/**
 * Main JavaScript
 * Owner: Scenario 8 - Navigation and Footer
 *
 * Expected functionality:
 * - Smooth scroll for anchor links
 * - Mobile hamburger menu toggle
 * - Copy-to-clipboard for code blocks (Scenario 4)
 */

// Smooth scroll for anchor links
document.addEventListener('DOMContentLoaded', function() {
  // Handle smooth scrolling for anchor links
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
      const targetId = this.getAttribute('href');
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
});
