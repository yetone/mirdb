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
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
  anchor.addEventListener('click', function (e) {
    e.preventDefault();
    const target = document.querySelector(this.getAttribute('href'));
    if (target) {
      target.scrollIntoView({
        behavior: 'smooth',
        block: 'start'
      });
    }
  });
});
