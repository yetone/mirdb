/**
 * Smooth Scroll Module
 * Owner: Scenario 14 - Navigation and Smooth Scrolling
 *
 * Exports:
 * - initSmoothScroll(): Set up smooth scrolling for anchor links
 */

export function initSmoothScroll() {
  const links = document.querySelectorAll('a[href^="#"]');

  links.forEach(link => {
    link.addEventListener('click', (e) => {
      const href = link.getAttribute('href');
      if (href === '#') return;

      const target = document.querySelector(href);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
}
