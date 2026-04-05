/**
 * Smooth Scroll Functionality
 * Owner: Scenario 12 - Navigation and Smooth Scrolling
 *
 * Provides smooth scrolling behavior for anchor links.
 * Accounts for fixed header offset.
 */

/**
 * Get the height of the fixed navigation header
 * @returns {number} Header height in pixels
 */
function getHeaderOffset() {
  const nav = document.querySelector('nav.nav');
  return nav ? nav.offsetHeight : 0;
}

/**
 * Scroll to a specific section by ID with smooth behavior
 * @param {string} sectionId - The ID of the section to scroll to (without #)
 */
export function scrollToSection(sectionId) {
  const section = document.getElementById(sectionId);
  if (!section) return;

  const headerOffset = getHeaderOffset();
  const sectionTop = section.getBoundingClientRect().top + window.scrollY;
  const targetPosition = sectionTop - headerOffset;

  window.scrollTo({
    top: targetPosition,
    behavior: 'smooth'
  });
}

/**
 * Initialize smooth scrolling for all anchor links
 */
export function initSmoothScroll() {
  // Select all anchor links that point to sections on this page
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach((link) => {
    link.addEventListener('click', (e) => {
      const href = link.getAttribute('href');

      // Skip if it's just "#" or empty
      if (!href || href === '#') return;

      const sectionId = href.substring(1);
      const targetSection = document.getElementById(sectionId);

      if (targetSection) {
        e.preventDefault();
        scrollToSection(sectionId);

        // Update URL hash without jumping
        history.pushState(null, '', href);
      }
    });
  });

  // Handle page load with hash in URL
  if (window.location.hash) {
    // Delay to ensure page is fully rendered
    setTimeout(() => {
      const sectionId = window.location.hash.substring(1);
      scrollToSection(sectionId);
    }, 100);
  }
}
