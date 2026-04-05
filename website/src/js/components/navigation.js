/**
 * Navigation Component
 * Owner: Scenario 12 - Navigation and Smooth Scrolling
 *
 * Handles mobile menu toggle and active nav item updates.
 */

let hamburgerBtn = null;
let mobileMenu = null;

/**
 * Toggle the mobile navigation menu visibility
 */
export function toggleMobileMenu() {
  if (!hamburgerBtn || !mobileMenu) return;

  const isExpanded = hamburgerBtn.getAttribute('aria-expanded') === 'true';
  hamburgerBtn.setAttribute('aria-expanded', String(!isExpanded));
  mobileMenu.classList.toggle('nav__mobile-menu--open');
}

/**
 * Close the mobile menu
 */
function closeMobileMenu() {
  if (!hamburgerBtn || !mobileMenu) return;

  hamburgerBtn.setAttribute('aria-expanded', 'false');
  mobileMenu.classList.remove('nav__mobile-menu--open');
}

/**
 * Update active navigation link based on scroll position
 */
function updateActiveNavLink() {
  const sections = document.querySelectorAll('main section[id]');
  const navLinks = document.querySelectorAll('.nav__links a');
  const headerOffset = document.querySelector('nav.nav')?.offsetHeight || 0;

  let currentSection = '';

  sections.forEach((section) => {
    const sectionTop = section.offsetTop - headerOffset - 50;
    const sectionBottom = sectionTop + section.offsetHeight;

    if (window.scrollY >= sectionTop && window.scrollY < sectionBottom) {
      currentSection = section.getAttribute('id');
    }
  });

  navLinks.forEach((link) => {
    link.classList.remove('nav__link--active');
    if (link.getAttribute('href') === `#${currentSection}`) {
      link.classList.add('nav__link--active');
    }
  });
}

/**
 * Initialize navigation functionality
 */
export function initNavigation() {
  hamburgerBtn = document.querySelector('[data-nav-toggle]');
  mobileMenu = document.querySelector('[data-nav-menu]');

  // Mobile menu toggle
  if (hamburgerBtn) {
    hamburgerBtn.addEventListener('click', toggleMobileMenu);
  }

  // Close mobile menu when clicking a link
  if (mobileMenu) {
    mobileMenu.querySelectorAll('a').forEach((link) => {
      link.addEventListener('click', closeMobileMenu);
    });
  }

  // Update active nav link on scroll
  window.addEventListener('scroll', updateActiveNavLink);

  // Initial update
  updateActiveNavLink();
}
