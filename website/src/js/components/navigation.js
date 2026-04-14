/**
 * Navigation Component
 * Owner: Scenario 7 - Navigation and Smooth Scrolling
 *
 * Expected exports:
 * - initNavigation(): void - Initialize navigation listeners
 * - smoothScrollTo(targetId: string): void - Scroll to section
 * - toggleMobileMenu(): void - Toggle mobile menu visibility
 */

/**
 * Initialize navigation functionality
 * Sets up smooth scrolling, mobile menu, and scroll-based header effects
 */
export function initNavigation() {
  initSmoothScrolling();
  initMobileMenu();
  initScrollEffects();
  initActiveNavHighlighting();
}

/**
 * Smoothly scroll to a target section
 * @param {string} targetId - The ID of the target element (without #)
 */
export function smoothScrollTo(targetId) {
  const target = document.getElementById(targetId);
  if (target) {
    target.scrollIntoView({
      behavior: 'smooth',
      block: 'start'
    });
    // Close mobile menu after navigation
    closeMobileMenu();
  }
}

/**
 * Toggle mobile menu visibility
 */
export function toggleMobileMenu() {
  const nav = document.querySelector('.nav');
  const toggleButton = document.querySelector('.mobile-menu-toggle');

  if (!nav || !toggleButton) return;

  const isOpen = nav.classList.contains('is-open');

  if (isOpen) {
    closeMobileMenu();
  } else {
    openMobileMenu();
  }
}

/**
 * Open mobile menu
 */
function openMobileMenu() {
  const nav = document.querySelector('.nav');
  const toggleButton = document.querySelector('.mobile-menu-toggle');

  if (!nav || !toggleButton) return;

  nav.classList.add('is-open');
  toggleButton.setAttribute('aria-expanded', 'true');
}

/**
 * Close mobile menu
 */
function closeMobileMenu() {
  const nav = document.querySelector('.nav');
  const toggleButton = document.querySelector('.mobile-menu-toggle');

  if (!nav || !toggleButton) return;

  nav.classList.remove('is-open');
  toggleButton.setAttribute('aria-expanded', 'false');
}

/**
 * Initialize smooth scrolling for navigation links
 */
function initSmoothScrolling() {
  const navLinks = document.querySelectorAll('.nav-link[href^="#"]');

  navLinks.forEach(link => {
    link.addEventListener('click', (e) => {
      const href = link.getAttribute('href');
      if (href && href !== '#') {
        e.preventDefault();
        const targetId = href.substring(1);
        smoothScrollTo(targetId);

        // Update URL hash without scrolling
        history.pushState(null, '', href);
      }
    });
  });
}

/**
 * Initialize mobile menu toggle
 */
function initMobileMenu() {
  const toggleButton = document.querySelector('.mobile-menu-toggle');

  if (!toggleButton) return;

  toggleButton.addEventListener('click', () => {
    toggleMobileMenu();
  });

  // Close menu when clicking outside
  document.addEventListener('click', (e) => {
    const nav = document.querySelector('.nav');
    const header = document.querySelector('.header');

    if (nav && nav.classList.contains('is-open')) {
      if (!header.contains(e.target)) {
        closeMobileMenu();
      }
    }
  });

  // Close menu on escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeMobileMenu();
    }
  });
}

/**
 * Initialize scroll-based header effects
 * Adds shadow to header when scrolled
 */
function initScrollEffects() {
  const header = document.querySelector('.header');

  if (!header) return;

  const handleScroll = () => {
    if (window.scrollY > 10) {
      header.classList.add('scrolled');
    } else {
      header.classList.remove('scrolled');
    }
  };

  // Run on load
  handleScroll();

  // Run on scroll with throttling
  let ticking = false;
  window.addEventListener('scroll', () => {
    if (!ticking) {
      window.requestAnimationFrame(() => {
        handleScroll();
        ticking = false;
      });
      ticking = true;
    }
  });
}

/**
 * Initialize active nav link highlighting based on scroll position
 */
function initActiveNavHighlighting() {
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.nav-link[href^="#"]');

  if (sections.length === 0 || navLinks.length === 0) return;

  const updateActiveLink = () => {
    const scrollPosition = window.scrollY + 100; // Offset for header

    let currentSection = '';

    sections.forEach(section => {
      const sectionTop = section.offsetTop;
      const sectionHeight = section.offsetHeight;

      if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
        currentSection = section.getAttribute('id');
      }
    });

    navLinks.forEach(link => {
      const href = link.getAttribute('href');
      if (href === `#${currentSection}`) {
        link.classList.add('active');
      } else {
        link.classList.remove('active');
      }
    });
  };

  // Run on scroll with throttling
  let ticking = false;
  window.addEventListener('scroll', () => {
    if (!ticking) {
      window.requestAnimationFrame(() => {
        updateActiveLink();
        ticking = false;
      });
      ticking = true;
    }
  });

  // Run on load
  updateActiveLink();
}

// Export for testing
export { closeMobileMenu, openMobileMenu };
