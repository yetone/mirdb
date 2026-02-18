/**
 * Navigation Module
 * Owner: Scenario 2 - Navigation & Menu
 *
 * Expected exports:
 * - initNavigation(): Initialize sticky nav behavior
 * - toggleMobileMenu(): Toggle mobile menu open/close
 * - closeMobileMenu(): Close mobile menu
 *
 * Features:
 * - Sticky navigation on scroll
 * - Mobile hamburger menu toggle
 * - Active link highlighting
 */

/**
 * Navigation state
 */
let mobileMenuOpen = false;

/**
 * DOM element references
 */
let header = null;
let mobileToggle = null;
let mobileMenu = null;
let navLinks = null;
let mobileLinks = null;

/**
 * Initialize navigation functionality
 */
function initNavigation() {
  // Get DOM elements
  header = document.querySelector('.header');
  mobileToggle = document.getElementById('nav-mobile-toggle');
  mobileMenu = document.getElementById('nav-mobile-menu');
  navLinks = document.querySelectorAll('.nav-link');
  mobileLinks = document.querySelectorAll('.nav-mobile-link');

  if (!header || !mobileToggle || !mobileMenu) {
    return;
  }

  // Initialize sticky navigation
  initStickyNav();

  // Initialize mobile menu
  initMobileMenu();

  // Initialize keyboard navigation
  initKeyboardNav();

  // Close mobile menu when clicking outside
  initOutsideClickHandler();

  // Close mobile menu when clicking nav links
  initNavLinkClickHandlers();
}

/**
 * Initialize sticky navigation behavior
 */
function initStickyNav() {
  let lastScrollY = window.scrollY;

  window.addEventListener('scroll', function() {
    const currentScrollY = window.scrollY;

    // Add scrolled class when scrolled past threshold
    if (currentScrollY > 50) {
      header.classList.add('scrolled');
    } else {
      header.classList.remove('scrolled');
    }

    lastScrollY = currentScrollY;
  }, { passive: true });
}

/**
 * Initialize mobile menu toggle functionality
 */
function initMobileMenu() {
  mobileToggle.addEventListener('click', function(e) {
    e.preventDefault();
    toggleMobileMenu();
  });
}

/**
 * Toggle mobile menu open/close
 */
function toggleMobileMenu() {
  mobileMenuOpen = !mobileMenuOpen;
  updateMobileMenuState();
}

/**
 * Open mobile menu
 */
function openMobileMenu() {
  mobileMenuOpen = true;
  updateMobileMenuState();
}

/**
 * Close mobile menu
 */
function closeMobileMenu() {
  mobileMenuOpen = false;
  updateMobileMenuState();
}

/**
 * Update mobile menu DOM state based on mobileMenuOpen flag
 */
function updateMobileMenuState() {
  if (mobileMenuOpen) {
    mobileToggle.setAttribute('aria-expanded', 'true');
    mobileMenu.classList.add('open');
    mobileMenu.setAttribute('aria-hidden', 'false');
    // Focus first link in mobile menu for accessibility
    const firstLink = mobileMenu.querySelector('.nav-mobile-link');
    if (firstLink) {
      firstLink.focus();
    }
  } else {
    mobileToggle.setAttribute('aria-expanded', 'false');
    mobileMenu.classList.remove('open');
    mobileMenu.setAttribute('aria-hidden', 'true');
  }
}

/**
 * Initialize keyboard navigation (Escape to close menu)
 */
function initKeyboardNav() {
  document.addEventListener('keydown', function(e) {
    // Close mobile menu on Escape key
    if (e.key === 'Escape' && mobileMenuOpen) {
      closeMobileMenu();
      mobileToggle.focus();
    }
  });
}

/**
 * Initialize outside click handler to close mobile menu
 */
function initOutsideClickHandler() {
  document.addEventListener('click', function(e) {
    if (!mobileMenuOpen) return;

    const isClickInsideMenu = mobileMenu.contains(e.target);
    const isClickOnToggle = mobileToggle.contains(e.target);

    if (!isClickInsideMenu && !isClickOnToggle) {
      closeMobileMenu();
    }
  });
}

/**
 * Initialize click handlers for nav links to close mobile menu
 */
function initNavLinkClickHandlers() {
  // Close mobile menu when clicking any mobile link
  mobileLinks.forEach(function(link) {
    link.addEventListener('click', function() {
      closeMobileMenu();
    });
  });

  // Also handle mobile CTA button
  const mobileCta = mobileMenu.querySelector('.nav-mobile-cta');
  if (mobileCta) {
    mobileCta.addEventListener('click', function() {
      closeMobileMenu();
    });
  }
}

/**
 * Check if mobile menu is currently open
 * @returns {boolean}
 */
function isMobileMenuOpen() {
  return mobileMenuOpen;
}

// Export functions for use in other modules and tests
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    initNavigation,
    toggleMobileMenu,
    openMobileMenu,
    closeMobileMenu,
    isMobileMenuOpen
  };
}

// Auto-initialize when DOM is ready
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initNavigation);
  } else {
    // DOM is already ready
    initNavigation();
  }
}
