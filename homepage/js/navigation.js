/**
 * Navigation Module
 * Owner: Scenario 6 - Navigation and Footer
 *
 * Handles smooth scrolling and mobile menu functionality.
 */

/**
 * Initialize navigation behavior
 */
function initNavigation() {
  setupSmoothScroll();
  setupMobileMenu();
  setupSkipLink();
}

/**
 * Set up smooth scrolling for anchor links
 */
function setupSmoothScroll() {
  // Get all anchor links that point to sections on the page
  const anchorLinks = document.querySelectorAll('a[href^="#"]');

  anchorLinks.forEach(function(link) {
    link.addEventListener('click', function(event) {
      const targetId = this.getAttribute('href');

      // Skip if it's just "#" (home link)
      if (targetId === '#') {
        return;
      }

      const targetElement = document.querySelector(targetId);

      if (targetElement) {
        event.preventDefault();
        handleSmoothScroll(targetElement);

        // Close mobile menu if open
        closeMobileMenu();
      }
    });
  });
}

/**
 * Smooth scroll to a target element
 * @param {HTMLElement} targetElement - The element to scroll to
 */
function handleSmoothScroll(targetElement) {
  // Get the navigation height for offset
  const nav = document.querySelector('.nav');
  const navHeight = nav ? nav.offsetHeight : 0;

  // Calculate the target position with offset for sticky nav
  const targetPosition = targetElement.getBoundingClientRect().top + window.scrollY - navHeight;

  // Use native smooth scroll with fallback
  if ('scrollBehavior' in document.documentElement.style) {
    window.scrollTo({
      top: targetPosition,
      behavior: 'smooth'
    });
  } else {
    // Fallback for older browsers
    smoothScrollPolyfill(targetPosition);
  }

  // Set focus on target for accessibility
  targetElement.setAttribute('tabindex', '-1');
  targetElement.focus({ preventScroll: true });
}

/**
 * Smooth scroll polyfill for older browsers
 * @param {number} targetPosition - The target scroll position
 */
function smoothScrollPolyfill(targetPosition) {
  var startPosition = window.scrollY;
  var distance = targetPosition - startPosition;
  var duration = 500;
  var start = null;

  function step(timestamp) {
    if (!start) start = timestamp;
    var progress = timestamp - start;
    var percentage = Math.min(progress / duration, 1);

    // Easing function for smooth effect
    var easing = percentage < 0.5
      ? 2 * percentage * percentage
      : 1 - Math.pow(-2 * percentage + 2, 2) / 2;

    window.scrollTo(0, startPosition + distance * easing);

    if (progress < duration) {
      window.requestAnimationFrame(step);
    }
  }

  window.requestAnimationFrame(step);
}

/**
 * Set up mobile menu toggle functionality
 */
function setupMobileMenu() {
  var toggleButton = document.querySelector('.nav__mobile-toggle');
  var mobileMenu = document.querySelector('.nav__mobile-menu');

  if (!toggleButton || !mobileMenu) {
    return;
  }

  // Toggle menu on button click
  toggleButton.addEventListener('click', function() {
    toggleMobileMenu();
  });

  // Close menu on link click
  var mobileLinks = mobileMenu.querySelectorAll('a');
  mobileLinks.forEach(function(link) {
    link.addEventListener('click', function() {
      closeMobileMenu();
    });
  });

  // Close menu on Escape key
  document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape' && isMobileMenuOpen()) {
      closeMobileMenu();
      toggleButton.focus();
    }
  });

  // Close menu when clicking outside
  mobileMenu.addEventListener('click', function(event) {
    if (event.target === mobileMenu) {
      closeMobileMenu();
    }
  });
}

/**
 * Toggle mobile menu visibility
 */
function toggleMobileMenu() {
  var toggleButton = document.querySelector('.nav__mobile-toggle');
  var mobileMenu = document.querySelector('.nav__mobile-menu');

  if (!toggleButton || !mobileMenu) {
    return;
  }

  var isOpen = toggleButton.getAttribute('aria-expanded') === 'true';

  if (isOpen) {
    closeMobileMenu();
  } else {
    openMobileMenu();
  }
}

/**
 * Open the mobile menu
 */
function openMobileMenu() {
  var toggleButton = document.querySelector('.nav__mobile-toggle');
  var mobileMenu = document.querySelector('.nav__mobile-menu');

  if (!toggleButton || !mobileMenu) {
    return;
  }

  toggleButton.setAttribute('aria-expanded', 'true');
  mobileMenu.classList.add('nav__mobile-menu--open');
  mobileMenu.setAttribute('aria-hidden', 'false');

  // Prevent body scroll
  document.body.style.overflow = 'hidden';

  // Focus first link in menu
  var firstLink = mobileMenu.querySelector('a');
  if (firstLink) {
    firstLink.focus();
  }
}

/**
 * Close the mobile menu
 */
function closeMobileMenu() {
  var toggleButton = document.querySelector('.nav__mobile-toggle');
  var mobileMenu = document.querySelector('.nav__mobile-menu');

  if (!toggleButton || !mobileMenu) {
    return;
  }

  toggleButton.setAttribute('aria-expanded', 'false');
  mobileMenu.classList.remove('nav__mobile-menu--open');
  mobileMenu.setAttribute('aria-hidden', 'true');

  // Restore body scroll
  document.body.style.overflow = '';
}

/**
 * Check if mobile menu is open
 * @returns {boolean}
 */
function isMobileMenuOpen() {
  var toggleButton = document.querySelector('.nav__mobile-toggle');
  return toggleButton && toggleButton.getAttribute('aria-expanded') === 'true';
}

/**
 * Set up skip link functionality
 */
function setupSkipLink() {
  var skipLink = document.querySelector('.skip-link');

  if (!skipLink) {
    return;
  }

  skipLink.addEventListener('click', function(event) {
    var targetId = this.getAttribute('href');
    var targetElement = document.querySelector(targetId);

    if (targetElement) {
      event.preventDefault();
      targetElement.setAttribute('tabindex', '-1');
      targetElement.focus();
    }
  });
}

// Export functions for testing and external use
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    initNavigation: initNavigation,
    handleSmoothScroll: handleSmoothScroll,
    toggleMobileMenu: toggleMobileMenu,
    openMobileMenu: openMobileMenu,
    closeMobileMenu: closeMobileMenu,
    isMobileMenuOpen: isMobileMenuOpen
  };
}
