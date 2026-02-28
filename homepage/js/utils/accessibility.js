/**
 * Accessibility Utilities
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * Expected exports:
 * - initAccessibility(): void - Initialize accessibility features
 * - handleKeyboardNavigation(event: KeyboardEvent): void
 * - setFocusTrap(container: HTMLElement): void
 * - announceToScreenReader(message: string): void
 *
 * Requirements: NFR-1
 */

// Live region for screen reader announcements
let liveRegion = null;

/**
 * Initialize all accessibility features
 * Sets up keyboard navigation, focus management, and screen reader support
 */
export function initAccessibility() {
  createLiveRegion();
  setupSkipLink();
  setupKeyboardNavigation();
  setupFocusManagement();
  setupReducedMotion();
}

/**
 * Create a live region for screen reader announcements
 * Uses aria-live="polite" for non-urgent messages
 */
function createLiveRegion() {
  if (liveRegion) return;

  liveRegion = document.createElement('div');
  liveRegion.setAttribute('role', 'status');
  liveRegion.setAttribute('aria-live', 'polite');
  liveRegion.setAttribute('aria-atomic', 'true');
  liveRegion.className = 'sr-only';
  liveRegion.id = 'a11y-live-region';
  document.body.appendChild(liveRegion);
}

/**
 * Announce a message to screen readers
 * @param {string} message - The message to announce
 * @param {boolean} urgent - If true, uses assertive announcement
 */
export function announceToScreenReader(message, urgent = false) {
  if (!liveRegion) {
    createLiveRegion();
  }

  if (urgent) {
    liveRegion.setAttribute('aria-live', 'assertive');
  } else {
    liveRegion.setAttribute('aria-live', 'polite');
  }

  // Clear and set message for announcement
  liveRegion.textContent = '';
  // Use setTimeout to ensure the screen reader picks up the change
  setTimeout(() => {
    liveRegion.textContent = message;
  }, 100);
}

/**
 * Set up the skip-to-content link functionality
 */
function setupSkipLink() {
  const skipLink = document.querySelector('.skip-link');
  if (!skipLink) return;

  skipLink.addEventListener('click', (event) => {
    const targetId = skipLink.getAttribute('href').slice(1);
    const target = document.getElementById(targetId);

    if (target) {
      event.preventDefault();
      target.setAttribute('tabindex', '-1');
      target.focus();
      announceToScreenReader('Skipped to main content');

      // Remove tabindex after blur to avoid persistent focus issues
      target.addEventListener('blur', () => {
        target.removeAttribute('tabindex');
      }, { once: true });
    }
  });
}

/**
 * Handle keyboard navigation events
 * Supports arrow key navigation within components
 * @param {KeyboardEvent} event - The keyboard event
 */
export function handleKeyboardNavigation(event) {
  const { key, target } = event;

  // Handle Escape key to close modals/menus
  if (key === 'Escape') {
    handleEscapeKey(event);
    return;
  }

  // Handle arrow keys for navigation within lists
  if (['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(key)) {
    handleArrowNavigation(event);
  }

  // Handle Enter/Space for activation
  if (key === 'Enter' || key === ' ') {
    handleActivation(event);
  }
}

/**
 * Handle Escape key press
 * Closes any open menus or modals
 * @param {KeyboardEvent} event
 */
function handleEscapeKey(event) {
  // Close mobile menu if open
  const navMenu = document.getElementById('nav-menu');
  const mobileToggle = document.querySelector('.nav__mobile-toggle');

  if (navMenu && navMenu.getAttribute('aria-hidden') === 'false') {
    navMenu.setAttribute('aria-hidden', 'true');
    mobileToggle?.setAttribute('aria-expanded', 'false');
    mobileToggle?.focus();
    announceToScreenReader('Menu closed');
  }
}

/**
 * Handle arrow key navigation within lists
 * @param {KeyboardEvent} event
 */
function handleArrowNavigation(event) {
  const { key, target } = event;

  // Check if we're in a navigation context
  const navLinks = document.querySelectorAll('.nav__link');
  const linkArray = Array.from(navLinks);
  const currentIndex = linkArray.indexOf(target);

  if (currentIndex === -1) return;

  let newIndex;
  if (key === 'ArrowRight' || key === 'ArrowDown') {
    newIndex = (currentIndex + 1) % linkArray.length;
  } else if (key === 'ArrowLeft' || key === 'ArrowUp') {
    newIndex = (currentIndex - 1 + linkArray.length) % linkArray.length;
  }

  if (newIndex !== undefined) {
    event.preventDefault();
    linkArray[newIndex].focus();
  }
}

/**
 * Handle Enter/Space activation
 * @param {KeyboardEvent} event
 */
function handleActivation(event) {
  const { target, key } = event;

  // Prevent space from scrolling on buttons
  if (key === ' ' && target.tagName === 'BUTTON') {
    event.preventDefault();
    target.click();
  }
}

/**
 * Set up keyboard navigation listeners
 */
function setupKeyboardNavigation() {
  document.addEventListener('keydown', handleKeyboardNavigation);
}

/**
 * Set up focus management for better accessibility
 */
function setupFocusManagement() {
  // Add focus-visible polyfill behavior for older browsers
  document.body.addEventListener('mousedown', () => {
    document.body.classList.add('using-mouse');
  });

  document.body.addEventListener('keydown', (event) => {
    if (event.key === 'Tab') {
      document.body.classList.remove('using-mouse');
    }
  });
}

/**
 * Create a focus trap within a container
 * Useful for modals and dialogs
 * @param {HTMLElement} container - The container to trap focus within
 * @returns {Object} Object with activate() and deactivate() methods
 */
export function setFocusTrap(container) {
  if (!container) return null;

  const focusableSelectors = [
    'a[href]',
    'button:not([disabled])',
    'textarea:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    '[tabindex]:not([tabindex="-1"])'
  ];

  const getFocusableElements = () => {
    return container.querySelectorAll(focusableSelectors.join(', '));
  };

  let previousActiveElement = null;

  const handleTrapKeydown = (event) => {
    if (event.key !== 'Tab') return;

    const focusableElements = getFocusableElements();
    const firstElement = focusableElements[0];
    const lastElement = focusableElements[focusableElements.length - 1];

    if (event.shiftKey && document.activeElement === firstElement) {
      event.preventDefault();
      lastElement.focus();
    } else if (!event.shiftKey && document.activeElement === lastElement) {
      event.preventDefault();
      firstElement.focus();
    }
  };

  return {
    activate() {
      previousActiveElement = document.activeElement;
      container.addEventListener('keydown', handleTrapKeydown);

      const focusableElements = getFocusableElements();
      if (focusableElements.length > 0) {
        focusableElements[0].focus();
      }

      announceToScreenReader('Dialog opened');
    },

    deactivate() {
      container.removeEventListener('keydown', handleTrapKeydown);

      if (previousActiveElement) {
        previousActiveElement.focus();
      }

      announceToScreenReader('Dialog closed');
    }
  };
}

/**
 * Set up reduced motion preference handling
 * Respects user's prefers-reduced-motion setting
 */
function setupReducedMotion() {
  const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)');

  const handleReducedMotion = (event) => {
    if (event.matches) {
      document.documentElement.classList.add('reduce-motion');
    } else {
      document.documentElement.classList.remove('reduce-motion');
    }
  };

  // Initial check
  handleReducedMotion(mediaQuery);

  // Listen for changes
  mediaQuery.addEventListener('change', handleReducedMotion);
}

/**
 * Get all focusable elements within a container
 * @param {HTMLElement} container - The container to search within
 * @returns {NodeList} List of focusable elements
 */
export function getFocusableElements(container = document) {
  const focusableSelectors = [
    'a[href]',
    'button:not([disabled])',
    'textarea:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    '[tabindex]:not([tabindex="-1"])'
  ];

  return container.querySelectorAll(focusableSelectors.join(', '));
}

/**
 * Check if an element is visible and focusable
 * @param {HTMLElement} element - The element to check
 * @returns {boolean} True if element is visible and focusable
 */
export function isElementFocusable(element) {
  if (!element) return false;

  // Check if element is visible
  const style = window.getComputedStyle(element);
  if (style.display === 'none' || style.visibility === 'hidden') {
    return false;
  }

  // Check if element has dimensions
  const rect = element.getBoundingClientRect();
  if (rect.width === 0 || rect.height === 0) {
    return false;
  }

  // Check tabindex
  const tabindex = element.getAttribute('tabindex');
  if (tabindex !== null && parseInt(tabindex, 10) < 0) {
    return false;
  }

  return true;
}

/**
 * Move focus to the next focusable element
 * @param {HTMLElement} currentElement - The currently focused element
 * @param {boolean} reverse - Move backwards if true
 */
export function moveFocus(currentElement, reverse = false) {
  const focusable = Array.from(getFocusableElements());
  const currentIndex = focusable.indexOf(currentElement);

  if (currentIndex === -1) return;

  let nextIndex;
  if (reverse) {
    nextIndex = currentIndex === 0 ? focusable.length - 1 : currentIndex - 1;
  } else {
    nextIndex = (currentIndex + 1) % focusable.length;
  }

  focusable[nextIndex]?.focus();
}
