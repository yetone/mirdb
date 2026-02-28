/**
 * Main JavaScript Entry Point
 * Owner: Scenario 8 - Keyboard Navigation Accessibility
 * Extended by: Other scenarios as needed
 *
 * Functions:
 * - initSkipLink(): Initialize skip-to-main functionality
 * - manageFocus(): Focus management utilities
 * - initKeyboardNav(): Keyboard navigation handlers
 */

document.addEventListener('DOMContentLoaded', () => {
  initSkipLink();
  initKeyboardNav();
});

/**
 * Initialize skip-to-main-content link functionality
 * Allows keyboard users to bypass navigation and jump to main content
 */
function initSkipLink() {
  const skipLink = document.querySelector('.skip-link');
  const mainContent = document.getElementById('main-content');

  if (skipLink && mainContent) {
    skipLink.addEventListener('click', (e) => {
      e.preventDefault();
      mainContent.setAttribute('tabindex', '-1');
      mainContent.focus();
      // Keep tabindex to allow focus styling
      // Remove after blur to maintain proper tab order
      mainContent.addEventListener('blur', function onBlur() {
        mainContent.removeAttribute('tabindex');
        mainContent.removeEventListener('blur', onBlur);
      }, { once: true });
    });

    // Also handle Enter key explicitly for accessibility
    skipLink.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        mainContent.setAttribute('tabindex', '-1');
        mainContent.focus();
        mainContent.addEventListener('blur', function onBlur() {
          mainContent.removeAttribute('tabindex');
          mainContent.removeEventListener('blur', onBlur);
        }, { once: true });
      }
    });
  }
}

/**
 * Initialize keyboard navigation handlers
 * Ensures all interactive elements respond appropriately to keyboard input
 */
function initKeyboardNav() {
  // Ensure all anchor links work with Enter key
  document.querySelectorAll('a[href^="#"]').forEach(link => {
    link.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        const targetId = link.getAttribute('href').slice(1);
        const targetElement = document.getElementById(targetId);
        if (targetElement) {
          // Smooth scroll to target
          targetElement.scrollIntoView({ behavior: 'smooth' });
          // Set focus to target for accessibility
          if (!targetElement.hasAttribute('tabindex')) {
            targetElement.setAttribute('tabindex', '-1');
            targetElement.addEventListener('blur', function onBlur() {
              targetElement.removeAttribute('tabindex');
              targetElement.removeEventListener('blur', onBlur);
            }, { once: true });
          }
          targetElement.focus();
        }
      }
    });
  });

  // Add roving tabindex for navigation menu
  initRovingTabindex('.nav__menu', '.nav__link');
}

/**
 * Implement roving tabindex pattern for menu navigation
 * Allows arrow key navigation within menu groups
 * @param {string} containerSelector - CSS selector for the menu container
 * @param {string} itemSelector - CSS selector for menu items
 */
function initRovingTabindex(containerSelector, itemSelector) {
  const container = document.querySelector(containerSelector);
  if (!container) return;

  const items = container.querySelectorAll(itemSelector);
  if (items.length === 0) return;

  container.addEventListener('keydown', (e) => {
    const currentIndex = Array.from(items).findIndex(item => item === document.activeElement);
    if (currentIndex === -1) return;

    let nextIndex;

    switch (e.key) {
      case 'ArrowRight':
      case 'ArrowDown':
        e.preventDefault();
        nextIndex = (currentIndex + 1) % items.length;
        items[nextIndex].focus();
        break;
      case 'ArrowLeft':
      case 'ArrowUp':
        e.preventDefault();
        nextIndex = (currentIndex - 1 + items.length) % items.length;
        items[nextIndex].focus();
        break;
      case 'Home':
        e.preventDefault();
        items[0].focus();
        break;
      case 'End':
        e.preventDefault();
        items[items.length - 1].focus();
        break;
    }
  });
}

/**
 * Focus management utility
 * Helps manage focus for dynamic content and modal-like interactions
 * @param {HTMLElement} element - Element to focus
 * @param {Object} options - Focus options
 */
function manageFocus(element, options = {}) {
  if (!element) return;

  const { preventScroll = false, temporary = false } = options;

  // Make element focusable if it isn't already
  if (!element.hasAttribute('tabindex') && !isFocusable(element)) {
    element.setAttribute('tabindex', '-1');
    if (temporary) {
      element.addEventListener('blur', function onBlur() {
        element.removeAttribute('tabindex');
        element.removeEventListener('blur', onBlur);
      }, { once: true });
    }
  }

  element.focus({ preventScroll });
}

/**
 * Check if an element is naturally focusable
 * @param {HTMLElement} element - Element to check
 * @returns {boolean} - Whether the element is focusable
 */
function isFocusable(element) {
  const focusableSelectors = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
    '[contenteditable="true"]'
  ];

  return focusableSelectors.some(selector => element.matches(selector));
}

/**
 * Get all focusable elements within a container
 * @param {HTMLElement} container - Container element
 * @returns {HTMLElement[]} - Array of focusable elements
 */
function getFocusableElements(container) {
  const focusableSelectors = [
    'a[href]',
    'button:not([disabled])',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])'
  ].join(', ');

  return Array.from(container.querySelectorAll(focusableSelectors))
    .filter(el => {
      // Filter out hidden elements
      const style = getComputedStyle(el);
      return style.display !== 'none' && style.visibility !== 'hidden';
    });
}

export { initSkipLink, initKeyboardNav, manageFocus, getFocusableElements, isFocusable };
