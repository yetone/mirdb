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
});

function initSkipLink() {
  const skipLink = document.querySelector('.skip-link');
  const mainContent = document.getElementById('main-content');

  if (skipLink && mainContent) {
    skipLink.addEventListener('click', (e) => {
      e.preventDefault();
      mainContent.setAttribute('tabindex', '-1');
      mainContent.focus();
      mainContent.removeAttribute('tabindex');
    });
  }
}

export { initSkipLink };
