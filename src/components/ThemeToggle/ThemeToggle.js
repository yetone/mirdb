/**
 * Theme Toggle Component
 * Owner: Scenario 8 - Theme Switching
 *
 * Handles theme toggle button interactions and delegates
 * actual theme management to the theme.js module.
 */

import { getTheme, setTheme, toggleTheme, STORAGE_KEY } from '../../scripts/theme.js';

/**
 * Initialize the theme toggle component
 */
export function initThemeToggle() {
  const toggleButton = document.getElementById('theme-toggle');

  if (!toggleButton) {
    console.warn('Theme toggle button not found');
    return;
  }

  // Update aria-pressed based on current theme
  updateToggleState(toggleButton);

  // Handle click events
  toggleButton.addEventListener('click', handleToggleClick);
}

/**
 * Handle theme toggle button click
 * @param {Event} event - Click event
 */
function handleToggleClick(event) {
  const newTheme = toggleTheme();
  const toggleButton = event.currentTarget;
  updateToggleState(toggleButton);
}

/**
 * Update the toggle button's aria-pressed state
 * @param {HTMLElement} toggleButton - The toggle button element
 */
function updateToggleState(toggleButton) {
  const currentTheme = getTheme();
  const isDark = currentTheme === 'dark';
  toggleButton.setAttribute('aria-pressed', isDark.toString());
  toggleButton.setAttribute('aria-label', isDark ? 'Switch to light mode' : 'Switch to dark mode');
}

// Auto-initialize when DOM is ready
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initThemeToggle);
  } else {
    initThemeToggle();
  }
}

// Export for testing
export { handleToggleClick, updateToggleState };
