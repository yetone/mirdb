/**
 * Theme Toggle Component JavaScript
 * Owner: Scenario 14 - Dark Mode Toggle
 *
 * Functions:
 * - initThemeToggle(): Initialize the toggle button event listener
 * - updateToggleState(): Update button aria-pressed attribute
 */

import { toggleTheme, getCurrentTheme } from '../../scripts/theme.js';

/**
 * Initialize the theme toggle button
 */
export function initThemeToggle() {
  const toggleButton = document.getElementById('theme-toggle');

  if (!toggleButton) {
    return;
  }

  // Set initial aria-pressed state
  updateToggleState(toggleButton);

  // Add click event listener
  toggleButton.addEventListener('click', () => {
    toggleTheme();
    updateToggleState(toggleButton);
  });
}

/**
 * Update toggle button aria-pressed attribute based on current theme
 * @param {HTMLButtonElement} toggleButton - The theme toggle button element
 */
function updateToggleState(toggleButton) {
  const isDark = getCurrentTheme() === 'dark';
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
