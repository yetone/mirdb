/**
 * Theme Toggle Component JavaScript
 * Owner: Scenario 14 - Dark Mode Toggle
 *
 * Initializes the theme toggle button and delegates theme management to theme.js
 */

import { initTheme, toggleTheme, getCurrentTheme } from '../../scripts/theme.js';

/**
 * Initialize the theme toggle button
 */
export function initThemeToggle() {
  const toggle = document.getElementById('theme-toggle');
  if (!toggle) return;

  // Update button state based on current theme
  updateToggleState(toggle);

  // Add click handler
  toggle.addEventListener('click', () => {
    toggleTheme();
    updateToggleState(toggle);
  });

  // Listen for theme changes (e.g., from system preference)
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    updateToggleState(toggle);
  });
}

/**
 * Update the toggle button aria-pressed state
 * @param {HTMLElement} toggle - The toggle button element
 */
function updateToggleState(toggle) {
  const isDark = getCurrentTheme() === 'dark';
  toggle.setAttribute('aria-pressed', isDark ? 'true' : 'false');
  toggle.setAttribute('aria-label', isDark ? 'Switch to light mode' : 'Switch to dark mode');
}

// Auto-initialize when DOM is ready
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      initTheme();
      initThemeToggle();
    });
  } else {
    initTheme();
    initThemeToggle();
  }
}
