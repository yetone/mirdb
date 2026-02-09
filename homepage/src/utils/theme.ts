/**
 * Theme Management Utility.
 * Owner: Scenario 9 - Dark Mode Support
 *
 * Handles theme state, localStorage persistence, and system preference detection.
 */

import type { Theme } from '../types';

const THEME_STORAGE_KEY = 'mirdb-theme';

/**
 * Get the user's system color scheme preference.
 */
function getSystemPreference(): Theme {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  return 'light';
}

/**
 * Get the current theme from localStorage or fall back to system preference.
 */
export function getTheme(): Theme {
  if (typeof window !== 'undefined' && window.localStorage) {
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === 'dark' || stored === 'light') {
      return stored;
    }
  }
  return getSystemPreference();
}

/**
 * Set the theme and persist to localStorage.
 */
export function setTheme(theme: Theme): void {
  if (typeof window !== 'undefined' && window.localStorage) {
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  }
  applyTheme(theme);
}

/**
 * Toggle between light and dark themes.
 */
export function toggleTheme(): void {
  const currentTheme = getTheme();
  const newTheme: Theme = currentTheme === 'light' ? 'dark' : 'light';
  setTheme(newTheme);
}

/**
 * Apply the theme to the document by setting the data-theme attribute.
 */
function applyTheme(theme: Theme): void {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
}

/**
 * Initialize the theme on page load.
 * Applies the stored theme or system preference.
 */
export function initTheme(): void {
  const theme = getTheme();
  applyTheme(theme);

  // Listen for system preference changes
  if (typeof window !== 'undefined' && window.matchMedia) {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    mediaQuery.addEventListener('change', (e) => {
      // Only update if user hasn't set a preference
      const stored = localStorage.getItem(THEME_STORAGE_KEY);
      if (!stored) {
        applyTheme(e.matches ? 'dark' : 'light');
      }
    });
  }
}
