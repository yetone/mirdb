/**
 * Theme Management
 * Owner: Scenario 14 - Dark Mode Toggle
 *
 * Functions:
 * - initTheme(): Check system preference and stored preference
 * - toggleTheme(): Switch between light/dark
 * - saveThemePreference(): Persist to localStorage
 * - applyTheme(theme): Apply theme class to document
 * - getCurrentTheme(): Get the current theme
 */

const THEME_KEY = 'mirdb-theme';
const DARK_THEME = 'dark';
const LIGHT_THEME = 'light';

/**
 * Get the current theme from localStorage, system preference, or default to light
 * @returns {string} 'dark' or 'light'
 */
export function getStoredTheme() {
  if (typeof localStorage !== 'undefined') {
    return localStorage.getItem(THEME_KEY);
  }
  return null;
}

/**
 * Get system color scheme preference
 * @returns {string} 'dark' or 'light'
 */
export function getSystemPreference() {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? DARK_THEME : LIGHT_THEME;
  }
  return LIGHT_THEME;
}

/**
 * Get the current theme (stored or system preference)
 * @returns {string} 'dark' or 'light'
 */
export function getCurrentTheme() {
  const storedTheme = getStoredTheme();
  if (storedTheme) {
    return storedTheme;
  }
  return getSystemPreference();
}

/**
 * Apply theme to the document
 * @param {string} theme - 'dark' or 'light'
 */
export function applyTheme(theme) {
  if (typeof document !== 'undefined') {
    if (theme === DARK_THEME) {
      document.documentElement.setAttribute('data-theme', DARK_THEME);
    } else {
      document.documentElement.removeAttribute('data-theme');
    }
  }
}

/**
 * Save theme preference to localStorage
 * @param {string} theme - 'dark' or 'light'
 */
export function saveThemePreference(theme) {
  if (typeof localStorage !== 'undefined') {
    localStorage.setItem(THEME_KEY, theme);
  }
}

/**
 * Initialize theme based on stored preference or system preference
 */
export function initTheme() {
  const theme = getCurrentTheme();
  applyTheme(theme);

  // Listen for system preference changes (only if no stored preference)
  if (typeof window !== 'undefined' && window.matchMedia) {
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
      // Only apply system preference if no stored preference
      if (!getStoredTheme()) {
        applyTheme(e.matches ? DARK_THEME : LIGHT_THEME);
      }
    });
  }
}

/**
 * Toggle between light and dark themes
 * @returns {string} The new theme
 */
export function toggleTheme() {
  const currentTheme = getCurrentTheme();
  const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
  applyTheme(newTheme);
  saveThemePreference(newTheme);
  return newTheme;
}

/**
 * Set a specific theme
 * @param {string} theme - 'dark' or 'light'
 */
export function setTheme(theme) {
  if (theme === DARK_THEME || theme === LIGHT_THEME) {
    applyTheme(theme);
    saveThemePreference(theme);
  }
}

/**
 * Clear stored theme preference (will fall back to system preference)
 */
export function clearThemePreference() {
  if (typeof localStorage !== 'undefined') {
    localStorage.removeItem(THEME_KEY);
  }
  applyTheme(getSystemPreference());
}
