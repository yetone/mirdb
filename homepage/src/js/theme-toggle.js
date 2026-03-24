/**
 * Theme Toggle Module
 * Owner: Scenario 10 - Dark Theme Support
 *
 * Handles dark/light theme switching with:
 * - localStorage persistence
 * - System preference detection (prefers-color-scheme)
 * - ARIA attributes for accessibility
 */

const STORAGE_KEY = 'mirdb-theme';
const THEME_ATTRIBUTE = 'data-theme';
const DARK_THEME = 'dark';
const LIGHT_THEME = 'light';

/**
 * Get the user's system color scheme preference
 * @returns {'dark' | 'light'} System preference
 */
function getSystemPreference() {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? DARK_THEME : LIGHT_THEME;
  }
  return LIGHT_THEME;
}

/**
 * Get the stored theme preference from localStorage
 * @returns {string | null} Stored theme or null
 */
function getStoredTheme() {
  if (typeof localStorage !== 'undefined') {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      // localStorage may be unavailable in some contexts
      return null;
    }
  }
  return null;
}

/**
 * Store theme preference in localStorage
 * @param {string} theme - Theme to store
 */
function storeTheme(theme) {
  if (typeof localStorage !== 'undefined') {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {
      // Silently fail if localStorage is unavailable
    }
  }
}

/**
 * Apply theme to the document
 * @param {string} theme - Theme to apply ('dark' or 'light')
 */
function applyTheme(theme) {
  const validTheme = theme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
  document.documentElement.setAttribute(THEME_ATTRIBUTE, validTheme);

  // Update theme toggle button aria-label
  const toggleBtn = document.getElementById('theme-toggle');
  if (toggleBtn) {
    const newLabel = validTheme === DARK_THEME
      ? 'Switch to light theme'
      : 'Switch to dark theme';
    toggleBtn.setAttribute('aria-label', newLabel);
  }
}

/**
 * Get the current theme
 * @returns {'dark' | 'light'} Current theme
 */
export function getTheme() {
  const currentTheme = document.documentElement.getAttribute(THEME_ATTRIBUTE);
  return currentTheme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
}

/**
 * Toggle between dark and light themes
 * @returns {'dark' | 'light'} New theme after toggle
 */
export function toggleTheme() {
  const currentTheme = getTheme();
  const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;

  applyTheme(newTheme);
  storeTheme(newTheme);

  return newTheme;
}

/**
 * Set a specific theme
 * @param {'dark' | 'light'} theme - Theme to set
 */
export function setTheme(theme) {
  const validTheme = theme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
  applyTheme(validTheme);
  storeTheme(validTheme);
}

/**
 * Initialize theme from stored preference or system preference
 * Sets up the theme toggle button event listener
 */
export function initTheme() {
  // Determine initial theme: stored preference > system preference > light
  const storedTheme = getStoredTheme();
  const initialTheme = storedTheme || getSystemPreference();

  // Apply the initial theme
  applyTheme(initialTheme);

  // Set up toggle button listener
  const toggleBtn = document.getElementById('theme-toggle');
  if (toggleBtn) {
    // Remove any existing inline handlers
    toggleBtn.onclick = null;

    // Add event listener for theme toggle
    toggleBtn.addEventListener('click', () => {
      toggleTheme();
    });
  }

  // Listen for system preference changes
  if (typeof window !== 'undefined' && window.matchMedia) {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    // Only react to system changes if no stored preference
    mediaQuery.addEventListener('change', (e) => {
      if (!getStoredTheme()) {
        applyTheme(e.matches ? DARK_THEME : LIGHT_THEME);
      }
    });
  }
}

// Export constants for testing
export { STORAGE_KEY, THEME_ATTRIBUTE, DARK_THEME, LIGHT_THEME };
