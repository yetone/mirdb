/**
 * Theme Management
 * Owner: Scenario 14 - Dark Mode Toggle
 *
 * Functions:
 * - initTheme(): Check system preference and stored preference
 * - toggleTheme(): Switch between light/dark
 * - saveThemePreference(): Persist to localStorage
 * - applyTheme(theme): Apply theme class to document
 * - getCurrentTheme(): Get current theme
 */

const STORAGE_KEY = 'mirdb-theme';
const THEME_ATTRIBUTE = 'data-theme';

/**
 * Get the system color scheme preference
 * @returns {'light' | 'dark'} The system color scheme preference
 */
export function getSystemPreference() {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  return 'light';
}

/**
 * Get the stored theme preference from localStorage
 * @returns {string | null} The stored theme or null if not set
 */
export function getStoredPreference() {
  if (typeof localStorage !== 'undefined') {
    return localStorage.getItem(STORAGE_KEY);
  }
  return null;
}

/**
 * Save theme preference to localStorage
 * @param {'light' | 'dark'} theme - The theme to save
 */
export function saveThemePreference(theme) {
  if (typeof localStorage !== 'undefined') {
    localStorage.setItem(STORAGE_KEY, theme);
  }
}

/**
 * Apply theme to the document
 * @param {'light' | 'dark'} theme - The theme to apply
 */
export function applyTheme(theme) {
  if (typeof document !== 'undefined') {
    if (theme === 'dark') {
      document.documentElement.setAttribute(THEME_ATTRIBUTE, 'dark');
    } else {
      document.documentElement.removeAttribute(THEME_ATTRIBUTE);
    }
  }
}

/**
 * Get the current active theme
 * @returns {'light' | 'dark'} The current theme
 */
export function getCurrentTheme() {
  if (typeof document !== 'undefined') {
    return document.documentElement.getAttribute(THEME_ATTRIBUTE) === 'dark' ? 'dark' : 'light';
  }
  return 'light';
}

/**
 * Toggle between light and dark themes
 * @returns {'light' | 'dark'} The new theme after toggling
 */
export function toggleTheme() {
  const currentTheme = getCurrentTheme();
  const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

  applyTheme(newTheme);
  saveThemePreference(newTheme);

  return newTheme;
}

/**
 * Initialize theme based on stored preference or system preference
 */
export function initTheme() {
  // Check for stored preference first
  const storedTheme = getStoredPreference();

  if (storedTheme === 'dark' || storedTheme === 'light') {
    applyTheme(storedTheme);
  } else {
    // Fall back to system preference
    const systemTheme = getSystemPreference();
    applyTheme(systemTheme);
  }

  // Listen for system preference changes
  if (typeof window !== 'undefined' && window.matchMedia) {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    mediaQuery.addEventListener('change', (e) => {
      // Only apply system preference if no stored preference
      if (!getStoredPreference()) {
        applyTheme(e.matches ? 'dark' : 'light');
      }
    });
  }
}

// Auto-initialize theme on load
if (typeof document !== 'undefined') {
  // Initialize as early as possible to prevent flash
  initTheme();
}
