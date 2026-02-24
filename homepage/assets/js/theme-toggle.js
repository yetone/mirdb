/**
 * Theme Toggle Module
 * Owner: Scenario 9 - Dark Mode Theme Toggle
 *
 * Exports:
 * - initTheme(): Initialize theme from localStorage or system preference
 * - toggleTheme(): Switch between light and dark modes
 * - getTheme(): Get current theme
 *
 * Implementation:
 * - Read/write to localStorage key 'theme'
 * - Set data-theme attribute on document root
 * - Support prefers-color-scheme media query
 */

const STORAGE_KEY = 'theme';
const DARK_THEME = 'dark';
const LIGHT_THEME = 'light';

/**
 * Get the user's preferred theme from system settings
 * @returns {'dark' | 'light'} The system preferred theme
 */
function getSystemPreference() {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? DARK_THEME : LIGHT_THEME;
  }
  return LIGHT_THEME;
}

/**
 * Get stored theme from localStorage
 * @returns {string | null} The stored theme or null if not set
 */
function getStoredTheme() {
  if (typeof localStorage !== 'undefined') {
    return localStorage.getItem(STORAGE_KEY);
  }
  return null;
}

/**
 * Store theme preference in localStorage
 * @param {string} theme - The theme to store ('light' or 'dark')
 */
function storeTheme(theme) {
  if (typeof localStorage !== 'undefined') {
    localStorage.setItem(STORAGE_KEY, theme);
  }
}

/**
 * Apply theme to the document
 * @param {string} theme - The theme to apply ('light' or 'dark')
 */
function applyTheme(theme) {
  if (typeof document !== 'undefined') {
    document.documentElement.setAttribute('data-theme', theme);
  }
}

/**
 * Get the current theme from the document
 * @returns {string} The current theme ('light' or 'dark')
 */
export function getTheme() {
  if (typeof document !== 'undefined') {
    const theme = document.documentElement.getAttribute('data-theme');
    return theme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
  }
  return LIGHT_THEME;
}

/**
 * Initialize theme from localStorage or system preference.
 * Should be called as early as possible to prevent flash of wrong theme.
 */
export function initTheme() {
  // Check localStorage first
  const storedTheme = getStoredTheme();

  if (storedTheme === DARK_THEME || storedTheme === LIGHT_THEME) {
    applyTheme(storedTheme);
    return storedTheme;
  }

  // Fall back to system preference
  const systemTheme = getSystemPreference();
  applyTheme(systemTheme);

  // Store the initial preference
  storeTheme(systemTheme);

  return systemTheme;
}

/**
 * Toggle between light and dark themes
 * @returns {string} The new theme after toggling
 */
export function toggleTheme() {
  const currentTheme = getTheme();
  const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;

  applyTheme(newTheme);
  storeTheme(newTheme);

  return newTheme;
}

/**
 * Set up event listeners for theme toggle buttons
 */
export function initThemeToggle() {
  // Initialize theme first
  initTheme();

  // Set up click handlers for all theme toggle buttons
  const toggleButtons = document.querySelectorAll('.theme-toggle');

  toggleButtons.forEach(button => {
    button.addEventListener('click', () => {
      const newTheme = toggleTheme();

      // Update aria-label based on current state
      const label = newTheme === DARK_THEME
        ? 'Switch to light mode'
        : 'Switch to dark mode';
      button.setAttribute('aria-label', label);
    });
  });

  // Listen for system preference changes
  if (typeof window !== 'undefined' && window.matchMedia) {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    // Only update if user hasn't set a preference
    mediaQuery.addEventListener('change', (e) => {
      const storedTheme = getStoredTheme();
      // If no stored preference, follow system
      if (!storedTheme) {
        const systemTheme = e.matches ? DARK_THEME : LIGHT_THEME;
        applyTheme(systemTheme);
      }
    });
  }
}

// Export for testing
export { STORAGE_KEY, DARK_THEME, LIGHT_THEME, getStoredTheme, storeTheme, applyTheme, getSystemPreference };
