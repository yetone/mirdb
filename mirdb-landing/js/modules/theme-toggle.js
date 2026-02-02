/**
 * Theme Toggle Module
 * Owner: Scenario 14 - Dark Mode Support
 *
 * Handles:
 * - Detecting prefers-color-scheme
 * - Toggle button click handler
 * - Persisting preference to localStorage
 * - Applying theme class to document
 */

const STORAGE_KEY = 'theme';
const THEME_DARK = 'dark';
const THEME_LIGHT = 'light';

/**
 * Get the user's system color scheme preference
 * @returns {'dark' | 'light'} The system preference
 */
function getSystemPreference() {
  if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    return THEME_DARK;
  }
  return THEME_LIGHT;
}

/**
 * Get the stored theme preference from localStorage
 * @returns {string | null} The stored theme or null if not set
 */
function getStoredPreference() {
  try {
    return localStorage.getItem(STORAGE_KEY);
  } catch (e) {
    // localStorage might be unavailable (e.g., private browsing)
    return null;
  }
}

/**
 * Store the theme preference in localStorage
 * @param {string} theme - The theme to store
 */
function setStoredPreference(theme) {
  try {
    localStorage.setItem(STORAGE_KEY, theme);
  } catch (e) {
    // localStorage might be unavailable
    console.warn('Unable to store theme preference:', e);
  }
}

/**
 * Apply the theme to the document
 * @param {string} theme - The theme to apply ('dark' or 'light')
 */
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  updateToggleButton(theme);
}

/**
 * Update the toggle button's aria-label and visual state
 * @param {string} currentTheme - The current theme
 */
function updateToggleButton(currentTheme) {
  const toggleButton = document.querySelector('[data-theme-toggle], .theme-toggle, #theme-toggle');
  if (toggleButton) {
    const nextTheme = currentTheme === THEME_DARK ? THEME_LIGHT : THEME_DARK;
    toggleButton.setAttribute('aria-label', `Switch to ${nextTheme} mode`);
    toggleButton.setAttribute('data-current-theme', currentTheme);

    // Update icon if present
    const sunIcon = toggleButton.querySelector('.sun-icon, [data-icon="sun"]');
    const moonIcon = toggleButton.querySelector('.moon-icon, [data-icon="moon"]');

    if (sunIcon && moonIcon) {
      if (currentTheme === THEME_DARK) {
        sunIcon.style.display = 'block';
        moonIcon.style.display = 'none';
      } else {
        sunIcon.style.display = 'none';
        moonIcon.style.display = 'block';
      }
    }
  }
}

/**
 * Get the current theme
 * @returns {'dark' | 'light'} The current theme
 */
export function getTheme() {
  const storedTheme = getStoredPreference();
  if (storedTheme === THEME_DARK || storedTheme === THEME_LIGHT) {
    return storedTheme;
  }
  return getSystemPreference();
}

/**
 * Toggle between light and dark themes
 */
export function toggleTheme() {
  const currentTheme = getTheme();
  const newTheme = currentTheme === THEME_DARK ? THEME_LIGHT : THEME_DARK;
  setStoredPreference(newTheme);
  applyTheme(newTheme);
  return newTheme;
}

/**
 * Set a specific theme
 * @param {'dark' | 'light'} theme - The theme to set
 */
export function setTheme(theme) {
  if (theme === THEME_DARK || theme === THEME_LIGHT) {
    setStoredPreference(theme);
    applyTheme(theme);
  }
}

/**
 * Initialize theme toggle functionality
 */
export function init() {
  // Apply the initial theme (stored preference or system preference)
  const initialTheme = getTheme();
  applyTheme(initialTheme);

  // Set up click handlers for theme toggle buttons
  const toggleButtons = document.querySelectorAll('[data-theme-toggle], .theme-toggle, #theme-toggle');
  toggleButtons.forEach((button) => {
    button.addEventListener('click', (e) => {
      e.preventDefault();
      toggleTheme();
    });

    // Also support keyboard activation
    button.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        toggleTheme();
      }
    });
  });

  // Listen for system preference changes
  if (window.matchMedia) {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    const handleSystemChange = (e) => {
      // Only update if user hasn't set a manual preference
      const storedPreference = getStoredPreference();
      if (!storedPreference) {
        applyTheme(e.matches ? THEME_DARK : THEME_LIGHT);
      }
    };

    // Modern browsers
    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener('change', handleSystemChange);
    } else if (mediaQuery.addListener) {
      // Older browsers (Safari < 14)
      mediaQuery.addListener(handleSystemChange);
    }
  }
}

export default { init, toggleTheme, getTheme, setTheme };
