/**
 * Theme Toggle Module
 * Owner: Scenario 6 - Theme Support
 *
 * Exports:
 * - initTheme(): void - Initialize theme from storage/system preference
 * - toggleTheme(): void - Toggle between light and dark
 * - getTheme(): 'light' | 'dark' - Get current theme
 * - setTheme(theme: string): void - Set specific theme
 *
 * Uses localStorage for persistence.
 * Respects prefers-color-scheme media query.
 */

const THEME_STORAGE_KEY = 'mirdb-theme';

/**
 * Initialize theme from localStorage or system preference.
 * Should be called on page load.
 */
function initTheme() {
  const savedTheme = localStorage.getItem(THEME_STORAGE_KEY);

  if (savedTheme) {
    // User has a saved preference
    setTheme(savedTheme);
  } else {
    // Check system preference
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    setTheme(prefersDark ? 'dark' : 'light');
  }

  // Listen for system preference changes
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function(e) {
    // Only apply system preference if no user preference is saved
    if (!localStorage.getItem(THEME_STORAGE_KEY)) {
      setTheme(e.matches ? 'dark' : 'light');
    }
  });

  // Setup toggle button event listener
  const toggleBtn = document.querySelector('[data-theme-toggle]');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', toggleTheme);
    updateToggleButton(getTheme());
  }
}

/**
 * Toggle between light and dark themes.
 */
function toggleTheme() {
  const currentTheme = getTheme();
  const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
  setTheme(newTheme);
  // Save user preference
  localStorage.setItem(THEME_STORAGE_KEY, newTheme);
  updateToggleButton(newTheme);
}

/**
 * Get the current theme.
 * @returns {'light' | 'dark'} The current theme
 */
function getTheme() {
  const dataTheme = document.documentElement.getAttribute('data-theme');
  if (dataTheme === 'dark' || dataTheme === 'light') {
    return dataTheme;
  }
  // Fallback to system preference
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/**
 * Set the theme.
 * @param {string} theme - 'light' or 'dark'
 */
function setTheme(theme) {
  if (theme !== 'light' && theme !== 'dark') {
    console.warn('Invalid theme:', theme);
    theme = 'light';
  }
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem(THEME_STORAGE_KEY, theme);
}

/**
 * Update the toggle button icon and aria-label.
 * @param {string} theme - Current theme
 */
function updateToggleButton(theme) {
  const toggleBtn = document.querySelector('[data-theme-toggle]');
  if (!toggleBtn) return;

  const sunIcon = toggleBtn.querySelector('.theme-toggle__icon--sun');
  const moonIcon = toggleBtn.querySelector('.theme-toggle__icon--moon');

  if (sunIcon && moonIcon) {
    if (theme === 'dark') {
      // Show sun icon (to switch to light)
      sunIcon.style.display = 'block';
      moonIcon.style.display = 'none';
      toggleBtn.setAttribute('aria-label', 'Switch to light theme');
    } else {
      // Show moon icon (to switch to dark)
      sunIcon.style.display = 'none';
      moonIcon.style.display = 'block';
      toggleBtn.setAttribute('aria-label', 'Switch to dark theme');
    }
  }
}

// Export functions for use in other modules and testing
if (typeof window !== 'undefined') {
  window.initTheme = initTheme;
  window.toggleTheme = toggleTheme;
  window.getTheme = getTheme;
  window.setTheme = setTheme;
}

// Also support CommonJS/module environments for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initTheme, toggleTheme, getTheme, setTheme };
}
