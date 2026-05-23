/**
 * Dark mode theme management.
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Provides:
 * - Theme detection from system preferences
 * - localStorage persistence
 * - Toggle behavior
 * - DOM attribute application
 *
 * Expected exports (for testing):
 * - getPreferredTheme(): 'light' | 'dark'
 * - setTheme(theme): void
 * - toggleTheme(): void
 * - initTheme(): void
 */

const THEME_KEY = 'mirdb-theme';

/**
 * Detect the user's preferred theme from system settings.
 * Falls back to 'light' if no preference is detected.
 * @returns {'light' | 'dark'}
 */
function getPreferredTheme() {
  if (typeof window !== 'undefined' && window.matchMedia) {
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)');
    if (prefersDark.matches) {
      return 'dark';
    }
  }
  return 'light';
}

/**
 * Apply a theme to the document and persist it.
 * @param {'light' | 'dark'} theme
 */
function setTheme(theme) {
  const root = document.documentElement;
  root.setAttribute('data-theme', theme);
  try {
    localStorage.setItem(THEME_KEY, theme);
  } catch (_err) {
    // localStorage may be unavailable (private mode, etc.)
  }
}

/**
 * Toggle between light and dark themes.
 */
function toggleTheme() {
  const root = document.documentElement;
  const current = root.getAttribute('data-theme') || getPreferredTheme();
  const next = current === 'dark' ? 'light' : 'dark';
  setTheme(next);
}

/**
 * Initialize the theme on page load.
 * Priority:
 * 1. Theme stored in localStorage
 * 2. System preference (prefers-color-scheme)
 * 3. Default to 'light'
 */
function initTheme() {
  let theme = 'light';
  try {
    const stored = localStorage.getItem(THEME_KEY);
    if (stored === 'dark' || stored === 'light') {
      theme = stored;
    } else {
      theme = getPreferredTheme();
    }
  } catch (_err) {
    theme = getPreferredTheme();
  }
  setTheme(theme);
}

/**
 * Create and bind the theme toggle button.
 */
function initThemeToggle() {
  const toggleButton = document.querySelector('[data-testid="theme-toggle"]');
  if (toggleButton) {
    toggleButton.addEventListener('click', toggleTheme);
  }
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function () {
  initTheme();
  initThemeToggle();
});

// Exports for testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { getPreferredTheme, setTheme, toggleTheme, initTheme };
}
