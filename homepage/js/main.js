/**
 * MirDB Homepage JavaScript
 * Owner: Scenario 15 - Dark Mode Theme
 *
 * Functionality:
 * - Dark mode toggle
 * - Theme preference persistence (localStorage)
 * - System preference detection (prefers-color-scheme)
 *
 * Note: Keep JavaScript minimal for performance.
 */

(function() {
  'use strict';

  const STORAGE_KEY = 'mirdb-theme';
  const DARK_THEME = 'dark';
  const LIGHT_THEME = 'light';

  /**
   * Get the user's preferred theme from localStorage or system preference
   * @returns {string|null} 'dark', 'light', or null (system default)
   */
  function getStoredTheme() {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  /**
   * Save theme preference to localStorage
   * @param {string} theme - 'dark', 'light', or 'auto'
   */
  function saveTheme(theme) {
    try {
      if (theme === 'auto') {
        localStorage.removeItem(STORAGE_KEY);
      } else {
        localStorage.setItem(STORAGE_KEY, theme);
      }
    } catch (e) {
      // localStorage not available
    }
  }

  /**
   * Check if system prefers dark mode
   * @returns {boolean}
   */
  function systemPrefersDark() {
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  }

  /**
   * Apply theme to the document
   * @param {string|null} theme - 'dark', 'light', or null for system default
   */
  function applyTheme(theme) {
    const html = document.documentElement;

    if (theme === DARK_THEME) {
      html.setAttribute('data-theme', DARK_THEME);
    } else if (theme === LIGHT_THEME) {
      html.setAttribute('data-theme', LIGHT_THEME);
    } else {
      // Remove attribute to follow system preference
      html.removeAttribute('data-theme');
    }
  }

  /**
   * Get current effective theme (what's actually displayed)
   * @returns {string} 'dark' or 'light'
   */
  function getCurrentTheme() {
    const stored = getStoredTheme();
    if (stored) {
      return stored;
    }
    return systemPrefersDark() ? DARK_THEME : LIGHT_THEME;
  }

  /**
   * Toggle between dark and light themes
   */
  function toggleTheme() {
    const current = getCurrentTheme();
    const newTheme = current === DARK_THEME ? LIGHT_THEME : DARK_THEME;
    applyTheme(newTheme);
    saveTheme(newTheme);
  }

  /**
   * Initialize dark mode functionality
   */
  function initDarkMode() {
    // Apply stored theme on page load
    const storedTheme = getStoredTheme();
    if (storedTheme) {
      applyTheme(storedTheme);
    }

    // Set up toggle button
    const toggleBtn = document.getElementById('dark-mode-toggle');
    if (toggleBtn) {
      toggleBtn.addEventListener('click', toggleTheme);
    }

    // Listen for system preference changes
    if (window.matchMedia) {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      mediaQuery.addEventListener('change', function(e) {
        // Only update if no stored preference
        if (!getStoredTheme()) {
          // Theme will automatically follow via CSS media query
          // No need to manually apply
        }
      });
    }
  }

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDarkMode);
  } else {
    initDarkMode();
  }

  // Expose for testing
  window.MirDBTheme = {
    toggleTheme: toggleTheme,
    getCurrentTheme: getCurrentTheme,
    applyTheme: applyTheme,
    getStoredTheme: getStoredTheme
  };
})();
