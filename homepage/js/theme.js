/**
 * Theme Toggle Module
 * Owner: Scenario 9 - Dark and Light Mode Theme
 *
 * Exports:
 * - initTheme(): Initialize theme from localStorage/system preference
 * - toggleTheme(): Toggle between dark and light mode
 * - getCurrentTheme(): Returns current theme ('dark' | 'light')
 *
 * Persists preference to localStorage
 */

(function() {
  'use strict';

  // Constants
  const STORAGE_KEY = 'mirdb-theme';
  const THEME_DARK = 'dark';
  const THEME_LIGHT = 'light';

  /**
   * Get the system's preferred color scheme
   * @returns {'dark' | 'light'} The system preference
   */
  function getSystemPreference() {
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return THEME_DARK;
    }
    return THEME_LIGHT;
  }

  /**
   * Get the saved theme from localStorage
   * @returns {string | null} The saved theme or null
   */
  function getSavedTheme() {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      // localStorage might be blocked
      console.warn('Unable to access localStorage for theme preference');
      return null;
    }
  }

  /**
   * Save theme preference to localStorage
   * @param {string} theme - The theme to save
   */
  function saveTheme(theme) {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {
      console.warn('Unable to save theme preference to localStorage');
    }
  }

  /**
   * Apply a theme to the document
   * @param {string} theme - The theme to apply ('dark' or 'light')
   */
  function applyTheme(theme) {
    const html = document.documentElement;

    if (theme === THEME_DARK) {
      html.setAttribute('data-theme', THEME_DARK);
    } else {
      html.setAttribute('data-theme', THEME_LIGHT);
    }

    // Update toggle button aria-label
    const toggleButton = document.querySelector('.theme-toggle');
    if (toggleButton) {
      const newLabel = theme === THEME_DARK
        ? 'Switch to light mode'
        : 'Switch to dark mode';
      toggleButton.setAttribute('aria-label', newLabel);
    }
  }

  /**
   * Get the current theme
   * @returns {'dark' | 'light'} The current theme
   */
  function getCurrentTheme() {
    const dataTheme = document.documentElement.getAttribute('data-theme');
    if (dataTheme === THEME_DARK) {
      return THEME_DARK;
    }
    if (dataTheme === THEME_LIGHT) {
      return THEME_LIGHT;
    }
    // No explicit theme set, check system preference
    return getSystemPreference();
  }

  /**
   * Toggle between dark and light themes
   */
  function toggleTheme() {
    const currentTheme = getCurrentTheme();
    const newTheme = currentTheme === THEME_DARK ? THEME_LIGHT : THEME_DARK;

    applyTheme(newTheme);
    saveTheme(newTheme);

    return newTheme;
  }

  /**
   * Initialize theme based on:
   * 1. Saved preference in localStorage (highest priority)
   * 2. System preference (fallback)
   */
  function initTheme() {
    const savedTheme = getSavedTheme();

    if (savedTheme) {
      // Use saved preference
      applyTheme(savedTheme);
    } else {
      // Use system preference but set the data-theme attribute
      // so the toggle button shows correct icon
      const systemTheme = getSystemPreference();
      applyTheme(systemTheme);
    }

    // Set up the toggle button listener
    setupToggleButton();

    // Listen for system preference changes
    setupSystemPreferenceListener();
  }

  /**
   * Set up the theme toggle button click handler
   */
  function setupToggleButton() {
    const toggleButton = document.querySelector('.theme-toggle');

    if (toggleButton) {
      toggleButton.addEventListener('click', function(e) {
        e.preventDefault();
        toggleTheme();
      });

      // Set initial aria-label
      const currentTheme = getCurrentTheme();
      const label = currentTheme === THEME_DARK
        ? 'Switch to light mode'
        : 'Switch to dark mode';
      toggleButton.setAttribute('aria-label', label);
    }
  }

  /**
   * Listen for system preference changes
   * Only apply if user hasn't explicitly set a preference
   */
  function setupSystemPreferenceListener() {
    if (window.matchMedia) {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

      mediaQuery.addEventListener('change', function(e) {
        // Only update if no saved preference
        const savedTheme = getSavedTheme();
        if (!savedTheme) {
          const newTheme = e.matches ? THEME_DARK : THEME_LIGHT;
          applyTheme(newTheme);
        }
      });
    }
  }

  // Export functions to global scope
  window.MirDBTheme = {
    init: initTheme,
    toggle: toggleTheme,
    getCurrent: getCurrentTheme
  };

  // Auto-initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initTheme);
  } else {
    // DOM already loaded
    initTheme();
  }
})();
