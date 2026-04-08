/**
 * Theme Switching Logic
 * Owner: Scenario 10 - Dark Mode and Light Mode
 *
 * Expected exports/functionality:
 * - initTheme(): Set initial theme based on localStorage or system preference
 * - toggleTheme(): Switch between dark and light mode
 * - getSystemTheme(): Detect system color scheme preference
 * - persistTheme(theme): Save theme choice to localStorage
 *
 * Dependencies: None (vanilla JS)
 */

(function() {
  'use strict';

  // Storage key for theme preference
  var THEME_STORAGE_KEY = 'mirdb-theme';

  /**
   * Get the system's preferred color scheme
   * @returns {string} 'dark' or 'light'
   */
  function getSystemTheme() {
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
    return 'light';
  }

  /**
   * Get the stored theme preference from localStorage
   * @returns {string|null} 'dark', 'light', or null if not set
   */
  function getStoredTheme() {
    try {
      return localStorage.getItem(THEME_STORAGE_KEY);
    } catch (e) {
      // localStorage may be unavailable (e.g., in private browsing)
      return null;
    }
  }

  /**
   * Save theme preference to localStorage
   * @param {string} theme - 'dark' or 'light'
   */
  function persistTheme(theme) {
    try {
      localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch (e) {
      // localStorage may be unavailable
    }
  }

  /**
   * Apply theme to the document
   * @param {string} theme - 'dark' or 'light'
   * @param {boolean} withTransition - whether to animate the transition
   */
  function applyTheme(theme, withTransition) {
    var html = document.documentElement;

    if (withTransition) {
      // Add transition class for smooth theme switching
      html.classList.add('theme-transition');

      // Remove transition class after animation completes
      setTimeout(function() {
        html.classList.remove('theme-transition');
      }, 300);
    }

    // Set the data-theme attribute
    html.setAttribute('data-theme', theme);

    // Update the toggle button's aria-label
    var toggleButton = document.querySelector('.theme-toggle');
    if (toggleButton) {
      var newLabel = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
      toggleButton.setAttribute('aria-label', newLabel);
    }
  }

  /**
   * Get the current active theme
   * @returns {string} 'dark' or 'light'
   */
  function getCurrentTheme() {
    var storedTheme = getStoredTheme();
    if (storedTheme) {
      return storedTheme;
    }
    return getSystemTheme();
  }

  /**
   * Toggle between dark and light mode
   */
  function toggleTheme() {
    var currentTheme = document.documentElement.getAttribute('data-theme');

    // If no explicit theme, detect from system
    if (!currentTheme) {
      currentTheme = getSystemTheme();
    }

    var newTheme = currentTheme === 'dark' ? 'light' : 'dark';

    applyTheme(newTheme, true);
    persistTheme(newTheme);
  }

  /**
   * Initialize the theme based on stored preference or system setting
   */
  function initTheme() {
    var storedTheme = getStoredTheme();

    if (storedTheme) {
      // User has a stored preference
      applyTheme(storedTheme, false);
    } else {
      // Use system preference (CSS handles this, but set attribute for consistency)
      var systemTheme = getSystemTheme();
      document.documentElement.setAttribute('data-theme', systemTheme);
    }

    // Set up the toggle button click handler
    var toggleButton = document.querySelector('.theme-toggle');
    if (toggleButton) {
      toggleButton.addEventListener('click', toggleTheme);
    }

    // Listen for system theme changes
    if (window.matchMedia) {
      var mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

      // Use addListener for older browsers, addEventListener for modern ones
      var listener = function(e) {
        // Only update if user hasn't set a preference
        if (!getStoredTheme()) {
          applyTheme(e.matches ? 'dark' : 'light', true);
        }
      };

      if (mediaQuery.addEventListener) {
        mediaQuery.addEventListener('change', listener);
      } else if (mediaQuery.addListener) {
        mediaQuery.addListener(listener);
      }
    }
  }

  // Expose functions globally for testing and external use
  window.MirDBTheme = {
    init: initTheme,
    toggle: toggleTheme,
    getSystemTheme: getSystemTheme,
    getCurrentTheme: getCurrentTheme,
    persistTheme: persistTheme,
    getStoredTheme: getStoredTheme
  };

  // Initialize theme when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initTheme);
  } else {
    initTheme();
  }
})();
