/**
 * Theme Toggle Module
 * Owner: Scenario 8 - Theme Toggle
 *
 * Exports:
 * - initTheme(): Initialize theme from localStorage or system preference
 * - toggleTheme(): Switch between light and dark themes
 * - getTheme(): Get current theme
 * - setTheme(theme): Set specific theme ('light' or 'dark')
 */

(function(global) {
  'use strict';

  // Constants
  var THEME_KEY = 'mirdb-theme';
  var THEME_LIGHT = 'light';
  var THEME_DARK = 'dark';

  /**
   * Get the current theme from the document
   * @returns {string} Current theme ('light' or 'dark')
   */
  function getTheme() {
    return document.documentElement.getAttribute('data-theme') || THEME_LIGHT;
  }

  /**
   * Set the theme on the document and persist to localStorage
   * @param {string} theme - The theme to set ('light' or 'dark')
   * @returns {string} The theme that was set
   */
  function setTheme(theme) {
    // Normalize theme value
    var normalizedTheme = theme === THEME_DARK ? THEME_DARK : THEME_LIGHT;

    // Update the document attribute
    document.documentElement.setAttribute('data-theme', normalizedTheme);

    // Persist to localStorage
    try {
      localStorage.setItem(THEME_KEY, normalizedTheme);
    } catch (e) {
      // localStorage may be unavailable (private browsing, etc.)
      console.warn('Unable to save theme preference:', e);
    }

    // Update theme toggle button aria-label
    updateToggleButtonLabel(normalizedTheme);

    return normalizedTheme;
  }

  /**
   * Toggle between light and dark themes
   * @returns {string} The new theme after toggling
   */
  function toggleTheme() {
    var currentTheme = getTheme();
    var newTheme = currentTheme === THEME_DARK ? THEME_LIGHT : THEME_DARK;
    return setTheme(newTheme);
  }

  /**
   * Initialize theme based on:
   * 1. Stored preference in localStorage
   * 2. System color scheme preference
   * 3. Default to light theme
   * @returns {string} The initialized theme
   */
  function initTheme() {
    var storedTheme = null;

    // Try to get stored preference
    try {
      storedTheme = localStorage.getItem(THEME_KEY);
    } catch (e) {
      // localStorage may be unavailable
      console.warn('Unable to read theme preference:', e);
    }

    // If stored preference exists, use it
    if (storedTheme === THEME_DARK || storedTheme === THEME_LIGHT) {
      return setTheme(storedTheme);
    }

    // Otherwise, check system preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return setTheme(THEME_DARK);
    }

    // Default to light theme
    return setTheme(THEME_LIGHT);
  }

  /**
   * Update the theme toggle button's aria-label for accessibility
   * @param {string} currentTheme - The current theme
   */
  function updateToggleButtonLabel(currentTheme) {
    var toggleBtn = document.querySelector('.theme-toggle');
    if (toggleBtn) {
      var label = currentTheme === THEME_DARK
        ? 'Switch to light theme'
        : 'Switch to dark theme';
      toggleBtn.setAttribute('aria-label', label);
    }
  }

  /**
   * Set up theme toggle button click handler
   */
  function setupToggleButton() {
    var toggleBtn = document.querySelector('.theme-toggle');
    if (toggleBtn) {
      toggleBtn.addEventListener('click', function(e) {
        e.preventDefault();
        toggleTheme();
      });
    }
  }

  /**
   * Listen for system theme changes and update if no stored preference
   */
  function setupSystemThemeListener() {
    if (window.matchMedia) {
      var mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

      // Listen for changes
      var handleChange = function(e) {
        // Only auto-switch if no stored preference
        try {
          var storedTheme = localStorage.getItem(THEME_KEY);
          if (!storedTheme) {
            setTheme(e.matches ? THEME_DARK : THEME_LIGHT);
          }
        } catch (err) {
          // If localStorage unavailable, still respond to system changes
          setTheme(e.matches ? THEME_DARK : THEME_LIGHT);
        }
      };

      // Use modern API if available, fallback to deprecated addListener
      if (mediaQuery.addEventListener) {
        mediaQuery.addEventListener('change', handleChange);
      } else if (mediaQuery.addListener) {
        mediaQuery.addListener(handleChange);
      }
    }
  }

  /**
   * Full initialization - call this on DOMContentLoaded
   */
  function init() {
    initTheme();
    setupToggleButton();
    setupSystemThemeListener();
  }

  // Expose API globally
  global.MirDBTheme = {
    init: init,
    initTheme: initTheme,
    toggleTheme: toggleTheme,
    getTheme: getTheme,
    setTheme: setTheme,
    THEME_KEY: THEME_KEY,
    THEME_LIGHT: THEME_LIGHT,
    THEME_DARK: THEME_DARK
  };

  // Support module exports for testing
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = global.MirDBTheme;
  }

})(typeof globalThis !== 'undefined' ? globalThis : (typeof window !== 'undefined' ? window : global));
