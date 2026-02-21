/**
 * Theme Switching Functionality
 * Owner: Scenario 15 - Theme Support - Dark Mode
 *
 * Expected exports/functionality:
 * - detectSystemTheme(): Detect user's system preference
 * - setTheme(theme): Apply light/dark theme
 * - toggleTheme(): Switch between themes
 * - persistThemePreference(): Save to localStorage
 */

(function() {
  'use strict';

  const STORAGE_KEY = 'mirdb-theme';
  const THEME_ATTR = 'data-theme';

  /**
   * Detects the user's system color scheme preference
   * @returns {string} 'dark' or 'light'
   */
  function detectSystemTheme() {
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
    return 'light';
  }

  /**
   * Gets the saved theme preference from localStorage
   * @returns {string|null} The saved theme or null
   */
  function getSavedTheme() {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  /**
   * Saves the theme preference to localStorage
   * @param {string} theme - The theme to save ('dark' or 'light')
   */
  function persistThemePreference(theme) {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {
      // localStorage not available
    }
  }

  /**
   * Applies the specified theme to the page
   * @param {string} theme - The theme to apply ('dark' or 'light')
   */
  function setTheme(theme) {
    const validTheme = theme === 'dark' ? 'dark' : 'light';
    document.documentElement.setAttribute(THEME_ATTR, validTheme);

    // Update toggle button state if present
    const toggleBtn = document.getElementById('theme-toggle');
    if (toggleBtn) {
      toggleBtn.setAttribute('aria-pressed', validTheme === 'dark');
      updateToggleIcon(validTheme);
    }

    // Dispatch custom event for theme change
    document.dispatchEvent(new CustomEvent('themechange', { detail: { theme: validTheme } }));
  }

  /**
   * Gets the current theme
   * @returns {string} 'dark' or 'light'
   */
  function getTheme() {
    return document.documentElement.getAttribute(THEME_ATTR) || detectSystemTheme();
  }

  /**
   * Toggles between light and dark themes
   */
  function toggleTheme() {
    const currentTheme = getTheme();
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
    persistThemePreference(newTheme);
  }

  /**
   * Updates the toggle button icon based on current theme
   * @param {string} theme - Current theme
   */
  function updateToggleIcon(theme) {
    const sunIcon = document.querySelector('.theme-icon-sun');
    const moonIcon = document.querySelector('.theme-icon-moon');

    if (sunIcon && moonIcon) {
      if (theme === 'dark') {
        sunIcon.style.display = 'block';
        moonIcon.style.display = 'none';
      } else {
        sunIcon.style.display = 'none';
        moonIcon.style.display = 'block';
      }
    }
  }

  /**
   * Initializes the theme based on saved preference or system preference
   */
  function initTheme() {
    const savedTheme = getSavedTheme();
    const theme = savedTheme || detectSystemTheme();
    setTheme(theme);

    // Listen for system theme changes
    if (window.matchMedia) {
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      const handler = function(e) {
        // Only auto-switch if no saved preference
        if (!getSavedTheme()) {
          setTheme(e.matches ? 'dark' : 'light');
        }
      };

      // Use modern API if available, fallback to deprecated
      if (mediaQuery.addEventListener) {
        mediaQuery.addEventListener('change', handler);
      } else if (mediaQuery.addListener) {
        mediaQuery.addListener(handler);
      }
    }
  }

  /**
   * Sets up the theme toggle button event listener
   */
  function setupToggleButton() {
    const toggleBtn = document.getElementById('theme-toggle');
    if (toggleBtn) {
      toggleBtn.addEventListener('click', function(e) {
        e.preventDefault();
        toggleTheme();
      });

      // Set initial state
      const currentTheme = getTheme();
      toggleBtn.setAttribute('aria-pressed', currentTheme === 'dark');
      updateToggleIcon(currentTheme);
    }
  }

  // Initialize on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
      initTheme();
      setupToggleButton();
    });
  } else {
    initTheme();
    setupToggleButton();
  }

  // Expose API for external use
  window.MirDBTheme = {
    detectSystemTheme: detectSystemTheme,
    setTheme: setTheme,
    getTheme: getTheme,
    toggleTheme: toggleTheme,
    persistThemePreference: persistThemePreference
  };
})();
