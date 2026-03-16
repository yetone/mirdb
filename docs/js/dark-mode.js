/**
 * MirDB Homepage - Dark Mode Toggle
 * Owner: Scenario 11 - Dark Mode Support
 *
 * Expected exports/functionality:
 * - initDarkMode(): Initialize dark mode from system preference
 * - toggleDarkMode(): Toggle between light/dark themes
 * - saveDarkModePreference(): Persist user choice to localStorage
 */

(function() {
  'use strict';

  const STORAGE_KEY = 'mirdb-theme';
  const TRANSITION_DURATION = 300;

  function getStoredTheme() {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  function saveTheme(theme) {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {
      // localStorage may be unavailable
    }
  }

  function getSystemTheme() {
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      return 'dark';
    }
    return 'light';
  }

  function getCurrentTheme() {
    const storedTheme = getStoredTheme();
    if (storedTheme) {
      return storedTheme;
    }
    return getSystemTheme();
  }

  function applyTheme(theme, animate) {
    if (animate) {
      document.documentElement.classList.add('theme-transitioning');
    }

    document.documentElement.setAttribute('data-theme', theme);
    updateToggleButton(theme);

    if (animate) {
      setTimeout(function() {
        document.documentElement.classList.remove('theme-transitioning');
      }, TRANSITION_DURATION);
    }
  }

  function updateToggleButton(theme) {
    var toggleButton = document.getElementById('dark-mode-toggle');
    if (toggleButton) {
      var isDark = theme === 'dark';
      toggleButton.setAttribute('aria-pressed', isDark.toString());
      toggleButton.setAttribute('aria-label', isDark ? 'Switch to light mode' : 'Switch to dark mode');
    }
  }

  function initDarkMode() {
    var storedTheme = getStoredTheme();
    if (storedTheme) {
      applyTheme(storedTheme, false);
    } else {
      // No stored preference, respect system preference
      // The CSS handles this via media query, but we update the button
      updateToggleButton(getSystemTheme());
    }

    // Set up toggle button click handler
    var toggleButton = document.getElementById('dark-mode-toggle');
    if (toggleButton) {
      toggleButton.addEventListener('click', toggleDarkMode);
    }

    // Listen for system theme changes
    if (window.matchMedia) {
      var mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      mediaQuery.addEventListener('change', function(e) {
        // Only apply system preference if user hasn't set a preference
        if (!getStoredTheme()) {
          updateToggleButton(e.matches ? 'dark' : 'light');
        }
      });
    }
  }

  function toggleDarkMode() {
    var currentTheme = document.documentElement.getAttribute('data-theme');
    var newTheme;

    if (currentTheme === 'dark') {
      newTheme = 'light';
    } else if (currentTheme === 'light') {
      newTheme = 'dark';
    } else {
      // No explicit theme set, toggle based on current system state
      newTheme = getSystemTheme() === 'dark' ? 'light' : 'dark';
    }

    applyTheme(newTheme, true);
    saveTheme(newTheme);
    return newTheme;
  }

  // Expose functions globally for potential use
  window.MirDBTheme = {
    init: initDarkMode,
    toggle: toggleDarkMode,
    getCurrentTheme: getCurrentTheme
  };

  // Initialize when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDarkMode);
  } else {
    initDarkMode();
  }
})();
