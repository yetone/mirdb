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

  function getStoredTheme() {
    return localStorage.getItem(STORAGE_KEY);
  }

  function saveTheme(theme) {
    localStorage.setItem(STORAGE_KEY, theme);
  }

  function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
  }

  function initDarkMode() {
    const storedTheme = getStoredTheme();
    if (storedTheme) {
      applyTheme(storedTheme);
    }
  }

  function toggleDarkMode() {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    applyTheme(newTheme);
    saveTheme(newTheme);
    return newTheme;
  }

  // Expose functions globally for potential use
  window.MirDBTheme = {
    init: initDarkMode,
    toggle: toggleDarkMode
  };

  // Initialize on load
  initDarkMode();
})();
