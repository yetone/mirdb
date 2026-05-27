/**
 * Dark/light theme toggle functionality.
 * Stub created by first scenario builder; owned by Scenario 8.
 */
(function () {
  'use strict';

  const html = document.documentElement;
  const STORAGE_KEY = 'mirdb-theme';

  function getTheme() {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) return stored;
    } catch (e) {}
    if (window.matchMedia('(prefers-color-scheme: light)').matches) {
      return 'light';
    }
    return 'dark';
  }

  function setTheme(theme) {
    html.setAttribute('data-theme', theme);
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {}
  }

  setTheme(getTheme());
})();
