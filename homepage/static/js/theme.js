/**
 * Dark/light theme toggle functionality.
 * Owner: Scenario 8 - Theme Toggle
 *
 * Expected behavior:
 * - Toggle button switches between dark and light themes
 * - Reads initial preference from localStorage
 * - Falls back to system preference (prefers-color-scheme)
 * - Persists choice to localStorage
 * - Applies theme by setting data-theme attribute on <html>
 * - Smooth color transition animation
 * - Respects reduced-motion preference
 */

(function() {
  'use strict';

  const STORAGE_KEY = 'mirdb-theme';
  const html = document.documentElement;

  function getInitialTheme() {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored === 'dark' || stored === 'light') {
        return stored;
      }
    } catch (e) {
      // localStorage may be unavailable
    }
    // Fall back to system preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
      return 'light';
    }
    return 'dark';
  }

  function setTheme(theme) {
    html.setAttribute('data-theme', theme);
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {
      // localStorage may be unavailable
    }
  }

  function toggleTheme() {
    const current = html.getAttribute('data-theme') || 'dark';
    const next = current === 'dark' ? 'light' : 'dark';
    setTheme(next);
  }

  // Initialize theme
  setTheme(getInitialTheme());

  // Bind toggle buttons
  document.addEventListener('DOMContentLoaded', function() {
    const toggles = document.querySelectorAll('.theme-toggle');
    toggles.forEach(function(btn) {
      btn.addEventListener('click', toggleTheme);
    });
  });

  // Listen for system preference changes
  if (window.matchMedia) {
    const mq = window.matchMedia('(prefers-color-scheme: light)');
    mq.addEventListener('change', function(e) {
      // Only auto-switch if user hasn't explicitly set a preference
      try {
        if (!localStorage.getItem(STORAGE_KEY)) {
          setTheme(e.matches ? 'light' : 'dark');
        }
      } catch (err) {
        setTheme(e.matches ? 'light' : 'dark');
      }
    });
  }
})();
