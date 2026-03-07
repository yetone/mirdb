/**
 * Theme Toggle Functionality
 * Owner: Scenario 10 - Dark/Light Theme Toggle
 */

function initTheme() {
  var savedTheme = localStorage.getItem('theme');
  var prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  var theme = savedTheme || (prefersDark ? 'dark' : 'light');
  document.documentElement.setAttribute('data-theme', theme);
}

function toggleTheme() {
  var current = document.documentElement.getAttribute('data-theme');
  var next = current === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
}

// Attach to button
document.addEventListener('DOMContentLoaded', function() {
  var btn = document.querySelector('.theme-toggle');
  if (btn) {
    btn.addEventListener('click', toggleTheme);
  }
  initTheme();
});
