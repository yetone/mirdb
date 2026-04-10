/**
 * Theme Toggle Module
 * Owner: Scenario 8 - Theme Toggle
 */

export function initTheme() {
  const toggle = document.getElementById('theme-toggle');
  const storedTheme = localStorage.getItem('theme');
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

  // Set initial theme
  const initialTheme = storedTheme || (prefersDark ? 'dark' : 'light');
  setTheme(initialTheme);

  if (toggle) {
    toggle.addEventListener('click', toggleTheme);
  }
}

export function toggleTheme() {
  const current = getTheme();
  const next = current === 'light' ? 'dark' : 'light';
  setTheme(next);
}

export function setTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('theme', theme);
}

export function getTheme() {
  return document.documentElement.getAttribute('data-theme') || 'light';
}
