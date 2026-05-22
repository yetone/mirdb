/**
 * Main JavaScript entry point.
 * Owner: Scenario 1 - HTTP Server and Routing (base setup)
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialize theme
  const savedTheme = localStorage.getItem('theme') || 'light';
  document.documentElement.setAttribute('data-theme', savedTheme);
});
