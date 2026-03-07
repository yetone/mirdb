/**
 * Main JavaScript Entry Point
 *
 * Imports and initializes all interactive features.
 * Implements progressive enhancement - core content
 * works without JavaScript.
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialize theme toggle if module exists
  if (typeof initTheme === 'function') {
    initTheme();
  }

  // Initialize copy buttons if module exists
  if (typeof initCopyButtons === 'function') {
    initCopyButtons();
  }

  // Smooth scroll for anchor links
  document.querySelectorAll('a[href^="#"]').forEach(function(anchor) {
    anchor.addEventListener('click', function(e) {
      var targetId = this.getAttribute('href');
      var target = document.querySelector(targetId);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
});
