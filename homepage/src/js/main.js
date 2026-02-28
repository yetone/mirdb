/**
 * Main JavaScript entry point
 * Owner: Shared - First Builder
 *
 * Initialize all modules, DOM ready handler,
 * import and setup clipboard.js, import and setup navigation.js
 */

// DOM Ready handler
document.addEventListener('DOMContentLoaded', function() {
  console.log('MirDB Homepage initialized');

  // Initialize modules when they are available
  if (typeof initCopyButtons === 'function') {
    initCopyButtons();
  }

  if (typeof initMobileMenu === 'function') {
    initMobileMenu();
  }

  if (typeof initExternalLinks === 'function') {
    initExternalLinks();
  }

  if (typeof handleSmoothScroll === 'function') {
    handleSmoothScroll();
  }
});
