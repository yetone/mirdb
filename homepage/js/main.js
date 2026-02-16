/**
 * Main JavaScript Entry Point
 *
 * Initializes all JavaScript modules for the MirDB Homepage.
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialize navigation (if module exists)
  if (typeof initNavigation === 'function') {
    initNavigation();
  }

  // Initialize code tabs (if module exists)
  if (typeof initCodeTabs === 'function') {
    initCodeTabs();
  }

  // Initialize copy buttons (if module exists)
  if (typeof initCopyButtons === 'function') {
    initCopyButtons();
  }

  // Initialize diagrams (if module exists)
  if (typeof initDiagrams === 'function') {
    initDiagrams();
  }

  console.log('MirDB Homepage initialized');
});
