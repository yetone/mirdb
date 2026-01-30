/**
 * MirDB Landing Page - Main JavaScript
 * Owner: First scenario builder
 *
 * This file initializes all JavaScript functionality:
 * - Import and initialize navigation
 * - Import and initialize clipboard functionality
 * - Import and initialize animations
 * - Set up event listeners for interactive elements
 *
 * Expected Exports: None (entry point)
 * Expected Behavior: DOMContentLoaded initialization
 */

document.addEventListener('DOMContentLoaded', function() {
  // Initialize modules when they are loaded
  if (typeof initNavigation === 'function') {
    initNavigation();
  }
  if (typeof initClipboard === 'function') {
    initClipboard();
  }
  if (typeof initAnimations === 'function') {
    initAnimations();
  }
});
