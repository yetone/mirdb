/**
 * Main JavaScript Entry Point
 * Owner: First scenario builder
 *
 * Initializes all JavaScript modules:
 * - Theme toggle
 * - Copy-to-clipboard
 * - Smooth scroll
 * - Mobile navigation
 */

import { initMobileNav } from './mobile-nav.js';
// import { initTheme } from './theme-toggle.js';
// import { initCopyButtons } from './copy-code.js';
// import { initSmoothScroll } from './smooth-scroll.js';

// Initialize modules when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  // Initialize mobile navigation
  initMobileNav();

  // Initialize theme (Scenario 10)
  // initTheme();

  // Initialize copy buttons (Scenario 4)
  // initCopyButtons();

  // Initialize smooth scroll (Scenario 13)
  // initSmoothScroll();
});
