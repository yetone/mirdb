/**
 * Main JavaScript Entry Point
 * Owner: First scenario builder
 *
 * Initializes all modules on page load.
 */

import { initMobileNav } from './mobile-nav.js';
import { initCopyButtons } from './copy-clipboard.js';
import { initThemeToggle, initTheme } from './theme-toggle.js';
import { initSmoothScroll } from './smooth-scroll.js';

// Initialize theme immediately to prevent flash of wrong theme
// This runs before DOMContentLoaded to apply theme as early as possible
initTheme();

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  // Initialize smooth scroll for anchor links (Scenario 14)
  initSmoothScroll();

  // Initialize mobile navigation (Scenario 2)
  initMobileNav();

  // Initialize copy-to-clipboard buttons (Scenario 4)
  initCopyButtons();

  // Initialize theme toggle buttons (Scenario 9)
  initThemeToggle();
});
