/**
 * Main JavaScript Entry Point
 * Owner: First builder
 *
 * Initialize all component modules on DOMContentLoaded
 */

import { initNavigation } from './components/navigation.js';
import { initSmoothScroll } from './components/smooth-scroll.js';
import { initCopyButtons } from './components/copy-to-clipboard.js';

document.addEventListener('DOMContentLoaded', () => {
  // Initialize navigation (mobile menu toggle)
  initNavigation();

  // Initialize smooth scroll for anchor links
  initSmoothScroll();

  // Initialize copy-to-clipboard buttons (Scenario 4)
  initCopyButtons();

  // Future initializations:
  // - initScrollAnimations() from scroll-animations.js (Scenario 15)
  // - Mermaid diagram initialization (Scenario 5)
});
