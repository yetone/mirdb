/**
 * Main JavaScript Entry Point
 * Owner: First builder
 *
 * Initialize all component modules on DOMContentLoaded
 */

import { initNavigation } from './components/navigation.js';
import { initSmoothScroll } from './components/smooth-scroll.js';

document.addEventListener('DOMContentLoaded', () => {
  // Initialize navigation (mobile menu toggle)
  initNavigation();

  // Initialize smooth scroll for anchor links
  initSmoothScroll();

  // Future initializations:
  // - initCopyButtons() from copy-to-clipboard.js (Scenario 4)
  // - initScrollAnimations() from scroll-animations.js (Scenario 15)
  // - Mermaid diagram initialization (Scenario 5)
});
