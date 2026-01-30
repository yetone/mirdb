/**
 * Main JavaScript Entry Point
 * Owner: First builder
 *
 * Initialize all component modules on DOMContentLoaded
 */

import { initNavigation } from './components/navigation.js';
import { initSmoothScroll } from './components/smooth-scroll.js';
import { initCopyButtons } from './components/copy-to-clipboard.js';
import { initScrollAnimations } from './components/scroll-animations.js';

document.addEventListener('DOMContentLoaded', () => {
  // Initialize navigation (mobile menu toggle)
  initNavigation();

  // Initialize smooth scroll for anchor links
  initSmoothScroll();

  // Initialize copy-to-clipboard buttons (Scenario 4)
  initCopyButtons();

  // Initialize scroll animations (Scenario 15)
  initScrollAnimations();

  // Future initializations:
  // - Mermaid diagram initialization (Scenario 5)
});
