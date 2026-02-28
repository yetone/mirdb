/**
 * MirDB Homepage - Main JavaScript
 * Owner: First builder
 *
 * Entry point for all JavaScript functionality.
 * Initializes components and sets up event listeners.
 *
 * Imports:
 * - navigation.js (Scenario 5)
 * - clipboard.js (Scenario 3)
 * - scroll.js (Scenario 7)
 * - accessibility.js (Scenario 8)
 */

import { initClipboard } from './components/clipboard.js';
import { initNavigation } from './components/navigation.js';
import { initSmoothScroll } from './components/scroll.js';

// Initialize the homepage when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  // Initialize navigation functionality (Scenario 5)
  initNavigation();

  // Initialize smooth scrolling (Scenario 7)
  initSmoothScroll();

  // Initialize clipboard functionality (Scenario 3)
  initClipboard();

  console.log('MirDB Homepage initialized');
});
