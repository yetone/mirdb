/**
 * Main JavaScript Entry Point
 * Owner: First builder
 */

// Import components (will be added by respective scenario owners)
import { initClipboard } from './components/clipboard.js';
import { initNavigation } from './components/navigation.js';
import { initSmoothScroll } from './components/smooth-scroll.js';

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  console.log('MirDB Homepage initialized');

  // Initialize clipboard functionality (Scenario 3)
  initClipboard();

  // Initialize navigation and smooth scrolling (Scenario 12)
  initNavigation();
  initSmoothScroll();
});
