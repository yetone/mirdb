/**
 * Main JavaScript Entry Point
 * Owner: First scenario builder
 *
 * Initializes all JavaScript functionality:
 * - Smooth scrolling for navigation
 * - Copy-to-clipboard functionality
 * - Syntax highlighting (if using JS-based highlighting)
 *
 * Note: Site must be fully functional without JavaScript
 * (progressive enhancement). This JS enhances the experience.
 */

// Import and initialize modules
import { initSmoothScroll } from './smooth-scroll.js';

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  initSmoothScroll();
});
