/**
 * Main JavaScript Entry Point
 * Owner: First builder (shared resource)
 *
 * Initializes:
 * - Smooth scrolling
 * - Copy to clipboard
 */

import { initSmoothScroll } from './smooth-scroll.js';
import { initCopyButtons } from './copy-to-clipboard.js';

document.addEventListener('DOMContentLoaded', () => {
  initSmoothScroll();
  initCopyButtons();
});
