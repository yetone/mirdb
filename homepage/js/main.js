/**
 * Main JavaScript Entry Point
 *
 * Initializes all homepage functionality:
 * - Theme toggle
 * - Mobile navigation
 * - Copy-to-clipboard
 * - Smooth scroll navigation
 */

import { initTheme } from './theme.js';
import { initNavigation } from './navigation.js';
import { initClipboard } from './clipboard.js';

document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  initNavigation();
  initClipboard();
});
