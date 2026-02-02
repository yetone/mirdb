/**
 * Main JavaScript Entry Point
 * Owner: First Builder
 *
 * Imports and initializes all JavaScript modules.
 */

import { onReady } from './utils/helpers.js';
import { init as initClipboard } from './modules/clipboard.js';
import { init as initNavigation } from './modules/navigation.js';
import { init as initThemeToggle } from './modules/theme-toggle.js';

// Initialize all modules when DOM is ready
onReady(() => {
  initClipboard();
  initNavigation();
  initThemeToggle();
  // Lazy loading - Scenario 18
});
