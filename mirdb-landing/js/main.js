/**
 * Main JavaScript Entry Point
 * Owner: First Builder
 *
 * Imports and initializes all JavaScript modules.
 */

import { onReady } from './utils/helpers.js';
import { init as initClipboard } from './modules/clipboard.js';

// Initialize all modules when DOM is ready
onReady(() => {
  initClipboard();
  // Theme toggle - Scenario 14
  // Navigation - Scenario 6
  // Lazy loading - Scenario 18
});
