/**
 * Main JavaScript Entry Point
 * Owner: First Builder
 *
 * Imports and initializes all JavaScript modules.
 */

import { onReady } from './utils/helpers.js';
import { init as initClipboard } from './modules/clipboard.js';
import { init as initNavigation } from './modules/navigation.js';

// Initialize all modules when DOM is ready
onReady(() => {
  initClipboard();
  initNavigation();
  // Theme toggle - Scenario 14
  // Lazy loading - Scenario 18
});
