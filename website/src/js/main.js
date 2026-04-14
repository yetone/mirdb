/**
 * Main JavaScript Entry Point
 * Owner: First Builder
 *
 * Initializes all JavaScript functionality:
 * - Navigation smooth scrolling
 * - Code copy functionality
 * - Mermaid diagram rendering
 * - Theme detection
 *
 * Imports and initializes modules from components/ and utils/
 */

import { initAndRenderMermaid } from './components/mermaid-init.js';
import { initNavigation } from './components/navigation.js';
import { initCodeCopy } from './components/code-copy.js';
import { initTheme } from './utils/theme.js';

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', async () => {
  // Initialize theme detection (dark mode support)
  initTheme();

  // Initialize navigation (smooth scrolling, mobile menu, scroll effects)
  initNavigation();

  // Initialize code copy functionality
  initCodeCopy();

  // Initialize and render Mermaid diagrams
  await initAndRenderMermaid();
});
