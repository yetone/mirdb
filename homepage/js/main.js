/**
 * Main JavaScript Entry
 * Owner: Scenario 1 - Hero Section Display
 *
 * This file initializes all JavaScript modules.
 * Uses progressive enhancement - all features
 * should degrade gracefully without JS.
 */

// Import copy button functionality (Scenario 4)
import { initCopyButtons } from './components/copy-button.js';

// Future imports - will be implemented by other scenarios:
// import { initMobileNav } from './components/mobile-nav.js';  // Scenario 8
// import { initTheme } from './utils/theme.js';                // Scenario 10
// import { initSmoothScroll } from './components/smooth-scroll.js'; // Scenario 15

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  console.log('MiRDB Homepage initialized');

  // Initialize copy buttons for code blocks (Scenario 4)
  const copyButtonCount = initCopyButtons();
  if (copyButtonCount > 0) {
    console.log(`Initialized ${copyButtonCount} copy button(s)`);
  }

  // Future initializations - will be implemented by other scenarios:
  // - Mobile navigation (Scenario 8)
  // - Theme detection (Scenario 10)
  // - Smooth scroll (Scenario 15)
});

// Export for module usage
export function initApp() {
  // Main initialization function
  // Called after all modules are loaded
}
