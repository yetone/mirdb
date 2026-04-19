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

// Import mobile navigation (Scenario 8)
import { initMobileNav } from './components/mobile-nav.js';

// Import theme detection (Scenario 10)
import { initTheme } from './utils/theme.js';

// Future imports - will be implemented by other scenarios:
// import { initSmoothScroll } from './components/smooth-scroll.js'; // Scenario 15

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  console.log('MiRDB Homepage initialized');

  // Initialize copy buttons for code blocks (Scenario 4)
  const copyButtonCount = initCopyButtons();
  if (copyButtonCount > 0) {
    console.log(`Initialized ${copyButtonCount} copy button(s)`);
  }

  // Initialize mobile navigation (Scenario 8)
  const mobileNavInitialized = initMobileNav();
  if (mobileNavInitialized) {
    console.log('Mobile navigation initialized');
  }

  // Initialize theme detection (Scenario 10)
  const themeInfo = initTheme();
  console.log(`Current theme: ${themeInfo.theme}`);

  // Future initializations - will be implemented by other scenarios:
  // - Smooth scroll (Scenario 15)
});

// Export for module usage
export function initApp() {
  // Main initialization function
  // Called after all modules are loaded
}
