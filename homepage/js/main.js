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

// Import smooth scroll (Scenario 15)
import { initSmoothScroll } from './components/smooth-scroll.js';

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

  // Initialize smooth scroll (Scenario 15)
  const smoothScrollCount = initSmoothScroll();
  if (smoothScrollCount > 0) {
    console.log(`Initialized smooth scroll for ${smoothScrollCount} anchor link(s)`);
  }
});

// Export for module usage
export function initApp() {
  // Main initialization function
  // Called after all modules are loaded
}
