/**
 * Main application entry point.
 */

import { renderFeatures } from './components/Features';
import { renderQuickStart } from './components/QuickStart';

function initApp(): void {
  const app = document.getElementById('app');
  if (!app) return;

  // Render Features section (Scenario 2)
  const features = renderFeatures();
  app.appendChild(features);

  // Render QuickStart section (Scenario 3)
  const quickStart = renderQuickStart();
  app.appendChild(quickStart);
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initApp);
} else {
  initApp();
}
