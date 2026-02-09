/**
 * Main application entry point.
 */

import { renderFeatures } from './components/Features';

function initApp(): void {
  const app = document.getElementById('app');
  if (!app) return;

  // Render Features section
  const features = renderFeatures();
  app.appendChild(features);
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initApp);
} else {
  initApp();
}
