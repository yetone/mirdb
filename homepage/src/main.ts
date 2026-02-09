/**
 * Main application entry point.
 */

import { renderHeader } from './components/Header';
import { renderHero } from './components/Hero';
import { renderFeatures } from './components/Features';
import { renderQuickStart } from './components/QuickStart';
import { renderProtocol } from './components/Protocol';
import { renderConfiguration } from './components/Configuration';
import { renderArchitecture } from './components/Architecture';

function initApp(): void {
  const app = document.getElementById('app');
  if (!app) return;

  // Render Header navigation (Scenario 7)
  const header = renderHeader();
  app.appendChild(header);

  // Render Hero section (Scenario 1)
  const hero = renderHero();
  app.appendChild(hero);

  // Render Features section (Scenario 2)
  const features = renderFeatures();
  app.appendChild(features);

  // Render QuickStart section (Scenario 3)
  const quickStart = renderQuickStart();
  app.appendChild(quickStart);

  // Render Protocol section (Scenario 5)
  const protocol = renderProtocol();
  app.appendChild(protocol);

  // Render Configuration section (Scenario 6)
  const configuration = renderConfiguration();
  app.appendChild(configuration);

  // Render Architecture section (Scenario 4)
  const architecture = renderArchitecture();
  app.appendChild(architecture);
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initApp);
} else {
  initApp();
}
