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
import { renderFooter } from './components/Footer';
import { renderThemeToggle } from './components/ThemeToggle';

function createSkipLink(): HTMLElement {
  const skipLink = document.createElement('a');
  skipLink.href = '#main-content';
  skipLink.className = 'skip-link';
  skipLink.textContent = 'Skip to main content';
  return skipLink;
}

function initApp(): void {
  const app = document.getElementById('app');
  if (!app) return;

  // Skip to main content link for accessibility
  const skipLink = createSkipLink();
  app.appendChild(skipLink);

  // Render Header navigation (Scenario 7)
  const header = renderHeader();
  app.appendChild(header);

  // Render Theme Toggle (Scenario 9)
  const themeToggle = renderThemeToggle();
  const headerContainer = header.querySelector('.header__container');
  if (headerContainer) {
    headerContainer.insertBefore(themeToggle, headerContainer.lastChild);
  }

  // Create main content wrapper for accessibility
  const main = document.createElement('main');
  main.id = 'main-content';
  main.setAttribute('role', 'main');

  // Render Hero section (Scenario 1)
  const hero = renderHero();
  main.appendChild(hero);

  // Render Features section (Scenario 2)
  const features = renderFeatures();
  main.appendChild(features);

  // Render QuickStart section (Scenario 3)
  const quickStart = renderQuickStart();
  main.appendChild(quickStart);

  // Render Protocol section (Scenario 5)
  const protocol = renderProtocol();
  main.appendChild(protocol);

  // Render Configuration section (Scenario 6)
  const configuration = renderConfiguration();
  main.appendChild(configuration);

  // Render Architecture section (Scenario 4)
  const architecture = renderArchitecture();
  main.appendChild(architecture);

  app.appendChild(main);

  // Render Footer section (Scenario 8)
  const footer = renderFooter();
  app.appendChild(footer);
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initApp);
} else {
  initApp();
}
