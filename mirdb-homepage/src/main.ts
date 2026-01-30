/**
 * Application Entry Point.
 *
 * Initializes all components and renders the homepage.
 * This file is created by the first scenario builder.
 *
 * Expected:
 * - Import and render all section components
 * - Initialize smooth scrolling
 * - Initialize any interactive behaviors
 */

import { renderFeatures } from './components/Features';
import { renderUsageExamples } from './components/UsageExamples';
import { renderArchitecture } from './components/Architecture';
import { renderGettingStarted } from './components/GettingStarted';
import { renderFooter } from './components/Footer';

function initApp(): void {
  // Render Features section (Scenario 2)
  const featuresContainer = document.getElementById('features');
  if (featuresContainer) {
    const featuresSection = renderFeatures();
    featuresContainer.replaceWith(featuresSection);
  }

  // Render Usage Examples section (Scenario 3)
  const usageContainer = document.getElementById('usage');
  if (usageContainer) {
    const usageSection = renderUsageExamples();
    usageContainer.replaceWith(usageSection);
  }

  // Render Architecture section (Scenario 4)
  const architectureContainer = document.getElementById('architecture');
  if (architectureContainer) {
    const architectureSection = renderArchitecture();
    architectureContainer.replaceWith(architectureSection);
  }

  // Render Getting Started section (Scenario 5)
  const gettingStartedContainer = document.getElementById('getting-started');
  if (gettingStartedContainer) {
    const gettingStartedSection = renderGettingStarted();
    gettingStartedContainer.replaceWith(gettingStartedSection);
  }

  // Render Footer section (Scenario 6)
  const footerContainer = document.getElementById('footer');
  if (footerContainer) {
    const footerSection = renderFooter();
    footerContainer.replaceWith(footerSection);
  }
}

document.addEventListener('DOMContentLoaded', initApp);
