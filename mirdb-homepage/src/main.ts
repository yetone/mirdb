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
}

document.addEventListener('DOMContentLoaded', initApp);
