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
}

document.addEventListener('DOMContentLoaded', initApp);
