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

import { renderUsageExamples } from './components/UsageExamples';

function initApp(): void {
  // Render Usage Examples section (Scenario 3)
  const usageContainer = document.getElementById('usage');
  if (usageContainer) {
    const usageSection = renderUsageExamples();
    usageContainer.replaceWith(usageSection);
  }
}

document.addEventListener('DOMContentLoaded', initApp);
