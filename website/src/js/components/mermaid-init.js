/**
 * Mermaid Diagram Initialization
 * Owner: Scenario 4 - Architecture Section
 *
 * Expected exports:
 * - initMermaid(): void - Initialize Mermaid.js library
 * - renderDiagrams(): void - Render all mermaid diagrams on page
 */

import mermaid from 'mermaid';

/**
 * Initialize Mermaid.js with custom configuration
 */
export function initMermaid() {
  mermaid.initialize({
    startOnLoad: false,
    theme: 'neutral',
    securityLevel: 'loose',
    flowchart: {
      useMaxWidth: true,
      htmlLabels: true,
      curve: 'basis',
      padding: 20,
    },
    themeVariables: {
      primaryColor: '#2563eb',
      primaryTextColor: '#1a1a1a',
      primaryBorderColor: '#1d4ed8',
      lineColor: '#6b7280',
      secondaryColor: '#f5f5f5',
      tertiaryColor: '#e5e7eb',
    },
  });
}

/**
 * Render all mermaid diagrams on the page
 * @returns {Promise<void>}
 */
export async function renderDiagrams() {
  const mermaidElements = document.querySelectorAll('.mermaid');

  if (mermaidElements.length === 0) {
    return;
  }

  try {
    await mermaid.run({
      nodes: mermaidElements,
    });
  } catch (error) {
    console.error('Mermaid rendering error:', error);
    // Add error indicator to failed diagrams
    mermaidElements.forEach((element) => {
      if (!element.querySelector('svg')) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'mermaid-error';
        errorDiv.textContent = 'Failed to render diagram';
        element.appendChild(errorDiv);
      }
    });
  }
}

/**
 * Initialize and render all mermaid diagrams
 */
export async function initAndRenderMermaid() {
  initMermaid();
  await renderDiagrams();
}
