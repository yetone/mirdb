/**
 * Diagrams Module (Mermaid.js Integration)
 * Owner: Scenario 4 - Architecture Section
 *
 * Handles initialization and rendering of Mermaid.js diagrams
 * with dark theme support and error handling.
 */

/**
 * Initialize Mermaid.js with configuration for dark mode
 * and responsive rendering.
 */
function initDiagrams() {
  // Check if Mermaid is available
  if (typeof mermaid === 'undefined') {
    console.warn('Mermaid.js not loaded, showing fallback content');
    showFallbackContent();
    return;
  }

  try {
    // Configure Mermaid with dark theme compatible with site design
    mermaid.initialize({
      startOnLoad: false,
      theme: 'dark',
      themeVariables: {
        primaryColor: '#6366f1',
        primaryTextColor: '#f1f5f9',
        primaryBorderColor: '#818cf8',
        secondaryColor: '#1e293b',
        secondaryTextColor: '#f1f5f9',
        tertiaryColor: '#334155',
        tertiaryTextColor: '#f1f5f9',
        lineColor: '#94a3b8',
        textColor: '#f1f5f9',
        mainBkg: '#1e293b',
        nodeBorder: '#6366f1',
        clusterBkg: '#0f172a',
        clusterBorder: '#475569',
        titleColor: '#f1f5f9',
        edgeLabelBackground: '#1e293b',
        actorBorder: '#6366f1',
        actorBkg: '#1e293b',
        actorTextColor: '#f1f5f9',
        actorLineColor: '#94a3b8',
        signalColor: '#94a3b8',
        signalTextColor: '#f1f5f9',
        labelBoxBkgColor: '#1e293b',
        labelBoxBorderColor: '#475569',
        labelTextColor: '#f1f5f9',
        loopTextColor: '#f1f5f9',
        noteBorderColor: '#6366f1',
        noteBkgColor: '#334155',
        noteTextColor: '#f1f5f9',
        activationBorderColor: '#6366f1',
        activationBkgColor: '#334155',
        sequenceNumberColor: '#f1f5f9'
      },
      flowchart: {
        useMaxWidth: true,
        htmlLabels: true,
        curve: 'basis'
      },
      sequence: {
        useMaxWidth: true,
        actorMargin: 50,
        messageMargin: 40
      },
      securityLevel: 'loose',
      fontFamily: 'Inter, -apple-system, BlinkMacSystemFont, sans-serif'
    });

    // Render all Mermaid diagrams
    renderDiagrams();
  } catch (error) {
    console.error('Failed to initialize Mermaid:', error);
    showFallbackContent();
  }
}

/**
 * Render all Mermaid diagrams on the page.
 * Handles errors gracefully by showing fallback content.
 */
async function renderDiagrams() {
  const diagrams = document.querySelectorAll('.mermaid');

  if (diagrams.length === 0) {
    return;
  }

  for (const diagram of diagrams) {
    try {
      await renderDiagram(diagram);
    } catch (error) {
      console.error('Failed to render diagram:', error);
      showDiagramError(diagram);
    }
  }
}

/**
 * Render a specific Mermaid diagram element.
 * @param {HTMLElement} element - The diagram element to render
 */
async function renderDiagram(element) {
  if (!element || typeof mermaid === 'undefined') {
    return;
  }

  const diagramText = element.textContent.trim();
  const diagramId = 'mermaid-' + Math.random().toString(36).substr(2, 9);

  try {
    const { svg } = await mermaid.render(diagramId, diagramText);

    // Create a wrapper div and insert the rendered SVG
    const wrapper = document.createElement('div');
    wrapper.className = 'architecture__diagram-rendered';
    wrapper.innerHTML = svg;

    // Preserve aria-label for accessibility
    const ariaLabel = element.getAttribute('aria-label');
    if (ariaLabel) {
      wrapper.setAttribute('role', 'img');
      wrapper.setAttribute('aria-label', ariaLabel);
    }

    // Replace the pre element with the rendered diagram
    element.parentNode.replaceChild(wrapper, element);

    // Mark the diagram container as successfully rendered
    const container = wrapper.closest('.architecture__diagram');
    if (container) {
      container.setAttribute('data-rendered', 'true');
    }
  } catch (error) {
    console.error('Mermaid render error:', error);
    throw error;
  }
}

/**
 * Show fallback content when Mermaid fails to load or render.
 */
function showFallbackContent() {
  const diagrams = document.querySelectorAll('.architecture__diagram');

  diagrams.forEach(container => {
    const mermaidEl = container.querySelector('.mermaid');
    const noscript = container.querySelector('noscript');

    if (mermaidEl && noscript) {
      // Hide the raw Mermaid code
      mermaidEl.style.display = 'none';

      // Create and show fallback content from noscript
      const fallback = document.createElement('div');
      fallback.className = 'architecture__fallback-visible';
      fallback.innerHTML = noscript.innerHTML;
      container.appendChild(fallback);
    }

    // Mark container as having an error
    container.setAttribute('data-error', 'true');
  });
}

/**
 * Show error state for a specific diagram that failed to render.
 * @param {HTMLElement} element - The diagram element that failed
 */
function showDiagramError(element) {
  const container = element.closest('.architecture__diagram');

  if (container) {
    // Hide the failed Mermaid element
    element.style.display = 'none';

    // Show fallback content from noscript if available
    const noscript = container.querySelector('noscript');
    if (noscript) {
      const fallback = document.createElement('div');
      fallback.className = 'architecture__fallback-visible';
      fallback.innerHTML = noscript.innerHTML;
      container.appendChild(fallback);
    } else {
      // Create generic error message
      const errorMsg = document.createElement('div');
      errorMsg.className = 'architecture__fallback-visible architecture__error';
      errorMsg.textContent = 'Diagram could not be loaded. Please refresh the page.';
      container.appendChild(errorMsg);
    }

    // Mark container as having an error
    container.setAttribute('data-error', 'true');
  }
}

// Export functions for use in main.js and testing
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { initDiagrams, renderDiagram, showFallbackContent };
}
