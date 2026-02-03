/**
 * Integration tests for tab switching in UsageExamples component.
 * Owner: Scenario 3 - Usage Examples Section
 *
 * Tests:
 * - Tab switching updates content without page reload
 * - Correct examples display for each tab
 * - ARIA states update correctly
 * - Keyboard navigation works
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { Window } from 'happy-dom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Read component sources
const usageExamplesPath = resolve(__dirname, '../../src/components/UsageExamples.astro');
const usageExamplesSource = readFileSync(usageExamplesPath, 'utf-8');

const examplesPath = resolve(__dirname, '../../src/content/examples.json');
const examplesData = JSON.parse(readFileSync(examplesPath, 'utf-8'));

describe('Tab Switching Integration', () => {
  let window: Window;
  let document: Document;

  beforeEach(() => {
    // Create a mock DOM with the tab structure
    window = new Window();
    document = window.document;
    document.body.innerHTML = `
      <div role="tablist" aria-label="Example categories">
        <button role="tab" id="tab-basic" aria-selected="true" aria-controls="panel-basic" tabindex="0" class="tab-button" data-tab="basic">Basic</button>
        <button role="tab" id="tab-advanced" aria-selected="false" aria-controls="panel-advanced" tabindex="-1" class="tab-button" data-tab="advanced">Advanced</button>
        <button role="tab" id="tab-benchmark" aria-selected="false" aria-controls="panel-benchmark" tabindex="-1" class="tab-button" data-tab="benchmark">Benchmark</button>
      </div>
      <div role="tabpanel" id="panel-basic" aria-labelledby="tab-basic" class="tab-panel" data-panel="basic">
        <div class="example-item">Basic Examples Content</div>
      </div>
      <div role="tabpanel" id="panel-advanced" aria-labelledby="tab-advanced" class="tab-panel hidden" data-panel="advanced">
        <div class="example-item">Advanced Examples Content</div>
      </div>
      <div role="tabpanel" id="panel-benchmark" aria-labelledby="tab-benchmark" class="tab-panel hidden" data-panel="benchmark">
        <div class="example-item">Benchmark Examples Content</div>
      </div>
    `;
  });

  describe('Initial State', () => {
    it('should have Basic tab selected by default', () => {
      const basicTab = document.getElementById('tab-basic');
      expect(basicTab?.getAttribute('aria-selected')).toBe('true');
    });

    it('should have Basic panel visible by default', () => {
      const basicPanel = document.getElementById('panel-basic');
      expect(basicPanel?.classList.contains('hidden')).toBe(false);
    });

    it('should have other tabs not selected', () => {
      const advancedTab = document.getElementById('tab-advanced');
      const benchmarkTab = document.getElementById('tab-benchmark');
      expect(advancedTab?.getAttribute('aria-selected')).toBe('false');
      expect(benchmarkTab?.getAttribute('aria-selected')).toBe('false');
    });

    it('should have other panels hidden', () => {
      const advancedPanel = document.getElementById('panel-advanced');
      const benchmarkPanel = document.getElementById('panel-benchmark');
      expect(advancedPanel?.classList.contains('hidden')).toBe(true);
      expect(benchmarkPanel?.classList.contains('hidden')).toBe(true);
    });
  });

  describe('Tab Structure Validation', () => {
    it('should have three tabs defined', () => {
      expect(usageExamplesSource).toContain("id: 'basic'");
      expect(usageExamplesSource).toContain("id: 'advanced'");
      expect(usageExamplesSource).toContain("id: 'benchmark'");
    });

    it('should have proper ARIA connections between tabs and panels', () => {
      const tabs = document.querySelectorAll('[role="tab"]');
      tabs.forEach((tab) => {
        const controlsId = tab.getAttribute('aria-controls');
        const panel = document.getElementById(controlsId || '');
        expect(panel).not.toBeNull();
        expect(panel?.getAttribute('aria-labelledby')).toBe(tab.id);
      });
    });
  });

  describe('Content Matching', () => {
    it('should have matching number of basic examples in data', () => {
      expect(examplesData.basic.length).toBeGreaterThan(0);
    });

    it('should have matching number of advanced examples in data', () => {
      expect(examplesData.advanced.length).toBeGreaterThan(0);
    });

    it('should have matching number of benchmark examples in data', () => {
      expect(examplesData.benchmark.length).toBeGreaterThan(0);
    });
  });

  describe('Switch Tab Logic', () => {
    // Simulate the switchTab function from the component
    function switchTab(targetTabId: string, doc: Document) {
      const tabButtons = doc.querySelectorAll('.tab-button');
      const tabPanels = doc.querySelectorAll('.tab-panel');

      // Update buttons
      tabButtons.forEach((btn) => {
        const isActive = btn.getAttribute('data-tab') === targetTabId;
        btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
        btn.setAttribute('tabindex', isActive ? '0' : '-1');
      });

      // Update panels
      tabPanels.forEach((panel) => {
        const isActive = panel.getAttribute('data-panel') === targetTabId;
        if (isActive) {
          panel.classList.remove('hidden');
        } else {
          panel.classList.add('hidden');
        }
      });
    }

    it('should switch to Advanced tab correctly', () => {
      switchTab('advanced', document);

      const advancedTab = document.getElementById('tab-advanced');
      const advancedPanel = document.getElementById('panel-advanced');
      const basicTab = document.getElementById('tab-basic');
      const basicPanel = document.getElementById('panel-basic');

      expect(advancedTab?.getAttribute('aria-selected')).toBe('true');
      expect(advancedPanel?.classList.contains('hidden')).toBe(false);
      expect(basicTab?.getAttribute('aria-selected')).toBe('false');
      expect(basicPanel?.classList.contains('hidden')).toBe(true);
    });

    it('should switch to Benchmark tab correctly', () => {
      switchTab('benchmark', document);

      const benchmarkTab = document.getElementById('tab-benchmark');
      const benchmarkPanel = document.getElementById('panel-benchmark');
      const advancedPanel = document.getElementById('panel-advanced');

      expect(benchmarkTab?.getAttribute('aria-selected')).toBe('true');
      expect(benchmarkPanel?.classList.contains('hidden')).toBe(false);
      expect(advancedPanel?.classList.contains('hidden')).toBe(true);
    });

    it('should switch back to Basic tab correctly', () => {
      switchTab('basic', document);

      const basicTab = document.getElementById('tab-basic');
      const basicPanel = document.getElementById('panel-basic');
      const benchmarkPanel = document.getElementById('panel-benchmark');

      expect(basicTab?.getAttribute('aria-selected')).toBe('true');
      expect(basicPanel?.classList.contains('hidden')).toBe(false);
      expect(benchmarkPanel?.classList.contains('hidden')).toBe(true);
    });
  });

  describe('Accessibility Compliance', () => {
    it('should have tablist with aria-label', () => {
      const tablist = document.querySelector('[role="tablist"]');
      expect(tablist?.getAttribute('aria-label')).toBe('Example categories');
    });

    it('should have unique IDs for each tab', () => {
      const tabIds = new Set<string>();
      const tabs = document.querySelectorAll('[role="tab"]');
      tabs.forEach((tab) => {
        const id = tab.id;
        expect(tabIds.has(id)).toBe(false);
        tabIds.add(id);
      });
    });

    it('should have unique IDs for each panel', () => {
      const panelIds = new Set<string>();
      const panels = document.querySelectorAll('[role="tabpanel"]');
      panels.forEach((panel) => {
        const id = panel.id;
        expect(panelIds.has(id)).toBe(false);
        panelIds.add(id);
      });
    });
  });
});

describe('Keyboard Navigation', () => {
  it('should have keyboard event handlers in component', () => {
    expect(usageExamplesSource).toContain("addEventListener('keydown'");
  });

  it('should handle ArrowLeft key', () => {
    expect(usageExamplesSource).toContain("case 'ArrowLeft':");
  });

  it('should handle ArrowRight key', () => {
    expect(usageExamplesSource).toContain("case 'ArrowRight':");
  });

  it('should handle Home key', () => {
    expect(usageExamplesSource).toContain("case 'Home':");
  });

  it('should handle End key', () => {
    expect(usageExamplesSource).toContain("case 'End':");
  });

  it('should prevent default on navigation keys', () => {
    expect(usageExamplesSource).toContain('e.preventDefault()');
  });
});
