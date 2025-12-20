/**
 * Unit Tests for Installation Steps Structure
 * Scenario: Installation and Setup Instructions
 * Test Case 3: Verify installation steps are numbered/ordered
 * Validates that installation follows clear step-by-step format
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Installation Steps Structure', () => {
  let document;
  let quickStartSection;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    quickStartSection = document.getElementById('quick-start');
  });

  describe('Test Case 3: Installation Steps Are Numbered/Ordered', () => {
    test('should have quick-start section in the document', () => {
      expect(quickStartSection).not.toBeNull();
    });

    test('should have ordered list or numbered steps for installation', () => {
      // Check for ordered list
      const orderedLists = quickStartSection.querySelectorAll('ol');
      const hasOrderedList = orderedLists.length > 0;

      // Check for step numbers in text or classes
      const stepElements = quickStartSection.querySelectorAll('[class*="step"], .step');
      const hasStepClasses = stepElements.length > 0;

      // Check for numbered headings/paragraphs (Step 1, Step 2, etc.)
      const textContent = quickStartSection.textContent;
      const hasNumberedSteps = /step\s*[1-9]|1\.|2\.|3\./i.test(textContent);

      expect(hasOrderedList || hasStepClasses || hasNumberedSteps).toBe(true);
    });

    test('should have at least 2 distinct installation steps', () => {
      // Check ordered list items
      const orderedLists = quickStartSection.querySelectorAll('ol');
      let listItemCount = 0;
      orderedLists.forEach(ol => {
        listItemCount += ol.querySelectorAll('li').length;
      });

      // Check step elements
      const stepElements = quickStartSection.querySelectorAll('[class*="step"], .step');

      // Check for numbered content
      const textContent = quickStartSection.textContent;
      const stepMatches = textContent.match(/step\s*[1-9]|^[1-9]\./gim) || [];

      const totalSteps = Math.max(listItemCount, stepElements.length, stepMatches.length);
      expect(totalSteps).toBeGreaterThanOrEqual(2);
    });

    test('installation steps should be in logical order', () => {
      const textContent = quickStartSection.textContent.toLowerCase();

      // Prerequisites/install should come before run/start
      const prerequisiteIndex = Math.max(
        textContent.indexOf('prerequisite'),
        textContent.indexOf('install rust'),
        textContent.indexOf('requirement')
      );

      const cloneIndex = textContent.indexOf('clone') !== -1 ? textContent.indexOf('clone') :
                          textContent.indexOf('cargo install');

      const runIndex = Math.max(
        textContent.indexOf('cargo run'),
        textContent.indexOf('start'),
        textContent.indexOf('./mirdb')
      );

      // If we have prerequisites mentioned, they should come before clone/install
      if (prerequisiteIndex !== -1 && cloneIndex !== -1) {
        expect(prerequisiteIndex).toBeLessThan(cloneIndex);
      }

      // Clone/install should come before run/start
      if (cloneIndex !== -1 && runIndex !== -1) {
        expect(cloneIndex).toBeLessThan(runIndex);
      }
    });
  });

  describe('Installation Content Structure', () => {
    test('should have code blocks for commands', () => {
      const codeBlocks = quickStartSection.querySelectorAll('pre, code');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('should have descriptive text alongside code blocks', () => {
      // Should have paragraphs or list items explaining the steps
      const textElements = quickStartSection.querySelectorAll('p, li');
      expect(textElements.length).toBeGreaterThan(0);
    });

    test('should have section heading', () => {
      const heading = quickStartSection.querySelector('h2, h3');
      expect(heading).not.toBeNull();
    });
  });

  describe('Installation Instructions Completeness', () => {
    test('should mention how to get the source code', () => {
      const text = quickStartSection.textContent.toLowerCase();
      const hasSourceInstructions =
        text.includes('git clone') ||
        text.includes('cargo install') ||
        text.includes('download');

      expect(hasSourceInstructions).toBe(true);
    });

    test('should mention how to build or install', () => {
      const text = quickStartSection.textContent.toLowerCase();
      const hasBuildInstructions =
        text.includes('cargo build') ||
        text.includes('cargo install') ||
        text.includes('build');

      expect(hasBuildInstructions).toBe(true);
    });

    test('should mention how to run the server', () => {
      const text = quickStartSection.textContent.toLowerCase();
      const hasRunInstructions =
        text.includes('cargo run') ||
        text.includes('./mirdb') ||
        text.includes('start');

      expect(hasRunInstructions).toBe(true);
    });
  });
});
