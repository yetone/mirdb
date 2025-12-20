/**
 * Tests for the Architecture Section and LSM-Tree Diagram
 *
 * Scenario: Architecture Diagram and LSM-Tree Explanation
 * Verifies that an architecture diagram illustrating LSM-tree data flow is present
 * with explanatory content, proper accessibility attributes, and SVG format.
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

// Load HTML content before tests
beforeEach(() => {
  // Load HTML for each test to ensure clean state
  const htmlPath = path.join(__dirname, '..', 'index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.body.innerHTML = html;
});

describe('Architecture Diagram and LSM-Tree Explanation', () => {

  /**
   * Test Case 1: Query DOM for diagram image or SVG element
   * Expected: Architecture diagram element found (img or svg) in architecture section
   */
  describe('Test Case 1: Architecture Diagram Presence', () => {
    test('should have an architecture section with proper ID', () => {
      const architectureSection = document.getElementById('architecture');
      expect(architectureSection).toBeInTheDocument();
      expect(architectureSection.tagName.toLowerCase()).toBe('section');
    });

    test('should have a diagram element (SVG) in the architecture section', () => {
      const architectureSection = document.getElementById('architecture');
      expect(architectureSection).toBeInTheDocument();

      const svgElement = architectureSection.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });

    test('diagram should be contained within architecture-diagram div', () => {
      const diagramContainer = document.querySelector('.architecture-diagram');
      expect(diagramContainer).toBeInTheDocument();

      const svgElement = diagramContainer.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });

    test('SVG diagram should have the lsm-tree-diagram class', () => {
      const svgElement = document.querySelector('svg.lsm-tree-diagram');
      expect(svgElement).toBeInTheDocument();
    });

    test('architecture section should have How It Works heading', () => {
      const architectureSection = document.getElementById('architecture');
      const heading = architectureSection.querySelector('h2');
      expect(heading).toBeInTheDocument();
      expect(heading.textContent).toContain('How It Works');
    });
  });

  /**
   * Test Case 2: Check diagram element for alt attribute
   * Expected: Diagram has descriptive alt text explaining the data flow
   */
  describe('Test Case 2: Diagram Accessibility (Alt Text)', () => {
    test('SVG should have role="img" for accessibility', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      expect(svgElement).toBeInTheDocument();
      expect(svgElement.getAttribute('role')).toBe('img');
    });

    test('SVG should have aria-labelledby referencing title and desc', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      expect(svgElement).toBeInTheDocument();

      const ariaLabelledBy = svgElement.getAttribute('aria-labelledby');
      expect(ariaLabelledBy).toBeTruthy();
      expect(ariaLabelledBy).toContain('diagram-title');
      expect(ariaLabelledBy).toContain('diagram-desc');
    });

    test('SVG should have a title element with descriptive text', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      const titleElement = svgElement.querySelector('title');
      expect(titleElement).toBeInTheDocument();
      expect(titleElement.textContent.length).toBeGreaterThan(10);
      expect(titleElement.textContent.toLowerCase()).toContain('lsm');
    });

    test('SVG should have a desc element explaining the data flow', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      const descElement = svgElement.querySelector('desc');
      expect(descElement).toBeInTheDocument();

      const descText = descElement.textContent.toLowerCase();
      expect(descText.length).toBeGreaterThan(50);
      expect(descText).toContain('data flow');
      expect(descText).toContain('wal');
      expect(descText).toContain('memtable');
      expect(descText).toContain('sstable');
    });

    test('SVG title element should have correct id', () => {
      const titleElement = document.getElementById('diagram-title');
      expect(titleElement).toBeInTheDocument();
      expect(titleElement.tagName.toLowerCase()).toBe('title');
    });

    test('SVG desc element should have correct id', () => {
      const descElement = document.getElementById('diagram-desc');
      expect(descElement).toBeInTheDocument();
      expect(descElement.tagName.toLowerCase()).toBe('desc');
    });

    test('architecture section should have aria-labelledby for section heading', () => {
      const architectureSection = document.getElementById('architecture');
      const labelledBy = architectureSection.getAttribute('aria-labelledby');
      expect(labelledBy).toBeTruthy();

      const heading = document.getElementById(labelledBy);
      expect(heading).toBeInTheDocument();
    });
  });

  /**
   * Test Case 3: Search section content for LSM-tree keywords
   * Expected: Section contains explanation of LSM-tree, memtable, SSTable, compaction
   */
  describe('Test Case 3: LSM-Tree Content Keywords', () => {
    test('section should contain LSM-tree explanation', () => {
      const architectureSection = document.getElementById('architecture');
      const sectionText = architectureSection.textContent.toLowerCase();

      expect(sectionText).toContain('lsm');
    });

    test('section should mention memtable', () => {
      const architectureSection = document.getElementById('architecture');
      const sectionText = architectureSection.textContent.toLowerCase();

      expect(sectionText).toContain('memtable');
    });

    test('section should mention SSTable', () => {
      const architectureSection = document.getElementById('architecture');
      const sectionText = architectureSection.textContent.toLowerCase();

      expect(sectionText).toContain('sstable');
    });

    test('section should mention compaction', () => {
      const architectureSection = document.getElementById('architecture');
      const sectionText = architectureSection.textContent.toLowerCase();

      expect(sectionText).toContain('compaction');
    });

    test('section should mention WAL (Write-Ahead Log)', () => {
      const architectureSection = document.getElementById('architecture');
      const sectionText = architectureSection.textContent.toLowerCase();

      expect(sectionText).toContain('wal');
    });

    test('section should have an explanation section', () => {
      const explanationSection = document.querySelector('.architecture-explanation');
      expect(explanationSection).toBeInTheDocument();
    });

    test('explanation should describe efficient write performance', () => {
      const explanationSection = document.querySelector('.architecture-explanation');
      const explanationText = explanationSection.textContent.toLowerCase();

      expect(explanationText).toContain('write');
      expect(explanationText).toContain('performance');
    });

    test('explanation should list benefits with proper structure', () => {
      const benefitsList = document.querySelector('.architecture-benefits');
      expect(benefitsList).toBeInTheDocument();

      const benefitItems = benefitsList.querySelectorAll('li');
      expect(benefitItems.length).toBeGreaterThanOrEqual(3);
    });
  });

  /**
   * Test Case 4: Verify diagram is SVG format for scalability
   * Expected: Diagram uses SVG format for crisp rendering at all sizes
   */
  describe('Test Case 4: SVG Format for Scalability', () => {
    test('diagram should be an SVG element', () => {
      const architectureSection = document.getElementById('architecture');
      const svgElement = architectureSection.querySelector('svg');

      expect(svgElement).toBeInTheDocument();
      expect(svgElement.tagName.toLowerCase()).toBe('svg');
    });

    test('SVG should have a viewBox for responsive scaling', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      const viewBox = svgElement.getAttribute('viewBox');

      expect(viewBox).toBeTruthy();
      expect(viewBox).toMatch(/^\d+\s+\d+\s+\d+\s+\d+$/);
    });

    test('SVG should have xmlns attribute', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      const xmlns = svgElement.getAttribute('xmlns');

      expect(xmlns).toBe('http://www.w3.org/2000/svg');
    });

    test('diagram should NOT be an img element (should be inline SVG)', () => {
      const diagramContainer = document.querySelector('.architecture-diagram');
      const imgElement = diagramContainer.querySelector('img');

      // Should be SVG, not img
      expect(imgElement).toBeNull();
    });

    test('SVG should contain visual elements representing data flow', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');

      // Should have rectangles for the boxes
      const rects = svgElement.querySelectorAll('rect');
      expect(rects.length).toBeGreaterThan(4);

      // Should have text labels
      const textElements = svgElement.querySelectorAll('text');
      expect(textElements.length).toBeGreaterThan(5);

      // Should have lines/arrows for flow
      const lines = svgElement.querySelectorAll('line');
      expect(lines.length).toBeGreaterThan(3);
    });

    test('SVG should include data flow components (WAL, Memtable, SST)', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      const svgText = svgElement.textContent;

      expect(svgText).toContain('WAL');
      expect(svgText).toContain('Memtable');
      expect(svgText).toContain('SST');
    });

    test('SVG should show compaction processes', () => {
      const svgElement = document.querySelector('.architecture-diagram svg');
      const svgText = svgElement.textContent.toLowerCase();

      expect(svgText).toContain('minor compaction');
      expect(svgText).toContain('major compaction');
    });
  });

  /**
   * Additional tests for comprehensive coverage
   */
  describe('Additional Architecture Section Tests', () => {
    test('architecture section should be navigable from navigation', () => {
      const navLink = document.querySelector('a[href="#architecture"]');
      expect(navLink).toBeInTheDocument();
    });

    test('architecture section should have a subtitle', () => {
      const architectureSection = document.getElementById('architecture');
      const subtitle = architectureSection.querySelector('.section-subtitle');
      expect(subtitle).toBeInTheDocument();
      expect(subtitle.textContent.toLowerCase()).toContain('lsm');
    });

    test('explanation title should use h3 for proper heading hierarchy', () => {
      const explanationTitle = document.querySelector('.architecture-explanation-title');
      expect(explanationTitle).toBeInTheDocument();
      expect(explanationTitle.tagName.toLowerCase()).toBe('h3');
    });

    test('section should explain data durability', () => {
      const explanationSection = document.querySelector('.architecture-explanation');
      const explanationText = explanationSection.textContent.toLowerCase();

      expect(explanationText).toContain('durability');
    });

    test('section should mention storage levels', () => {
      const architectureSection = document.getElementById('architecture');
      const sectionText = architectureSection.textContent.toLowerCase();

      expect(sectionText).toContain('level');
    });
  });
});
