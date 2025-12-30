/**
 * E2E and Unit Tests for Architecture Overview Display (REQ-4)
 *
 * These tests verify that the MirDB homepage correctly displays
 * the architecture overview with LSM tree visualization as specified in REQ-4.
 */

const fs = require('fs');
const path = require('path');

describe('Architecture Overview Display - REQ-4', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Architecture Diagram Presence', () => {
    /**
     * Test Case ID: 1
     * Input: Check for architecture diagram presence
     * Expected: SVG or image diagram showing LSM tree architecture is visible
     */
    test('should have an architecture section with id "architecture"', () => {
      const architectureSection = document.getElementById('architecture');
      expect(architectureSection).toBeTruthy();
    });

    test('should have a section title for Architecture Overview', () => {
      const architectureSection = document.getElementById('architecture');
      const sectionTitle = architectureSection.querySelector('.section-title, h2');
      expect(sectionTitle).toBeTruthy();
      expect(sectionTitle.textContent.toLowerCase()).toContain('architecture');
    });

    test('should contain an SVG diagram for LSM tree architecture', () => {
      const architectureSection = document.getElementById('architecture');
      const svgDiagram = architectureSection.querySelector('svg[data-testid="lsm-tree-diagram"]');
      expect(svgDiagram).toBeTruthy();
    });

    test('SVG diagram should be visible with appropriate dimensions', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      expect(svgDiagram).toBeTruthy();

      // SVG should have width and height or viewBox attributes
      const hasViewBox = svgDiagram.hasAttribute('viewBox');
      const hasWidth = svgDiagram.hasAttribute('width');
      const hasHeight = svgDiagram.hasAttribute('height');

      expect(hasViewBox || (hasWidth && hasHeight)).toBe(true);
    });
  });

  describe('Test Case 2: Diagram Content Shows Data Flow', () => {
    /**
     * Test Case ID: 2
     * Input: Verify diagram content shows data flow
     * Expected: Diagram shows write path through WAL, memtable, and SSTable levels
     */
    test('should show WAL (Write-Ahead Log) component in diagram', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      const walElement = svgDiagram.querySelector('[data-component="wal"]');
      expect(walElement).toBeTruthy();

      // Check if WAL text is present somewhere in the SVG
      const svgText = svgDiagram.textContent || svgDiagram.innerHTML;
      expect(svgText.toLowerCase()).toMatch(/wal|write.?ahead.?log/i);
    });

    test('should show Memtable component in diagram', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      const memtableElement = svgDiagram.querySelector('[data-component="memtable"]');
      expect(memtableElement).toBeTruthy();

      const svgText = svgDiagram.textContent || svgDiagram.innerHTML;
      expect(svgText.toLowerCase()).toContain('memtable');
    });

    test('should show Immutable Memtable component in diagram', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      const immMemtableElement = svgDiagram.querySelector('[data-component="immutable-memtable"]');
      expect(immMemtableElement).toBeTruthy();

      const svgText = svgDiagram.textContent || svgDiagram.innerHTML;
      expect(svgText.toLowerCase()).toMatch(/imm|immutable/i);
    });

    test('should show SSTable levels in diagram', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      const sstableElement = svgDiagram.querySelector('[data-component="sstable"]');
      expect(sstableElement).toBeTruthy();

      const svgText = svgDiagram.textContent || svgDiagram.innerHTML;
      expect(svgText.toLowerCase()).toMatch(/sstable|sst|level/i);
    });

    test('should show data flow arrows or connections', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');

      // Check for arrow elements (lines, paths, or polygons that indicate flow)
      const arrows = svgDiagram.querySelectorAll('path, line, polygon, marker');
      expect(arrows.length).toBeGreaterThan(0);
    });

    test('should show write path through components', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      const svgText = svgDiagram.textContent || svgDiagram.innerHTML;

      // Check that write path components exist in order
      expect(svgText.toLowerCase()).toContain('write');
    });

    test('should show Level 0 SSTables', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      const svgText = svgDiagram.textContent || svgDiagram.innerHTML;

      expect(svgText.toLowerCase()).toMatch(/level\s*0|l0/i);
    });

    test('should show higher level SSTables', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');
      const svgText = svgDiagram.textContent || svgDiagram.innerHTML;

      expect(svgText.toLowerCase()).toMatch(/level\s*[1-9]|l[1-9]/i);
    });
  });

  describe('Test Case 3: Architecture Diagram Accessibility', () => {
    /**
     * Test Case ID: 3
     * Input: Check architecture diagram alt text
     * Expected: Image has descriptive alt text explaining the LSM tree architecture
     */
    test('SVG diagram should have aria-label or role="img" with aria-labelledby', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');

      const hasAriaLabel = svgDiagram.hasAttribute('aria-label');
      const hasRole = svgDiagram.getAttribute('role') === 'img';
      const hasAriaLabelledby = svgDiagram.hasAttribute('aria-labelledby');
      const hasTitle = svgDiagram.querySelector('title');

      // SVG should have at least one accessibility attribute
      expect(hasAriaLabel || (hasRole && (hasAriaLabelledby || hasTitle))).toBe(true);
    });

    test('SVG diagram should have descriptive alt text mentioning LSM tree', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');

      // Check aria-label
      const ariaLabel = svgDiagram.getAttribute('aria-label') || '';

      // Check title element
      const titleElement = svgDiagram.querySelector('title');
      const titleText = titleElement ? titleElement.textContent : '';

      // Check aria-labelledby
      const ariaLabelledby = svgDiagram.getAttribute('aria-labelledby');
      let labelledbyText = '';
      if (ariaLabelledby) {
        const labelElement = document.getElementById(ariaLabelledby);
        labelledbyText = labelElement ? labelElement.textContent : '';
      }

      const combinedText = `${ariaLabel} ${titleText} ${labelledbyText}`.toLowerCase();

      // Alt text should mention LSM tree or its components
      expect(combinedText).toMatch(/lsm|log.?structured|memtable|sstable|architecture/i);
    });

    test('SVG diagram alt text should explain the data flow concept', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');

      const ariaLabel = svgDiagram.getAttribute('aria-label') || '';
      const titleElement = svgDiagram.querySelector('title');
      const titleText = titleElement ? titleElement.textContent : '';

      const combinedText = `${ariaLabel} ${titleText}`.toLowerCase();

      // Alt text should describe what the diagram shows
      expect(combinedText).toMatch(/data|flow|write|storage|architecture|tree/i);
    });

    test('SVG diagram should have focusable attribute for keyboard navigation', () => {
      const svgDiagram = document.querySelector('[data-testid="lsm-tree-diagram"]');

      // SVG should be focusable for screen reader users
      const tabIndex = svgDiagram.getAttribute('tabindex');
      const isFocusable = tabIndex !== null && parseInt(tabIndex) >= 0;

      // Either focusable via tabindex or has role="img" (not focusable, but accessible)
      const hasRole = svgDiagram.getAttribute('role') === 'img';

      expect(isFocusable || hasRole).toBe(true);
    });
  });

  describe('Architecture Section Content', () => {
    test('should have explanatory text about LSM tree architecture', () => {
      const architectureSection = document.getElementById('architecture');
      const textContent = architectureSection.textContent.toLowerCase();

      expect(textContent).toMatch(/lsm|log.?structured.?merge/i);
    });

    test('should have a section subtitle or description', () => {
      const architectureSection = document.getElementById('architecture');
      const subtitle = architectureSection.querySelector('.section-subtitle, .architecture-description, p');

      expect(subtitle).toBeTruthy();
      expect(subtitle.textContent.length).toBeGreaterThan(10);
    });

    test('should mention key components in explanatory text', () => {
      const architectureSection = document.getElementById('architecture');
      const textContent = architectureSection.textContent.toLowerCase();

      // Should mention at least some key components
      const mentionsWAL = textContent.match(/wal|write.?ahead/i);
      const mentionsMemtable = textContent.includes('memtable');
      const mentionsSSTable = textContent.match(/sstable|sorted.?string/i);
      const mentionsCompaction = textContent.includes('compaction');

      // At least 2 of these should be mentioned
      const mentionCount = [mentionsWAL, mentionsMemtable, mentionsSSTable, mentionsCompaction]
        .filter(Boolean).length;

      expect(mentionCount).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Visual Styling and Layout', () => {
    test('architecture section should have proper data-testid attribute', () => {
      const architectureSection = document.querySelector('[data-testid="architecture-section"]');
      expect(architectureSection).toBeTruthy();
    });

    test('diagram container should have proper styling class', () => {
      const architectureSection = document.getElementById('architecture');
      const diagramContainer = architectureSection.querySelector('.architecture-diagram, .diagram-container');

      expect(diagramContainer).toBeTruthy();
    });
  });
});
