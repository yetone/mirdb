/**
 * Tests for Architecture Diagram Display
 * Verifies the architecture section displays correctly with LSM tree structure,
 * including memtable, SSTable levels, and proper accessibility attributes
 */

const fs = require('fs');
const path = require('path');

describe('Architecture Diagram Display', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: Locate architecture diagram element', () => {
    test('Image or SVG element displaying LSM tree architecture should be present', () => {
      // Check for architecture section
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      expect(architectureSection.classList.contains('architecture')).toBe(true);

      // Check for architecture diagram container with role="img" for accessibility
      const diagram = document.querySelector('.architecture-diagram');
      expect(diagram).not.toBeNull();

      // Verify it has role="img" to be treated as an image by screen readers
      expect(diagram.getAttribute('role')).toBe('img');

      // Verify the diagram has visual layers representing LSM tree
      const layers = document.querySelectorAll('.architecture-layers .layer');
      expect(layers.length).toBeGreaterThanOrEqual(4); // At least 4 layers for LSM tree
    });
  });

  describe('Test Case 2: Check diagram shows memtable layer', () => {
    test('Diagram or accompanying text should mention memtable/skip list', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();

      // Get all text content from the architecture section
      const sectionText = architectureSection.textContent.toLowerCase();

      // Verify memtable is mentioned
      expect(sectionText).toContain('memtable');

      // Verify skip list is mentioned
      expect(sectionText).toContain('skip list');

      // Check for specific memtable layer element
      const memtableLayer = document.querySelector('[data-testid="layer-memtable"]');
      expect(memtableLayer).not.toBeNull();
      expect(memtableLayer.textContent).toContain('Memtable');
      expect(memtableLayer.textContent).toContain('Skip List');
    });
  });

  describe('Test Case 3: Check diagram shows SSTable levels', () => {
    test('Diagram or accompanying text should show L0-L6 or SSTable levels', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();

      // Get all text content from the architecture section
      const sectionText = architectureSection.textContent.toLowerCase();

      // Verify SSTable is mentioned
      expect(sectionText).toContain('sstable');

      // Verify levels are mentioned (L0 - L6)
      expect(sectionText).toMatch(/l0.*l6|level/i);

      // Check for specific SSTable layer element
      const sstableLayer = document.querySelector('[data-testid="layer-sstable"]');
      expect(sstableLayer).not.toBeNull();
      expect(sstableLayer.textContent).toContain('SSTable');
      expect(sstableLayer.textContent).toContain('L0');
      expect(sstableLayer.textContent).toContain('L6');
    });
  });

  describe('Test Case 4: Verify architecture diagram has alt text', () => {
    test('Diagram image should have descriptive alt attribute', () => {
      // Check for architecture diagram with aria-label (acts as alt text for role="img")
      const diagram = document.querySelector('.architecture-diagram');
      expect(diagram).not.toBeNull();

      // Verify the diagram has an aria-label (alternative text for screen readers)
      const ariaLabel = diagram.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.length).toBeGreaterThan(0);

      // Verify the aria-label is descriptive and contains key architecture terms
      const labelLower = ariaLabel.toLowerCase();
      expect(labelLower).toContain('lsm');
      expect(labelLower).toContain('memtable');
      expect(labelLower).toContain('sstable');
      expect(labelLower).toContain('architecture');
    });
  });

  describe('Additional Architecture Section Validations', () => {
    test('Architecture section should have proper heading', () => {
      const heading = document.querySelector('#architecture h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent).toBe('Architecture');
    });

    test('Architecture diagram should show client layer', () => {
      const clientLayer = document.querySelector('[data-testid="layer-client"]');
      expect(clientLayer).not.toBeNull();
      expect(clientLayer.textContent.toLowerCase()).toContain('client');
      expect(clientLayer.textContent.toLowerCase()).toContain('memcached');
    });

    test('Architecture diagram should show immutable memtable layer', () => {
      const immutableLayer = document.querySelector('[data-testid="layer-immutable"]');
      expect(immutableLayer).not.toBeNull();
      expect(immutableLayer.textContent.toLowerCase()).toContain('immutable');
      expect(immutableLayer.textContent.toLowerCase()).toContain('memtable');
    });

    test('Architecture section should mention compaction', () => {
      const architectureSection = document.querySelector('#architecture');
      const sectionText = architectureSection.textContent.toLowerCase();

      // Compaction is an important part of LSM tree architecture
      expect(sectionText).toContain('compaction');
    });

    test('Architecture diagram should have visual flow indicators (arrows)', () => {
      const arrows = document.querySelectorAll('#architecture .arrow');
      expect(arrows.length).toBeGreaterThanOrEqual(3); // At least 3 arrows between layers
    });

    test('Architecture layers should be properly hidden from screen readers to avoid duplication', () => {
      const layers = document.querySelector('.architecture-layers');
      expect(layers).not.toBeNull();
      // Inner content should be hidden since aria-label provides full description
      expect(layers.getAttribute('aria-hidden')).toBe('true');
    });

    test('Architecture description should explain LSM tree', () => {
      const description = document.querySelector('.architecture-description');
      expect(description).not.toBeNull();

      const descText = description.textContent.toLowerCase();
      expect(descText).toContain('lsm');
      expect(descText).toContain('tree');
    });
  });
});
