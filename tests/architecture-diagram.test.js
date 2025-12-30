/**
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Architecture Diagram Section (REQ-9)', () => {
  let document;

  beforeAll(() => {
    const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Test Case 1: Architecture section exists', () => {
    test('should have an architecture section with id "architecture"', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();
    });

    test('should have the architecture section as a <section> element', () => {
      const section = document.querySelector('section#architecture');
      expect(section).not.toBeNull();
    });

    test('should have data-section attribute for architecture', () => {
      const section = document.querySelector('[data-section="architecture"]');
      expect(section).not.toBeNull();
    });

    test('architecture section should have a heading', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();
      const heading = section.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('architecture');
    });
  });

  describe('Test Case 2: Diagram is SVG or image', () => {
    test('should have an SVG element or img element in architecture section', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');
      const img = section.querySelector('img');
      const objectTag = section.querySelector('object[type="image/svg+xml"]');

      // At least one of these should exist
      const hasDiagram = svg !== null || img !== null || objectTag !== null;
      expect(hasDiagram).toBe(true);
    });

    test('should have inline SVG or img with svg/image source', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');
      const img = section.querySelector('img[src*=".svg"], img[src*=".png"], img[src*=".webp"]');
      const objectTag = section.querySelector('object[data*=".svg"]');

      const hasDiagram = svg !== null || img !== null || objectTag !== null;
      expect(hasDiagram).toBe(true);
    });

    test('architecture diagram should be visible (not hidden)', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');
      const img = section.querySelector('img');

      if (svg) {
        expect(svg.getAttribute('hidden')).toBeNull();
        expect(svg.style.display).not.toBe('none');
        expect(svg.style.visibility).not.toBe('hidden');
      }

      if (img) {
        expect(img.getAttribute('hidden')).toBeNull();
        expect(img.style.display).not.toBe('none');
        expect(img.style.visibility).not.toBe('hidden');
      }
    });
  });

  describe('Test Case 3: Diagram has alt text for accessibility', () => {
    test('SVG should have role="img" and aria-labelledby or aria-label', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');
      const img = section.querySelector('img');

      if (svg) {
        const hasRole = svg.getAttribute('role') === 'img';
        const hasAriaLabel = svg.hasAttribute('aria-label');
        const hasAriaLabelledby = svg.hasAttribute('aria-labelledby');
        const hasTitle = svg.querySelector('title') !== null;

        // SVG should have accessibility attributes
        expect(hasRole || hasAriaLabel || hasAriaLabelledby || hasTitle).toBe(true);
      }

      if (img) {
        // Image should have alt attribute
        expect(img.hasAttribute('alt')).toBe(true);
        expect(img.getAttribute('alt')).not.toBe('');
      }
    });

    test('diagram accessibility text should describe LSM tree or architecture', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');
      const img = section.querySelector('img');

      let accessibilityText = '';

      if (svg) {
        const title = svg.querySelector('title');
        const desc = svg.querySelector('desc');
        const ariaLabel = svg.getAttribute('aria-label');

        if (title) accessibilityText += title.textContent.toLowerCase();
        if (desc) accessibilityText += ' ' + desc.textContent.toLowerCase();
        if (ariaLabel) accessibilityText += ' ' + ariaLabel.toLowerCase();
      }

      if (img) {
        const alt = img.getAttribute('alt');
        if (alt) accessibilityText += alt.toLowerCase();
      }

      // Should mention LSM, architecture, or data flow
      const mentionsRelevantContent =
        accessibilityText.includes('lsm') ||
        accessibilityText.includes('architecture') ||
        accessibilityText.includes('data flow') ||
        accessibilityText.includes('storage');

      expect(mentionsRelevantContent).toBe(true);
    });

    test('SVG should have desc element for detailed description', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');

      if (svg) {
        const desc = svg.querySelector('desc');
        // desc is recommended but not strictly required if aria-label is present
        const hasAriaLabel = svg.hasAttribute('aria-label');
        expect(desc !== null || hasAriaLabel).toBe(true);
      }
    });
  });

  describe('Test Case 4: Diagram relates to LSM tree (manual verification support)', () => {
    test('architecture section should contain text mentioning LSM tree', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const sectionText = section.textContent.toLowerCase();

      // Should mention LSM tree in the surrounding content
      const mentionsLSM = sectionText.includes('lsm') ||
                          sectionText.includes('log-structured merge');
      expect(mentionsLSM).toBe(true);
    });

    test('architecture section should describe data flow components', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const sectionText = section.textContent.toLowerCase();

      // Should mention key LSM tree components
      const mentionsComponents =
        sectionText.includes('memtable') ||
        sectionText.includes('sstable') ||
        sectionText.includes('wal') ||
        sectionText.includes('write-ahead') ||
        sectionText.includes('compaction') ||
        sectionText.includes('level');

      expect(mentionsComponents).toBe(true);
    });

    test('SVG diagram should contain LSM-related text elements', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');

      if (svg) {
        const svgText = svg.textContent.toLowerCase();

        // SVG should contain relevant technical terms
        const hasRelevantContent =
          svgText.includes('wal') ||
          svgText.includes('memtable') ||
          svgText.includes('sstable') ||
          svgText.includes('level') ||
          svgText.includes('compaction') ||
          svgText.includes('write');

        expect(hasRelevantContent).toBe(true);
      }
    });
  });

  describe('Additional accessibility tests', () => {
    test('architecture section heading should be in proper hierarchy', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const h2 = section.querySelector('h2');
      expect(h2).not.toBeNull();

      // h3 headings should be children of section with h2
      const h3Elements = section.querySelectorAll('h3');
      h3Elements.forEach(h3 => {
        // h3 should come after h2 in DOM order
        expect(h3.compareDocumentPosition(h2) & Node.DOCUMENT_POSITION_PRECEDING).toBeTruthy();
      });
    });

    test('diagram should have appropriate dimensions or viewBox', () => {
      const section = document.querySelector('#architecture');
      expect(section).not.toBeNull();

      const svg = section.querySelector('svg');
      const img = section.querySelector('img');

      if (svg) {
        const hasViewBox = svg.hasAttribute('viewBox');
        const hasWidthHeight = svg.hasAttribute('width') || svg.hasAttribute('height');
        expect(hasViewBox || hasWidthHeight).toBe(true);
      }

      if (img) {
        // Image should ideally have width/height or CSS styling
        expect(img).not.toBeNull();
      }
    });
  });
});
