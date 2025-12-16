import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Architecture Overview Section', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Check architecture section exists
  describe('Test Case 1: Architecture section exists', () => {
    it('should have a section with id="architecture"', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
    });

    it('should have architecture section as a <section> element', () => {
      const architectureSection = document.querySelector('section#architecture');
      expect(architectureSection).not.toBeNull();
    });

    it('should have architecture-related heading', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const heading = architectureSection.querySelector('h2, h3');
      expect(heading).not.toBeNull();
      const headingText = heading.textContent.toLowerCase();
      expect(headingText).toContain('architecture');
    });

    it('architecture section should be navigable from nav', () => {
      const nav = document.querySelector('nav');
      const architectureLink = nav.querySelector('a[href="#architecture"]');
      expect(architectureLink).not.toBeNull();
    });
  });

  // Test Case 2: Check for architecture diagram
  describe('Test Case 2: Architecture diagram presence', () => {
    it('should have an SVG or img element in architecture section', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const diagram = architectureSection.querySelector('svg, img');
      expect(diagram).not.toBeNull();
    });

    it('diagram should be an SVG for scalability (preferred)', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const svg = architectureSection.querySelector('svg');
      // SVG is preferred but img is also acceptable
      const img = architectureSection.querySelector('img');
      expect(svg !== null || img !== null).toBe(true);
    });

    it('diagram should have visual content (not empty)', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const diagram = architectureSection.querySelector('svg, img');
      expect(diagram).not.toBeNull();

      if (diagram.tagName.toLowerCase() === 'svg') {
        // SVG should have child elements (shapes, paths, etc.)
        expect(diagram.children.length).toBeGreaterThan(0);
      } else {
        // Image should have src attribute
        const src = diagram.getAttribute('src');
        expect(src).not.toBeNull();
        expect(src.length).toBeGreaterThan(0);
      }
    });

    it('diagram should show LSM-tree related components', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const sectionText = architectureSection.textContent.toLowerCase();

      // Check for LSM-tree components mentioned in diagram labels or surrounding text
      const hasMemtable = sectionText.includes('memtable');
      const hasSSTable = sectionText.includes('sstable') || sectionText.includes('ss table') || sectionText.includes('sst');
      const hasCompaction = sectionText.includes('compaction') || sectionText.includes('compact');
      const hasWAL = sectionText.includes('wal') || sectionText.includes('write-ahead') || sectionText.includes('write ahead');

      // At least some LSM-tree components should be present
      expect(hasMemtable || hasSSTable || hasCompaction || hasWAL).toBe(true);
    });
  });

  // Test Case 3: Verify diagram has alt text for accessibility
  describe('Test Case 3: Diagram accessibility (alt text)', () => {
    it('architecture image/SVG should have descriptive alt attribute or aria-label', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const diagram = architectureSection.querySelector('svg, img');
      expect(diagram).not.toBeNull();

      if (diagram.tagName.toLowerCase() === 'img') {
        const alt = diagram.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.length).toBeGreaterThan(10); // Descriptive, not just "image"
      } else {
        // SVG should have aria-label or role with title
        const ariaLabel = diagram.getAttribute('aria-label');
        const role = diagram.getAttribute('role');
        const title = diagram.querySelector('title');
        const hasAccessibility = ariaLabel || (role === 'img' && title);
        expect(hasAccessibility).toBeTruthy();
      }
    });

    it('alt text or aria-label should be descriptive of architecture', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const diagram = architectureSection.querySelector('svg, img');
      expect(diagram).not.toBeNull();

      let description = '';
      if (diagram.tagName.toLowerCase() === 'img') {
        description = diagram.getAttribute('alt') || '';
      } else {
        description = diagram.getAttribute('aria-label') || '';
        const title = diagram.querySelector('title');
        if (title) {
          description = title.textContent || '';
        }
      }

      const descLower = description.toLowerCase();
      // Should mention architecture, LSM, or data flow
      const isDescriptive = descLower.includes('architecture') ||
                           descLower.includes('lsm') ||
                           descLower.includes('data flow') ||
                           descLower.includes('diagram');
      expect(isDescriptive).toBe(true);
    });
  });

  // Test Case 4: Check for LSM-tree explanation text
  describe('Test Case 4: LSM-tree explanation text', () => {
    it('should have text explaining LSM-tree', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const sectionText = architectureSection.textContent.toLowerCase();

      // Check for LSM-tree mention
      const hasLSMTree = sectionText.includes('lsm') ||
                         sectionText.includes('log-structured merge');
      expect(hasLSMTree).toBe(true);
    });

    it('should explain LSM-tree benefits', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const sectionText = architectureSection.textContent.toLowerCase();

      // Check for benefit-related keywords
      const hasBenefits = sectionText.includes('write') ||
                          sectionText.includes('performance') ||
                          sectionText.includes('efficient') ||
                          sectionText.includes('optimized') ||
                          sectionText.includes('fast');
      expect(hasBenefits).toBe(true);
    });

    it('should accompany the diagram with explanation text', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();

      // Check that there's both a diagram and explanatory text (p elements)
      const diagram = architectureSection.querySelector('svg, img');
      const paragraphs = architectureSection.querySelectorAll('p');

      expect(diagram).not.toBeNull();
      expect(paragraphs.length).toBeGreaterThan(0);

      // At least one paragraph should have substantial content
      const hasSubstantialText = Array.from(paragraphs).some(p => p.textContent.length > 50);
      expect(hasSubstantialText).toBe(true);
    });

    it('should describe data flow from write to compaction', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const sectionText = architectureSection.textContent.toLowerCase();

      // Check for data flow description
      const describesWritePath = sectionText.includes('write') &&
                                  (sectionText.includes('memtable') ||
                                   sectionText.includes('sstable') ||
                                   sectionText.includes('compaction'));
      expect(describesWritePath).toBe(true);
    });
  });

  // Additional accessibility and structure tests
  describe('Architecture section structure', () => {
    it('should have proper heading hierarchy', () => {
      const architectureSection = document.querySelector('#architecture');
      expect(architectureSection).not.toBeNull();
      const h2 = architectureSection.querySelector('h2');
      expect(h2).not.toBeNull();
    });

    it('should be placed logically within page flow', () => {
      const sections = document.querySelectorAll('main > section, main section');
      const architectureIndex = Array.from(sections).findIndex(
        s => s.id === 'architecture'
      );
      // Architecture section should exist somewhere in the page
      expect(architectureIndex).toBeGreaterThanOrEqual(0);
    });
  });
});
