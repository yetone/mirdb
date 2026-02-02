/**
 * Architecture Section Accessibility Unit Tests
 * Owner: Scenario 4 - Architecture Overview Section
 *
 * Tests:
 * - Alt text presence and quality for architecture diagram
 * - ARIA attributes for architecture section
 * - Semantic HTML structure
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('Architecture Section Accessibility', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  describe('TC2: Architecture diagram alt text', () => {
    test('Architecture diagram has alt attribute', () => {
      // Look for img with architecture-diagram class and alt attribute
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      expect(architectureSectionMatch).toBeTruthy();

      const architectureSection = architectureSectionMatch[0];

      // Check for img with alt attribute
      expect(architectureSection).toMatch(/<img[^>]+alt="[^"]+"/);
    });

    test('Architecture diagram alt text is descriptive (minimum 50 characters)', () => {
      // Extract alt text from architecture section
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      expect(architectureSectionMatch).toBeTruthy();

      const architectureSection = architectureSectionMatch[0];

      // Extract alt attribute value
      const altMatch = architectureSection.match(/<img[^>]+alt="([^"]+)"/);
      expect(altMatch).toBeTruthy();

      const altText = altMatch[1];

      // Alt text should be descriptive (at least 50 characters)
      expect(altText.length).toBeGreaterThan(50);
    });

    test('Architecture diagram alt text mentions LSM-tree architecture', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      const altMatch = architectureSection.match(/<img[^>]+alt="([^"]+)"/);
      const altText = altMatch[1].toLowerCase();

      // Should mention architecture
      expect(altText).toMatch(/lsm-tree|architecture/i);
    });

    test('Architecture diagram alt text mentions memtable', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      const altMatch = architectureSection.match(/<img[^>]+alt="([^"]+)"/);
      const altText = altMatch[1].toLowerCase();

      expect(altText).toContain('memtable');
    });

    test('Architecture diagram alt text mentions immutable memtables', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      const altMatch = architectureSection.match(/<img[^>]+alt="([^"]+)"/);
      const altText = altMatch[1].toLowerCase();

      expect(altText).toContain('immutable');
    });

    test('Architecture diagram alt text mentions SSTable levels', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      const altMatch = architectureSection.match(/<img[^>]+alt="([^"]+)"/);
      const altText = altMatch[1].toLowerCase();

      expect(altText).toMatch(/sstable|level/i);
    });
  });

  describe('Architecture section semantic structure', () => {
    test('Architecture section has aria-labelledby attribute', () => {
      expect(htmlContent).toMatch(/<section[^>]+id="architecture"[^>]+aria-labelledby="architecture-heading"/);
    });

    test('Architecture section has heading with matching id', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      // Should have h2 with id="architecture-heading"
      expect(architectureSection).toMatch(/<h2[^>]+id="architecture-heading"/);
    });

    test('Architecture section has proper heading hierarchy (h2 > h3 > h4)', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      // Main section heading should be h2
      expect(architectureSection).toMatch(/<h2/);

      // If there are component headings, they should be h3 or h4
      if (architectureSection.includes('component-title')) {
        // Component titles should be h4 (after h3 for components heading)
        expect(architectureSection).toMatch(/<h4[^>]+class="component-title"/);
      }
    });
  });

  describe('Architecture SVG accessibility', () => {
    let svgContent;

    beforeAll(() => {
      try {
        const svgPath = path.join(__dirname, '../../assets/images/architecture.svg');
        svgContent = fs.readFileSync(svgPath, 'utf8');
      } catch (e) {
        svgContent = null;
      }
    });

    test('Architecture SVG file exists', () => {
      expect(svgContent).toBeTruthy();
    });

    test('Architecture SVG has title element', () => {
      if (svgContent) {
        expect(svgContent).toMatch(/<title[^>]*>[^<]+<\/title>/);
      }
    });

    test('Architecture SVG has desc element for description', () => {
      if (svgContent) {
        expect(svgContent).toMatch(/<desc[^>]*>[^<]+<\/desc>/);
      }
    });

    test('Architecture SVG has role="img" attribute', () => {
      if (svgContent) {
        expect(svgContent).toMatch(/role="img"/);
      }
    });

    test('Architecture SVG has aria-labelledby for title and desc', () => {
      if (svgContent) {
        expect(svgContent).toMatch(/aria-labelledby="[^"]+"/);
      }
    });
  });

  describe('Architecture diagram loading attributes', () => {
    test('Architecture diagram has width attribute for layout stability', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      // Check for width attribute on img
      expect(architectureSection).toMatch(/<img[^>]+width="[^"]+"/);
    });

    test('Architecture diagram has height attribute for layout stability', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      // Check for height attribute on img
      expect(architectureSection).toMatch(/<img[^>]+height="[^"]+"/);
    });

    test('Architecture diagram has loading="lazy" attribute', () => {
      const architectureSectionMatch = htmlContent.match(/<section[^>]+id="architecture"[^>]*>[\s\S]*?<\/section>/);
      const architectureSection = architectureSectionMatch[0];

      // Check for loading attribute
      expect(architectureSection).toMatch(/loading="lazy"/);
    });
  });
});
