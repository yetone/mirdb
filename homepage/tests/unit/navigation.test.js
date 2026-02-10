/**
 * Navigation Unit Tests
 * Owner: Scenario 4 - Navigation and Resources
 *
 * Unit tests for navigation link attributes and structure.
 *
 * Expected test coverage:
 * - External links have proper security attributes
 * - Navigation structure follows expected pattern
 *
 * Requirements traced:
 * - REQ-4: Navigation to additional resources
 * - USR-4: Links navigate to correct resources
 */

const fs = require('fs');
const path = require('path');

describe('Navigation Unit Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  // Test Case 8: Verify external links open in new tab
  describe('External Link Attributes', () => {
    test('external links have target="_blank" attribute', () => {
      // Find all external links (https://)
      const externalLinkPattern = /<a\s+[^>]*href=["']https:\/\/[^"']+["'][^>]*>/gi;
      const matches = htmlContent.match(externalLinkPattern) || [];

      expect(matches.length).toBeGreaterThan(0);

      matches.forEach((linkTag) => {
        expect(linkTag).toMatch(/target=["']_blank["']/);
      });
    });

    test('external links have rel="noopener" or rel="noopener noreferrer"', () => {
      const externalLinkPattern = /<a\s+[^>]*href=["']https:\/\/[^"']+["'][^>]*>/gi;
      const matches = htmlContent.match(externalLinkPattern) || [];

      expect(matches.length).toBeGreaterThan(0);

      matches.forEach((linkTag) => {
        expect(linkTag).toMatch(/rel=["'][^"']*noopener[^"']*["']/);
      });
    });
  });

  describe('Navigation Structure', () => {
    test('header contains nav element', () => {
      expect(htmlContent).toMatch(/<header[\s\S]*?<nav[\s\S]*?<\/nav>[\s\S]*?<\/header>/i);
    });

    test('navigation contains Features, Quick Start, and Resources links', () => {
      expect(htmlContent).toMatch(/href=["']#features["']/i);
      expect(htmlContent).toMatch(/href=["']#quickstart["']/i);
      expect(htmlContent).toMatch(/href=["']#resources["']/i);
    });

    test('features section exists with id="features"', () => {
      expect(htmlContent).toMatch(/id=["']features["']/i);
    });

    test('quickstart section exists with id="quickstart"', () => {
      expect(htmlContent).toMatch(/id=["']quickstart["']/i);
    });

    test('resources section exists with id="resources"', () => {
      expect(htmlContent).toMatch(/id=["']resources["']/i);
    });
  });

  describe('GitHub Repository Link', () => {
    test('page contains GitHub repository link', () => {
      expect(htmlContent).toMatch(/github\.com\/yetone\/mirdb/i);
    });

    test('GitHub link is properly formatted', () => {
      expect(htmlContent).toMatch(/href=["']https:\/\/github\.com\/yetone\/mirdb["']/i);
    });
  });

  describe('Footer Content', () => {
    test('footer contains copyright information', () => {
      // Match footer section
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/i);
      expect(footerMatch).toBeTruthy();

      const footerContent = footerMatch[0];
      const hasCopyright = footerContent.includes('©') ||
                           footerContent.toLowerCase().includes('copyright');
      expect(hasCopyright).toBeTruthy();
    });

    test('footer contains license information', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/i);
      expect(footerMatch).toBeTruthy();

      const footerContent = footerMatch[0].toLowerCase();
      const hasLicense = footerContent.includes('license') || footerContent.includes('mit');
      expect(hasLicense).toBeTruthy();
    });

    test('footer has a link to license file', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/i);
      expect(footerMatch).toBeTruthy();

      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/href=["'][^"']*license[^"']*["']/i);
    });
  });

  describe('Resources Section', () => {
    test('resources section has GitHub link', () => {
      // Find resources section
      const resourcesMatch = htmlContent.match(/<!--\s*BEGIN:\s*Resources[\s\S]*?<!--\s*END:\s*Resources/i);
      expect(resourcesMatch).toBeTruthy();

      const resourcesContent = resourcesMatch[0];
      expect(resourcesContent).toMatch(/github\.com\/yetone\/mirdb/i);
    });

    test('resources section has documentation link', () => {
      const resourcesMatch = htmlContent.match(/<!--\s*BEGIN:\s*Resources[\s\S]*?<!--\s*END:\s*Resources/i);
      expect(resourcesMatch).toBeTruthy();

      const resourcesContent = resourcesMatch[0].toLowerCase();
      expect(resourcesContent).toMatch(/documentation|readme|docs/i);
    });
  });
});
