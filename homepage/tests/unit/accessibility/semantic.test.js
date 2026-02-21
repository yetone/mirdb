/**
 * Accessibility - Screen Reader Support Tests
 * Owner: Scenario 13 - Accessibility - Screen Reader Support
 *
 * Tests semantic HTML structure, heading hierarchy, and alt text
 * to verify the page is accessible to screen reader users.
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility - Screen Reader Support', () => {
  let htmlContent;
  let architectureHtml;

  beforeAll(() => {
    // Load the built HTML files
    const htmlPath = path.join(__dirname, '../../../book/index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Load architecture page if it exists
    try {
      const architectureHtmlPath = path.join(__dirname, '../../../book/architecture.html');
      architectureHtml = fs.readFileSync(architectureHtmlPath, 'utf-8');
    } catch (e) {
      architectureHtml = null;
    }
  });

  // Test Case 1: Page contains semantic header element
  describe('Semantic HTML Structure', () => {
    test('Page contains semantic header element', () => {
      // Check for <header> tag
      expect(htmlContent).toMatch(/<header[^>]*>/);
      expect(htmlContent).toMatch(/<\/header>/);
    });

    // Test Case 2: Page contains semantic nav element
    test('Page contains semantic nav element', () => {
      // Check for <nav> tag with aria-label
      expect(htmlContent).toMatch(/<nav[^>]*>/);
      expect(htmlContent).toMatch(/<nav[^>]*aria-label="[^"]+"/);
      expect(htmlContent).toMatch(/<\/nav>/);
    });

    // Test Case 3: Page contains semantic main element
    test('Page contains semantic main element', () => {
      // Check for <main> tag
      expect(htmlContent).toMatch(/<main[^>]*>/);
      expect(htmlContent).toMatch(/<\/main>/);
    });

    // Test Case 4: Page contains semantic footer element
    test('Page contains semantic footer element', () => {
      // Check for <footer> tag
      expect(htmlContent).toMatch(/<footer[^>]*>/);
      expect(htmlContent).toMatch(/<\/footer>/);
    });
  });

  describe('Heading Hierarchy', () => {
    // Test Case 5: Page has exactly one h1 element
    test('Page has exactly one h1 element', () => {
      // Count h1 elements using regex
      const h1Matches = htmlContent.match(/<h1[^>]*>/g);
      expect(h1Matches).not.toBeNull();
      expect(h1Matches.length).toBe(1);
    });

    // Test Case 6: Headings follow sequential order without skipping levels
    test('Headings follow sequential order without skipping levels', () => {
      // Extract all headings in order
      const headingPattern = /<h([1-6])[^>]*>/g;
      const headings = [];
      let match;

      while ((match = headingPattern.exec(htmlContent)) !== null) {
        headings.push(parseInt(match[1]));
      }

      // Verify there are headings
      expect(headings.length).toBeGreaterThan(0);

      // Check for proper hierarchy (no skipping levels when going deeper)
      for (let i = 1; i < headings.length; i++) {
        const currentLevel = headings[i];
        const previousLevel = headings[i - 1];

        // When going deeper, should only go one level at a time
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });
  });

  describe('Image Alt Text', () => {
    // Test Case 7: Logo image has alt text describing MirDB logo
    test('Logo image has alt text describing MirDB logo', () => {
      // Check for header logo with alt text containing MirDB and Logo
      const headerLogoPattern = /<img[^>]*class="[^"]*header-logo-img[^"]*"[^>]*>/;
      const headerLogoMatch = htmlContent.match(headerLogoPattern);
      expect(headerLogoMatch).not.toBeNull();
      expect(headerLogoMatch[0]).toMatch(/alt="[^"]*MirDB[^"]*"/i);
      expect(headerLogoMatch[0]).toMatch(/alt="[^"]*Logo[^"]*"/i);

      // Check for hero logo with alt text
      const heroLogoPattern = /<img[^>]*class="[^"]*hero-logo[^"]*"[^>]*>/;
      const heroLogoMatch = htmlContent.match(heroLogoPattern);
      expect(heroLogoMatch).not.toBeNull();
      expect(heroLogoMatch[0]).toMatch(/alt="[^"]*MirDB[^"]*"/i);
      expect(heroLogoMatch[0]).toMatch(/alt="[^"]*Logo[^"]*"/i);
    });

    // Test Case 8: Architecture diagram has descriptive alt text
    test('Architecture diagram has descriptive alt text', () => {
      // The architecture diagram should be on the architecture page
      expect(architectureHtml).not.toBeNull();

      // Check for architecture diagram image with descriptive alt text
      // The img element may have alt before or after class, so check for both patterns
      const hasArchitectureDiagram = architectureHtml.includes('class="architecture-diagram"') ||
                                      architectureHtml.includes("class='architecture-diagram'");
      expect(hasArchitectureDiagram).toBe(true);

      // Extract alt text from the img with architecture-diagram class
      // Pattern handles alt before or after class attribute
      const altPattern = /<img[^>]*alt="([^"]*)"[^>]*class="[^"]*architecture-diagram[^"]*"[^>]*>|<img[^>]*class="[^"]*architecture-diagram[^"]*"[^>]*alt="([^"]*)"[^>]*>/i;
      const match = architectureHtml.match(altPattern);

      expect(match).not.toBeNull();

      // Extract alt text (could be in first or second capture group)
      const altText = (match[1] || match[2]).toLowerCase();

      // Alt text should be descriptive (more than just "diagram")
      expect(altText.length).toBeGreaterThan(20);

      // Should contain meaningful description
      expect(
        altText.includes('architecture') ||
        altText.includes('lsm') ||
        altText.includes('mirdb') ||
        altText.includes('data flow')
      ).toBe(true);
    });
  });

  describe('Additional Screen Reader Support', () => {
    test('Skip navigation link exists', () => {
      // Check for skip navigation link
      expect(htmlContent).toMatch(/<a[^>]*href="#content"[^>]*class="[^"]*skip-nav[^"]*"[^>]*>/i);
    });

    test('Mobile menu toggle has proper aria attributes', () => {
      // Check mobile menu toggle has aria-label and aria-expanded
      expect(htmlContent).toMatch(/<button[^>]*class="[^"]*mobile-menu-toggle[^"]*"[^>]*aria-label="[^"]+"[^>]*>/);
      expect(htmlContent).toMatch(/<button[^>]*class="[^"]*mobile-menu-toggle[^"]*"[^>]*aria-expanded="[^"]+"[^>]*>/);
    });

    test('External links have aria labels indicating new tab', () => {
      // External links should have aria-label mentioning "opens in new tab"
      const externalLinkPattern = /<a[^>]*target="_blank"[^>]*aria-label="[^"]*\(opens in new tab\)[^"]*"[^>]*>/gi;
      const matches = htmlContent.match(externalLinkPattern);

      // Should have at least one external link with proper aria-label
      expect(matches).not.toBeNull();
      expect(matches.length).toBeGreaterThan(0);
    });

    test('Page language is specified', () => {
      // Check html element has lang attribute set to "en"
      expect(htmlContent).toMatch(/<html[^>]*lang="en"[^>]*>/);
    });

    test('Content sections use semantic section elements with IDs', () => {
      // Check major sections exist with IDs
      expect(htmlContent).toMatch(/<section[^>]*id="hero"[^>]*>/);
      expect(htmlContent).toMatch(/<section[^>]*id="features"[^>]*>/);
      expect(htmlContent).toMatch(/<section[^>]*id="quickstart"[^>]*>/);
      expect(htmlContent).toMatch(/<section[^>]*id="roadmap"[^>]*>/);
    });

    test('Navigation has aria-label for screen readers', () => {
      // Navigation should have aria-label
      expect(htmlContent).toMatch(/<nav[^>]*aria-label="Main navigation"[^>]*>/);
    });

    test('Decorative icons are hidden from screen readers', () => {
      // Check that decorative icons (like external link arrows) have aria-hidden
      expect(htmlContent).toMatch(/<span[^>]*class="[^"]*external-icon[^"]*"[^>]*aria-hidden="true"[^>]*>/);
    });
  });
});
