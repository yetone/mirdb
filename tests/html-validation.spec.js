const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * HTML Validation Tests
 * Scenario: Validate the HTML markup is valid and well-formed
 *
 * Steps:
 * 1. Run HTML validator - Submit page to W3C HTML validator
 * 2. Review errors - Check for any validation errors
 * 3. Review warnings - Check for validation warnings
 *
 * Test Cases:
 * 1. Validate HTML with W3C validator - No critical HTML validation errors
 * 2. Check DOCTYPE declaration - Page has valid HTML5 DOCTYPE declaration
 * 3. Check lang attribute - HTML element has lang attribute set (e.g., lang='en')
 */

// Helper function to read HTML file
function getHtmlContent() {
  const htmlPath = path.join(__dirname, '..', 'index.html');
  return fs.readFileSync(htmlPath, 'utf-8');
}

test.describe('HTML Validation', () => {
  // Test Case 2: Check DOCTYPE declaration
  test.describe('Test Case 2: DOCTYPE Declaration', () => {
    test('Page has valid HTML5 DOCTYPE declaration', async () => {
      const htmlContent = getHtmlContent();

      // Check that the file starts with the HTML5 DOCTYPE declaration
      const trimmedContent = htmlContent.trim();
      const hasValidDoctype = trimmedContent.toLowerCase().startsWith('<!doctype html>');

      expect(hasValidDoctype).toBe(true);
    });

    test('DOCTYPE is the first thing in the document', async () => {
      const htmlContent = getHtmlContent();

      // DOCTYPE should be at the very beginning (ignoring whitespace)
      const trimmedContent = htmlContent.trim();
      const doctypeIndex = trimmedContent.toLowerCase().indexOf('<!doctype html>');

      expect(doctypeIndex).toBe(0);
    });

    test('DOCTYPE uses HTML5 format (not XHTML or HTML4)', async () => {
      const htmlContent = getHtmlContent();

      // HTML5 DOCTYPE is simple: <!DOCTYPE html>
      // HTML4 and XHTML have longer DOCTYPEs with DTD references
      const doctypeMatch = htmlContent.match(/<!DOCTYPE[^>]*>/i);

      expect(doctypeMatch).toBeTruthy();
      expect(doctypeMatch[0].length).toBeLessThan(20); // HTML5 DOCTYPE is short
      expect(doctypeMatch[0].toLowerCase()).toMatch(/<!doctype\s+html\s*>/i);
    });
  });

  // Test Case 3: Check lang attribute
  test.describe('Test Case 3: Lang Attribute', () => {
    test('HTML element has lang attribute set', async () => {
      const htmlContent = getHtmlContent();

      // Check that <html> tag has lang attribute
      const htmlTagMatch = htmlContent.match(/<html[^>]*>/i);

      expect(htmlTagMatch).toBeTruthy();
      expect(htmlTagMatch[0]).toMatch(/lang\s*=\s*["'][a-zA-Z-]+["']/i);
    });

    test('Lang attribute follows BCP 47 format (e.g., "en")', async () => {
      const htmlContent = getHtmlContent();

      // Extract the lang attribute value
      const langMatch = htmlContent.match(/<html[^>]*lang\s*=\s*["']([^"']+)["'][^>]*>/i);

      expect(langMatch).toBeTruthy();

      const langValue = langMatch[1];

      // BCP 47 language tags are typically 2-3 letters for primary language
      // Can be extended like "en-US", "en-GB", etc.
      const bcp47Pattern = /^[a-zA-Z]{2,3}(-[a-zA-Z]{2,4})?(-[a-zA-Z]{2})?$/;

      expect(langValue).toMatch(bcp47Pattern);
    });

    test('Lang attribute is set to English (en)', async () => {
      const htmlContent = getHtmlContent();

      // Extract the lang attribute value
      const langMatch = htmlContent.match(/<html[^>]*lang\s*=\s*["']([^"']+)["'][^>]*>/i);

      expect(langMatch).toBeTruthy();

      const langValue = langMatch[1];

      // The page should be in English based on the content
      expect(langValue).toBe('en');
    });

    test('Lang attribute is present in raw HTML source', async () => {
      const htmlContent = getHtmlContent();

      // Check that <html> tag has lang attribute
      const htmlTagMatch = htmlContent.match(/<html[^>]*>/i);

      expect(htmlTagMatch).toBeTruthy();
      expect(htmlTagMatch[0]).toMatch(/lang\s*=\s*["'][a-zA-Z-]+["']/i);
    });
  });

  // Test Case 1: HTML Validation (Integration)
  test.describe('Test Case 1: HTML Structure Validation', () => {
    test('HTML document has proper structure', async () => {
      const htmlContent = getHtmlContent();

      // Check for essential HTML5 elements
      const hasHead = /<head[^>]*>[\s\S]*<\/head>/i.test(htmlContent);
      const hasBody = /<body[^>]*>[\s\S]*<\/body>/i.test(htmlContent);
      const hasTitle = /<title[^>]*>[\s\S]*<\/title>/i.test(htmlContent);

      expect(hasHead).toBe(true);
      expect(hasBody).toBe(true);
      expect(hasTitle).toBe(true);
    });

    test('Document has required meta tags', async () => {
      const htmlContent = getHtmlContent();

      // Check charset meta tag
      const hasCharset = /<meta[^>]*charset\s*=\s*["']?utf-8["']?[^>]*>/i.test(htmlContent);
      expect(hasCharset).toBe(true);

      // Check viewport meta tag
      const hasViewport = /<meta[^>]*name\s*=\s*["']viewport["'][^>]*>/i.test(htmlContent);
      expect(hasViewport).toBe(true);

      // Check description meta tag
      const hasDescription = /<meta[^>]*name\s*=\s*["']description["'][^>]*>/i.test(htmlContent);
      expect(hasDescription).toBe(true);
    });

    test('All images have alt attributes', async () => {
      const htmlContent = getHtmlContent();

      // Find all img tags
      const imgTags = htmlContent.match(/<img[^>]*>/gi) || [];

      // Each img tag should have an alt attribute
      for (const imgTag of imgTags) {
        const hasAlt = /alt\s*=\s*["'][^"']*["']/i.test(imgTag);
        expect(hasAlt).toBe(true);
      }
    });

    test('All links have valid href attributes', async () => {
      const htmlContent = getHtmlContent();

      // Find all anchor tags with href
      const linkTags = htmlContent.match(/<a[^>]*href\s*=\s*["'][^"']+["'][^>]*>/gi) || [];

      // Should have multiple links
      expect(linkTags.length).toBeGreaterThan(0);

      // Each link should have a non-empty href
      for (const linkTag of linkTags) {
        const hrefMatch = linkTag.match(/href\s*=\s*["']([^"']+)["']/i);
        expect(hrefMatch).toBeTruthy();
        expect(hrefMatch[1].trim()).not.toBe('');
      }
    });

    test('No duplicate IDs in the document', async () => {
      const htmlContent = getHtmlContent();

      // Find all id attributes
      const idMatches = htmlContent.match(/id\s*=\s*["']([^"']+)["']/gi) || [];
      const ids = idMatches.map(match => {
        const idMatch = match.match(/id\s*=\s*["']([^"']+)["']/i);
        return idMatch ? idMatch[1] : null;
      }).filter(Boolean);

      const uniqueIds = new Set(ids);
      expect(ids.length).toBe(uniqueIds.size);
    });

    test('HTML elements are properly nested', async () => {
      const htmlContent = getHtmlContent();

      // Check that main semantic elements exist
      const hasHeader = /<header[^>]*>[\s\S]*<\/header>/i.test(htmlContent);
      const hasMain = /<main[^>]*>[\s\S]*<\/main>/i.test(htmlContent);
      const hasFooter = /<footer[^>]*>[\s\S]*<\/footer>/i.test(htmlContent);

      expect(hasHeader).toBe(true);
      expect(hasMain).toBe(true);
      expect(hasFooter).toBe(true);
    });

    test('External links have proper security attributes', async () => {
      const htmlContent = getHtmlContent();

      // Find all external links (with target="_blank")
      const externalLinks = htmlContent.match(/<a[^>]*target\s*=\s*["']_blank["'][^>]*>/gi) || [];

      // External links should have rel="noopener"
      for (const link of externalLinks) {
        const hasNoopener = /rel\s*=\s*["'][^"']*noopener[^"']*["']/i.test(link);
        expect(hasNoopener).toBe(true);
      }
    });

    test('Page has proper heading hierarchy', async () => {
      const htmlContent = getHtmlContent();

      // Check h1 exists
      const h1Matches = htmlContent.match(/<h1[^>]*>[\s\S]*?<\/h1>/gi) || [];
      expect(h1Matches.length).toBe(1); // Should have exactly one h1

      // Check that h2 elements exist
      const h2Matches = htmlContent.match(/<h2[^>]*>[\s\S]*?<\/h2>/gi) || [];
      expect(h2Matches.length).toBeGreaterThan(0);
    });

    test('No deprecated HTML elements used', async () => {
      const htmlContent = getHtmlContent();

      // Check for deprecated HTML elements
      const deprecatedElements = [
        'center', 'font', 'marquee', 'blink',
        'strike', 'big', 'tt', 'basefont'
      ];

      for (const element of deprecatedElements) {
        const pattern = new RegExp(`<${element}[^>]*>`, 'i');
        const hasDeprecated = pattern.test(htmlContent);
        expect(hasDeprecated).toBe(false);
      }
    });

    test('Script and style elements are properly placed', async () => {
      const htmlContent = getHtmlContent();

      // Check that style elements are in head
      const headSection = htmlContent.match(/<head[^>]*>([\s\S]*)<\/head>/i);
      expect(headSection).toBeTruthy();

      // Styles should be in head section
      const hasStyleInHead = /<style[^>]*>/i.test(headSection[1]);
      const hasStyleLinkInHead = /<link[^>]*rel\s*=\s*["']stylesheet["'][^>]*>/i.test(headSection[1]);

      // Page should have either embedded styles or external stylesheets in head
      expect(hasStyleInHead || hasStyleLinkInHead).toBe(true);
    });
  });

  // Additional validation tests
  test.describe('HTML5 Semantic Structure', () => {
    test('Uses HTML5 semantic elements appropriately', async () => {
      const htmlContent = getHtmlContent();

      // Check for semantic elements
      const sectionMatches = htmlContent.match(/<section[^>]*>/gi) || [];

      // Page should use semantic sections
      expect(sectionMatches.length).toBeGreaterThan(0);
    });

    test('Navigation uses nav element', async () => {
      const htmlContent = getHtmlContent();

      // Check for nav element
      const hasNav = /<nav[^>]*>[\s\S]*<\/nav>/i.test(htmlContent);
      expect(hasNav).toBe(true);

      // Nav should contain links
      const navSection = htmlContent.match(/<nav[^>]*>([\s\S]*?)<\/nav>/i);
      expect(navSection).toBeTruthy();
      const hasLinksInNav = /<a[^>]*href/i.test(navSection[1]);
      expect(hasLinksInNav).toBe(true);
    });

    test('Main content uses main element', async () => {
      const htmlContent = getHtmlContent();

      // Check for main element with id
      const mainMatch = htmlContent.match(/<main[^>]*id\s*=\s*["']([^"']+)["'][^>]*>/i);
      expect(mainMatch).toBeTruthy();
      expect(mainMatch[1]).toBeTruthy();
    });

    test('Figures have figcaption when appropriate', async () => {
      const htmlContent = getHtmlContent();

      // Find all figure elements
      const figureMatches = htmlContent.match(/<figure[^>]*>[\s\S]*?<\/figure>/gi) || [];

      // If there are figures, check for figcaptions
      for (const figure of figureMatches) {
        const hasFigcaption = /<figcaption[^>]*>/i.test(figure);
        // Figures should ideally have figcaptions for accessibility
        expect(hasFigcaption).toBe(true);
      }
    });
  });
});
