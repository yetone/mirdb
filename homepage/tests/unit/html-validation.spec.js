/**
 * HTML Validation and Structure Tests
 * Owner: Scenario 15 - HTML Validation and Semantic Structure
 *
 * Tests:
 * - DOCTYPE declaration
 * - HTML lang attribute
 * - Semantic elements (header, nav, main, footer)
 * - Single h1 element
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

const ROOT_DIR = path.join(__dirname, '../..');
const HTML_PATH = path.join(ROOT_DIR, 'index.html');

// Helper to read HTML content
function getHtmlContent() {
  return fs.readFileSync(HTML_PATH, 'utf-8');
}

test.describe('HTML Validation and Semantic Structure', () => {
  test.describe('Document Structure', () => {
    test('page has DOCTYPE html at the beginning', async () => {
      const html = getHtmlContent();

      // Remove any leading HTML comments and whitespace
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '').trim();

      // DOCTYPE should be at the very beginning (case-insensitive)
      expect(withoutComments.toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });

    test('html element has lang attribute set to en', async () => {
      const html = getHtmlContent();

      // Match <html with lang="en" or lang='en'
      const htmlTagMatch = html.match(/<html[^>]*>/i);
      expect(htmlTagMatch).not.toBeNull();

      const htmlTag = htmlTagMatch[0];
      // Check for lang="en" or lang='en'
      const hasLangEn = /lang\s*=\s*["']en["']/i.test(htmlTag);
      expect(hasLangEn).toBe(true);
    });
  });

  test.describe('Semantic Elements', () => {
    test('page uses semantic header element', async () => {
      const html = getHtmlContent();

      // Check for <header> element (not inside a comment)
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '');
      const hasHeader = /<header[^>]*>/i.test(withoutComments);
      expect(hasHeader).toBe(true);

      // Ensure it's closed properly
      const hasClosingHeader = /<\/header>/i.test(withoutComments);
      expect(hasClosingHeader).toBe(true);
    });

    test('navigation uses semantic nav element', async () => {
      const html = getHtmlContent();

      // Check for <nav> element
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '');
      const hasNav = /<nav[^>]*>/i.test(withoutComments);
      expect(hasNav).toBe(true);

      // Ensure it's closed properly
      const hasClosingNav = /<\/nav>/i.test(withoutComments);
      expect(hasClosingNav).toBe(true);
    });

    test('page has exactly one main element', async () => {
      const html = getHtmlContent();

      // Check for <main> element
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '');

      // Count occurrences of <main> opening tags
      const mainMatches = withoutComments.match(/<main[^>]*>/gi) || [];
      expect(mainMatches.length).toBe(1);

      // Ensure it's closed properly
      const hasClosingMain = /<\/main>/i.test(withoutComments);
      expect(hasClosingMain).toBe(true);
    });

    test('page uses semantic footer element', async () => {
      const html = getHtmlContent();

      // Check for <footer> element
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '');
      const hasFooter = /<footer[^>]*>/i.test(withoutComments);
      expect(hasFooter).toBe(true);

      // Ensure it's closed properly
      const hasClosingFooter = /<\/footer>/i.test(withoutComments);
      expect(hasClosingFooter).toBe(true);
    });
  });

  test.describe('Content Structure', () => {
    test('page has exactly one h1 element', async () => {
      const html = getHtmlContent();

      // Remove comments to avoid matching commented-out h1 tags
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '');

      // Count h1 tags
      const h1Matches = withoutComments.match(/<h1[^>]*>/gi) || [];
      expect(h1Matches.length).toBe(1);
    });

    test('page has proper head section', async () => {
      const html = getHtmlContent();

      // Check for <head> and </head>
      const hasHead = /<head[^>]*>/i.test(html);
      const hasClosingHead = /<\/head>/i.test(html);

      expect(hasHead).toBe(true);
      expect(hasClosingHead).toBe(true);
    });

    test('page has body element', async () => {
      const html = getHtmlContent();

      // Check for <body> and </body>
      const hasBody = /<body[^>]*>/i.test(html);
      const hasClosingBody = /<\/body>/i.test(html);

      expect(hasBody).toBe(true);
      expect(hasClosingBody).toBe(true);
    });

    test('page has charset declaration', async () => {
      const html = getHtmlContent();

      // Check for <meta charset="UTF-8"> or similar
      const hasCharset = /<meta[^>]*charset\s*=\s*["']?UTF-8["']?[^>]*>/i.test(html);
      expect(hasCharset).toBe(true);
    });

    test('page has viewport meta tag', async () => {
      const html = getHtmlContent();

      // Check for viewport meta tag
      const hasViewport = /<meta[^>]*name\s*=\s*["']viewport["'][^>]*>/i.test(html);
      expect(hasViewport).toBe(true);
    });
  });

  test.describe('Semantic Sections', () => {
    test('main content sections use section elements', async () => {
      const html = getHtmlContent();

      // Remove comments
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '');

      // Check for section elements within main
      const mainMatch = withoutComments.match(/<main[^>]*>([\s\S]*?)<\/main>/i);
      expect(mainMatch).not.toBeNull();

      const mainContent = mainMatch[1];
      const hasSections = /<section[^>]*>/i.test(mainContent);
      expect(hasSections).toBe(true);
    });

    test('sections have id attributes for navigation', async () => {
      const html = getHtmlContent();

      // Remove comments
      const withoutComments = html.replace(/<!--[\s\S]*?-->/g, '');

      // Find all section tags and check they have ids
      const sectionMatches = withoutComments.match(/<section[^>]*>/gi) || [];

      // At least some sections should have id attributes for anchor navigation
      const sectionsWithIds = sectionMatches.filter(tag => /\bid\s*=/i.test(tag));
      expect(sectionsWithIds.length).toBeGreaterThan(0);
    });
  });

  test.describe('HTML Closing Tags', () => {
    test('html element is properly closed', async () => {
      const html = getHtmlContent();

      const hasClosingHtml = /<\/html>/i.test(html);
      expect(hasClosingHtml).toBe(true);
    });

    test('document ends with closing html tag', async () => {
      const html = getHtmlContent().trim();

      // Document should end with </html>
      expect(html.toLowerCase().endsWith('</html>')).toBe(true);
    });
  });
});
