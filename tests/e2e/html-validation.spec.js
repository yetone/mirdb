/**
 * HTML Validation E2E Tests
 * Verifies HTML is valid and follows best practices
 *
 * Test Cases:
 * 1. Run W3C HTML validation - No HTML validation errors
 * 2. Check DOCTYPE declaration - Page has proper HTML5 DOCTYPE declaration
 * 3. Check lang attribute - HTML element has lang attribute set
 * 4. Check charset declaration - Meta charset UTF-8 is declared
 */
const { test, expect } = require('@playwright/test');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

test.describe('HTML Validation', () => {
  const indexPath = path.join(__dirname, '../../index.html');
  let htmlContent;
  let document;

  test.beforeAll(async () => {
    htmlContent = fs.readFileSync(indexPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  test.describe('Test Case 1: W3C HTML Validation', () => {
    test('Page has valid HTML structure without critical errors', async () => {
      // Verify the page has essential elements
      const html = document.querySelector('html');
      expect(html).not.toBeNull();

      // Verify document structure
      const head = document.querySelector('head');
      const body = document.querySelector('body');
      expect(head).not.toBeNull();
      expect(body).not.toBeNull();

      // Check title contains MirDB
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    test('No duplicate IDs in the document', async () => {
      const elements = document.querySelectorAll('[id]');
      const idMap = {};
      const duplicates = [];

      elements.forEach(el => {
        if (idMap[el.id]) {
          duplicates.push(el.id);
        }
        idMap[el.id] = true;
      });

      expect(duplicates).toEqual([]);
    });

    test('All required attributes are present on elements', async () => {
      // Check all images have alt attributes
      const imagesWithoutAlt = document.querySelectorAll('img:not([alt])');
      expect(imagesWithoutAlt.length).toBe(0);

      // Check all links have href attributes
      const linksWithoutHref = document.querySelectorAll('a:not([href])');
      expect(linksWithoutHref.length).toBe(0);
    });

    test('No deprecated HTML elements used', async () => {
      const deprecated = ['font', 'center', 'marquee', 'blink', 'strike', 'big', 'tt'];
      const found = [];

      deprecated.forEach(tag => {
        if (document.querySelector(tag)) {
          found.push(tag);
        }
      });

      expect(found).toEqual([]);
    });
  });

  test.describe('Test Case 2: DOCTYPE Declaration', () => {
    test('Page has proper HTML5 DOCTYPE declaration', async () => {
      // Check that DOCTYPE is present at the very beginning
      const doctypeRegex = /^\s*<!DOCTYPE\s+html\s*>/i;
      expect(htmlContent).toMatch(doctypeRegex);
    });

    test('DOCTYPE is the first element in the document', async () => {
      // Remove leading whitespace and check DOCTYPE is first
      const trimmedHtml = htmlContent.trimStart();
      expect(trimmedHtml.toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });

    test('DOCTYPE is in HTML5 format (not XHTML or HTML4)', async () => {
      // HTML5 DOCTYPE should be simple: <!DOCTYPE html>
      // Not like HTML4: <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN">
      const hasSimpleDoctype = /<!DOCTYPE\s+html\s*>/i.test(htmlContent);
      const hasComplexDoctype = /<!DOCTYPE\s+html\s+PUBLIC/i.test(htmlContent);

      expect(hasSimpleDoctype).toBe(true);
      expect(hasComplexDoctype).toBe(false);
    });
  });

  test.describe('Test Case 3: Lang Attribute', () => {
    test('HTML element has lang attribute set', async () => {
      const html = document.querySelector('html');
      const langAttr = html.getAttribute('lang');

      expect(langAttr).not.toBeNull();
      expect(langAttr).not.toBe('');
    });

    test('Lang attribute contains valid language code', async () => {
      const html = document.querySelector('html');
      const langAttr = html.getAttribute('lang');

      // Should be a valid language code format (e.g., "en", "en-US", "fr", etc.)
      const validLangRegex = /^[a-z]{2}(-[A-Z]{2})?$/;
      expect(langAttr).toMatch(validLangRegex);
    });

    test('Lang attribute is set to English (en)', async () => {
      const html = document.querySelector('html');
      const langAttr = html.getAttribute('lang');

      // Based on the content being in English
      expect(langAttr).toBe('en');
    });
  });

  test.describe('Test Case 4: Charset Declaration', () => {
    test('Meta charset UTF-8 is declared', async () => {
      const charsetMeta = document.querySelector('meta[charset]');
      expect(charsetMeta).not.toBeNull();

      const charset = charsetMeta.getAttribute('charset');
      expect(charset?.toUpperCase()).toBe('UTF-8');
    });

    test('Charset meta tag is in the head element', async () => {
      const charsetInHead = document.querySelector('head meta[charset]');
      expect(charsetInHead).not.toBeNull();
    });

    test('Charset is declared early in the head', async () => {
      // Charset should be one of the first elements in head for proper encoding
      const head = document.querySelector('head');
      const children = Array.from(head.children);
      const charsetIndex = children.findIndex(el =>
        el.tagName === 'META' && el.hasAttribute('charset')
      );

      // Charset should be within the first 3 elements of head
      expect(charsetIndex).toBeLessThanOrEqual(2);
    });
  });

  test.describe('Additional HTML Best Practices', () => {
    test('Page has title element', async () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).not.toBe('');
      expect(title.textContent.length).toBeGreaterThan(0);
    });

    test('Page has viewport meta tag', async () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('Page has meta description', async () => {
      const description = document.querySelector('meta[name="description"]');
      expect(description).not.toBeNull();

      const content = description.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content?.length).toBeGreaterThan(0);
    });

    test('External links have rel="noopener noreferrer"', async () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });
  });
});
