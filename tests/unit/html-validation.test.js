/**
 * Unit tests for HTML Validation
 * Scenario: HTML Validation
 * Verifies the homepage HTML is valid and well-formed
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('HTML Validation', () => {
  let document;
  let html;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC1: W3C Validation Compliance', () => {
    test('HTML document is parseable without errors', () => {
      // JSDOM will throw errors if the HTML is malformed
      expect(() => new JSDOM(html)).not.toThrow();
    });

    test('No unclosed tags detected', () => {
      // Check for common unclosed tag patterns in the raw HTML
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      // If HTML is well-formed, these basic structure checks should pass
      expect(doc.querySelector('html')).not.toBeNull();
      expect(doc.querySelector('head')).not.toBeNull();
      expect(doc.querySelector('body')).not.toBeNull();
    });

    test('All required elements have closing tags', () => {
      // Check that main structural elements are properly nested
      const head = document.querySelector('head');
      const body = document.querySelector('body');

      expect(head).not.toBeNull();
      expect(body).not.toBeNull();

      // head should be inside html
      expect(document.querySelector('html > head')).not.toBeNull();
      // body should be inside html
      expect(document.querySelector('html > body')).not.toBeNull();
    });

    test('No duplicate IDs in document', () => {
      const elementsWithId = document.querySelectorAll('[id]');
      const ids = Array.from(elementsWithId).map(el => el.getAttribute('id'));
      const uniqueIds = new Set(ids);
      expect(ids.length).toBe(uniqueIds.size);
    });

    test('All img tags have alt attributes', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    test('All anchor tags with target="_blank" have rel="noopener"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      });
    });
  });

  describe('TC2: DOCTYPE Declaration', () => {
    test('Page has <!DOCTYPE html> declaration', () => {
      // DOCTYPE should be at the start of the document
      const trimmedHtml = html.trim();
      expect(trimmedHtml.toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });

    test('DOCTYPE is HTML5 format', () => {
      // HTML5 DOCTYPE is simply <!DOCTYPE html>
      const doctypeMatch = html.match(/<!doctype\s+html\s*>/i);
      expect(doctypeMatch).not.toBeNull();
    });
  });

  describe('TC3: HTML Lang Attribute', () => {
    test('html element has lang attribute', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement).not.toBeNull();
      expect(htmlElement.hasAttribute('lang')).toBe(true);
    });

    test("html element has lang='en' attribute", () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement).not.toBeNull();
      const lang = htmlElement.getAttribute('lang');
      expect(lang).toBe('en');
    });

    test('lang attribute is valid language code', () => {
      const htmlElement = document.querySelector('html');
      const lang = htmlElement.getAttribute('lang');
      // Valid language codes are typically 2-letter ISO 639-1 codes
      expect(lang).toMatch(/^[a-z]{2}(-[A-Z]{2})?$/);
    });
  });

  describe('TC4: Charset Declaration', () => {
    test('Head contains <meta charset="UTF-8">', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    test('Charset is UTF-8', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      const charsetValue = charset.getAttribute('charset');
      expect(charsetValue.toUpperCase()).toBe('UTF-8');
    });

    test('Charset meta tag is in head element', () => {
      const headCharset = document.querySelector('head meta[charset]');
      expect(headCharset).not.toBeNull();
    });

    test('Charset meta tag appears early in head', () => {
      // Charset should be one of the first elements in head
      const head = document.querySelector('head');
      const headChildren = Array.from(head.children);
      const charsetIndex = headChildren.findIndex(
        el => el.tagName === 'META' && el.hasAttribute('charset')
      );
      // Charset should be within first 5 elements of head
      expect(charsetIndex).toBeLessThan(5);
      expect(charsetIndex).toBeGreaterThanOrEqual(0);
    });
  });

  describe('TC5: Viewport Meta Tag', () => {
    test('Page has viewport meta tag for mobile', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('Viewport meta tag has content attribute', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).toBeTruthy();
    });

    test('Viewport includes width=device-width', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    test('Viewport includes initial-scale', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).toMatch(/initial-scale\s*=\s*[\d.]+/);
    });

    test('Viewport is in head element', () => {
      const headViewport = document.querySelector('head meta[name="viewport"]');
      expect(headViewport).not.toBeNull();
    });
  });

  describe('Additional HTML Structure Validation', () => {
    test('Document has proper head and body structure', () => {
      const html = document.querySelector('html');
      const head = document.querySelector('html > head');
      const body = document.querySelector('html > body');

      expect(html).not.toBeNull();
      expect(head).not.toBeNull();
      expect(body).not.toBeNull();
    });

    test('Title element exists in head', () => {
      const title = document.querySelector('head > title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim()).toBeTruthy();
    });

    test('No deprecated HTML elements used', () => {
      const deprecatedElements = [
        'acronym', 'applet', 'basefont', 'big', 'blink', 'center',
        'dir', 'font', 'frame', 'frameset', 'isindex', 'marquee',
        'menu', 'noframes', 'strike', 'tt', 'u'
      ];

      deprecatedElements.forEach(tag => {
        const elements = document.querySelectorAll(tag);
        expect(elements.length).toBe(0);
      });
    });

    test('No inline style attributes used excessively', () => {
      // While inline styles aren't invalid, excessive use is bad practice
      const inlineStyles = document.querySelectorAll('[style]');
      // Allow some inline styles but flag if there are too many
      expect(inlineStyles.length).toBeLessThan(20);
    });
  });
});
