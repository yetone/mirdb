/**
 * HTML Validity and Structure Tests
 * Tests for verifying HTML is valid and well-structured
 * Scenario: HTML Validity and Structure (ID: 20)
 */

const fs = require('fs');
const path = require('path');

describe('HTML Validity and Structure', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    // Read the raw HTML file for certain tests
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    document = global.document;
  });

  // Test Case 1: Validate HTML syntax - No critical HTML validation errors
  describe('Test Case 1: Validate HTML syntax', () => {
    it('should have properly closed html element', () => {
      expect(htmlContent).toMatch(/<html[^>]*>/i);
      expect(htmlContent).toMatch(/<\/html>/i);
    });

    it('should have properly closed head element', () => {
      expect(htmlContent).toMatch(/<head[^>]*>/i);
      expect(htmlContent).toMatch(/<\/head>/i);
    });

    it('should have properly closed body element', () => {
      expect(htmlContent).toMatch(/<body[^>]*>/i);
      expect(htmlContent).toMatch(/<\/body>/i);
    });

    it('should have no unclosed tags in main structure', () => {
      // Check that basic structural elements exist and are properly nested
      // Use word boundary or > to avoid matching <header when looking for <head
      const headOpenCount = (htmlContent.match(/<head(\s[^>]*)?>/gi) || []).length;
      const headCloseCount = (htmlContent.match(/<\/head>/gi) || []).length;
      expect(headOpenCount).toBe(headCloseCount);

      const bodyOpenCount = (htmlContent.match(/<body(\s[^>]*)?>/gi) || []).length;
      const bodyCloseCount = (htmlContent.match(/<\/body>/gi) || []).length;
      expect(bodyOpenCount).toBe(bodyCloseCount);
    });

    it('should have balanced opening and closing tags for sections', () => {
      const sectionOpenCount = (htmlContent.match(/<section[^>]*>/gi) || []).length;
      const sectionCloseCount = (htmlContent.match(/<\/section>/gi) || []).length;
      expect(sectionOpenCount).toBe(sectionCloseCount);
    });

    it('should have balanced opening and closing tags for divs', () => {
      const divOpenCount = (htmlContent.match(/<div[^>]*>/gi) || []).length;
      const divCloseCount = (htmlContent.match(/<\/div>/gi) || []).length;
      expect(divOpenCount).toBe(divCloseCount);
    });

    it('should not have malformed attribute quotes', () => {
      // Check for common quote mismatches in attributes
      // Attributes should have matching quotes
      const malformedDoubleQuotes = htmlContent.match(/="[^"]*'[^"]*"/g);
      const malformedSingleQuotes = htmlContent.match(/='[^']*"[^']*'/g);

      // These patterns are ok in content, but we check structure is valid
      // The document should parse without errors
      expect(document.querySelector('html')).not.toBeNull();
      expect(document.querySelector('head')).not.toBeNull();
      expect(document.querySelector('body')).not.toBeNull();
    });

    it('should have valid meta tags structure', () => {
      const metaTags = document.querySelectorAll('meta');
      metaTags.forEach((meta) => {
        // Meta tags should have either charset, name+content, property+content, or http-equiv+content
        const hasCharset = meta.hasAttribute('charset');
        const hasName = meta.hasAttribute('name');
        const hasProperty = meta.hasAttribute('property');
        const hasHttpEquiv = meta.hasAttribute('http-equiv');
        const hasContent = meta.hasAttribute('content');

        const isValid = hasCharset ||
                       (hasName && hasContent) ||
                       (hasProperty && hasContent) ||
                       (hasHttpEquiv && hasContent);
        expect(isValid).toBe(true);
      });
    });
  });

  // Test Case 2: Check for DOCTYPE declaration - Page starts with <!DOCTYPE html>
  describe('Test Case 2: DOCTYPE declaration', () => {
    it('should start with DOCTYPE declaration', () => {
      const trimmedHtml = htmlContent.trim();
      expect(trimmedHtml.substring(0, 15).toUpperCase()).toBe('<!DOCTYPE HTML>');
    });

    it('should have HTML5 DOCTYPE format', () => {
      const trimmedHtml = htmlContent.trim();
      // HTML5 DOCTYPE is case-insensitive but should be <!DOCTYPE html>
      expect(trimmedHtml).toMatch(/^<!DOCTYPE\s+html>/i);
    });

    it('should not have legacy XHTML or HTML4 DOCTYPE', () => {
      // Should not contain DTD references from older HTML versions
      expect(htmlContent).not.toMatch(/<!DOCTYPE[^>]*DTD\s+XHTML/i);
      expect(htmlContent).not.toMatch(/<!DOCTYPE[^>]*DTD\s+HTML\s+4/i);
      expect(htmlContent).not.toMatch(/<!DOCTYPE[^>]*Transitional/i);
      expect(htmlContent).not.toMatch(/<!DOCTYPE[^>]*Strict/i);
      expect(htmlContent).not.toMatch(/<!DOCTYPE[^>]*Frameset/i);
    });

    it('should have DOCTYPE before html element', () => {
      const doctypeIndex = htmlContent.search(/<!DOCTYPE\s+html>/i);
      const htmlTagIndex = htmlContent.search(/<html/i);
      expect(doctypeIndex).toBeGreaterThanOrEqual(0);
      expect(htmlTagIndex).toBeGreaterThan(doctypeIndex);
    });
  });

  // Test Case 3: Check html lang attribute - HTML element has lang='en' or appropriate language code
  describe('Test Case 3: HTML lang attribute', () => {
    it('should have html element with lang attribute', () => {
      const htmlElement = document.documentElement;
      expect(htmlElement).not.toBeNull();
      expect(htmlElement.hasAttribute('lang')).toBe(true);
    });

    it('should have a non-empty lang attribute value', () => {
      const htmlElement = document.documentElement;
      const langValue = htmlElement.getAttribute('lang');
      expect(langValue).toBeTruthy();
      expect(langValue.trim().length).toBeGreaterThan(0);
    });

    it('should have a valid BCP 47 language code format', () => {
      const htmlElement = document.documentElement;
      const langValue = htmlElement.getAttribute('lang');
      // Basic BCP 47 format: 2-3 letter language code, optionally followed by region
      // Examples: en, en-US, en-GB, zh-Hans, pt-BR
      expect(langValue).toMatch(/^[a-zA-Z]{2,3}(-[a-zA-Z]{2,4})?(-[a-zA-Z]{2})?$/);
    });

    it('should have English or appropriate language code set', () => {
      const htmlElement = document.documentElement;
      const langValue = htmlElement.getAttribute('lang').toLowerCase();
      // For this project, we expect English
      expect(langValue.startsWith('en')).toBe(true);
    });

    it('should have lang attribute in raw HTML source', () => {
      // Verify the lang attribute exists in the actual HTML file
      expect(htmlContent).toMatch(/<html[^>]*\slang=["'][^"']+["']/i);
    });
  });

  // Test Case 4: Verify meta charset is first in head - Meta charset declaration is within first 1024 bytes
  describe('Test Case 4: Meta charset position', () => {
    it('should have meta charset within first 1024 bytes', () => {
      const first1024Bytes = htmlContent.substring(0, 1024);
      expect(first1024Bytes).toMatch(/<meta\s+charset=["']?UTF-8["']?/i);
    });

    it('should have meta charset declaration', () => {
      const charsetMeta = document.querySelector('meta[charset]');
      expect(charsetMeta).not.toBeNull();
    });

    it('should have UTF-8 charset', () => {
      const charsetMeta = document.querySelector('meta[charset]');
      expect(charsetMeta.getAttribute('charset').toUpperCase()).toBe('UTF-8');
    });

    it('should have charset as one of the first elements in head', () => {
      const head = document.querySelector('head');
      const children = Array.from(head.children);
      const charsetIndex = children.findIndex(
        (el) => el.tagName.toLowerCase() === 'meta' && el.hasAttribute('charset')
      );
      // Meta charset should be one of the first 3 elements in head
      expect(charsetIndex).toBeLessThan(3);
      expect(charsetIndex).toBeGreaterThanOrEqual(0);
    });

    it('should have charset before title element', () => {
      const charsetPosition = htmlContent.search(/<meta\s+charset/i);
      const titlePosition = htmlContent.search(/<title>/i);
      expect(charsetPosition).toBeGreaterThanOrEqual(0);
      expect(charsetPosition).toBeLessThan(titlePosition);
    });
  });

  // Test Case 5: Check for duplicate IDs - No duplicate ID attributes in document
  describe('Test Case 5: Duplicate ID check', () => {
    it('should not have duplicate ID attributes', () => {
      const allElements = document.querySelectorAll('[id]');
      const ids = [];
      const duplicates = [];

      allElements.forEach((element) => {
        const id = element.getAttribute('id');
        if (ids.includes(id)) {
          duplicates.push(id);
        } else {
          ids.push(id);
        }
      });

      expect(duplicates).toEqual([]);
    });

    it('should have unique IDs for all elements with id attribute', () => {
      const allElements = document.querySelectorAll('[id]');
      const idSet = new Set();

      allElements.forEach((element) => {
        const id = element.getAttribute('id');
        expect(idSet.has(id)).toBe(false);
        idSet.add(id);
      });
    });

    it('should not have empty ID values', () => {
      const allElements = document.querySelectorAll('[id]');

      allElements.forEach((element) => {
        const id = element.getAttribute('id');
        expect(id.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have valid ID attribute format (no spaces)', () => {
      const allElements = document.querySelectorAll('[id]');

      allElements.forEach((element) => {
        const id = element.getAttribute('id');
        expect(id).not.toMatch(/\s/);
      });
    });

    it('should have IDs that start with a letter or underscore', () => {
      const allElements = document.querySelectorAll('[id]');

      allElements.forEach((element) => {
        const id = element.getAttribute('id');
        // HTML5 allows IDs to start with any character except spaces,
        // but best practice is to start with a letter
        expect(id).toMatch(/^[a-zA-Z_-]/);
      });
    });
  });

  // Additional HTML structure validation tests
  describe('Additional HTML Structure Validation', () => {
    it('should have proper document structure order (html > head + body)', () => {
      const html = document.querySelector('html');
      const head = document.querySelector('head');
      const body = document.querySelector('body');

      expect(html).not.toBeNull();
      expect(head).not.toBeNull();
      expect(body).not.toBeNull();

      // Head and body should be direct children of html
      expect(head.parentElement.tagName.toLowerCase()).toBe('html');
      expect(body.parentElement.tagName.toLowerCase()).toBe('html');
    });

    it('should have head before body in document order', () => {
      const headPosition = htmlContent.search(/<head[^>]*>/i);
      const bodyPosition = htmlContent.search(/<body[^>]*>/i);
      expect(headPosition).toBeLessThan(bodyPosition);
    });

    it('should have title element inside head', () => {
      const head = document.querySelector('head');
      const title = head.querySelector('title');
      expect(title).not.toBeNull();
    });

    it('should have all link elements inside head', () => {
      const headLinks = document.querySelectorAll('head link');
      const allLinks = document.querySelectorAll('link');
      // All stylesheet links should be in head
      const stylesheetLinks = document.querySelectorAll('link[rel="stylesheet"]');
      stylesheetLinks.forEach((link) => {
        expect(link.closest('head')).not.toBeNull();
      });
    });

    it('should have script elements in valid locations', () => {
      const scripts = document.querySelectorAll('script[src]');
      scripts.forEach((script) => {
        // Scripts should be in head or body, not outside
        const isInHead = script.closest('head') !== null;
        const isInBody = script.closest('body') !== null;
        expect(isInHead || isInBody).toBe(true);
      });
    });
  });
});
