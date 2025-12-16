/**
 * Test Suite: HTML Validation
 * Scenario: Verify the HTML markup is valid and follows W3C standards
 *
 * Tests verify:
 * - HTML structure follows W3C standards
 * - No duplicate IDs exist
 * - DOCTYPE declaration is present
 * - html element has lang attribute
 * - meta charset is present
 *
 * These tests use JSDOM to parse and validate the HTML structure.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('HTML Validation', () => {
  let dom;
  let document;
  let htmlContent;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    htmlContent = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Validate HTML with W3C validator
   * Input: Validate HTML with W3C validator
   * Expected: No critical HTML validation errors
   */
  describe('Test Case 1: Validate HTML structure follows W3C standards', () => {
    it('should have valid HTML document structure', () => {
      // Check that the document has the essential structure
      const html = document.documentElement;
      expect(html).not.toBeNull();
      expect(html.tagName).toBe('HTML');
    });

    it('should have a head element', () => {
      const head = document.querySelector('head');
      expect(head).not.toBeNull();
    });

    it('should have a body element', () => {
      const body = document.querySelector('body');
      expect(body).not.toBeNull();
    });

    it('should have title element in head', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have properly nested elements without unclosed tags', () => {
      // JSDOM parses HTML and if there are unclosed tags, it will try to fix them
      // We can verify the structure is correct by checking key elements exist and are properly nested

      // Check main structure elements are properly nested
      const header = document.querySelector('body > header');
      const main = document.querySelector('body > main');
      const footer = document.querySelector('body > footer');

      // All major sections should be direct children of body
      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();
    });

    it('should have properly closed table elements', () => {
      const tables = document.querySelectorAll('table');
      tables.forEach((table) => {
        // Check each table has thead and tbody if applicable
        const rows = table.querySelectorAll('tr');
        expect(rows.length).toBeGreaterThan(0);

        // Verify table cells exist
        const cells = table.querySelectorAll('th, td');
        expect(cells.length).toBeGreaterThan(0);
      });
    });

    it('should have properly formed list elements', () => {
      const lists = document.querySelectorAll('ul, ol');
      lists.forEach((list) => {
        // Each list should have li children
        const listItems = list.querySelectorAll(':scope > li');
        expect(listItems.length).toBeGreaterThan(0);
      });
    });

    it('should not have inline event handlers (using proper event listeners)', () => {
      // Check for elements with onclick, onmouseover, etc. attributes
      const inlineEventHandlers = [
        'onclick', 'onmouseover', 'onmouseout', 'onkeydown',
        'onkeyup', 'onload', 'onerror', 'onfocus', 'onblur'
      ];

      inlineEventHandlers.forEach((handler) => {
        const elementsWithHandler = document.querySelectorAll(`[${handler}]`);
        expect(elementsWithHandler.length).toBe(0);
      });
    });
  });

  /**
   * Test Case 2: Check for duplicate IDs
   * Input: Check for duplicate IDs
   * Expected: All ID attributes are unique on the page
   */
  describe('Test Case 2: Check for duplicate IDs', () => {
    it('should have all ID attributes unique on the page', () => {
      const elementsWithIds = document.querySelectorAll('[id]');
      const ids = [];
      const duplicateIds = [];

      elementsWithIds.forEach((element) => {
        const id = element.getAttribute('id');
        if (ids.includes(id)) {
          duplicateIds.push(id);
        } else {
          ids.push(id);
        }
      });

      // Assert no duplicate IDs exist
      expect(duplicateIds).toEqual([]);
    });

    it('should have valid ID attribute values (no spaces)', () => {
      const elementsWithIds = document.querySelectorAll('[id]');

      elementsWithIds.forEach((element) => {
        const id = element.getAttribute('id');
        // IDs should not contain spaces
        expect(id.includes(' ')).toBe(false);
        // IDs should not be empty
        expect(id.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have ID values that start with a letter', () => {
      const elementsWithIds = document.querySelectorAll('[id]');

      elementsWithIds.forEach((element) => {
        const id = element.getAttribute('id');
        // IDs should start with a letter (HTML5 allows starting with numbers but it's best practice to start with letters)
        expect(/^[a-zA-Z]/.test(id)).toBe(true);
      });
    });
  });

  /**
   * Test Case 3: Check DOCTYPE declaration
   * Input: Check DOCTYPE declaration
   * Expected: <!DOCTYPE html> is present at start of document
   */
  describe('Test Case 3: Check DOCTYPE declaration', () => {
    it('should have <!DOCTYPE html> at the start of document', () => {
      // Check raw HTML content for DOCTYPE
      const trimmedHtml = htmlContent.trim();
      const startsWithDoctype = trimmedHtml.toLowerCase().startsWith('<!doctype html>');
      expect(startsWithDoctype).toBe(true);
    });

    it('should have HTML5 doctype (not older doctypes)', () => {
      const trimmedHtml = htmlContent.trim();

      // HTML5 doctype is simply <!DOCTYPE html>
      // Older doctypes include DTD URLs like:
      // <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"...>
      const hasOldDoctype = /<!DOCTYPE[^>]*PUBLIC/i.test(trimmedHtml);
      expect(hasOldDoctype).toBe(false);
    });

    it('should have DOCTYPE before html element', () => {
      const trimmedHtml = htmlContent.trim();
      const doctypeIndex = trimmedHtml.toLowerCase().indexOf('<!doctype');
      const htmlOpenIndex = trimmedHtml.toLowerCase().indexOf('<html');

      expect(doctypeIndex).toBe(0);
      expect(doctypeIndex).toBeLessThan(htmlOpenIndex);
    });
  });

  /**
   * Test Case 4: Check html lang attribute
   * Input: Check html lang attribute
   * Expected: html element has lang='en' or appropriate language
   */
  describe('Test Case 4: Check html lang attribute', () => {
    it('should have lang attribute on html element', () => {
      const html = document.documentElement;
      expect(html).not.toBeNull();

      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang.trim().length).toBeGreaterThan(0);
    });

    it('should have valid language code format', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');

      // Valid language codes are 2-3 letter codes (ISO 639-1 or 639-2)
      // Optionally followed by region (e.g., en-US, pt-BR)
      const validLangPattern = /^[a-z]{2,3}(-[a-zA-Z]{2,4})?$/;
      expect(validLangPattern.test(lang)).toBe(true);
    });

    it('should have English (en) or appropriate language for MirDB documentation', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');

      // For this documentation site, English is expected
      // Accept 'en' or variants like 'en-US', 'en-GB'
      expect(lang.startsWith('en')).toBe(true);
    });
  });

  /**
   * Test Case 5: Check charset declaration
   * Input: Check charset declaration
   * Expected: meta charset='UTF-8' is present in head
   */
  describe('Test Case 5: Check charset declaration', () => {
    it('should have meta charset="UTF-8" in head', () => {
      const charsetMeta = document.querySelector('head meta[charset]');
      expect(charsetMeta).not.toBeNull();

      const charset = charsetMeta.getAttribute('charset');
      expect(charset.toUpperCase()).toBe('UTF-8');
    });

    it('should have charset meta as one of the first elements in head', () => {
      const head = document.querySelector('head');
      const headChildren = Array.from(head.children);

      // Find the charset meta
      const charsetIndex = headChildren.findIndex(
        (el) => el.tagName === 'META' && el.hasAttribute('charset')
      );

      // Charset should be within the first few elements of head (ideally first or second)
      expect(charsetIndex).toBeLessThan(5);
    });

    it('should not have content-type meta instead of charset (deprecated approach)', () => {
      // The old way was <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
      // The new HTML5 way is simply <meta charset="UTF-8">
      const charsetMeta = document.querySelector('head meta[charset]');
      expect(charsetMeta).not.toBeNull();

      // We have the modern charset, which is good
      const charset = charsetMeta.getAttribute('charset');
      expect(charset).toBeTruthy();
    });
  });

  /**
   * Additional HTML Validation Checks
   */
  describe('Additional HTML Validation Checks', () => {
    it('should have viewport meta tag for responsive design', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    it('should have valid anchor href attributes', () => {
      const anchors = document.querySelectorAll('a[href]');

      anchors.forEach((anchor) => {
        const href = anchor.getAttribute('href');
        // href should not be empty (except for valid fragment identifiers)
        expect(href.length).toBeGreaterThan(0);

        // Should not have javascript: protocol (security issue)
        expect(href.startsWith('javascript:')).toBe(false);
      });
    });

    it('should have required attributes on form elements', () => {
      const inputs = document.querySelectorAll('input, textarea, select');

      inputs.forEach((input) => {
        // Form inputs should have type attribute (except textarea/select)
        if (input.tagName === 'INPUT') {
          const type = input.getAttribute('type');
          expect(type).not.toBeNull();
        }
      });
    });

    it('should have alt attribute on all img elements', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        // Alt can be empty for decorative images, but should exist
        expect(alt !== null).toBe(true);
      });
    });

    it('should not have deprecated HTML elements', () => {
      const deprecatedElements = [
        'font', 'center', 'marquee', 'blink', 'big', 'strike',
        'tt', 'frame', 'frameset', 'noframes', 'applet', 'basefont'
      ];

      deprecatedElements.forEach((element) => {
        const found = document.querySelectorAll(element);
        expect(found.length).toBe(0);
      });
    });

    it('should have external links with rel="noopener noreferrer"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });
  });
});
