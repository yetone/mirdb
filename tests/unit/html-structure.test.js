/**
 * HTML Structure Unit Tests
 * Owner: Scenarios 13 & 17
 *
 * Test cases:
 * - Semantic header element present
 * - Semantic main element present
 * - Single h1 element
 * - Proper heading hierarchy
 * - Viewport meta tag present
 * - Page title contains MirDB
 * - No server-side includes
 * - Relative asset paths
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('HTML Structure Tests', () => {
  let dom;
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Semantic Structure', () => {
    test('should have a semantic header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('should have a semantic main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('should have exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('should have proper heading hierarchy', () => {
      // Get all headings
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const levels = Array.from(headings).map((h) => parseInt(h.tagName[1], 10));

      // Check that no heading level is skipped
      for (let i = 1; i < levels.length; i++) {
        const diff = levels[i] - levels[i - 1];
        // Heading level should not increase by more than 1
        expect(diff).toBeLessThanOrEqual(1);
      }
    });

    test('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('should have page title containing MirDB', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.toLowerCase()).toContain('mirdb');
    });
  });

  describe('Static Hosting Compatibility - No Server-Side Dependencies', () => {
    test('should not contain server-side include markers (<!--#include)', () => {
      expect(htmlContent).not.toMatch(/<!--\s*#include/i);
    });

    test('should not contain SSI directives (<!--#)', () => {
      expect(htmlContent).not.toMatch(/<!--\s*#\w+/);
    });

    test('should not contain PHP tags', () => {
      expect(htmlContent).not.toMatch(/<\?php/i);
      expect(htmlContent).not.toMatch(/<\?=/);
    });

    test('should not contain ASP/ASP.NET tags', () => {
      expect(htmlContent).not.toMatch(/<%/);
      expect(htmlContent).not.toMatch(/<asp:/i);
    });

    test('should not contain JSP tags', () => {
      expect(htmlContent).not.toMatch(/<%@/);
      expect(htmlContent).not.toMatch(/<jsp:/i);
    });

    test('should not contain template engine markers (EJS, Handlebars, Jinja)', () => {
      // EJS
      expect(htmlContent).not.toMatch(/<%[^-]/);
      // Handlebars/Mustache
      expect(htmlContent).not.toMatch(/\{\{[^{]/);
      // Jinja2
      expect(htmlContent).not.toMatch(/\{%/);
    });

    test('should not require server-side rendering markers', () => {
      // Common SSR hydration markers
      expect(htmlContent).not.toMatch(/data-reactroot/);
      expect(htmlContent).not.toMatch(/__NEXT_DATA__/);
      expect(htmlContent).not.toMatch(/data-server-rendered/);
      expect(htmlContent).not.toMatch(/__NUXT__/);
    });
  });

  describe('Static Hosting Compatibility - Relative Asset Paths', () => {
    test('CSS links should use relative paths', () => {
      const cssLinks = document.querySelectorAll('link[rel="stylesheet"]');
      expect(cssLinks.length).toBeGreaterThan(0);

      cssLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // Should not start with / (root-relative)
        expect(href).not.toMatch(/^\//);
        // Should not be absolute URL (http/https)
        expect(href).not.toMatch(/^https?:\/\//);
        // Should be a relative path
        expect(href).toMatch(/^[a-zA-Z0-9._-]/);
      });
    });

    test('JS scripts should use relative paths', () => {
      const scripts = document.querySelectorAll('script[src]');

      scripts.forEach((script) => {
        const src = script.getAttribute('src');
        // Should not start with / (root-relative)
        expect(src).not.toMatch(/^\//);
        // Should not be absolute URL (http/https)
        expect(src).not.toMatch(/^https?:\/\//);
        // Should be a relative path
        expect(src).toMatch(/^[a-zA-Z0-9._-]/);
      });
    });

    test('Local images should use relative paths', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const src = img.getAttribute('src');
        // External images (like CI badges) are allowed
        if (src.match(/^https?:\/\//)) {
          // External URLs are allowed for badges, etc.
          return;
        }
        // Local images should not start with /
        expect(src).not.toMatch(/^\//);
        // Should be a relative path
        expect(src).toMatch(/^[a-zA-Z0-9._-]/);
      });
    });

    test('Internal links should use relative paths or hash anchors', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"], a:not([href^="http"])');

      internalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (!href) return;

        // Allow hash anchors
        if (href.startsWith('#')) return;

        // Should not start with / for local files
        expect(href).not.toMatch(/^\//);
      });
    });

    test('CSS @import statements should use relative paths', () => {
      // Check inline styles and style tags
      const styleElements = document.querySelectorAll('style');
      styleElements.forEach((style) => {
        const content = style.textContent;
        // If @import is used, it should be relative
        if (content.includes('@import')) {
          expect(content).not.toMatch(/@import\s+url\s*\(\s*['"]?\//);
          expect(content).not.toMatch(/@import\s+['"]?\//);
        }
      });
    });
  });

  describe('Accessibility Basics', () => {
    test('html element should have lang attribute', () => {
      const html = document.documentElement;
      expect(html.getAttribute('lang')).toBeTruthy();
    });

    test('all images should have alt attributes', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });
  });
});
