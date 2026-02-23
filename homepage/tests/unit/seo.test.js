/**
 * SEO Unit Tests
 * Owner: Scenario 17 - SEO Requirements
 *
 * Tests for SEO meta tag validation:
 * - Title tag presence and content
 * - Meta description presence and content
 * - Viewport meta tag
 * - Open Graph tags (og:title, og:description, og:image)
 * - Canonical URL
 */

const fs = require('fs');
const path = require('path');

describe('SEO Meta Tag Validation', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the index.html file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: Page Title', () => {
    test('Page has descriptive <title> including MirDB', () => {
      const titleElement = document.querySelector('title');

      // Title element should exist
      expect(titleElement).not.toBeNull();

      // Title should not be empty
      const titleText = titleElement.textContent;
      expect(titleText.length).toBeGreaterThan(0);

      // Title should include MirDB
      expect(titleText.toLowerCase()).toContain('mirdb');

      // Title should be descriptive (reasonable length)
      expect(titleText.length).toBeGreaterThan(10);
      expect(titleText.length).toBeLessThan(70); // Optimal for search engines
    });
  });

  describe('Test Case 2: Meta Description', () => {
    test('Page has <meta name="description"> with MirDB description', () => {
      const metaDescription = document.querySelector('meta[name="description"]');

      // Meta description should exist
      expect(metaDescription).not.toBeNull();

      // Get content attribute
      const content = metaDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);

      // Should mention MirDB
      expect(content.toLowerCase()).toContain('mirdb');

      // Should have reasonable length for SEO (50-160 characters)
      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  describe('Test Case 3: Viewport Meta Tag', () => {
    test('Page has proper viewport meta tag for responsive design', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');

      // Viewport meta should exist
      expect(viewportMeta).not.toBeNull();

      // Get content attribute
      const content = viewportMeta.getAttribute('content');
      expect(content).not.toBeNull();

      // Should include width=device-width
      expect(content).toContain('width=device-width');

      // Should include initial-scale
      expect(content).toContain('initial-scale');
    });
  });

  describe('Test Case 4: Open Graph Title', () => {
    test('Page has Open Graph title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');

      // og:title should exist
      expect(ogTitle).not.toBeNull();

      // Get content attribute
      const content = ogTitle.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);

      // Should mention MirDB
      expect(content.toLowerCase()).toContain('mirdb');
    });
  });

  describe('Test Case 5: Open Graph Description', () => {
    test('Page has Open Graph description meta tag', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');

      // og:description should exist
      expect(ogDescription).not.toBeNull();

      // Get content attribute
      const content = ogDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);

      // Should be a meaningful description
      expect(content.length).toBeGreaterThan(20);
    });
  });

  describe('Test Case 6: Open Graph Image', () => {
    test('Page has Open Graph image for social sharing', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');

      // og:image should exist
      expect(ogImage).not.toBeNull();

      // Get content attribute
      const content = ogImage.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);

      // Should be an image path or URL
      expect(content).toMatch(/\.(png|jpg|jpeg|gif|webp|svg)$/i);
    });
  });

  describe('Test Case 7: Canonical URL', () => {
    test('Page has canonical link element', () => {
      const canonicalLink = document.querySelector('link[rel="canonical"]');

      // Canonical link should exist
      expect(canonicalLink).not.toBeNull();

      // Get href attribute
      const href = canonicalLink.getAttribute('href');
      expect(href).not.toBeNull();
      expect(href.length).toBeGreaterThan(0);

      // Should be a valid URL format
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  describe('Additional SEO Checks', () => {
    test('Page has charset meta tag', () => {
      const charsetMeta = document.querySelector('meta[charset]');
      expect(charsetMeta).not.toBeNull();
      expect(charsetMeta.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('Page has og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    test('HTML has lang attribute for SEO', () => {
      const html = document.querySelector('html');
      expect(html.hasAttribute('lang')).toBe(true);
      expect(html.getAttribute('lang')).toBe('en');
    });

    test('Page has favicon', () => {
      const favicon = document.querySelector('link[rel="icon"]');
      expect(favicon).not.toBeNull();
      expect(favicon.getAttribute('href')).toBeTruthy();
    });
  });
});
