/**
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('SEO Optimization', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    htmlContent = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Title Tag', () => {
    test('Page has title tag containing MirDB', () => {
      const titleElement = document.querySelector('title');
      expect(titleElement).not.toBeNull();
      expect(titleElement.textContent).toContain('MirDB');
    });

    test('Title tag is descriptive and meaningful', () => {
      const titleElement = document.querySelector('title');
      expect(titleElement).not.toBeNull();
      // Title should be reasonably long and descriptive
      expect(titleElement.textContent.length).toBeGreaterThan(10);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    test('Page has meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    test('Meta description has relevant content about MirDB', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(50);
      // Should mention key aspects of MirDB
      expect(content.toLowerCase()).toMatch(/mirdb|key-value|memcached|persistent/i);
    });

    test('Meta description is within recommended length', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      // Meta descriptions should be 50-160 characters for optimal SEO
      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  describe('Test Case 3: Open Graph Tags', () => {
    test('Page has og:title tag for social sharing', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toBeTruthy();
    });

    test('Page has og:description tag for social sharing', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      expect(ogDescription.getAttribute('content')).toBeTruthy();
    });

    test('Page has og:type tag for social sharing', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBeTruthy();
    });

    test('Open Graph title contains MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toContain('MirDB');
    });

    test('Open Graph description is meaningful', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      const content = ogDescription.getAttribute('content');
      expect(content.length).toBeGreaterThan(30);
    });

    test('Open Graph type is website', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });
  });

  describe('Test Case 4: Canonical URL', () => {
    test('Page has canonical link tag', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    test('Canonical link has valid href attribute', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      // Should be a valid URL format
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  describe('Test Case 5: Lang Attribute', () => {
    test('HTML element has lang attribute', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement).not.toBeNull();
      expect(htmlElement.hasAttribute('lang')).toBe(true);
    });

    test('HTML element has lang="en" attribute', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement).not.toBeNull();
      expect(htmlElement.getAttribute('lang')).toBe('en');
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('Page has charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('Page has viewport meta tag for mobile', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('Page has proper heading hierarchy with h1', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });
  });
});
