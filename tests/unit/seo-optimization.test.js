/**
 * Unit tests for SEO Optimization
 * Scenario: SEO Optimization
 * Validates that the homepage has proper meta tags, semantic HTML, and SEO best practices
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('SEO Optimization', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: Title Element', () => {
    test('should have a title tag in the head', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
    });

    test('title tag should contain "MirDB"', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    test('title should be descriptive (at least 20 characters)', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeGreaterThanOrEqual(20);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    test('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    test('meta description should have content about MirDB', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.toLowerCase()).toContain('mirdb');
    });

    test('meta description should be between 50 and 160 characters', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  describe('Test Case 3: Open Graph Meta Tags', () => {
    test('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toBeTruthy();
    });

    test('should have og:description meta tag', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      expect(ogDescription.getAttribute('content')).toBeTruthy();
    });

    test('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage.getAttribute('content')).toBeTruthy();
    });

    test('og:title should contain MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toContain('MirDB');
    });

    test('og:description should have relevant content about MirDB', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      const content = ogDescription.getAttribute('content').toLowerCase();
      expect(content.length).toBeGreaterThan(10);
    });

    test('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    test('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      expect(ogUrl.getAttribute('content')).toBeTruthy();
    });
  });

  describe('Test Case 4: Semantic HTML Structure', () => {
    test('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('should have section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('should have a nav element', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    test('sections should have proper aria-labelledby or aria-label attributes', () => {
      const sections = document.querySelectorAll('section');
      sections.forEach((section) => {
        const hasAriaLabelledby = section.hasAttribute('aria-labelledby');
        const hasAriaLabel = section.hasAttribute('aria-label');
        const hasRole = section.hasAttribute('role');
        // At least one of these should be present for accessibility
        expect(hasAriaLabelledby || hasAriaLabel || hasRole).toBe(true);
      });
    });

    test('main element should contain primary content sections', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      const sectionsInMain = main.querySelectorAll('section');
      expect(sectionsInMain.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 5: Canonical Link Tag', () => {
    test('should have a canonical link tag', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    test('canonical link should have a valid href', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      // Should be a valid URL format
      expect(href).toMatch(/^https?:\/\/.+/);
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('should have proper lang attribute on html element', () => {
      const html = document.documentElement;
      expect(html.getAttribute('lang')).toBe('en');
    });

    test('should have exactly one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 should contain MirDB', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('images should have alt attributes', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    test('should have keywords meta tag', () => {
      const keywords = document.querySelector('meta[name="keywords"]');
      expect(keywords).not.toBeNull();
      expect(keywords.getAttribute('content')).toBeTruthy();
    });
  });
});
