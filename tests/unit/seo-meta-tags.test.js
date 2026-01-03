/**
 * Unit tests for SEO Meta Tags
 * Scenario: SEO - Meta Tags
 * Verifies the homepage has appropriate meta tags for search engine optimization
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('SEO Meta Tags', () => {
  let document;
  let html;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC1: Page Title Tag', () => {
    test('Title tag contains "MirDB"', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    test('Title tag is under 60 characters', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeLessThanOrEqual(60);
    });

    test('Title tag is descriptive', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      // Title should not just be "MirDB" but should have a descriptor
      expect(title.textContent.length).toBeGreaterThan(5);
    });
  });

  describe('TC2: Meta Description', () => {
    test('Meta description exists', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    test('Meta description is under 160 characters', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content.length).toBeLessThanOrEqual(160);
    });

    test('Meta description mentions key features', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content').toLowerCase();
      // Should mention at least one key feature: persistence, memcached, key-value, or Rust
      const hasKeyFeature =
        content.includes('persistent') ||
        content.includes('memcached') ||
        content.includes('key-value') ||
        content.includes('rust');
      expect(hasKeyFeature).toBe(true);
    });
  });

  describe('TC3: Open Graph Title Tag', () => {
    test('og:title meta tag is present', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
    });

    test('og:title has meaningful content', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      const content = ogTitle.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });
  });

  describe('TC4: Open Graph Description Tag', () => {
    test('og:description meta tag is present', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
    });

    test('og:description has meaningful content', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      const content = ogDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });
  });

  describe('TC5: Open Graph Image Tag', () => {
    test('og:image meta tag is present for social previews', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
    });

    test('og:image has a valid URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      const content = ogImage.getAttribute('content');
      expect(content).toBeTruthy();
      // Should be a valid URL (http/https) or relative path
      const isValidUrl = content.startsWith('http://') ||
                         content.startsWith('https://') ||
                         content.startsWith('/') ||
                         content.includes('.');
      expect(isValidUrl).toBe(true);
    });
  });

  describe('TC6: Canonical URL', () => {
    test('Canonical link element is present', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    test('Canonical link has an href attribute', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('og:type meta tag is present', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
    });

    test('og:url meta tag is present', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
    });

    test('Document has lang attribute', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement).not.toBeNull();
      const lang = htmlElement.getAttribute('lang');
      expect(lang).toBeTruthy();
    });

    test('Document has charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    test('Document has viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });
  });
});
