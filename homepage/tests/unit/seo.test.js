/**
 * SEO Unit Tests
 * Owner: Scenario 11 - SEO and Meta Tags
 *
 * Test cases:
 * - Page title contains MirDB and is under 60 characters
 * - Meta description is present, between 150-160 characters, and describes MirDB
 * - Open Graph tags are present (og:title, og:description, og:image)
 * - Twitter Card tags are present
 */

const fs = require('fs');
const path = require('path');
const { parseHTML } = require('linkedom');

describe('SEO Meta Tags', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  describe('Page Title', () => {
    test('should have a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    test('title should contain "MirDB"', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toMatch(/MirDB/i);
    });

    test('title should be under 60 characters', () => {
      const title = document.querySelector('title');
      expect(title.textContent.length).toBeLessThan(60);
    });
  });

  describe('Meta Description', () => {
    test('should have a meta description tag', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    test('meta description should be between 150-160 characters', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      expect(content.length).toBeGreaterThanOrEqual(150);
      expect(content.length).toBeLessThanOrEqual(160);
    });

    test('meta description should describe MirDB', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content').toLowerCase();
      expect(content).toMatch(/mirdb/i);
      expect(content).toMatch(/key-value|memcached|persist/i);
    });
  });

  describe('Open Graph Tags', () => {
    test('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
    });

    test('og:title should contain MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content).toMatch(/MirDB/i);
    });

    test('should have og:description meta tag', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
    });

    test('og:description should have product description', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      const content = ogDesc.getAttribute('content');
      expect(content.length).toBeGreaterThan(50);
      expect(content.toLowerCase()).toMatch(/key-value|memcached|persist/i);
    });

    test('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
    });

    test('og:image should have valid image URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      // Should contain a path to an image file
      expect(content).toMatch(/\.(png|jpg|jpeg|svg|webp)/i);
    });

    test('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    test('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
    });

    test('should have og:site_name meta tag', () => {
      const ogSiteName = document.querySelector('meta[property="og:site_name"]');
      expect(ogSiteName).not.toBeNull();
    });
  });

  describe('Twitter Card Tags', () => {
    test('should have twitter:card meta tag', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();
    });

    test('twitter:card should be summary or summary_large_image', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      const content = twitterCard.getAttribute('content');
      expect(['summary', 'summary_large_image']).toContain(content);
    });

    test('should have twitter:title meta tag', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle).not.toBeNull();
    });

    test('should have twitter:description meta tag', () => {
      const twitterDesc = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDesc).not.toBeNull();
    });

    test('should have twitter:image meta tag', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      expect(twitterImage).not.toBeNull();
    });

    test('twitter:image should have valid image URL', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      const content = twitterImage.getAttribute('content');
      expect(content).toMatch(/\.(png|jpg|jpeg|svg|webp)/i);
    });
  });

  describe('Viewport and Charset', () => {
    test('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toUpperCase()).toBe('UTF-8');
    });

    test('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toMatch(/width=device-width/);
    });
  });

  describe('Additional SEO Elements', () => {
    test('should have favicon link', () => {
      const favicon = document.querySelector('link[rel="icon"]');
      expect(favicon).not.toBeNull();
    });

    test('should have canonical URL link', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });
  });
});
