/**
 * SEO Meta Tags E2E Tests
 * Verifies that the page has proper meta tags for SEO (NFR-5)
 *
 * Test Cases:
 * 1. Check title tag - Page has descriptive title tag containing 'MirDB'
 * 2. Check meta description - Meta description tag is present with compelling, relevant content
 * 3. Check viewport meta tag - Viewport meta tag is set for responsive design
 * 4. Check Open Graph tags - OG tags (og:title, og:description, og:image) are present for social sharing
 * 5. Check canonical URL - Canonical link tag is present
 */
const { test, expect } = require('@playwright/test');
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

test.describe('SEO Meta Tags', () => {
  const indexPath = path.join(__dirname, '../../index.html');
  let htmlContent;
  let document;

  test.beforeAll(async () => {
    htmlContent = fs.readFileSync(indexPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  test.describe('Test Case 1: Title Tag', () => {
    test('Page has title tag', async () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    test('Title tag contains MirDB', async () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    test('Title tag is descriptive (not just "MirDB")', async () => {
      const title = document.querySelector('title');
      expect(title.textContent.length).toBeGreaterThan(10);
    });
  });

  test.describe('Test Case 2: Meta Description', () => {
    test('Meta description tag is present', async () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    test('Meta description has content', async () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);
    });

    test('Meta description contains relevant keywords', async () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content').toLowerCase();
      // Should contain at least some relevant terms
      const relevantTerms = ['mirdb', 'key-value', 'memcached', 'persistent'];
      const hasRelevantContent = relevantTerms.some(term => content.includes(term));
      expect(hasRelevantContent).toBe(true);
    });
  });

  test.describe('Test Case 3: Viewport Meta Tag', () => {
    test('Viewport meta tag is present', async () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('Viewport has width=device-width for responsive design', async () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      const content = viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    test('Viewport has initial-scale=1.0', async () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      const content = viewport.getAttribute('content');
      expect(content).toContain('initial-scale=1');
    });
  });

  test.describe('Test Case 4: Open Graph Tags', () => {
    test('og:title tag is present', async () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
    });

    test('og:title has content', async () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);
    });

    test('og:description tag is present', async () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
    });

    test('og:description has content', async () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      const content = ogDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);
    });

    test('og:image tag is present', async () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
    });

    test('og:image has valid URL', async () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);
    });

    test('og:type tag is present', async () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
    });

    test('og:url tag is present', async () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
    });
  });

  test.describe('Test Case 5: Canonical URL', () => {
    test('Canonical link tag is present', async () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    test('Canonical link has href attribute', async () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).not.toBeNull();
      expect(href.length).toBeGreaterThan(0);
    });

    test('Canonical URL is a valid URL format', async () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      // Should start with http:// or https://
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Additional SEO Best Practices', () => {
    test('Twitter Card meta tags are present', async () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();
    });

    test('Twitter title meta tag is present', async () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle).not.toBeNull();
    });

    test('Twitter description meta tag is present', async () => {
      const twitterDescription = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDescription).not.toBeNull();
    });
  });
});
