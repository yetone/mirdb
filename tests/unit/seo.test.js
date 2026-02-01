/**
 * SEO Optimization Tests
 * Owner: Scenario 11 - SEO Optimization
 *
 * Tests:
 * - Title tag with MirDB
 * - Meta description
 * - Open Graph tags
 * - Single H1 tag
 * - Image alt attributes
 */
const { loadHTML } = require('../helpers/dom-utils');

describe('SEO Optimization', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Test Case 1: Title Tag', () => {
    it('should have a title tag that exists and contains "MirDB"', () => {
      const title = document.querySelector('title');

      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description with relevant keywords', () => {
      const metaDescription = document.querySelector('meta[name="description"]');

      expect(metaDescription).not.toBeNull();
      expect(metaDescription.getAttribute('content')).toBeTruthy();

      const content = metaDescription.getAttribute('content').toLowerCase();
      // Check for relevant keywords
      const hasRelevantKeywords =
        content.includes('key-value') ||
        content.includes('memcached') ||
        content.includes('persistent') ||
        content.includes('mirdb');

      expect(hasRelevantKeywords).toBe(true);
    });
  });

  describe('Test Case 3: Open Graph Tags', () => {
    it('should have og:title tag present', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');

      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toBeTruthy();
    });

    it('should have og:description tag present', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');

      expect(ogDescription).not.toBeNull();
      expect(ogDescription.getAttribute('content')).toBeTruthy();
    });

    it('should have og:image tag present', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');

      expect(ogImage).not.toBeNull();
      expect(ogImage.getAttribute('content')).toBeTruthy();
    });
  });

  describe('Test Case 4: H1 Tag Count', () => {
    it('should have exactly one H1 tag on the page', () => {
      const h1Tags = document.querySelectorAll('h1');

      expect(h1Tags.length).toBe(1);
    });

    it('H1 should contain meaningful content', () => {
      const h1 = document.querySelector('h1');

      expect(h1).not.toBeNull();
      expect(h1.textContent.trim()).toBeTruthy();
    });
  });

  describe('Test Case 5: Image Alt Attributes', () => {
    it('should have non-empty alt attributes on all img tags', () => {
      const images = document.querySelectorAll('img');

      expect(images.length).toBeGreaterThan(0);

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim()).not.toBe('');
      });
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have proper HTML lang attribute', () => {
      // Load raw HTML to check for lang attribute (loadHTML replaces innerHTML, losing outer element attributes)
      const fs = require('fs');
      const path = require('path');
      const rawHtml = fs.readFileSync(path.resolve(__dirname, '../../index.html'), 'utf8');
      const hasLangAttr = /<html[^>]*\slang\s*=\s*["'][^"']+["'][^>]*>/i.test(rawHtml);
      expect(hasLangAttr).toBe(true);
    });

    it('should have meta viewport tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have proper heading hierarchy (H2s and H3s)', () => {
      const h2s = document.querySelectorAll('h2');
      const h3s = document.querySelectorAll('h3');

      // Should have H2 headings for main sections
      expect(h2s.length).toBeGreaterThan(0);
    });
  });
});
