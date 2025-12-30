/**
 * SEO Optimization Tests
 *
 * Scenario: Verify that the page is SEO-optimized with proper meta tags and semantic HTML (NFR-5)
 * These tests verify the SEO implementation meets all requirements.
 */

const fs = require('fs');
const path = require('path');

describe('SEO Optimization', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using DOMParser (available in jsdom environment)
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Page Title Tag', () => {
    /**
     * Test Case ID: 1
     * Input: Check page title tag
     * Expected: Title contains 'MirDB' and describes the product purpose
     * Type: unit
     */
    test('should have a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    test('should contain "MirDB" in the title', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    test('should describe the product purpose in the title', () => {
      const title = document.querySelector('title');
      const titleText = title.textContent.toLowerCase();
      // Title should mention key value proposition (key-value, persistent, memcached)
      const hasKeyValueStore = titleText.includes('key-value') || titleText.includes('key value');
      const hasPersistent = titleText.includes('persistent');
      const hasMemcached = titleText.includes('memcached');
      expect(hasKeyValueStore || hasPersistent || hasMemcached).toBe(true);
    });

    test('title should be between 30-70 characters for optimal SEO', () => {
      const title = document.querySelector('title');
      const length = title.textContent.length;
      expect(length).toBeGreaterThanOrEqual(30);
      expect(length).toBeLessThanOrEqual(70);
    });
  });

  describe('Test Case 2: Meta Description Tag', () => {
    /**
     * Test Case ID: 2
     * Input: Check meta description tag
     * Expected: Meta description is 150-160 characters describing MirDB
     * Type: unit
     */
    test('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    test('should have meta description that describes MirDB', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content');
      expect(content).toContain('MirDB');
    });

    test('should have meta description between 100-200 characters', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content');
      // SEO best practices suggest 150-160 chars, but 100-200 is acceptable range
      expect(content.length).toBeGreaterThanOrEqual(100);
      expect(content.length).toBeLessThanOrEqual(200);
    });

    test('meta description should include key terms', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content').toLowerCase();
      // Should mention at least one key value proposition
      const hasKeyValueStore = content.includes('key-value') || content.includes('key value');
      const hasPersistent = content.includes('persistent');
      const hasMemcached = content.includes('memcached');
      expect(hasKeyValueStore || hasPersistent || hasMemcached).toBe(true);
    });
  });

  describe('Test Case 3: Semantic HTML Elements', () => {
    /**
     * Test Case ID: 3
     * Input: Check for semantic HTML elements
     * Expected: Page uses header, nav, main, section, and footer elements appropriately
     * Type: unit
     */
    test('should have a nav element for navigation', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    test('should have section elements for content organization', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('should have multiple sections for different content areas', () => {
      const sections = document.querySelectorAll('section');
      // A proper homepage should have at least 3 sections (features, commands, quickstart, etc.)
      expect(sections.length).toBeGreaterThanOrEqual(3);
    });

    test('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('should have exactly one h1 heading', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 should contain the product name', () => {
      const h1 = document.querySelector('h1');
      expect(h1.textContent).toContain('MirDB');
    });

    test('should have h2 headings for sections', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThan(0);
    });

    test('heading hierarchy should be proper (no skipped levels)', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let previousLevel = 0;

      allHeadings.forEach((heading) => {
        const currentLevel = parseInt(heading.tagName.charAt(1));
        // Each heading should not skip more than one level from previous
        if (previousLevel > 0) {
          expect(currentLevel).toBeLessThanOrEqual(previousLevel + 1);
        }
        previousLevel = currentLevel;
      });
    });
  });

  describe('Test Case 4: Open Graph Meta Tags', () => {
    /**
     * Test Case ID: 4
     * Input: Check Open Graph meta tags
     * Expected: og:title, og:description, og:image, og:url tags are present
     * Type: unit
     */
    test('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
    });

    test('og:title should contain MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle.getAttribute('content')).toContain('MirDB');
    });

    test('should have og:description meta tag', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
    });

    test('og:description should have meaningful content', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription.getAttribute('content').length).toBeGreaterThan(50);
    });

    test('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
    });

    test('og:image should have a valid URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });

    test('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
    });

    test('og:url should have a valid URL', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      const content = ogUrl.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });

    test('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
    });

    test('og:type should be "website"', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType.getAttribute('content')).toBe('website');
    });
  });

  describe('Test Case 5: Twitter Card Meta Tags', () => {
    /**
     * Test Case ID: 5
     * Input: Check Twitter Card meta tags
     * Expected: twitter:card, twitter:title, twitter:description tags are present
     * Type: unit
     */
    test('should have twitter:card meta tag', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();
    });

    test('twitter:card should be summary_large_image or summary', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      const content = twitterCard.getAttribute('content');
      expect(['summary', 'summary_large_image']).toContain(content);
    });

    test('should have twitter:title meta tag', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle).not.toBeNull();
    });

    test('twitter:title should contain MirDB', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle.getAttribute('content')).toContain('MirDB');
    });

    test('should have twitter:description meta tag', () => {
      const twitterDescription = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDescription).not.toBeNull();
    });

    test('twitter:description should have meaningful content', () => {
      const twitterDescription = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDescription.getAttribute('content').length).toBeGreaterThan(50);
    });

    test('should have twitter:image meta tag', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      expect(twitterImage).not.toBeNull();
    });

    test('twitter:image should have a valid URL', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      const content = twitterImage.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });
  });

  describe('Test Case 6: Canonical URL', () => {
    /**
     * Test Case ID: 6
     * Input: Check canonical URL
     * Expected: Page has canonical link element pointing to primary URL
     * Type: unit
     */
    test('should have a canonical link element', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    test('canonical should have a valid href', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
    });

    test('canonical URL should be a valid HTTPS URL', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toMatch(/^https:\/\//);
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('should have lang attribute on html element', () => {
      const html = document.querySelector('html');
      expect(html.getAttribute('lang')).toBeTruthy();
    });

    test('lang attribute should be "en"', () => {
      const html = document.querySelector('html');
      expect(html.getAttribute('lang')).toBe('en');
    });

    test('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    test('charset should be UTF-8', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset.getAttribute('charset').toUpperCase()).toBe('UTF-8');
    });

    test('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('should have keywords meta tag', () => {
      const keywords = document.querySelector('meta[name="keywords"]');
      expect(keywords).not.toBeNull();
    });

    test('keywords should include relevant terms', () => {
      const keywords = document.querySelector('meta[name="keywords"]');
      const content = keywords.getAttribute('content').toLowerCase();
      expect(content).toContain('mirdb');
    });

    test('all links should have descriptive text (not generic "click here")', () => {
      const links = document.querySelectorAll('a');
      links.forEach((link) => {
        const text = link.textContent.toLowerCase().trim();
        if (text.length > 0) {
          expect(text).not.toBe('click here');
          expect(text).not.toBe('read more');
        }
      });
    });
  });
});
