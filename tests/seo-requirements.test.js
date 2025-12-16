/**
 * SEO Requirements Tests
 * Scenario: Verify that the page has appropriate SEO meta tags and semantic HTML (NFR-7)
 */

const fs = require('fs');
const path = require('path');

describe('SEO Requirements', () => {
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  describe('Test Case 1: Title element exists and contains MirDB', () => {
    test('should have a title element', () => {
      // Query for title element
      const titleElement = document.querySelector('title');

      expect(titleElement).not.toBeNull();
      expect(titleElement.textContent.length).toBeGreaterThan(0);
    });

    test('title should contain MirDB', () => {
      const titleElement = document.querySelector('title');

      expect(titleElement).not.toBeNull();
      expect(titleElement.textContent).toContain('MirDB');
    });

    test('title should be descriptive (more than just the brand name)', () => {
      const titleElement = document.querySelector('title');

      expect(titleElement).not.toBeNull();
      // Title should be descriptive, not just "MirDB"
      expect(titleElement.textContent.length).toBeGreaterThan(6);
    });
  });

  describe('Test Case 2: Meta description exists with meaningful content', () => {
    test('should have a meta description tag', () => {
      // Query for meta description
      const metaDescription = document.querySelector('meta[name="description"]');

      expect(metaDescription).not.toBeNull();
    });

    test('meta description should have meaningful content', () => {
      const metaDescription = document.querySelector('meta[name="description"]');

      expect(metaDescription).not.toBeNull();

      const content = metaDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(20);
    });

    test('meta description should contain relevant keywords', () => {
      const metaDescription = document.querySelector('meta[name="description"]');

      expect(metaDescription).not.toBeNull();

      const content = metaDescription.getAttribute('content').toLowerCase();
      // Should contain at least some relevant keywords
      const hasRelevantContent =
        content.includes('mirdb') ||
        content.includes('key-value') ||
        content.includes('memcached') ||
        content.includes('database') ||
        content.includes('persistent');

      expect(hasRelevantContent).toBe(true);
    });
  });

  describe('Test Case 3: Semantic main element exists', () => {
    test('should have a main element', () => {
      // Query for semantic main element
      const mainElement = document.querySelector('main');

      expect(mainElement).not.toBeNull();
    });

    test('main element should wrap primary content', () => {
      const mainElement = document.querySelector('main');

      expect(mainElement).not.toBeNull();

      // Main should contain sections or significant content
      const sections = mainElement.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('there should be only one main element', () => {
      const mainElements = document.querySelectorAll('main');

      expect(mainElements.length).toBe(1);
    });
  });

  describe('Test Case 4: Semantic header element exists', () => {
    test('should have a header element', () => {
      // Query for semantic header element
      const headerElement = document.querySelector('header');

      expect(headerElement).not.toBeNull();
    });

    test('header should contain navigation or branding', () => {
      const headerElement = document.querySelector('header');

      expect(headerElement).not.toBeNull();

      // Header should contain nav or branding elements
      const nav = headerElement.querySelector('nav');
      const brand = headerElement.querySelector('.nav-brand, .brand, .logo, a[href="#hero"], a[href="/"]');

      const hasNavOrBrand = nav !== null || brand !== null;
      expect(hasNavOrBrand).toBe(true);
    });

    test('header should be positioned at the top of the body', () => {
      const bodyChildren = document.body.children;
      const headerElement = document.querySelector('header');

      expect(headerElement).not.toBeNull();

      // Header should be one of the first few elements (allowing for skip links, etc.)
      let headerIndex = -1;
      for (let i = 0; i < bodyChildren.length; i++) {
        if (bodyChildren[i] === headerElement) {
          headerIndex = i;
          break;
        }
      }

      // Header should be within the first 3 elements
      expect(headerIndex).toBeGreaterThanOrEqual(0);
      expect(headerIndex).toBeLessThan(3);
    });
  });

  describe('Test Case 5: Open Graph title tag exists', () => {
    test('should have og:title meta tag', () => {
      // Query for Open Graph title
      const ogTitle = document.querySelector('meta[property="og:title"]');

      expect(ogTitle).not.toBeNull();
    });

    test('og:title should have meaningful content', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');

      expect(ogTitle).not.toBeNull();

      const content = ogTitle.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);
    });

    test('og:title should contain MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');

      expect(ogTitle).not.toBeNull();

      const content = ogTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });
  });

  describe('Test Case 6: Open Graph description tag exists', () => {
    test('should have og:description meta tag', () => {
      // Query for Open Graph description
      const ogDescription = document.querySelector('meta[property="og:description"]');

      expect(ogDescription).not.toBeNull();
    });

    test('og:description should have meaningful content', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');

      expect(ogDescription).not.toBeNull();

      const content = ogDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(20);
    });

    test('og:description should describe the product', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');

      expect(ogDescription).not.toBeNull();

      const content = ogDescription.getAttribute('content').toLowerCase();
      // Should contain relevant product keywords
      const hasProductInfo =
        content.includes('key-value') ||
        content.includes('memcached') ||
        content.includes('database') ||
        content.includes('persistent') ||
        content.includes('store');

      expect(hasProductInfo).toBe(true);
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('should have semantic footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('should have proper heading hierarchy (h1 followed by h2)', () => {
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');

      expect(h1).not.toBeNull();
      expect(h2s.length).toBeGreaterThan(0);
    });

    test('should have lang attribute on html element', () => {
      const htmlElement = document.documentElement;
      const lang = htmlElement.getAttribute('lang');

      expect(lang).not.toBeNull();
      expect(lang.length).toBeGreaterThan(0);
    });

    test('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });
  });
});
