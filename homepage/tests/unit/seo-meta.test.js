/**
 * SEO and Meta Tags Unit Tests
 * Owner: Scenario 15 - SEO and Meta Tags
 *
 * Tests for validating SEO best practices:
 * - Title tag present and descriptive
 * - Meta description present
 * - Open Graph tags present
 * - Canonical URL set
 * - Viewport meta tag configured
 * - Semantic HTML elements used
 * - Robots meta tag (or default)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('SEO and Meta Tags', () => {
  let dom;
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('Title Tag', () => {
    test('page has a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    test('title tag contains MirDB', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    test('title tag is descriptive', () => {
      const title = document.querySelector('title');
      expect(title.textContent.length).toBeGreaterThan(10);
    });
  });

  describe('Meta Description', () => {
    test('meta description tag exists', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    test('meta description has relevant content about MirDB', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content');
      expect(content).toContain('MirDB');
      expect(content.length).toBeGreaterThan(50);
    });
  });

  describe('Open Graph Tags', () => {
    test('og:title tag is present', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toBeTruthy();
    });

    test('og:description tag is present', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      expect(ogDescription.getAttribute('content')).toBeTruthy();
    });

    test('og:image tag is present for social sharing', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage.getAttribute('content')).toBeTruthy();
    });

    test('og:type tag is present', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
    });
  });

  describe('Semantic HTML Elements', () => {
    test('page uses header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('page uses nav element', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    test('page uses main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('page uses section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('page uses article elements', () => {
      const articles = document.querySelectorAll('article');
      expect(articles.length).toBeGreaterThan(0);
    });

    test('page uses footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('html element has lang attribute', () => {
      const html = document.querySelector('html');
      expect(html.getAttribute('lang')).toBeTruthy();
    });
  });

  describe('Canonical URL', () => {
    test('canonical link tag is present', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    test('canonical URL has href attribute', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
    });
  });

  describe('Viewport Meta Tag', () => {
    test('viewport meta tag is set', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('viewport is configured for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      const content = viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });
  });

  describe('Robots Meta Tag', () => {
    test('robots meta tag allows indexing or is not set (default allows)', () => {
      const robots = document.querySelector('meta[name="robots"]');
      // If robots tag is present, it should allow indexing
      if (robots) {
        const content = robots.getAttribute('content').toLowerCase();
        // Should NOT contain noindex
        expect(content).not.toContain('noindex');
      }
      // If not present, default behavior allows indexing - test passes
      expect(true).toBe(true);
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('charset meta tag is present', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('heading hierarchy starts with h1', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
    });

    test('only one h1 exists on the page', () => {
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);
    });

    test('images have alt attributes', () => {
      const images = document.querySelectorAll('img');
      images.forEach(img => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });
  });
});
