/**
 * SEO Optimization Tests
 *
 * Verifies proper meta tags and semantic HTML for SEO (NFR-5)
 * Tests ensure the page has proper SEO elements including title, meta tags,
 * and semantic HTML structure.
 */

const fs = require('fs');
const path = require('path');

describe('SEO Optimization', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Title Tag', () => {
    it('should have a title tag containing "MirDB"', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    it('should have a descriptive title', () => {
      const title = document.querySelector('title');
      expect(title.textContent.length).toBeGreaterThan(10);
      expect(title.textContent.length).toBeLessThanOrEqual(60);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    it('should have a relevant product description', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(50);
      expect(content.length).toBeLessThanOrEqual(160);
      // Should contain relevant keywords
      expect(content.toLowerCase()).toMatch(/mirdb|key-value|memcached|persistent/i);
    });
  });

  describe('Test Case 3: Viewport Meta Tag', () => {
    it('should have a viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should be set for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });
  });

  describe('Test Case 4: Charset Meta Tag', () => {
    it('should have a charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('should be set to UTF-8', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      const charsetValue = charset.getAttribute('charset');
      expect(charsetValue.toUpperCase()).toBe('UTF-8');
    });
  });

  describe('Test Case 5: Semantic HTML Structure', () => {
    it('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have section elements for content organization', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have proper heading hierarchy starting with h1', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    it('should have only one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should use article elements for self-contained content', () => {
      const articles = document.querySelectorAll('article');
      expect(articles.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 6: Additional SEO Best Practices', () => {
    it('should have lang attribute on html element', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      const lang = html.getAttribute('lang');
      expect(lang).toBeTruthy();
      expect(lang).toBe('en');
    });

    it('should have proper doctype declaration', () => {
      expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);
    });

    it('should have external links with rel="noopener"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });

    it('should have descriptive link text (no generic "click here")', () => {
      const links = document.querySelectorAll('a');
      links.forEach(link => {
        const text = link.textContent.toLowerCase().trim();
        expect(text).not.toBe('click here');
        expect(text).not.toBe('read more');
        expect(text).not.toBe('here');
      });
    });
  });
});
