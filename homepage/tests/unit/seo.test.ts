/**
 * SEO Unit Tests
 * Owner: Scenario 11 - SEO and Semantic HTML
 *
 * Tests for:
 * - Document title verification
 * - Meta description presence
 * - Open Graph tags
 * - Semantic HTML structure
 * - Image alt text
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('SEO and Semantic HTML', () => {
  let document: Document;

  beforeAll(() => {
    const htmlPath = resolve(__dirname, '../../src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('Test Case 1: Document Title', () => {
    it('should have a title tag containing "MirDB"', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title?.textContent).toContain('MirDB');
    });

    it('should have a descriptive title', () => {
      const title = document.querySelector('title');
      expect(title?.textContent?.length).toBeGreaterThan(10);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    it('should have relevant content in meta description', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription?.getAttribute('content') || '';

      expect(content.length).toBeGreaterThan(50);
      expect(content.toLowerCase()).toMatch(/mirdb|memcached|key-value|persistent/i);
    });

    it('should have meta description between 50-160 characters (SEO best practice)', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription?.getAttribute('content') || '';

      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  describe('Test Case 3: Open Graph Tags', () => {
    it('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle?.getAttribute('content')).toBeTruthy();
    });

    it('should have og:description meta tag', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      expect(ogDescription?.getAttribute('content')).toBeTruthy();
    });

    it('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage?.getAttribute('content')).toBeTruthy();
    });

    it('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType?.getAttribute('content')).toBe('website');
    });

    it('og:title should contain MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle?.getAttribute('content')).toContain('MirDB');
    });
  });

  describe('Test Case 4: Semantic HTML Structure', () => {
    it('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have proper heading hierarchy starting with h1', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
    });

    it('should have only one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have nav element for navigation', () => {
      const nav = document.querySelectorAll('nav');
      expect(nav.length).toBeGreaterThan(0);
    });

    it('should have article elements for feature cards', () => {
      const articles = document.querySelectorAll('article');
      expect(articles.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 5: Image Alt Text', () => {
    it('should have all img elements with non-empty alt attributes', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt, `Image ${index + 1} should have alt attribute`).not.toBeNull();
        expect(alt?.trim(), `Image ${index + 1} should have non-empty alt text`).not.toBe('');
      });
    });

    it('should have decorative SVGs marked with aria-hidden', () => {
      const svgs = document.querySelectorAll('svg');

      svgs.forEach((svg) => {
        const ariaHidden = svg.getAttribute('aria-hidden');
        const role = svg.getAttribute('role');
        const ariaLabel = svg.getAttribute('aria-label');

        // Check if any parent has aria-hidden="true" (inherited accessibility)
        let parent = svg.parentElement;
        let parentHasAriaHidden = false;
        while (parent) {
          if (parent.getAttribute('aria-hidden') === 'true') {
            parentHasAriaHidden = true;
            break;
          }
          parent = parent.parentElement;
        }

        // SVGs should either be decorative (aria-hidden on self or parent) or have accessible name
        const isAccessible = ariaHidden === 'true' || parentHasAriaHidden || role === 'img' || ariaLabel;
        expect(isAccessible, 'SVG should be marked aria-hidden or have accessible name').toBeTruthy();
      });
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset?.getAttribute('charset')?.toLowerCase()).toBe('utf-8');
    });

    it('should have viewport meta tag for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport?.getAttribute('content')).toContain('width=device-width');
    });

    it('should have lang attribute on html element', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang).toBe('en');
    });

    it('should have external links with rel="noopener"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel, 'External links should have rel attribute').not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    it('should have skip navigation link for accessibility', () => {
      const skipLink = document.querySelector('a[href="#main-content"]');
      expect(skipLink).not.toBeNull();
    });
  });
});
