/**
 * SEO tests for MirDB homepage.
 * Owner: Scenario 9 - SEO and Meta Tags
 *
 * Test framework: Vitest + jsdom
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import path from 'node:path';

function loadDOM() {
  const html = fs.readFileSync(
    path.resolve(__dirname, '../index.html'),
    'utf-8'
  );
  const dom = new JSDOM(html, { url: 'https://mirdb.io' });
  return dom;
}

describe('SEO and Meta Tags', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadDOM();
    document = dom.window.document;
  });

  describe('Test Case 1: Title tag content', () => {
    it('should have a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    it('should contain "MirDB" in the title', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toMatch(/MirDB/i);
    });

    it('should contain relevant keywords (key-value store, persistent, memcached)', () => {
      const title = document.querySelector('title');
      const text = title.textContent.toLowerCase();
      const hasKeyValue = text.includes('key-value') || text.includes('key value');
      const hasPersistent = text.includes('persistent');
      const hasMemcached = text.includes('memcached');
      expect(hasKeyValue || hasPersistent || hasMemcached).toBe(true);
    });

    it('should have title length between 50-60 characters for optimal display', () => {
      const title = document.querySelector('title');
      const length = title.textContent.trim().length;
      expect(length).toBeGreaterThanOrEqual(50);
      expect(length).toBeLessThanOrEqual(60);
    });
  });

  describe('Test Case 2: Meta description tag content', () => {
    it('should have a meta description tag', () => {
      const meta = document.querySelector('meta[name="description"]');
      expect(meta).not.toBeNull();
    });

    it('should describe MirDB as a persistent key-value store with memcached protocol', () => {
      const meta = document.querySelector('meta[name="description"]');
      const content = meta.getAttribute('content').toLowerCase();
      expect(content).toMatch(/key.value|persistent|memcached/);
    });

    it('should have description between 120-160 characters', () => {
      const meta = document.querySelector('meta[name="description"]');
      const content = meta.getAttribute('content').trim();
      expect(content.length).toBeGreaterThanOrEqual(120);
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  describe('Test Case 3: Open Graph meta tags', () => {
    it('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have og:description meta tag', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
      expect(ogDesc.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have og:image meta tag pointing to logo.gif', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage.getAttribute('content')).toMatch(/logo\.gif/i);
    });

    it('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      expect(ogUrl.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have og:type meta tag set to "website"', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    it('should have Twitter card meta tags or OG tags serve as fallback', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      // Twitter card should be present
      expect(twitterCard).not.toBeNull();
      expect(twitterCard.getAttribute('content')).toMatch(/summary/);
    });
  });

  describe('Test Case 4: Heading hierarchy', () => {
    it('should have a single h1 for the main headline', () => {
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);
    });

    it('should have h2 headings for major section titles', () => {
      const h2s = document.querySelectorAll('h2');
      expect(h2s.length).toBeGreaterThanOrEqual(1);
    });

    it('should have h3 headings for sub-sections', () => {
      const h3s = document.querySelectorAll('h3');
      expect(h3s.length).toBeGreaterThanOrEqual(1);
    });

    it('should not skip heading levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const levels = Array.from(headings).map((h) =>
        parseInt(h.tagName.charAt(1), 10)
      );

      for (let i = 1; i < levels.length; i++) {
        const diff = levels[i] - levels[i - 1];
        expect(diff).toBeLessThanOrEqual(1);
      }
    });

    it('should have h1 come before any h2 on the page', () => {
      const headings = document.querySelectorAll('h1, h2');
      if (headings.length > 0) {
        const firstHeading = headings[0];
        expect(firstHeading.tagName).toBe('H1');
      }
    });
  });

  describe('Test Case 5: Canonical link tag', () => {
    it('should have a canonical link tag', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    it('should have a valid href on the canonical link', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      expect(() => new URL(href)).not.toThrow();
    });

    it('should not have conflicting canonical URLs', () => {
      const canonicals = document.querySelectorAll('link[rel="canonical"]');
      expect(canonicals.length).toBe(1);
    });
  });

  describe('Test Case 6: Viewport meta tag', () => {
    it('should have a viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have content="width=device-width, initial-scale=1.0"', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      const content = viewport.getAttribute('content');
      expect(content).toMatch(/width=device-width/);
      expect(content).toMatch(/initial-scale=1(.0)?/);
    });
  });

  describe('Test Case 7: Robots meta tag', () => {
    it('should have a robots meta tag that allows indexing', () => {
      const robots = document.querySelector('meta[name="robots"]');
      expect(robots).not.toBeNull();
      const content = robots.getAttribute('content');
      expect(content).not.toMatch(/noindex/);
    });

    it('should allow following links', () => {
      const robots = document.querySelector('meta[name="robots"]');
      const content = robots.getAttribute('content');
      expect(content).not.toMatch(/nofollow/);
    });
  });

  describe('Test Case 8: Additional SEO best practices', () => {
    it('should have html lang attribute set', () => {
      const html = document.querySelector('html');
      expect(html.getAttribute('lang')).toBe('en');
    });

    it('should have meta charset defined', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('should have meaningful link text (no "click here" links)', () => {
      const links = document.querySelectorAll('a');
      for (const link of links) {
        const text = link.textContent.trim().toLowerCase();
        if (text.length > 0) {
          expect(text).not.toBe('click here');
          expect(text).not.toBe('here');
        }
      }
    });

    it('should have meta keywords tag', () => {
      const keywords = document.querySelector('meta[name="keywords"]');
      expect(keywords).not.toBeNull();
      const content = keywords.getAttribute('content');
      expect(content).toMatch(/mirdb|key.value|memcached/i);
    });
  });
});
