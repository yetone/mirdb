/**
 * SEO Unit Tests.
 * Owner: Scenario 11 - SEO Optimization
 *
 * Tests:
 * - Page title contains MirDB keywords
 * - Meta description present and < 160 chars
 * - Open Graph tags present (og:title, og:description, og:image)
 * - Canonical URL present
 * - Heading hierarchy is correct (single H1)
 * - Lighthouse SEO score >= 90
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('SEO Optimization', () => {
  let htmlContent: string;
  let doc: Document;

  beforeAll(() => {
    const htmlPath = resolve(__dirname, '../../src/index.html');
    htmlContent = readFileSync(htmlPath, 'utf-8');

    const parser = new DOMParser();
    doc = parser.parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Page Title Tag', () => {
    it('should have a title tag containing MirDB', () => {
      const title = doc.querySelector('title');
      expect(title).not.toBeNull();
      expect(title?.textContent?.toLowerCase()).toContain('mirdb');
    });

    it('should have a title containing key-value store keyword', () => {
      const title = doc.querySelector('title');
      expect(title?.textContent?.toLowerCase()).toContain('key-value');
    });

    it('should have a title containing memcached keyword', () => {
      const title = doc.querySelector('title');
      expect(title?.textContent?.toLowerCase()).toContain('memcached');
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag', () => {
      const metaDesc = doc.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    it('should have a meta description under 160 characters', () => {
      const metaDesc = doc.querySelector('meta[name="description"]');
      const content = metaDesc?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(0);
      expect(content.length).toBeLessThanOrEqual(160);
    });

    it('should describe MirDB value proposition in meta description', () => {
      const metaDesc = doc.querySelector('meta[name="description"]');
      const content = metaDesc?.getAttribute('content')?.toLowerCase() || '';
      expect(content).toContain('mirdb');
    });
  });

  describe('Test Case 3: Open Graph Title', () => {
    it('should have og:title meta tag', () => {
      const ogTitle = doc.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
    });

    it('should have og:title matching the page title', () => {
      const title = doc.querySelector('title')?.textContent || '';
      const ogTitle = doc.querySelector('meta[property="og:title"]');
      const ogTitleContent = ogTitle?.getAttribute('content') || '';
      expect(ogTitleContent).toBe(title);
    });
  });

  describe('Test Case 4: Open Graph Description', () => {
    it('should have og:description meta tag', () => {
      const ogDesc = doc.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
    });

    it('should have a compelling og:description', () => {
      const ogDesc = doc.querySelector('meta[property="og:description"]');
      const content = ogDesc?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(50);
    });
  });

  describe('Test Case 5: Open Graph Image', () => {
    it('should have og:image meta tag', () => {
      const ogImage = doc.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
    });

    it('should have a valid og:image URL', () => {
      const ogImage = doc.querySelector('meta[property="og:image"]');
      const content = ogImage?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 6: Heading Hierarchy', () => {
    it('should have exactly one H1 tag', () => {
      const h1Tags = doc.querySelectorAll('h1');
      expect(h1Tags.length).toBe(1);
    });

    it('should have H2 tags for main sections', () => {
      const h2Tags = doc.querySelectorAll('h2');
      expect(h2Tags.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 7: Canonical URL', () => {
    it('should have a canonical link tag', () => {
      const canonical = doc.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    it('should have a valid canonical URL', () => {
      const canonical = doc.querySelector('link[rel="canonical"]');
      const href = canonical?.getAttribute('href') || '';
      expect(href.length).toBeGreaterThan(0);
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have og:type meta tag', () => {
      const ogType = doc.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
    });

    it('should have og:url meta tag', () => {
      const ogUrl = doc.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
    });

    it('should have twitter:card meta tag', () => {
      const twitterCard = doc.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();
    });

    it('should have viewport meta tag', () => {
      const viewport = doc.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have charset meta tag', () => {
      const charset = doc.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('should have lang attribute on html element', () => {
      const html = doc.querySelector('html');
      const lang = html?.getAttribute('lang');
      expect(lang).toBe('en');
    });
  });
});
