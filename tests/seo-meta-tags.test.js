/**
 * Test Suite: SEO and Meta Tags
 * Scenario: Verify the homepage has proper meta tags for SEO and social sharing
 *
 * Tests verify:
 * - Title tag contains 'MirDB' and is 50-60 characters
 * - Meta description exists and is 150-160 characters
 * - Open Graph tags for social sharing (og:title, og:description, og:image)
 * - Semantic HTML structure (header, main, section, article, footer)
 * - Canonical URL tag
 *
 * These tests use JSDOM to parse and validate the HTML structure.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('SEO and Meta Tags', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Check title tag content
   * Expected: Title tag contains 'MirDB' and is 50-60 characters
   */
  describe('Test Case 1: Title tag content', () => {
    it('should have a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    it('should contain MirDB in the title', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    it('should have title between 50-60 characters', () => {
      const title = document.querySelector('title');
      const titleLength = title.textContent.trim().length;
      expect(titleLength).toBeGreaterThanOrEqual(50);
      expect(titleLength).toBeLessThanOrEqual(60);
    });
  });

  /**
   * Test Case 2: Check meta description
   * Expected: Meta description exists and is 150-160 characters
   */
  describe('Test Case 2: Meta description', () => {
    it('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    it('should have meta description between 150-160 characters', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const descriptionLength = metaDescription.getAttribute('content').trim().length;
      expect(descriptionLength).toBeGreaterThanOrEqual(150);
      expect(descriptionLength).toBeLessThanOrEqual(160);
    });

    it('should contain relevant keywords in meta description', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content').toLowerCase();
      // Should mention key features
      expect(content).toContain('mirdb');
    });
  });

  /**
   * Test Case 3: Check og:title tag
   * Expected: Open Graph title tag is present
   */
  describe('Test Case 3: og:title tag', () => {
    it('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
    });

    it('should have non-empty og:title content', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.trim().length).toBeGreaterThan(0);
    });

    it('should contain MirDB in og:title', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });
  });

  /**
   * Test Case 4: Check og:description tag
   * Expected: Open Graph description tag is present
   */
  describe('Test Case 4: og:description tag', () => {
    it('should have og:description meta tag', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
    });

    it('should have non-empty og:description content', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      const content = ogDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.trim().length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 5: Check og:image tag
   * Expected: Open Graph image tag points to valid image
   */
  describe('Test Case 5: og:image tag', () => {
    it('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
    });

    it('should have og:image with valid image path', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      expect(content).toBeTruthy();
      // Should be a valid URL or image path
      expect(content).toMatch(/\.(png|jpg|jpeg|gif|svg|webp)$/i);
    });
  });

  /**
   * Test Case 6: Verify semantic HTML structure
   * Expected: Page uses header, main, section, article, footer elements appropriately
   */
  describe('Test Case 6: Semantic HTML structure', () => {
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

    it('should have semantic elements in proper hierarchy', () => {
      // Header should be before main
      const header = document.querySelector('header');
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();

      // Check document order using compareDocumentPosition
      // DOCUMENT_POSITION_FOLLOWING = 4 means the reference node follows the argument node
      expect(header.compareDocumentPosition(main) & 4).toBe(4); // main follows header
      expect(main.compareDocumentPosition(footer) & 4).toBe(4); // footer follows main
    });

    it('should have nav element for navigation', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });
  });

  /**
   * Test Case 7: Check canonical URL
   * Expected: Canonical link tag is present with valid URL
   */
  describe('Test Case 7: Canonical URL', () => {
    it('should have a canonical link tag', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    it('should have valid URL in canonical link', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      // Should be a valid URL format
      expect(href).toMatch(/^https?:\/\/.+/);
    });
  });

  /**
   * Additional SEO checks
   */
  describe('Additional SEO checks', () => {
    it('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    it('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      const content = ogUrl.getAttribute('content');
      expect(content).toMatch(/^https?:\/\/.+/);
    });

    it('should have proper charset declaration', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    it('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    it('should have html lang attribute', () => {
      const html = document.querySelector('html');
      expect(html.getAttribute('lang')).toBeTruthy();
    });
  });
});
