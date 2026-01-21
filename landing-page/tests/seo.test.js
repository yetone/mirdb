import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * SEO Optimization Tests
 * Testing NFR-5: SEO-optimized structure with proper meta tags, headings, and semantic HTML
 */

describe('SEO Optimization', () => {
  let dom;
  let document;

  beforeEach(() => {
    // Load the HTML file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost:3000',
    });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Check page title tag
   * Expected: Title tag is present with descriptive content under 60 characters
   */
  describe('Test Case 1: Page Title Tag', () => {
    it('should have a title tag in the head', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
    });

    it('should have descriptive content in the title', () => {
      const title = document.querySelector('head title');
      const titleText = title?.textContent?.trim() || '';
      expect(titleText.length).toBeGreaterThan(0);
      expect(titleText).toContain('MirDB');
    });

    it('should have title under 60 characters for optimal SEO', () => {
      const title = document.querySelector('head title');
      const titleText = title?.textContent?.trim() || '';
      expect(titleText.length).toBeLessThanOrEqual(60);
    });
  });

  /**
   * Test Case 2: Verify meta description
   * Expected: Meta description is present with 150-160 characters
   */
  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    it('should have content in the meta description', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have meta description between 150-160 characters', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThanOrEqual(150);
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  /**
   * Test Case 3: Check Open Graph meta tags
   * Expected: OG title, description, and image tags are present
   */
  describe('Test Case 3: Open Graph Meta Tags', () => {
    it('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      const content = ogTitle?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have og:description meta tag', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
      const content = ogDesc?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      const content = ogImage?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      const content = ogType?.getAttribute('content') || '';
      expect(content).toBe('website');
    });

    it('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      const content = ogUrl?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 4: Verify semantic HTML elements
   * Expected: Page uses header, main, section, and footer elements
   */
  describe('Test Case 4: Semantic HTML Elements', () => {
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

    it('should have nav element for navigation', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('should have main content inside main element', () => {
      const main = document.querySelector('main');
      const sections = main?.querySelectorAll('section') || [];
      expect(sections.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 5: Check canonical URL
   * Expected: Canonical link tag is present
   */
  describe('Test Case 5: Canonical URL', () => {
    it('should have a canonical link tag', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    it('should have href attribute in canonical link', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical?.getAttribute('href') || '';
      expect(href.length).toBeGreaterThan(0);
    });

    it('should have a valid URL in canonical link', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical?.getAttribute('href') || '';
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  /**
   * Additional SEO Tests: Heading Structure
   */
  describe('Heading Structure', () => {
    it('should have exactly one H1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have H1 with meaningful content', () => {
      const h1 = document.querySelector('h1');
      const content = h1?.textContent?.trim() || '';
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have H2 elements for section headings', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThan(0);
    });

    it('should follow proper heading hierarchy (no skipped levels)', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let previousLevel = 0;

      for (const heading of headings) {
        const currentLevel = parseInt(heading.tagName.charAt(1));
        // Heading level should not skip more than 1 level from previous
        if (previousLevel > 0) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
        previousLevel = currentLevel;
      }
    });
  });

  /**
   * Additional SEO Tests: Essential Meta Tags
   */
  describe('Essential Meta Tags', () => {
    it('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset?.getAttribute('charset')?.toLowerCase()).toBe('utf-8');
    });

    it('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have lang attribute on html element', () => {
      const html = document.querySelector('html');
      const lang = html?.getAttribute('lang') || '';
      expect(lang.length).toBeGreaterThan(0);
    });
  });
});
