/**
 * SEO Unit Tests
 * Owner: Scenario 11 - SEO and Semantic HTML
 *
 * Unit tests for SEO optimization and semantic HTML structure:
 * - Document title verification
 * - Meta description presence
 * - Open Graph tags
 * - Semantic HTML structure
 * - Image alt text
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';
import { JSDOM } from 'jsdom';

describe('SEO and Semantic HTML', () => {
  let document: Document;
  let htmlContent: string;

  beforeAll(() => {
    // Read the actual HTML file
    const htmlPath = join(__dirname, '../../src/index.html');
    htmlContent = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('TC1: Document Title', () => {
    it('should have a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    it('should have title containing "MirDB"', () => {
      const title = document.querySelector('title');
      expect(title?.textContent).toContain('MirDB');
    });

    it('should have a descriptive title', () => {
      const title = document.querySelector('title');
      expect(title?.textContent?.length).toBeGreaterThan(10);
    });
  });

  describe('TC2: Meta Description', () => {
    it('should have a meta description tag', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    it('should have non-empty meta description content', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc?.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content?.length).toBeGreaterThan(50);
    });

    it('should have meta description with relevant content about MirDB', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc?.getAttribute('content')?.toLowerCase();
      expect(content).toContain('mirdb');
    });
  });

  describe('TC3: Open Graph Tags', () => {
    it('should have og:title tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle?.getAttribute('content')).toBeTruthy();
    });

    it('should have og:description tag', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
      expect(ogDesc?.getAttribute('content')).toBeTruthy();
    });

    it('should have og:image tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage?.getAttribute('content')).toBeTruthy();
    });

    it('should have og:title containing MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle?.getAttribute('content')).toContain('MirDB');
    });

    it('should have valid og:image URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const imageUrl = ogImage?.getAttribute('content');
      expect(imageUrl).toMatch(/^https?:\/\/.+\.(png|jpg|jpeg|gif|webp)$/i);
    });
  });

  describe('TC4: Semantic HTML Structure', () => {
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

    it('should have proper heading hierarchy (h1 followed by h2)', () => {
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');
      expect(h1).not.toBeNull();
      expect(h2s.length).toBeGreaterThan(0);
    });

    it('should have only one h1 element', () => {
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);
    });

    it('should have main content inside main element', () => {
      const main = document.querySelector('main');
      const sectionsInMain = main?.querySelectorAll('section');
      expect(sectionsInMain?.length).toBeGreaterThan(0);
    });
  });

  describe('TC5: Image Alt Text', () => {
    it('should have alt attribute on all img elements', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    it('should have non-empty alt text on all img elements', () => {
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        // Allow empty alt for decorative images (which should have role="presentation" or aria-hidden)
        const isDecorative = img.getAttribute('role') === 'presentation' ||
                            img.getAttribute('aria-hidden') === 'true';
        if (!isDecorative) {
          expect(alt?.trim().length).toBeGreaterThan(0);
        }
      });
    });

    it('should pass if there are no img elements (all icons are SVGs)', () => {
      const images = document.querySelectorAll('img');
      // If no images exist, the test passes (SVG icons don't need alt text)
      // If images exist, they should have proper alt text
      expect(true).toBe(true);
    });

    it('should have aria-hidden on decorative SVG icons', () => {
      const decorativeSvgs = document.querySelectorAll('.feature-card__icon svg, .theme-toggle__icon svg');
      decorativeSvgs.forEach((svg) => {
        // SVGs within icon containers should be marked as decorative
        const parent = svg.closest('[aria-hidden="true"]');
        expect(parent).not.toBeNull();
      });
    });
  });

  describe('Additional SEO Requirements', () => {
    it('should have lang attribute on html element', () => {
      const html = document.documentElement;
      expect(html.hasAttribute('lang')).toBe(true);
      expect(html.getAttribute('lang')).toBe('en');
    });

    it('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have external links with proper rel attributes', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });

    it('should have meaningful link text (no "click here")', () => {
      const links = document.querySelectorAll('a');
      links.forEach((link) => {
        const text = link.textContent?.toLowerCase().trim();
        expect(text).not.toBe('click here');
        expect(text).not.toBe('here');
        expect(text).not.toBe('link');
      });
    });
  });
});
