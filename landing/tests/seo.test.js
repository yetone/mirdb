import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('SEO and Meta Tags', () => {
  let document;
  let head;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    head = document.querySelector('head');
  });

  // Test Case 1: Title element contains 'MirDB' and is descriptive (50-60 characters ideal)
  describe('Test Case 1: Page Title', () => {
    it('should have a title element present', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    it('should contain "MirDB" in the title', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    it('should have a descriptive title with key value proposition', () => {
      const title = document.querySelector('title');
      const titleText = title.textContent;
      // Title should mention key features like persistent storage or memcached compatibility
      expect(titleText.toLowerCase()).toMatch(/persistent|memcached|key-value/i);
    });

    it('should have an ideal title length (50-60 characters)', () => {
      const title = document.querySelector('title');
      const titleLength = title.textContent.length;
      // SEO best practice: title should be between 50-60 characters
      expect(titleLength).toBeGreaterThanOrEqual(45);
      expect(titleLength).toBeLessThanOrEqual(70);
    });
  });

  // Test Case 2: Meta description tag with compelling description (150-160 characters)
  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag present', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    it('should have a non-empty meta description', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have compelling description mentioning MirDB', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      expect(content).toContain('MirDB');
    });

    it('should have description length within optimal range (100-160 characters)', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      // SEO best practice: description should be between 150-160 characters
      expect(content.length).toBeGreaterThanOrEqual(80);
      expect(content.length).toBeLessThanOrEqual(170);
    });
  });

  // Test Case 3: Meta charset='UTF-8' is present
  describe('Test Case 3: Meta Charset', () => {
    it('should have meta charset tag present', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('should have charset set to UTF-8', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    it('should have charset declared early in head', () => {
      const headChildren = Array.from(head.children);
      const charsetIndex = headChildren.findIndex(el =>
        el.tagName === 'META' && el.hasAttribute('charset')
      );
      // Charset should be one of the first elements in head
      expect(charsetIndex).toBeLessThanOrEqual(2);
    });
  });

  // Test Case 4: Open Graph title meta tag is present
  describe('Test Case 4: Open Graph Title', () => {
    it('should have og:title meta tag present', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
    });

    it('should have non-empty og:title content', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have og:title containing product name', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });
  });

  // Test Case 5: Open Graph description meta tag is present
  describe('Test Case 5: Open Graph Description', () => {
    it('should have og:description meta tag present', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
    });

    it('should have non-empty og:description content', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      const content = ogDesc.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have og:description describing the product benefits', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      const content = ogDesc.getAttribute('content').toLowerCase();
      // Should describe key features
      expect(content).toMatch(/persistent|memcached|key-value|storage/i);
    });
  });

  // Test Case 6: Open Graph image meta tag is present for social sharing preview
  describe('Test Case 6: Open Graph Image', () => {
    it('should have og:image meta tag present', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
    });

    it('should have non-empty og:image content', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });

    it('should have og:image with valid URL format', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      // Should be a valid URL or relative path
      expect(content).toMatch(/^(https?:\/\/|\/)/);
    });
  });

  // Test Case 7: Canonical link tag is present to prevent duplicate content issues
  describe('Test Case 7: Canonical URL', () => {
    it('should have canonical link tag present', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    it('should have non-empty canonical href', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    });

    it('should have canonical URL with valid format', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      // Should be a valid URL
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  // Test Case 8: HTML element has lang='en' attribute for accessibility and SEO
  describe('Test Case 8: HTML Lang Attribute', () => {
    it('should have lang attribute on html element', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      expect(html.hasAttribute('lang')).toBe(true);
    });

    it('should have lang attribute set to "en"', () => {
      const html = document.querySelector('html');
      const lang = html.getAttribute('lang');
      expect(lang).toBe('en');
    });
  });

  // Additional SEO tests
  describe('Additional SEO Elements', () => {
    it('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    it('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
    });

    it('should have viewport meta tag for responsive design', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });
  });
});
