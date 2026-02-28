/**
 * SEO Meta Tags Unit Tests
 * Owner: Scenario 11 - SEO Meta Tags
 *
 * Tests:
 * - Title tag present with product name
 * - Meta description present and valid length
 * - Viewport meta tag configured
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { loadDocument, getDocument } from '../../setup.js';

describe('SEO Meta Tags', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadDocument();
    document = getDocument(dom);
  });

  describe('Page Title', () => {
    it('should have a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    it('should contain "MirDB" in the title', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    it('should have descriptive text in the title', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      // Title should be more than just the product name
      expect(title.textContent.length).toBeGreaterThan(6); // More than "MirDB"
    });

    it('should have a title containing MirDB and descriptive text', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
      // Title should describe the product
      expect(title.textContent.toLowerCase()).toMatch(/key.*value|store|database|persistent/i);
    });
  });

  describe('Meta Description', () => {
    it('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    it('should have content in the meta description', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);
    });

    it('should describe the key-value store product', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content).not.toBeNull();
      // Content should describe MirDB as a key-value store
      expect(content.toLowerCase()).toMatch(/key.*value|store|database|persistent/i);
    });

    it('should have meta description between 120-160 characters for optimal SEO', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content).not.toBeNull();
      const length = content.length;
      expect(length).toBeGreaterThanOrEqual(120);
      expect(length).toBeLessThanOrEqual(160);
    });
  });

  describe('Viewport Meta Tag', () => {
    it('should have a viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have width=device-width in viewport', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content).toContain('width=device-width');
    });

    it('should have initial-scale=1 in viewport', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content).toMatch(/initial-scale\s*=\s*1/);
    });

    it('should have viewport configured with width=device-width and initial-scale=1', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content).toContain('width=device-width');
      expect(content).toMatch(/initial-scale\s*=\s*1/);
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    it('should have html lang attribute', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      expect(html.hasAttribute('lang')).toBe(true);
      expect(html.getAttribute('lang')).toBe('en');
    });
  });
});
