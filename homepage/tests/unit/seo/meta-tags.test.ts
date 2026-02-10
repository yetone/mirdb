/**
 * SEO Meta Tags Unit Tests
 * Owner: Scenario 11 - SEO and Meta Tags
 *
 * Tests for validating SEO meta tags in index.html:
 * - Title tag contains 'MirDB'
 * - Meta description is present
 * - Open Graph meta tags are present
 * - Canonical link tag is present
 * - Robots meta tag allows indexing
 */

import { describe, it, expect, beforeAll } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

describe('SEO Meta Tags', () => {
  let htmlContent: string;
  let parser: DOMParser;
  let doc: Document;

  beforeAll(() => {
    const indexPath = path.resolve(__dirname, '../../../index.html');
    htmlContent = fs.readFileSync(indexPath, 'utf-8');
    parser = new DOMParser();
    doc = parser.parseFromString(htmlContent, 'text/html');
  });

  describe('Title Tag', () => {
    it('should contain MirDB in the title', () => {
      const title = doc.querySelector('title');
      expect(title).toBeTruthy();
      expect(title?.textContent).toContain('MirDB');
    });

    it('should have a descriptive title', () => {
      const title = doc.querySelector('title');
      expect(title?.textContent?.length).toBeGreaterThan(10);
    });
  });

  describe('Meta Description', () => {
    it('should have a meta description tag', () => {
      const description = doc.querySelector('meta[name="description"]');
      expect(description).toBeTruthy();
    });

    it('should have relevant content in description', () => {
      const description = doc.querySelector('meta[name="description"]');
      const content = description?.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content?.length).toBeGreaterThan(50);
      expect(content).toMatch(/mirdb|key-value|memcached/i);
    });
  });

  describe('Open Graph Meta Tags', () => {
    it('should have og:title tag', () => {
      const ogTitle = doc.querySelector('meta[property="og:title"]');
      expect(ogTitle).toBeTruthy();
      expect(ogTitle?.getAttribute('content')).toContain('MirDB');
    });

    it('should have og:description tag', () => {
      const ogDescription = doc.querySelector('meta[property="og:description"]');
      expect(ogDescription).toBeTruthy();
      const content = ogDescription?.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content?.length).toBeGreaterThan(50);
    });

    it('should have og:image tag', () => {
      const ogImage = doc.querySelector('meta[property="og:image"]');
      expect(ogImage).toBeTruthy();
      const content = ogImage?.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });

    it('should have og:type tag', () => {
      const ogType = doc.querySelector('meta[property="og:type"]');
      expect(ogType).toBeTruthy();
      expect(ogType?.getAttribute('content')).toBe('website');
    });

    it('should have og:url tag', () => {
      const ogUrl = doc.querySelector('meta[property="og:url"]');
      expect(ogUrl).toBeTruthy();
      const content = ogUrl?.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });
  });

  describe('Canonical Link', () => {
    it('should have a canonical link tag', () => {
      const canonical = doc.querySelector('link[rel="canonical"]');
      expect(canonical).toBeTruthy();
    });

    it('should have a valid URL in canonical link', () => {
      const canonical = doc.querySelector('link[rel="canonical"]');
      const href = canonical?.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  describe('Robots Meta Tag', () => {
    it('should have a robots meta tag', () => {
      const robots = doc.querySelector('meta[name="robots"]');
      expect(robots).toBeTruthy();
    });

    it('should allow indexing', () => {
      const robots = doc.querySelector('meta[name="robots"]');
      const content = robots?.getAttribute('content');
      expect(content).toBeTruthy();
      // Should contain "index" and "follow" or equivalent
      expect(content).toMatch(/index/i);
      expect(content).not.toMatch(/noindex/i);
    });
  });

  describe('Additional SEO Tags', () => {
    it('should have charset defined', () => {
      const charset = doc.querySelector('meta[charset]');
      expect(charset).toBeTruthy();
      expect(charset?.getAttribute('charset')?.toLowerCase()).toBe('utf-8');
    });

    it('should have viewport meta tag', () => {
      const viewport = doc.querySelector('meta[name="viewport"]');
      expect(viewport).toBeTruthy();
      expect(viewport?.getAttribute('content')).toContain('width=device-width');
    });

    it('should have lang attribute on html element', () => {
      const html = doc.documentElement;
      expect(html.getAttribute('lang')).toBe('en');
    });
  });
});
