/**
 * SEO and Meta Tags Unit Tests
 * Owner: Scenario 14 - SEO and Meta Tags
 *
 * Tests for:
 * - Title tag presence and content
 * - Meta description tag
 * - Open Graph meta tags (og:title, og:description, og:image, og:type, og:url)
 * - Twitter Card meta tags
 * - Canonical URL link
 * - Robots meta tag
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('SEO and Meta Tags', () => {
  let html: string;

  beforeAll(() => {
    const htmlPath = resolve(__dirname, '../../index.html');
    html = readFileSync(htmlPath, 'utf-8');
  });

  describe('Basic SEO', () => {
    it('should have a title tag containing MirDB', () => {
      const titleMatch = html.match(/<title[^>]*>(.*?)<\/title>/i);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch![1]).toContain('MirDB');
    });

    it('should have a meta description tag describing MirDB', () => {
      const descMatch = html.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["']/i);
      expect(descMatch).not.toBeNull();
      expect(descMatch![1].toLowerCase()).toContain('mirdb');
      expect(descMatch![1].length).toBeGreaterThan(50); // Descriptions should be meaningful
      expect(descMatch![1].length).toBeLessThan(160); // SEO best practice
    });
  });

  describe('Open Graph Meta Tags', () => {
    it('should have og:title meta tag', () => {
      const ogTitleMatch = html.match(/<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i);
      expect(ogTitleMatch).not.toBeNull();
      expect(ogTitleMatch![1]).toContain('MirDB');
    });

    it('should have og:description meta tag', () => {
      const ogDescMatch = html.match(/<meta\s+property=["']og:description["']\s+content=["']([^"']+)["']/i);
      expect(ogDescMatch).not.toBeNull();
      expect(ogDescMatch![1].length).toBeGreaterThan(20);
    });

    it('should have og:image meta tag for social preview', () => {
      const ogImageMatch = html.match(/<meta\s+property=["']og:image["']\s+content=["']([^"']+)["']/i);
      expect(ogImageMatch).not.toBeNull();
      expect(ogImageMatch![1]).toMatch(/\.(png|jpg|jpeg|webp|gif)$/i);
    });

    it('should have og:type meta tag', () => {
      const ogTypeMatch = html.match(/<meta\s+property=["']og:type["']\s+content=["']([^"']+)["']/i);
      expect(ogTypeMatch).not.toBeNull();
      expect(ogTypeMatch![1]).toBe('website');
    });

    it('should have og:url meta tag', () => {
      const ogUrlMatch = html.match(/<meta\s+property=["']og:url["']\s+content=["']([^"']+)["']/i);
      expect(ogUrlMatch).not.toBeNull();
      expect(ogUrlMatch![1]).toMatch(/^https?:\/\//);
    });
  });

  describe('Twitter Card Meta Tags', () => {
    it('should have twitter:card meta tag', () => {
      const twitterCardMatch = html.match(/<meta\s+name=["']twitter:card["']\s+content=["']([^"']+)["']/i);
      expect(twitterCardMatch).not.toBeNull();
      expect(['summary', 'summary_large_image']).toContain(twitterCardMatch![1]);
    });

    it('should have twitter:title meta tag', () => {
      const twitterTitleMatch = html.match(/<meta\s+name=["']twitter:title["']\s+content=["']([^"']+)["']/i);
      expect(twitterTitleMatch).not.toBeNull();
      expect(twitterTitleMatch![1]).toContain('MirDB');
    });

    it('should have twitter:description meta tag', () => {
      const twitterDescMatch = html.match(/<meta\s+name=["']twitter:description["']\s+content=["']([^"']+)["']/i);
      expect(twitterDescMatch).not.toBeNull();
      expect(twitterDescMatch![1].length).toBeGreaterThan(20);
    });

    it('should have twitter:image meta tag', () => {
      const twitterImageMatch = html.match(/<meta\s+name=["']twitter:image["']\s+content=["']([^"']+)["']/i);
      expect(twitterImageMatch).not.toBeNull();
      expect(twitterImageMatch![1]).toMatch(/\.(png|jpg|jpeg|webp|gif)$/i);
    });
  });

  describe('Canonical URL', () => {
    it('should have canonical link tag pointing to homepage URL', () => {
      const canonicalMatch = html.match(/<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/i);
      expect(canonicalMatch).not.toBeNull();
      expect(canonicalMatch![1]).toMatch(/^https?:\/\//);
    });
  });

  describe('Robots Meta Tag', () => {
    it('should allow indexing (index, follow) or have no robots meta tag', () => {
      const robotsMatch = html.match(/<meta\s+name=["']robots["']\s+content=["']([^"']+)["']/i);

      if (robotsMatch) {
        // If robots meta exists, it should allow indexing
        const content = robotsMatch[1].toLowerCase();
        expect(content).not.toContain('noindex');
        expect(content).not.toContain('nofollow');
      }
      // If no robots meta tag, search engines default to index, follow - which is acceptable
      expect(true).toBe(true);
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have proper charset meta tag', () => {
      const charsetMatch = html.match(/<meta\s+charset=["']([^"']+)["']/i);
      expect(charsetMatch).not.toBeNull();
      expect(charsetMatch![1].toLowerCase()).toBe('utf-8');
    });

    it('should have viewport meta tag for mobile', () => {
      const viewportMatch = html.match(/<meta\s+name=["']viewport["']\s+content=["']([^"']+)["']/i);
      expect(viewportMatch).not.toBeNull();
      expect(viewportMatch![1]).toContain('width=device-width');
    });

    it('should have lang attribute on html element', () => {
      const langMatch = html.match(/<html[^>]*\slang=["']([^"']+)["']/i);
      expect(langMatch).not.toBeNull();
      expect(langMatch![1]).toBe('en');
    });
  });
});
