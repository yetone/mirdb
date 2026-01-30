/**
 * SEO and Meta Tags Tests
 * Owner: Scenario 14 - SEO and Meta Tags
 *
 * Tests:
 * - Title tag content
 * - Meta description
 * - Open Graph tags
 * - Canonical URL
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

const HTML_PATH = path.join(__dirname, '../../index.html');

test.describe('SEO and Meta Tags', () => {
  let htmlContent;

  test.beforeAll(() => {
    htmlContent = fs.readFileSync(HTML_PATH, 'utf-8');
  });

  test.describe('Title Tag', () => {
    test('page has a title tag', () => {
      const titleMatch = htmlContent.match(/<title[^>]*>(.*?)<\/title>/i);
      expect(titleMatch).not.toBeNull();
    });

    test('title contains MirDB', () => {
      const titleMatch = htmlContent.match(/<title[^>]*>(.*?)<\/title>/i);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch[1]).toContain('MirDB');
    });

    test('title describes the product', () => {
      const titleMatch = htmlContent.match(/<title[^>]*>(.*?)<\/title>/i);
      expect(titleMatch).not.toBeNull();
      const title = titleMatch[1].toLowerCase();
      // Title should mention key aspects of the product
      expect(
        title.includes('key-value') ||
          title.includes('memcached') ||
          title.includes('persistent') ||
          title.includes('store')
      ).toBe(true);
    });
  });

  test.describe('Meta Description', () => {
    test('meta description exists', () => {
      const descMatch = htmlContent.match(
        /<meta\s+name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const descMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*name=["']description["'][^>]*>/i
      );
      expect(descMatch || descMatchAlt).not.toBeNull();
    });

    test('meta description mentions key-value store', () => {
      const descMatch = htmlContent.match(
        /<meta\s+name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const descMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*name=["']description["'][^>]*>/i
      );
      const match = descMatch || descMatchAlt;
      expect(match).not.toBeNull();
      const description = match[1].toLowerCase();
      expect(description.includes('key-value') || description.includes('key value')).toBe(true);
    });

    test('meta description mentions memcached', () => {
      const descMatch = htmlContent.match(
        /<meta\s+name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const descMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*name=["']description["'][^>]*>/i
      );
      const match = descMatch || descMatchAlt;
      expect(match).not.toBeNull();
      const description = match[1].toLowerCase();
      expect(description.includes('memcached')).toBe(true);
    });

    test('meta description has reasonable length', () => {
      const descMatch = htmlContent.match(
        /<meta\s+name=["']description["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const descMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*name=["']description["'][^>]*>/i
      );
      const match = descMatch || descMatchAlt;
      expect(match).not.toBeNull();
      const description = match[1];
      // SEO best practice: description should be 50-160 characters
      expect(description.length).toBeGreaterThanOrEqual(50);
      expect(description.length).toBeLessThanOrEqual(160);
    });
  });

  test.describe('Open Graph Tags', () => {
    test('og:title is present', () => {
      const ogTitleMatch = htmlContent.match(
        /<meta\s+property=["']og:title["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const ogTitleMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*property=["']og:title["'][^>]*>/i
      );
      expect(ogTitleMatch || ogTitleMatchAlt).not.toBeNull();
    });

    test('og:title has meaningful content', () => {
      const ogTitleMatch = htmlContent.match(
        /<meta\s+property=["']og:title["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const ogTitleMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*property=["']og:title["'][^>]*>/i
      );
      const match = ogTitleMatch || ogTitleMatchAlt;
      expect(match).not.toBeNull();
      const ogTitle = match[1];
      expect(ogTitle.length).toBeGreaterThan(0);
      expect(ogTitle).toContain('MirDB');
    });

    test('og:description is present', () => {
      const ogDescMatch = htmlContent.match(
        /<meta\s+property=["']og:description["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const ogDescMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*property=["']og:description["'][^>]*>/i
      );
      expect(ogDescMatch || ogDescMatchAlt).not.toBeNull();
    });

    test('og:description has meaningful content', () => {
      const ogDescMatch = htmlContent.match(
        /<meta\s+property=["']og:description["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const ogDescMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*property=["']og:description["'][^>]*>/i
      );
      const match = ogDescMatch || ogDescMatchAlt;
      expect(match).not.toBeNull();
      const ogDesc = match[1];
      expect(ogDesc.length).toBeGreaterThan(0);
    });

    test('og:type is present', () => {
      const ogTypeMatch = htmlContent.match(
        /<meta\s+property=["']og:type["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const ogTypeMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*property=["']og:type["'][^>]*>/i
      );
      expect(ogTypeMatch || ogTypeMatchAlt).not.toBeNull();
    });

    test('og:url is present', () => {
      const ogUrlMatch = htmlContent.match(
        /<meta\s+property=["']og:url["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      const ogUrlMatchAlt = htmlContent.match(
        /<meta\s+content=["']([^"']*)["'][^>]*property=["']og:url["'][^>]*>/i
      );
      expect(ogUrlMatch || ogUrlMatchAlt).not.toBeNull();
    });
  });

  test.describe('Canonical URL', () => {
    test('canonical link element is present', () => {
      const canonicalMatch = htmlContent.match(/<link\s+rel=["']canonical["'][^>]*href=["']([^"']*)["'][^>]*>/i);
      const canonicalMatchAlt = htmlContent.match(/<link\s+href=["']([^"']*)["'][^>]*rel=["']canonical["'][^>]*>/i);
      expect(canonicalMatch || canonicalMatchAlt).not.toBeNull();
    });

    test('canonical URL has valid format', () => {
      const canonicalMatch = htmlContent.match(/<link\s+rel=["']canonical["'][^>]*href=["']([^"']*)["'][^>]*>/i);
      const canonicalMatchAlt = htmlContent.match(/<link\s+href=["']([^"']*)["'][^>]*rel=["']canonical["'][^>]*>/i);
      const match = canonicalMatch || canonicalMatchAlt;
      expect(match).not.toBeNull();
      const canonicalUrl = match[1];
      // Canonical URL should be a valid URL (starts with http:// or https://)
      expect(canonicalUrl.startsWith('http://') || canonicalUrl.startsWith('https://')).toBe(true);
    });
  });

  test.describe('Additional SEO Elements', () => {
    test('robots meta tag is present or not blocked', () => {
      // Check that robots are not blocked (either no robots tag or robots allow indexing)
      const robotsMatch = htmlContent.match(
        /<meta\s+name=["']robots["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      if (robotsMatch) {
        const robotsContent = robotsMatch[1].toLowerCase();
        // Should not contain noindex
        expect(robotsContent.includes('noindex')).toBe(false);
      }
      // If no robots tag, that's fine - default is to allow indexing
    });

    test('viewport meta tag is present', () => {
      const viewportMatch = htmlContent.match(
        /<meta\s+name=["']viewport["'][^>]*content=["']([^"']*)["'][^>]*>/i
      );
      expect(viewportMatch).not.toBeNull();
    });

    test('charset is declared', () => {
      const charsetMatch = htmlContent.match(/<meta\s+charset=["']([^"']*)["'][^>]*>/i);
      expect(charsetMatch).not.toBeNull();
      expect(charsetMatch[1].toLowerCase()).toBe('utf-8');
    });
  });
});
