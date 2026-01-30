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

// Read and parse HTML content
function getHtmlContent() {
  return fs.readFileSync(HTML_PATH, 'utf8');
}

// Simple helper to extract tag content
function extractTagContent(html, tagName) {
  const regex = new RegExp(`<${tagName}[^>]*>([^<]*)</${tagName}>`, 'i');
  const match = html.match(regex);
  return match ? match[1] : null;
}

// Simple helper to extract meta tag content by name attribute
function extractMetaContent(html, name) {
  const regex = new RegExp(`<meta\\s+name=["']${name}["']\\s+content=["']([^"']*)["']`, 'i');
  const match = html.match(regex);
  return match ? match[1] : null;
}

// Simple helper to extract meta tag content by property attribute (for OG tags)
function extractMetaProperty(html, property) {
  const regex = new RegExp(`<meta\\s+property=["']${property}["']\\s+content=["']([^"']*)["']`, 'i');
  const match = html.match(regex);
  return match ? match[1] : null;
}

// Simple helper to extract canonical URL
function extractCanonicalUrl(html) {
  const regex = /<link\s+rel=["']canonical["']\s+href=["']([^"']*)["']/i;
  const match = html.match(regex);
  return match ? match[1] : null;
}

test.describe('SEO and Meta Tags', () => {
  let htmlContent;

  test.beforeAll(() => {
    htmlContent = getHtmlContent();
  });

  test.describe('Title Tag', () => {
    test('page has a title tag', () => {
      const title = extractTagContent(htmlContent, 'title');
      expect(title).not.toBeNull();
    });

    test('title contains MirDB', () => {
      const title = extractTagContent(htmlContent, 'title');
      expect(title).toContain('MirDB');
    });

    test('title describes the product', () => {
      const title = extractTagContent(htmlContent, 'title');
      // Title should describe what MirDB is
      expect(title.toLowerCase()).toMatch(/key-value|persistent|memcached/);
    });

    test('title is descriptive and appropriate length', () => {
      const title = extractTagContent(htmlContent, 'title');
      // SEO best practice: title should be 30-60 characters
      expect(title.length).toBeGreaterThan(20);
      expect(title.length).toBeLessThan(70);
    });
  });

  test.describe('Meta Description', () => {
    test('meta description exists', () => {
      const description = extractMetaContent(htmlContent, 'description');
      expect(description).not.toBeNull();
    });

    test('meta description mentions key-value store', () => {
      const description = extractMetaContent(htmlContent, 'description');
      expect(description.toLowerCase()).toContain('key-value');
    });

    test('meta description mentions memcached', () => {
      const description = extractMetaContent(htmlContent, 'description');
      expect(description.toLowerCase()).toContain('memcached');
    });

    test('meta description is appropriate length', () => {
      const description = extractMetaContent(htmlContent, 'description');
      // SEO best practice: description should be 120-160 characters
      expect(description.length).toBeGreaterThan(100);
      expect(description.length).toBeLessThan(200);
    });
  });

  test.describe('Open Graph Tags', () => {
    test('og:title tag is present', () => {
      const ogTitle = extractMetaProperty(htmlContent, 'og:title');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.length).toBeGreaterThan(0);
    });

    test('og:description tag is present', () => {
      const ogDescription = extractMetaProperty(htmlContent, 'og:description');
      expect(ogDescription).not.toBeNull();
      expect(ogDescription.length).toBeGreaterThan(0);
    });

    test('og:type tag is present', () => {
      const ogType = extractMetaProperty(htmlContent, 'og:type');
      expect(ogType).not.toBeNull();
      expect(ogType).toBe('website');
    });

    test('og:url tag is present', () => {
      const ogUrl = extractMetaProperty(htmlContent, 'og:url');
      expect(ogUrl).not.toBeNull();
      expect(ogUrl).toMatch(/^https?:\/\//);
    });

    test('og:title matches page title', () => {
      const pageTitle = extractTagContent(htmlContent, 'title');
      const ogTitle = extractMetaProperty(htmlContent, 'og:title');
      expect(ogTitle).toBe(pageTitle);
    });
  });

  test.describe('Canonical URL', () => {
    test('canonical link element is present', () => {
      const canonicalUrl = extractCanonicalUrl(htmlContent);
      expect(canonicalUrl).not.toBeNull();
    });

    test('canonical URL is a valid HTTPS URL', () => {
      const canonicalUrl = extractCanonicalUrl(htmlContent);
      expect(canonicalUrl).toMatch(/^https:\/\//);
    });

    test('canonical URL matches og:url', () => {
      const canonicalUrl = extractCanonicalUrl(htmlContent);
      const ogUrl = extractMetaProperty(htmlContent, 'og:url');
      expect(canonicalUrl).toBe(ogUrl);
    });
  });

  test.describe('Additional SEO Checks', () => {
    test('HTML has lang attribute', () => {
      const langMatch = htmlContent.match(/<html[^>]*lang=["']([^"']*)["']/i);
      expect(langMatch).not.toBeNull();
      expect(langMatch[1]).toBe('en');
    });

    test('charset meta tag is present', () => {
      const charsetMatch = htmlContent.match(/<meta\s+charset=["']([^"']*)["']/i);
      expect(charsetMatch).not.toBeNull();
      expect(charsetMatch[1].toLowerCase()).toBe('utf-8');
    });

    test('viewport meta tag is present', () => {
      const viewportMatch = htmlContent.match(/<meta\s+name=["']viewport["'][^>]*content=["']([^"']*)["']/i);
      expect(viewportMatch).not.toBeNull();
      expect(viewportMatch[1]).toContain('width=device-width');
    });
  });
});
