/**
 * SEO Meta Tags Unit Tests
 * Owner: Scenario 13 - SEO Optimization
 *
 * Tests:
 * - Title tag content
 * - Meta description
 * - Open Graph tags
 * - Canonical URL
 * - Structured data
 * - Semantic HTML elements
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('SEO Meta Tags', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  describe('Test Case 1: Page Title Tag', () => {
    test('Title contains MirDB', () => {
      expect(htmlContent).toMatch(/<title>.*MirDB.*<\/title>/);
    });

    test('Title contains relevant keywords', () => {
      const titleMatch = htmlContent.match(/<title>(.*?)<\/title>/);
      expect(titleMatch).not.toBeNull();
      const title = titleMatch[1];
      expect(title.toLowerCase()).toMatch(/key-value|memcached|persistent/i);
    });

    test('Title is within recommended length (50-60 characters)', () => {
      const titleMatch = htmlContent.match(/<title>(.*?)<\/title>/);
      expect(titleMatch).not.toBeNull();
      const title = titleMatch[1];
      expect(title.length).toBeGreaterThanOrEqual(30);
      expect(title.length).toBeLessThanOrEqual(80);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    test('Meta description tag exists', () => {
      expect(htmlContent).toMatch(/<meta[^>]+name="description"[^>]+content="[^"]+"/);
    });

    test('Meta description mentions key-value store', () => {
      const descMatch = htmlContent.match(/<meta[^>]+name="description"[^>]+content="([^"]+)"/);
      expect(descMatch).not.toBeNull();
      const description = descMatch[1];
      expect(description.toLowerCase()).toContain('key-value');
    });

    test('Meta description mentions Memcached', () => {
      const descMatch = htmlContent.match(/<meta[^>]+name="description"[^>]+content="([^"]+)"/);
      expect(descMatch).not.toBeNull();
      const description = descMatch[1];
      expect(description.toLowerCase()).toContain('memcached');
    });

    test('Meta description is within recommended length (150-160 characters)', () => {
      const descMatch = htmlContent.match(/<meta[^>]+name="description"[^>]+content="([^"]+)"/);
      expect(descMatch).not.toBeNull();
      const description = descMatch[1];
      expect(description.length).toBeGreaterThanOrEqual(100);
      expect(description.length).toBeLessThanOrEqual(200);
    });
  });

  describe('Test Case 3: Open Graph Title Tag', () => {
    test('og:title tag exists', () => {
      expect(htmlContent).toMatch(/<meta[^>]+property="og:title"[^>]+content="[^"]+"/);
    });

    test('og:title is populated with MirDB', () => {
      const ogTitleMatch = htmlContent.match(/<meta[^>]+property="og:title"[^>]+content="([^"]+)"/);
      expect(ogTitleMatch).not.toBeNull();
      const ogTitle = ogTitleMatch[1];
      expect(ogTitle).toContain('MirDB');
    });
  });

  describe('Test Case 4: Open Graph Description Tag', () => {
    test('og:description tag exists', () => {
      expect(htmlContent).toMatch(/<meta[^>]+property="og:description"[^>]+content="[^"]+"/);
    });

    test('og:description is populated', () => {
      const ogDescMatch = htmlContent.match(/<meta[^>]+property="og:description"[^>]+content="([^"]+)"/);
      expect(ogDescMatch).not.toBeNull();
      const ogDesc = ogDescMatch[1];
      expect(ogDesc.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 5: Open Graph Image Tag', () => {
    test('og:image tag exists', () => {
      expect(htmlContent).toMatch(/<meta[^>]+property="og:image"[^>]+content="[^"]+"/);
    });

    test('og:image has a valid URL', () => {
      const ogImageMatch = htmlContent.match(/<meta[^>]+property="og:image"[^>]+content="([^"]+)"/);
      expect(ogImageMatch).not.toBeNull();
      const ogImage = ogImageMatch[1];
      expect(ogImage).toMatch(/^https?:\/\/.+\.(png|jpg|jpeg|gif|webp)$/i);
    });
  });

  describe('Test Case 6: Semantic HTML Elements', () => {
    test('Page uses header element', () => {
      expect(htmlContent).toMatch(/<header[^>]*>/);
    });

    test('Page uses nav element', () => {
      expect(htmlContent).toMatch(/<nav[^>]*>/);
    });

    test('Page uses main element', () => {
      expect(htmlContent).toMatch(/<main[^>]*>/);
    });

    test('Page uses section elements', () => {
      expect(htmlContent).toMatch(/<section[^>]*>/);
    });

    test('Page uses footer element', () => {
      expect(htmlContent).toMatch(/<footer[^>]*>/);
    });

    test('Sections have aria-labelledby attributes for accessibility', () => {
      const sectionMatches = htmlContent.match(/<section[^>]+aria-labelledby="[^"]+"/g);
      expect(sectionMatches).not.toBeNull();
      expect(sectionMatches.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Test Case 7: Canonical URL', () => {
    test('Canonical link tag is present', () => {
      expect(htmlContent).toMatch(/<link[^>]+rel="canonical"[^>]+href="[^"]+"/);
    });

    test('Canonical URL is a valid absolute URL', () => {
      const canonicalMatch = htmlContent.match(/<link[^>]+rel="canonical"[^>]+href="([^"]+)"/);
      expect(canonicalMatch).not.toBeNull();
      const canonicalUrl = canonicalMatch[1];
      expect(canonicalUrl).toMatch(/^https?:\/\//);
    });
  });

  describe('Additional SEO Elements', () => {
    test('og:type tag exists', () => {
      expect(htmlContent).toMatch(/<meta[^>]+property="og:type"[^>]+content="[^"]+"/);
    });

    test('og:url tag exists', () => {
      expect(htmlContent).toMatch(/<meta[^>]+property="og:url"[^>]+content="[^"]+"/);
    });

    test('Twitter card meta tags exist', () => {
      expect(htmlContent).toMatch(/<meta[^>]+name="twitter:card"[^>]+content="[^"]+"/);
      expect(htmlContent).toMatch(/<meta[^>]+name="twitter:title"[^>]+content="[^"]+"/);
    });

    test('Structured data JSON-LD exists', () => {
      expect(htmlContent).toMatch(/<script[^>]+type="application\/ld\+json"[^>]*>/);
    });

    test('Structured data contains SoftwareApplication schema', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();
      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd['@type']).toBe('SoftwareApplication');
      expect(jsonLd.name).toBe('MirDB');
    });

    test('Keywords meta tag exists', () => {
      expect(htmlContent).toMatch(/<meta[^>]+name="keywords"[^>]+content="[^"]+"/);
    });

    test('Keywords include relevant terms', () => {
      const keywordsMatch = htmlContent.match(/<meta[^>]+name="keywords"[^>]+content="([^"]+)"/);
      expect(keywordsMatch).not.toBeNull();
      const keywords = keywordsMatch[1].toLowerCase();
      expect(keywords).toContain('key-value');
      expect(keywords).toContain('memcached');
    });

    test('Robots meta tag allows indexing', () => {
      expect(htmlContent).toMatch(/<meta[^>]+name="robots"[^>]+content="[^"]*index[^"]*"/);
    });
  });
});
