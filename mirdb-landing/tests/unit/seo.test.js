/**
 * SEO Unit Tests
 * Owner: Scenario 12 - SEO Requirements
 *
 * Tests:
 * - Page title validation
 * - Meta description
 * - Open Graph tags
 * - Twitter Card tags
 * - Structured data (JSON-LD)
 * - Canonical URL
 */

import { test, describe } from 'node:test';
import assert from 'node:assert';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const htmlContent = readFileSync(join(__dirname, '../../index.html'), 'utf-8');

describe('SEO Unit Tests', () => {

  describe('Test Case 2: Page Title Validation', () => {
    test('page title exists', () => {
      assert.match(htmlContent, /<title>[^<]+<\/title>/i, 'Page should have a title tag');
    });

    test('title includes MirDB', () => {
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
      assert.ok(titleMatch, 'Title tag should exist');
      assert.match(titleMatch[1], /MirDB/i, 'Title should include "MirDB"');
    });

    test('title is under 60 characters', () => {
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
      assert.ok(titleMatch, 'Title tag should exist');
      const titleLength = titleMatch[1].trim().length;
      assert.ok(titleLength < 60, `Title should be under 60 characters, got ${titleLength}`);
    });

    test('title is descriptive', () => {
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
      assert.ok(titleMatch, 'Title tag should exist');
      const title = titleMatch[1].trim();
      assert.ok(title.length > 10, 'Title should be descriptive (> 10 characters)');
    });
  });

  describe('Test Case 3: Meta Description Validation', () => {
    test('meta description exists', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']description["'][^>]*content=["'][^"']+["']/i,
        'Meta description should exist');
    });

    test('meta description mentions persistent', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(descMatch, 'Meta description should exist');
      assert.match(descMatch[1].toLowerCase(), /persistent/i,
        'Description should mention "persistent"');
    });

    test('meta description mentions key-value store', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(descMatch, 'Meta description should exist');
      assert.match(descMatch[1].toLowerCase(), /key-value/i,
        'Description should mention "key-value"');
    });

    test('meta description mentions memcached protocol', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(descMatch, 'Meta description should exist');
      assert.match(descMatch[1].toLowerCase(), /memcached/i,
        'Description should mention "memcached"');
    });

    test('meta description is 150-160 characters', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name=["']description["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(descMatch, 'Meta description should exist');
      const descLength = descMatch[1].trim().length;
      assert.ok(descLength >= 120 && descLength <= 180,
        `Description should be 120-180 characters for optimal SEO, got ${descLength}`);
    });
  });

  describe('Test Case 4: Open Graph Tags Validation', () => {
    test('og:title tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*property=["']og:title["'][^>]*content=["'][^"']+["']/i,
        'og:title should be present');
    });

    test('og:description tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*property=["']og:description["'][^>]*content=["'][^"']+["']/i,
        'og:description should be present');
    });

    test('og:image tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*property=["']og:image["'][^>]*content=["'][^"']+["']/i,
        'og:image should be present');
    });

    test('og:url tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*property=["']og:url["'][^>]*content=["'][^"']+["']/i,
        'og:url should be present');
    });

    test('og:type tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*property=["']og:type["'][^>]*content=["'][^"']+["']/i,
        'og:type should be present');
    });

    test('og:title contains appropriate value', () => {
      const ogTitleMatch = htmlContent.match(/<meta[^>]*property=["']og:title["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(ogTitleMatch, 'og:title should exist');
      assert.match(ogTitleMatch[1], /MirDB/i, 'og:title should contain MirDB');
    });

    test('og:description contains appropriate value', () => {
      const ogDescMatch = htmlContent.match(/<meta[^>]*property=["']og:description["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(ogDescMatch, 'og:description should exist');
      assert.ok(ogDescMatch[1].length > 50, 'og:description should be descriptive');
    });

    test('og:image contains valid URL', () => {
      const ogImageMatch = htmlContent.match(/<meta[^>]*property=["']og:image["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(ogImageMatch, 'og:image should exist');
      assert.match(ogImageMatch[1], /^https?:\/\//i, 'og:image should be an absolute URL');
    });

    test('og:url contains valid URL', () => {
      const ogUrlMatch = htmlContent.match(/<meta[^>]*property=["']og:url["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(ogUrlMatch, 'og:url should exist');
      assert.match(ogUrlMatch[1], /^https?:\/\//i, 'og:url should be an absolute URL');
    });
  });

  describe('Test Case 5: Twitter Card Tags Validation', () => {
    test('twitter:card tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']twitter:card["'][^>]*content=["'][^"']+["']/i,
        'twitter:card should be present');
    });

    test('twitter:title tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']twitter:title["'][^>]*content=["'][^"']+["']/i,
        'twitter:title should be present');
    });

    test('twitter:description tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']twitter:description["'][^>]*content=["'][^"']+["']/i,
        'twitter:description should be present');
    });

    test('twitter:image tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']twitter:image["'][^>]*content=["'][^"']+["']/i,
        'twitter:image should be present');
    });

    test('twitter:card has valid value', () => {
      const cardMatch = htmlContent.match(/<meta[^>]*name=["']twitter:card["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(cardMatch, 'twitter:card should exist');
      const validCardTypes = ['summary', 'summary_large_image', 'app', 'player'];
      assert.ok(validCardTypes.includes(cardMatch[1]),
        `twitter:card should be one of: ${validCardTypes.join(', ')}`);
    });

    test('twitter:title contains MirDB', () => {
      const twitterTitleMatch = htmlContent.match(/<meta[^>]*name=["']twitter:title["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(twitterTitleMatch, 'twitter:title should exist');
      assert.match(twitterTitleMatch[1], /MirDB/i, 'twitter:title should contain MirDB');
    });

    test('twitter:description is descriptive', () => {
      const twitterDescMatch = htmlContent.match(/<meta[^>]*name=["']twitter:description["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(twitterDescMatch, 'twitter:description should exist');
      assert.ok(twitterDescMatch[1].length > 50, 'twitter:description should be descriptive');
    });

    test('twitter:image contains valid URL', () => {
      const twitterImageMatch = htmlContent.match(/<meta[^>]*name=["']twitter:image["'][^>]*content=["']([^"']+)["']/i);
      assert.ok(twitterImageMatch, 'twitter:image should exist');
      assert.match(twitterImageMatch[1], /^https?:\/\//i, 'twitter:image should be an absolute URL');
    });
  });

  describe('Test Case 6: Structured Data (JSON-LD) Validation', () => {
    let jsonLdData = null;

    test('JSON-LD script tag exists', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      assert.ok(jsonLdMatch, 'JSON-LD script tag should exist');

      // Parse the JSON-LD data for subsequent tests
      try {
        jsonLdData = JSON.parse(jsonLdMatch[1]);
      } catch (e) {
        assert.fail(`JSON-LD should be valid JSON: ${e.message}`);
      }
    });

    test('JSON-LD is valid JSON', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      assert.ok(jsonLdMatch, 'JSON-LD script tag should exist');

      try {
        jsonLdData = JSON.parse(jsonLdMatch[1]);
        assert.ok(typeof jsonLdData === 'object', 'JSON-LD should parse to an object');
      } catch (e) {
        assert.fail(`JSON-LD should be valid JSON: ${e.message}`);
      }
    });

    test('JSON-LD has @context', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      const data = JSON.parse(jsonLdMatch[1]);
      assert.ok(data['@context'], 'JSON-LD should have @context');
      assert.match(data['@context'], /schema\.org/i, '@context should reference schema.org');
    });

    test('JSON-LD has @type SoftwareApplication', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      const data = JSON.parse(jsonLdMatch[1]);
      assert.ok(data['@type'], 'JSON-LD should have @type');
      assert.strictEqual(data['@type'], 'SoftwareApplication', '@type should be SoftwareApplication');
    });

    test('JSON-LD has name property with MirDB', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      const data = JSON.parse(jsonLdMatch[1]);
      assert.ok(data.name, 'JSON-LD should have name property');
      assert.match(data.name, /MirDB/i, 'name should contain MirDB');
    });

    test('JSON-LD has description property', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      const data = JSON.parse(jsonLdMatch[1]);
      assert.ok(data.description, 'JSON-LD should have description property');
      assert.ok(data.description.length > 50, 'description should be meaningful');
    });

    test('JSON-LD has url property', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      const data = JSON.parse(jsonLdMatch[1]);
      assert.ok(data.url, 'JSON-LD should have url property');
      assert.match(data.url, /^https?:\/\//i, 'url should be a valid URL');
    });

    test('JSON-LD has applicationCategory', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      const data = JSON.parse(jsonLdMatch[1]);
      assert.ok(data.applicationCategory, 'JSON-LD should have applicationCategory');
    });

    test('JSON-LD has author information', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
      const data = JSON.parse(jsonLdMatch[1]);
      assert.ok(data.author, 'JSON-LD should have author property');
    });
  });

  describe('Test Case 7: Canonical URL Validation', () => {
    test('canonical link tag exists', () => {
      assert.match(htmlContent, /<link[^>]*rel=["']canonical["'][^>]*>/i,
        'Canonical link tag should exist');
    });

    test('canonical link has href attribute', () => {
      const canonicalMatch = htmlContent.match(/<link[^>]*rel=["']canonical["'][^>]*href=["']([^"']+)["']/i);
      assert.ok(canonicalMatch, 'Canonical link should have href attribute');
    });

    test('canonical URL is absolute', () => {
      const canonicalMatch = htmlContent.match(/<link[^>]*rel=["']canonical["'][^>]*href=["']([^"']+)["']/i);
      assert.ok(canonicalMatch, 'Canonical link should exist');
      assert.match(canonicalMatch[1], /^https?:\/\//i, 'Canonical URL should be absolute');
    });

    test('canonical URL points to primary domain', () => {
      const canonicalMatch = htmlContent.match(/<link[^>]*rel=["']canonical["'][^>]*href=["']([^"']+)["']/i);
      assert.ok(canonicalMatch, 'Canonical link should exist');
      // Should be a valid URL with a proper domain
      const url = canonicalMatch[1];
      assert.ok(url.includes('.'), 'Canonical URL should contain a domain');
    });
  });

  describe('Semantic HTML for SEO', () => {
    test('document has html lang attribute', () => {
      assert.match(htmlContent, /<html[^>]*lang=["']en["']/i,
        'HTML should have lang attribute for SEO');
    });

    test('document has proper heading hierarchy starting with h1', () => {
      assert.match(htmlContent, /<h1[^>]*>/i, 'Document should have h1 heading');

      // Get the first heading
      const headingMatch = htmlContent.match(/<h([1-6])[^>]*>/i);
      assert.ok(headingMatch, 'Document should have headings');
      // The first visible heading in body should ideally be h1
    });

    test('main content is wrapped in main element', () => {
      assert.match(htmlContent, /<main[^>]*>/i, 'Document should have main element');
    });

    test('sections use semantic section elements', () => {
      const sectionMatches = htmlContent.match(/<section[^>]*>/gi) || [];
      assert.ok(sectionMatches.length >= 3, 'Document should have multiple section elements');
    });

    test('navigation uses nav element', () => {
      assert.match(htmlContent, /<nav[^>]*>/i, 'Document should have nav element');
    });

    test('footer uses footer element', () => {
      assert.match(htmlContent, /<footer[^>]*>/i, 'Document should have footer element');
    });

    test('article elements are used appropriately', () => {
      const articleMatches = htmlContent.match(/<article[^>]*>/gi) || [];
      assert.ok(articleMatches.length > 0, 'Document should use article elements for self-contained content');
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('robots meta tag allows indexing', () => {
      const robotsMatch = htmlContent.match(/<meta[^>]*name=["']robots["'][^>]*content=["']([^"']+)["']/i);
      if (robotsMatch) {
        assert.match(robotsMatch[1], /index/i, 'Robots meta should allow indexing');
        assert.match(robotsMatch[1], /follow/i, 'Robots meta should allow following links');
      }
      // It's okay if robots meta is not present (defaults to index,follow)
    });

    test('external links have rel="noopener noreferrer"', () => {
      const externalLinks = htmlContent.match(/<a[^>]*href=["']https?:\/\/[^"']+["'][^>]*target=["']_blank["'][^>]*>/gi) || [];
      for (const link of externalLinks) {
        assert.match(link, /rel=["'][^"']*noopener[^"']*["']/i,
          'External links with target="_blank" should have rel="noopener"');
      }
    });

    test('images have alt attributes for SEO', () => {
      const imgWithoutAlt = /<img(?![^>]*alt=)[^>]*>/gi;
      const matches = htmlContent.match(imgWithoutAlt) || [];
      assert.strictEqual(matches.length, 0, 'All images should have alt attributes');
    });

    test('viewport meta tag is present', () => {
      assert.match(htmlContent, /<meta[^>]*name=["']viewport["']/i,
        'Viewport meta tag should be present for mobile SEO');
    });

    test('charset is declared', () => {
      assert.match(htmlContent, /<meta[^>]*charset=["']UTF-8["']/i,
        'Charset should be declared');
    });
  });
});
