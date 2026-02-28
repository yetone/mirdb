/**
 * SEO & Meta Tags Tests
 * Owner: Scenario 9 - SEO & Meta Tags
 *
 * Tests for:
 * - Title tag presence and content
 * - Meta description tag
 * - Open Graph meta tags (og:title, og:description, og:image, og:url)
 * - Twitter Card meta tags
 * - Canonical URL link tag
 * - Semantic HTML structure (header, main, footer, section)
 *
 * Requirements: NFR-4
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('SEO & Meta Tags', () => {
  beforeEach(() => {
    // Load the homepage HTML
    const htmlPath = resolve(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = html;
  });

  /**
   * Test Case 1: Title tag exists and contains 'MirDB'
   * Input: Query for title element in head
   * Expected: Title tag exists and contains 'MirDB'
   */
  describe('Test Case 1: Title Tag', () => {
    test('title tag exists in document', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title).toBeInTheDocument();
    });

    test('title tag contains "MirDB"', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    test('title tag is descriptive and includes key value proposition', () => {
      const title = document.querySelector('title');
      const titleText = title.textContent.toLowerCase();
      // Title should mention what the product is
      expect(titleText).toMatch(/key-value|persistent|memcached/);
    });

    test('title tag length is appropriate for SEO (under 60 chars recommended)', () => {
      const title = document.querySelector('title');
      // Title should be reasonably concise for SEO
      expect(title.textContent.length).toBeLessThanOrEqual(70);
    });
  });

  /**
   * Test Case 2: Meta description tag
   * Input: Query for meta description tag
   * Expected: Meta description tag exists with content about persistent key-value store
   */
  describe('Test Case 2: Meta Description', () => {
    test('meta description tag exists', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
      expect(metaDesc).toBeInTheDocument();
    });

    test('meta description has content attribute', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });

    test('meta description mentions key product features', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content').toLowerCase();
      // Should mention key features
      expect(content).toContain('persistent');
      expect(content).toContain('key-value');
    });

    test('meta description mentions Memcached protocol', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content').toLowerCase();
      expect(content).toContain('memcached');
    });

    test('meta description length is appropriate for SEO (under 160 chars)', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  /**
   * Test Case 3: og:title meta tag
   * Input: Query for og:title meta tag
   * Expected: Meta tag with property='og:title' exists with MirDB content
   */
  describe('Test Case 3: Open Graph Title', () => {
    test('og:title meta tag exists', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle).toBeInTheDocument();
    });

    test('og:title contains MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });

    test('og:title has meaningful content', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      const content = ogTitle.getAttribute('content');
      expect(content.length).toBeGreaterThan(5);
    });
  });

  /**
   * Test Case 4: og:description meta tag
   * Input: Query for og:description meta tag
   * Expected: Meta tag with property='og:description' exists with relevant description
   */
  describe('Test Case 4: Open Graph Description', () => {
    test('og:description meta tag exists', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
      expect(ogDesc).toBeInTheDocument();
    });

    test('og:description has content about the product', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      const content = ogDesc.getAttribute('content').toLowerCase();
      expect(content).toMatch(/persistent|key-value|memcached/);
    });

    test('og:description is not empty', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      const content = ogDesc.getAttribute('content');
      expect(content.length).toBeGreaterThan(10);
    });
  });

  /**
   * Test Case 5: og:image meta tag
   * Input: Query for og:image meta tag
   * Expected: Meta tag with property='og:image' exists pointing to valid image URL
   */
  describe('Test Case 5: Open Graph Image', () => {
    test('og:image meta tag exists', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage).toBeInTheDocument();
    });

    test('og:image has a content attribute', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      expect(content).toBeTruthy();
    });

    test('og:image points to an image file', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage.getAttribute('content');
      // Should be a valid image path or URL
      expect(content).toMatch(/\.(png|jpg|jpeg|gif|webp|svg)$/i);
    });
  });

  /**
   * Test Case 6: og:url meta tag
   * Input: Query for og:url meta tag
   * Expected: Meta tag with property='og:url' exists with canonical URL
   */
  describe('Test Case 6: Open Graph URL', () => {
    test('og:url meta tag exists', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      expect(ogUrl).toBeInTheDocument();
    });

    test('og:url has a content attribute with URL', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      const content = ogUrl.getAttribute('content');
      expect(content).toBeTruthy();
      // Should be a valid URL format
      expect(content).toMatch(/^https?:\/\//);
    });
  });

  /**
   * Test Case 7: Twitter card meta tags
   * Input: Query for Twitter card meta tags
   * Expected: Twitter card meta tags exist (twitter:card, twitter:title, twitter:description)
   */
  describe('Test Case 7: Twitter Card Meta Tags', () => {
    test('twitter:card meta tag exists', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();
      expect(twitterCard).toBeInTheDocument();
    });

    test('twitter:card has valid card type', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      const content = twitterCard.getAttribute('content');
      // Valid Twitter card types
      expect(['summary', 'summary_large_image', 'app', 'player']).toContain(content);
    });

    test('twitter:title meta tag exists', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle).not.toBeNull();
      expect(twitterTitle).toBeInTheDocument();
    });

    test('twitter:title contains MirDB', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      const content = twitterTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });

    test('twitter:description meta tag exists', () => {
      const twitterDesc = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDesc).not.toBeNull();
      expect(twitterDesc).toBeInTheDocument();
    });

    test('twitter:description has meaningful content', () => {
      const twitterDesc = document.querySelector('meta[name="twitter:description"]');
      const content = twitterDesc.getAttribute('content');
      expect(content.length).toBeGreaterThan(10);
    });

    test('twitter:image meta tag exists', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      expect(twitterImage).not.toBeNull();
      expect(twitterImage).toBeInTheDocument();
    });
  });

  /**
   * Test Case 8: Canonical URL tag
   * Input: Check for canonical URL tag
   * Expected: Link tag with rel='canonical' exists
   */
  describe('Test Case 8: Canonical URL', () => {
    test('canonical link tag exists', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
      expect(canonical).toBeInTheDocument();
    });

    test('canonical link has href attribute with URL', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  /**
   * Test Case 9: Semantic HTML element usage
   * Input: Check semantic HTML element usage
   * Expected: Page contains header, main, footer, and section/article elements
   */
  describe('Test Case 9: Semantic HTML Structure', () => {
    test('page contains header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header).toBeInTheDocument();
    });

    test('page contains main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      expect(main).toBeInTheDocument();
    });

    test('page contains footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer).toBeInTheDocument();
    });

    test('page contains section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('page contains nav element for navigation', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
      expect(nav).toBeInTheDocument();
    });

    test('main element has id for skip link target', () => {
      const main = document.querySelector('main');
      expect(main.id).toBeTruthy();
    });

    test('sections are properly labeled for accessibility', () => {
      const sections = document.querySelectorAll('section[aria-labelledby]');
      // At least some sections should have aria-labelledby
      expect(sections.length).toBeGreaterThan(0);
    });

    test('HTML source has lang attribute on html element', () => {
      // Note: When loading HTML via innerHTML, the outer html tag attributes
      // may not be preserved in JSDOM. We check the source HTML file directly.
      const htmlPath = resolve(__dirname, '../../index.html');
      const htmlSource = readFileSync(htmlPath, 'utf-8');
      expect(htmlSource).toMatch(/<html[^>]+lang=["'][^"']+["']/);
    });
  });

  /**
   * Additional SEO best practices tests
   */
  describe('Additional SEO Best Practices', () => {
    test('og:type meta tag exists', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    test('viewport meta tag exists for mobile', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('charset meta tag exists', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('favicon link exists', () => {
      const favicon = document.querySelector('link[rel="icon"]');
      expect(favicon).not.toBeNull();
    });

    test('page has only one h1 element', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('headings follow proper hierarchy', () => {
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');
      expect(h1).not.toBeNull();
      expect(h2s.length).toBeGreaterThan(0);
    });
  });
});
