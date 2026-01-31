/**
 * SEO Meta Tags Unit Tests
 * Owner: Scenario 13 - SEO Requirements
 *
 * Tests:
 * - Title tag content
 * - Meta description
 * - Open Graph tags
 * - Twitter card tags
 *
 * Requirements: NFR-4
 */

const fs = require('fs');
const path = require('path');

describe('SEO Meta Tags', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Title Element', () => {
    test('title tag exists', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    test('title contains MirDB', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    test('title describes the product', () => {
      const title = document.querySelector('title');
      const titleText = title.textContent.toLowerCase();
      expect(
        titleText.includes('key-value') ||
        titleText.includes('persistent') ||
        titleText.includes('store') ||
        titleText.includes('database')
      ).toBe(true);
    });
  });

  describe('Meta Description', () => {
    test('meta description tag exists', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    test('meta description has content', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(0);
    });

    test('meta description is between 120-160 characters', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      expect(content.length).toBeGreaterThanOrEqual(120);
      expect(content.length).toBeLessThanOrEqual(160);
    });
  });

  describe('Open Graph Tags', () => {
    test('og:title tag exists with MirDB title', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toContain('MirDB');
    });

    test('og:description tag exists', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
      expect(ogDesc.getAttribute('content')).toBeTruthy();
    });

    test('og:image tag exists with absolute URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      const imageUrl = ogImage.getAttribute('content');
      expect(imageUrl).toBeTruthy();
      // Absolute URLs should start with http:// or https://
      expect(
        imageUrl.startsWith('http://') || imageUrl.startsWith('https://')
      ).toBe(true);
    });

    test('og:type tag exists', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBeTruthy();
    });
  });

  describe('Twitter Card Tags', () => {
    test('twitter:card tag exists', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();
    });

    test('twitter:title tag exists', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle).not.toBeNull();
    });

    test('twitter:description tag exists', () => {
      const twitterDesc = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDesc).not.toBeNull();
    });
  });

  describe('Additional SEO Elements', () => {
    test('keywords meta tag exists', () => {
      const keywords = document.querySelector('meta[name="keywords"]');
      expect(keywords).not.toBeNull();
      expect(keywords.getAttribute('content')).toBeTruthy();
    });

    test('charset meta tag is present', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('viewport meta tag is present', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('html lang attribute is set', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      expect(html.getAttribute('lang')).toBeTruthy();
    });
  });
});
